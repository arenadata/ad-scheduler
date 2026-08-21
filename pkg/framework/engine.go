/*
Package framework wires ad-scheduler's engine to the kube-scheduler scheduling
framework. There is exactly one Engine per scheduler profile; every plugin
factory closes over the same Engine via GetOrInitEngine so all extension points
share one queue tree (decisions q26/q28).

The Engine owns the live queue tree (queue.QueueManager, swapped atomically by
the coordinator on every rebuild), the flat node/pod cache, and the per-pod
booking ledger that keeps queue `allocated`/`allocating` correct across the
Reserve→bind→terminate lifecycle. Until the coordinator has assembled a first
tree, admission rejects every pod (fail-closed, decision q30).
*/
package framework

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	clientcache "k8s.io/client-go/tools/cache"

	"github.com/arenadata/ad-scheduler/pkg/cache"
	"github.com/arenadata/ad-scheduler/pkg/config"
	"github.com/arenadata/ad-scheduler/pkg/queue"
	"github.com/arenadata/ad-scheduler/pkg/resource"
)

// ErrRejectAll is the fail-closed admission result when no queue tree has been
// built yet (empty cluster / pre-first-build). Per decision q30 an unbuilt tree
// rejects everything rather than failing open.
var ErrRejectAll = errors.New("ad-scheduler: no queue tree built — rejecting (fail-closed, q30)")

// Snapshot is the immutable, per-rebuild handle to the live queue tree that
// plugins read lock-free. The QueueManager it wraps carries its own RWMutex, so
// reads (headroom/CanFit) are lock-free of tree writers; a rebuild swaps the
// whole Snapshot atomically.
type Snapshot struct {
	generation uint64
	tree       *queue.QueueManager
}

// Generation returns the monotonic rebuild id of this snapshot.
func (s *Snapshot) Generation() uint64 {
	if s == nil {
		return 0
	}
	return s.generation
}

// Tree returns the queue manager this snapshot points at (nil for a treeless
// snapshot, as used by some tests).
func (s *Snapshot) Tree() *queue.QueueManager {
	if s == nil {
		return nil
	}
	return s.tree
}

// bookState is a pod's position in the per-pod accounting lifecycle.
type bookState int

const (
	bkReserved  bookState = iota // allocating held (Reserve), not yet bound
	bkCommitted                  // allocated held (bind observed / PostBind)
)

type bookRec struct {
	state bookState
	leaf  string
	req   resource.Resource
}

// gangBooking records an admitted gang's virtual reservation so it survives a
// tree rebuild (re-seeded onto the fresh tree, like per-pod bookings).
type gangBooking struct {
	leaf   string
	minRes resource.Resource
}

// holBid is a per-leaf head-of-line reservation for gang admission (wound-wait,
// decision q4): while the highest-ranked waiting gang cannot fit, lower-ranked
// gangs are held back so freed headroom accumulates for it — preventing a stream
// of small/young gangs from starving an older/larger one. The bid is refreshed on
// contention and expires after GangScheduleTimeout so a vanished gang can never
// deadlock the leaf.
type holBid struct {
	key queue.WoundWaitKey
	at  time.Time
}

// Engine is the shared in-process brain for one scheduler profile.
type Engine struct {
	profile string
	config  config.ProcessConfig

	cache *cache.Cache

	snapshot         atomic.Pointer[Snapshot]
	ready            atomic.Bool
	gen              atomic.Uint64
	reclaimEvictions atomic.Uint64

	// mu guards the booking ledgers and serialises them with rebuilds so
	// re-seeding a fresh tree sees a consistent set of bookings.
	mu       sync.Mutex
	book     map[types.UID]*bookRec
	gangBook map[string]gangBooking
	holBids  map[string]holBid // leaf -> head-of-line gang reservation (wound-wait)

	// coordMu guards lazy one-time coordinator creation, shared by all plugins.
	coordMu sync.Mutex
	coord   *Coordinator
}

// EnsureCoordinator lazily creates and starts the single informer coordinator
// for this engine, returning the same instance to every plugin factory (capacity
// and gang share it, so there is one set of informers). Idempotent.
func (e *Engine) EnsureCoordinator(ctx context.Context, dynClient dynamic.Interface, podInf, nodeInf, rqInf clientcache.SharedIndexInformer) (*Coordinator, error) {
	e.coordMu.Lock()
	defer e.coordMu.Unlock()
	if e.coord != nil {
		return e.coord, nil
	}
	c, err := NewCoordinator(e, dynClient, podInf, nodeInf, rqInf)
	if err != nil {
		return nil, err
	}
	c.Start(ctx)
	e.coord = c
	return c, nil
}

// Coordinator returns the started coordinator (nil before EnsureCoordinator).
func (e *Engine) Coordinator() *Coordinator {
	e.coordMu.Lock()
	defer e.coordMu.Unlock()
	return e.coord
}

// Profile returns the scheduler profile this engine serves.
func (e *Engine) Profile() string { return e.profile }

// Config returns the process configuration.
func (e *Engine) Config() config.ProcessConfig { return e.config }

// Cache returns the shared node/pod cache.
func (e *Engine) Cache() *cache.Cache { return e.cache }

// Tree returns the live queue manager, or nil if none is built yet.
func (e *Engine) Tree() *queue.QueueManager { return e.snapshot.Load().Tree() }

// SwapSnapshot atomically installs a treeless snapshot. It exists for tests and
// the M0 bootstrap gate; production installs trees through Rebuild. A non-nil
// snapshot latches readiness.
func (e *Engine) SwapSnapshot(s *Snapshot) {
	e.snapshot.Store(s)
	if s != nil {
		e.ready.Store(true)
	}
}

// Snapshot returns the current snapshot, or nil if none is built.
func (e *Engine) Snapshot() *Snapshot { return e.snapshot.Load() }

// HasTree reports whether a snapshot has been installed and is live.
func (e *Engine) HasTree() bool { return e.snapshot.Load() != nil }

// Ready reports whether the engine has completed at least one successful build.
func (e *Engine) Ready() bool { return e.ready.Load() }

// Admit is the bootstrap admission gate: with no built tree every pod is
// rejected (fail-closed, q30). Per-pod queue resolution/headroom is done by the
// capacity plugin against Tree().
func (e *Engine) Admit() error {
	if !e.HasTree() {
		return ErrRejectAll
	}
	return nil
}

// Rebuild re-seeds newTree from the current booking ledger (so a config change
// never loses live allocations) and swaps it in atomically. Bookings whose leaf
// no longer exists in newTree are dropped (orphaned by the config change).
func (e *Engine) Rebuild(newTree *queue.QueueManager) {
	if newTree == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for uid, rec := range e.book {
		if _, ok := newTree.Queue(rec.leaf); !ok {
			delete(e.book, uid) // orphaned leaf: drop
			continue
		}
		switch rec.state {
		case bkCommitted:
			_ = newTree.Allocate(rec.leaf, rec.req)
		case bkReserved:
			_ = newTree.Reserve(rec.leaf, rec.req)
		}
	}
	for key, gb := range e.gangBook {
		if _, ok := newTree.Queue(gb.leaf); !ok {
			delete(e.gangBook, key) // orphaned leaf: drop the gang reservation
			continue
		}
		_, _ = newTree.AdmitGang(gb.leaf, key, gb.minRes)
	}
	gen := e.gen.Add(1)
	e.snapshot.Store(&Snapshot{generation: gen, tree: newTree})
	e.ready.Store(true)
}

// AdmitGang reserves a gang's aggregate minResources on its leaf (idempotent by
// key) and records the booking so it survives rebuilds. Returns whether the gang
// is admitted (fits headroom).
func (e *Engine) AdmitGang(leaf, key string, minRes resource.Resource) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if gb, ok := e.gangBook[key]; ok {
		if gb.leaf != leaf {
			return false, errGangLeafMismatch(key, gb.leaf, leaf)
		}
		return true, nil
	}
	t := e.treeLocked()
	if t == nil {
		return false, fmt.Errorf("no queue tree built")
	}
	admitted, err := t.AdmitGang(leaf, key, minRes)
	if err != nil || !admitted {
		return admitted, err
	}
	e.gangBook[key] = gangBooking{leaf: leaf, minRes: minRes.Clone()}
	return true, nil
}

// AdmitGangWW is AdmitGang with wound-wait head-of-line reservation (decision q4):
// among gangs contending for the same leaf, a lower-ranked gang is held back
// while a higher-ranked one waits for headroom, so freed capacity accumulates for
// the older/larger gang and a stream of small/young gangs cannot starve it. wwkey
// ranks the gang (priority DESC, age ASC, uid) via queue.WoundWaitLess. The bid is
// refreshed under contention and expires after GangScheduleTimeout so a vanished
// gang never deadlocks the leaf. The gang plugin uses this; recovery uses the
// plain AdmitGang (no head-of-line, it is restoring an already-admitted gang).
func (e *Engine) AdmitGangWW(leaf, key string, minRes resource.Resource, wwkey queue.WoundWaitKey) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if gb, ok := e.gangBook[key]; ok {
		if gb.leaf != leaf {
			return false, errGangLeafMismatch(key, gb.leaf, leaf)
		}
		return true, nil
	}
	t := e.treeLocked()
	if t == nil {
		return false, fmt.Errorf("no queue tree built")
	}
	now := time.Now()
	if bid, ok := e.holBids[leaf]; ok && now.Sub(bid.at) > e.config.GangScheduleTimeout {
		delete(e.holBids, leaf) // stale bid (its gang vanished): never deadlock the leaf
	}
	// A higher-ranked waiting gang reserves this leaf's headroom: a lower-ranked
	// gang yields even if it would fit. Refresh the bid so it stays alive.
	if bid, ok := e.holBids[leaf]; ok && bid.key != wwkey && queue.WoundWaitLess(bid.key, wwkey) {
		bid.at = now
		e.holBids[leaf] = bid
		return false, nil
	}
	admitted, err := t.AdmitGang(leaf, key, minRes)
	if err != nil {
		return false, err
	}
	if admitted {
		if bid, ok := e.holBids[leaf]; ok && bid.key == wwkey {
			delete(e.holBids, leaf) // head-of-line gang got in: release the reservation
		}
		e.gangBook[key] = gangBooking{leaf: leaf, minRes: minRes.Clone()}
		return true, nil
	}
	// Did not fit: claim/refresh head-of-line if we outrank the current bid.
	if bid, ok := e.holBids[leaf]; !ok || bid.key == wwkey || queue.WoundWaitLess(wwkey, bid.key) {
		e.holBids[leaf] = holBid{key: wwkey, at: now}
	}
	return false, nil
}

// ReleaseGang frees a gang's reservation (idempotent).
func (e *Engine) ReleaseGang(key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.gangBook[key]; !ok {
		return nil
	}
	if t := e.treeLocked(); t != nil {
		_ = t.ReleaseGang(key)
	}
	delete(e.gangBook, key)
	return nil
}

// IsGangAdmitted reports whether the gang currently holds a reservation.
func (e *Engine) IsGangAdmitted(key string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.gangBook[key]
	return ok
}

// GangLeaf returns the leaf a gang is currently bound to (the single queue its
// whole reservation lives on) and whether the gang is admitted. The gang plugin
// uses it to enforce one-leaf-per-gang: every member must resolve to this leaf,
// else its footprint would ride the wrong queue's reservation.
func (e *Engine) GangLeaf(key string) (string, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	gb, ok := e.gangBook[key]
	if !ok {
		return "", false
	}
	return gb.leaf, true
}

// errGangLeafMismatch reports a member trying to admit a gang on a leaf other
// than the one it is already bound to (heterogeneous ServiceAccounts routing
// members of one gang to different leaves — a cross-member invariant the
// apiserver/VAP cannot express, so the engine is the sole gate).
func errGangLeafMismatch(key, bound, got string) error {
	return fmt.Errorf("gang %s is bound to leaf %q; member resolves to %q — all gang members must map to one leaf", key, bound, got)
}

// GangCount returns the number of currently-admitted gangs (metrics).
func (e *Engine) GangCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.gangBook)
}

// GangReservation is a snapshot of one admitted gang's virtual hold, exported so
// the coordinator can persist it to PodGroup.status for durable recovery (q1).
type GangReservation struct {
	Leaf   string
	MinRes resource.Resource
}

// AdmittedGangs returns a snapshot of the current gang ledger keyed by gangID.
// The coordinator writes these to PodGroup.status so a restart can restore them.
func (e *Engine) AdmittedGangs() map[string]GangReservation {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]GangReservation, len(e.gangBook))
	for key, gb := range e.gangBook {
		out[key] = GangReservation{Leaf: gb.leaf, MinRes: gb.minRes.Clone()}
	}
	return out
}

// IncReclaimEvictions counts one reclaim eviction (metrics).
func (e *Engine) IncReclaimEvictions() { e.reclaimEvictions.Add(1) }

// ReclaimEvictions returns the cumulative reclaim eviction count.
func (e *Engine) ReclaimEvictions() uint64 { return e.reclaimEvictions.Load() }

// treeLocked returns the current tree; caller holds e.mu.
func (e *Engine) treeLocked() *queue.QueueManager { return e.snapshot.Load().Tree() }

// Reserve books an in-flight reservation (allocating) for a pod at its leaf,
// keyed by uid and idempotent. The next scheduling cycle's headroom check sees
// it, which is what enforces queue max under a burst of pods.
func (e *Engine) Reserve(uid types.UID, leaf string, req resource.Resource) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.book[uid]; ok {
		return
	}
	t := e.treeLocked()
	if t == nil {
		return
	}
	if err := t.Reserve(leaf, req); err != nil {
		return
	}
	e.book[uid] = &bookRec{state: bkReserved, leaf: leaf, req: req.Clone()}
}

// Unreserve reverses Reserve on a bind failure / un-assume (framework Unreserve).
func (e *Engine) Unreserve(uid types.UID) {
	e.mu.Lock()
	defer e.mu.Unlock()
	rec := e.book[uid]
	if rec == nil || rec.state != bkReserved {
		return
	}
	if t := e.treeLocked(); t != nil {
		_ = t.Unreserve(rec.leaf, rec.req)
	}
	delete(e.book, uid)
}

// Commit moves a reservation to confirmed allocated (PostBind). Idempotent.
func (e *Engine) Commit(uid types.UID) {
	e.mu.Lock()
	defer e.mu.Unlock()
	rec := e.book[uid]
	if rec == nil || rec.state != bkReserved {
		return
	}
	if t := e.treeLocked(); t != nil {
		_ = t.Commit(rec.leaf, rec.req)
	}
	rec.state = bkCommitted
}

// ObserveBound is the informer safety net / restart-recovery path for a bound
// pod of ours: if it was reserved it is committed; if it was never seen (bound
// by a previous instance) its usage is allocated directly. Idempotent.
func (e *Engine) ObserveBound(uid types.UID, leaf string, req resource.Resource) {
	e.mu.Lock()
	defer e.mu.Unlock()
	t := e.treeLocked()
	if t == nil {
		return
	}
	rec := e.book[uid]
	if rec == nil {
		if _, ok := t.Queue(leaf); !ok {
			return
		}
		_ = t.Allocate(leaf, req)
		e.book[uid] = &bookRec{state: bkCommitted, leaf: leaf, req: req.Clone()}
		return
	}
	if rec.state == bkReserved {
		_ = t.Commit(rec.leaf, rec.req)
		rec.state = bkCommitted
	}
}

// ObserveDeleted releases a pod's booking on termination/deletion. Idempotent.
func (e *Engine) ObserveDeleted(uid types.UID) {
	e.mu.Lock()
	defer e.mu.Unlock()
	rec := e.book[uid]
	if rec == nil {
		return
	}
	if t := e.treeLocked(); t != nil {
		releaseBookLocked(t, rec)
	}
	delete(e.book, uid)
}

// releaseBookLocked frees a booking's held resources on the tree: allocated for a
// committed booking, allocating for a still-reserved one. Caller holds e.mu.
func releaseBookLocked(t *queue.QueueManager, rec *bookRec) {
	switch rec.state {
	case bkCommitted:
		_ = t.Release(rec.leaf, rec.req)
	case bkReserved:
		_ = t.Unreserve(rec.leaf, rec.req)
	}
}

// GCBookings releases every booking whose pod UID is absent from live — a pod
// whose delete event was missed (informer gap / dropped watch) that would
// otherwise leak allocating|allocated headroom until the process restarts. live
// is the set of currently-present, non-terminal pod UIDs. Returns the count
// released. Idempotent with Unreserve/ObserveDeleted (all under e.mu): a
// concurrent real delete finds the entry already gone.
func (e *Engine) GCBookings(live map[types.UID]bool) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	t := e.treeLocked()
	if t == nil {
		return 0
	}
	var n int
	for uid, rec := range e.book {
		if live[uid] {
			continue
		}
		releaseBookLocked(t, rec)
		delete(e.book, uid)
		n++
	}
	return n
}

// GCGangs releases every gang reservation whose PodGroup key (namespace/name) is
// absent from live — an abandoned gang whose PodGroup delete was missed.
// checkGangTimeouts covers the Failed/Hard-timeout case; this covers the
// vanished-object case. Returns the count released.
func (e *Engine) GCGangs(live map[string]bool) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	t := e.treeLocked()
	var n int
	for key := range e.gangBook {
		if live[key] {
			continue
		}
		if t != nil {
			_ = t.ReleaseGang(key)
		}
		delete(e.gangBook, key)
		n++
	}
	return n
}

var (
	enginesMu sync.Mutex
	engines   = map[string]*Engine{}
)

// GetOrInitEngine returns the single Engine for the given scheduler profile,
// creating it on first call and returning the same instance thereafter. cfg is
// used only on first creation.
func GetOrInitEngine(profile string, cfg config.ProcessConfig) *Engine {
	enginesMu.Lock()
	defer enginesMu.Unlock()
	if e, ok := engines[profile]; ok {
		return e
	}
	e := &Engine{
		profile:  profile,
		config:   cfg,
		cache:    cache.New(cfg.SchedulerName),
		book:     map[types.UID]*bookRec{},
		gangBook: map[string]gangBooking{},
		holBids:  map[string]holBid{},
	}
	engines[profile] = e
	return e
}

// resetEnginesForTest clears the process-global engine registry. Test-only.
func resetEnginesForTest() {
	enginesMu.Lock()
	defer enginesMu.Unlock()
	engines = map[string]*Engine{}
}
