package queue

import (
	"fmt"

	"github.com/arenadata/ad-scheduler/pkg/resource"
)

// Gang scheduling without placeholders is "admission by accounting" (decisions
// q1/q9): instead of pause-pods holding real capacity, an admitted gang holds a
// virtual reservation of its aggregate minResources as an allocating booking on
// its leaf, rolled up to root. The whole gang is admitted atomically or not at
// all — a per-leaf compare-and-swap against headroom — so partial gangs never
// consume capacity, and the reservation is keyed by gangID for exactly-once
// release across churn, retries and recovery.

// gangHold is one admitted gang's virtual reservation.
type gangHold struct {
	leaf string
	held resource.Resource // aggregate minResources still held as allocating
}

// AdmitGang atomically admits a gang: if its aggregate minResources fit the
// leaf's headroom (borrow-up-to-max, counting every already-admitted gang's
// reservation), it books them as an allocating reservation keyed by gangID and
// returns true. If they do not fit it returns false and books nothing. Re-admit
// of an already-admitted gangID is idempotent and returns true. This is the CAS
// at the heart of gang admission — it runs under the manager write lock so the
// headroom check and the reservation are one atomic step.
func (m *QueueManager) AdmitGang(leaf, gangID string, minResources resource.Resource) (bool, error) {
	if gangID == "" {
		return false, fmt.Errorf("empty gangID")
	}
	if !minResources.StrictlyGtZero() {
		return false, fmt.Errorf("gang %q minResources must be strictly positive", gangID)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.gangs[gangID]; ok {
		return true, nil // already admitted: idempotent
	}
	q, ok := m.index[leaf]
	if !ok {
		return false, fmt.Errorf("unknown queue %q", leaf)
	}
	if !q.IsLeaf() {
		return false, fmt.Errorf("gang must target a leaf, %q is not", leaf)
	}
	// Distinguish a permanent misconfiguration (minResources exceed the queue
	// ceiling — will never fit, surface it) from a transient headroom shortfall
	// (gate quietly and retry when capacity frees).
	if !q.fitsMax(minResources) {
		return false, fmt.Errorf("gang %q minResources %v exceed queue %q max — will never fit", gangID, minResources, leaf)
	}
	if !q.CanFit(minResources) {
		return false, nil // transient headroom gate: whole gang or nothing
	}
	q.reserve(minResources)
	m.gangs[gangID] = &gangHold{leaf: leaf, held: minResources.Clone()}
	return true, nil
}

// ReleaseGang releases a gang's remaining virtual reservation, keyed by gangID
// and idempotent: releasing an unknown or already-released gang is a no-op. Used
// on gang completion, failure, timeout/abort, and reservation GC.
func (m *QueueManager) ReleaseGang(gangID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.gangs[gangID]
	if !ok {
		return nil // idempotent
	}
	if q, ok := m.index[h.leaf]; ok && !h.held.IsEmpty() {
		q.unreserve(h.held)
	}
	delete(m.gangs, gangID)
	return nil
}

// IsGangAdmitted reports whether the gang currently holds a reservation.
func (m *QueueManager) IsGangAdmitted(gangID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.gangs[gangID]
	return ok
}

// GangHeld returns the resources a gang still holds virtually (empty if the gang
// is unknown). Exposed for recovery/introspection.
func (m *QueueManager) GangHeld(gangID string) resource.Resource {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if h, ok := m.gangs[gangID]; ok {
		return h.held.Clone()
	}
	return resource.Resource{}
}

// WoundWaitKey is the ordering key for head-of-line gang admission (decision q4):
// among gangs waiting on the same leaf, the winner is chosen lexicographically by
// (EffectivePriority DESC, Age ASC, UID ASC) — higher priority first, then the
// older gang (larger Age), then a stable UID tiebreak. Modelling Age as a
// monotonic counter (or nanoseconds waited) keeps the comparison pure and free of
// wall-clock reads.
type WoundWaitKey struct {
	EffectivePriority int32
	Age               int64 // larger = older = waited longer
	UID               string
}

// WoundWaitLess reports whether gang a should be admitted before gang b. It is a
// strict weak ordering: higher priority wins; ties go to the older gang; then to
// the smaller UID for determinism.
func WoundWaitLess(a, b WoundWaitKey) bool {
	if a.EffectivePriority != b.EffectivePriority {
		return a.EffectivePriority > b.EffectivePriority
	}
	if a.Age != b.Age {
		return a.Age > b.Age // older (larger Age) first
	}
	return a.UID < b.UID
}
