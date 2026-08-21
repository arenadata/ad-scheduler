package queue

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/arenadata/ad-scheduler/pkg/resource"
)

// Spec is the declarative shape of one queue, as assembled by controller/queue
// from the Queue CRDs (plus the cluster-admin envelope). It is intentionally
// K8s-free so the engine and its tests never touch the apimachinery types.
type Spec struct {
	Name       string
	Guaranteed resource.Resource
	Max        resource.Resource // absent dimension = unlimited
	// ServiceAccounts routes (namespace, SA) pods to this leaf. Meaningful on
	// leaves only.
	ServiceAccounts []string
	// Default marks this leaf as the namespace default queue (≤1 per namespace);
	// serviceAccounts ["*"] is an equivalent alias. Meaningful on leaves only.
	Default bool
	// MaxApplications caps the number of distinct running applications (app-ids)
	// admitted to this leaf; 0 = unlimited. Meaningful on leaves only.
	MaxApplications int32
	// Limits are per-service-account-group sub-caps within this leaf.
	Limits    []Limit
	SubmitACL string // wire form (see ParseACL); empty inherits parent
	AdminACL  string
	// Fence marks this queue as a preemption boundary (spec.preemption.policy=
	// fence): preemptors at or below it may not reclaim outside its subtree.
	// Top-level queues are fences implicitly regardless of this flag (decision
	// q3); this only adds tighter, deeper boundaries.
	Fence    bool
	Children []*Spec
}

// Limit is a per-service-account-group sub-cap within a leaf (decision q19): the
// listed ServiceAccounts (or ["*"]) may run at most MaxApplications apps and use
// at most MaxResources within the queue. 0/absent = unlimited on that axis.
type Limit struct {
	ServiceAccounts []string
	MaxApplications int32
	MaxResources    resource.Resource
}

// QueueManager owns the whole tree behind one RWMutex: every allocate/release
// roll-up runs under the write lock, snapshots and fit checks under the read
// lock. There is exactly one per scheduler profile (decision: single brain).
type QueueManager struct {
	mu    sync.RWMutex
	root  *Queue
	index map[string]*Queue    // path -> queue, for O(1) leaf lookup
	gangs map[string]*gangHold // gangID -> virtual reservation (see gang.go)
}

// NewManager builds a manager from a root spec. The root spec's name is forced
// to RootName. Build validates structure (unique siblings, non-negative vectors,
// child.guaranteed sum ≤ parent.guaranteed, child.max ≤ parent.max where both
// bounded) and returns an error rather than a partial tree — fail-closed.
func NewManager(root *Spec) (*QueueManager, error) {
	if root == nil {
		return nil, fmt.Errorf("nil root spec")
	}
	m := &QueueManager{index: map[string]*Queue{}, gangs: map[string]*gangHold{}}
	r := *root // shallow copy; Name is forced to RootName, other fields carried
	r.Name = RootName
	built, err := m.build(&r, nil)
	if err != nil {
		return nil, err
	}
	m.root = built
	if err := m.validatePlacement(); err != nil {
		return nil, err
	}
	return m, nil
}

// validatePlacement enforces the SA-routing invariants across the built tree:
// serviceAccounts / the default marker appear on leaves only; within a namespace
// subtree each SA routes to at most one leaf and there is at most one default
// queue (spec.default or the ["*"] alias).
func (m *QueueManager) validatePlacement() error {
	// namespace -> SA -> leaf path (for the disjointness check)
	saLeaf := map[string]map[string]string{}
	defaultLeaf := map[string]string{}
	for path, q := range m.index {
		if len(q.serviceAccounts) == 0 && !q.defaultLeaf {
			continue
		}
		if !q.IsLeaf() {
			return fmt.Errorf("queue %q: serviceAccounts/default allowed on leaves only", path)
		}
		ns := namespaceOf(path)
		if ns == "" {
			return fmt.Errorf("queue %q: serviceAccounts/default require a namespace level under root", path)
		}
		if q.isDefaultLeaf() {
			if prev, ok := defaultLeaf[ns]; ok {
				return fmt.Errorf("namespace %q: two default queues %q and %q (only one allowed)", ns, prev, path)
			}
			defaultLeaf[ns] = path
		}
		for _, sa := range q.serviceAccounts {
			if sa == "*" {
				continue // the default marker, handled above
			}
			if saLeaf[ns] == nil {
				saLeaf[ns] = map[string]string{}
			}
			// prev == path means the same leaf listed the SA twice — harmless
			// (routes to one queue); only a cross-leaf collision is a conflict.
			if prev, ok := saLeaf[ns][sa]; ok && prev != path {
				return fmt.Errorf("namespace %q: SA %q maps to both %q and %q", ns, sa, prev, path)
			}
			saLeaf[ns][sa] = path
		}
	}
	return nil
}

// NamespaceOf returns the namespace segment of a queue path ("root.<ns>...").
// It is the convention link between a pod's namespace and its subtree apex.
func NamespaceOf(path string) string {
	parts := strings.Split(path, ".")
	if len(parts) < 2 || parts[0] != RootName {
		return ""
	}
	return parts[1]
}

func namespaceOf(path string) string { return NamespaceOf(path) }

func (m *QueueManager) build(spec *Spec, parent *Queue) (*Queue, error) {
	if spec.Name == "" {
		return nil, fmt.Errorf("queue with empty name under %q", pathOf(parent))
	}
	if strings.Contains(spec.Name, ".") {
		return nil, fmt.Errorf("queue name %q must not contain '.'", spec.Name)
	}
	if err := nonNegative(spec.Guaranteed); err != nil {
		return nil, fmt.Errorf("queue %q guaranteed: %w", spec.Name, err)
	}
	if err := nonNegative(spec.Max); err != nil {
		return nil, fmt.Errorf("queue %q max: %w", spec.Name, err)
	}
	path := spec.Name
	if parent != nil {
		path = parent.path + "." + spec.Name
	}
	if _, dup := m.index[path]; dup {
		return nil, fmt.Errorf("duplicate queue path %q", path)
	}
	q := &Queue{
		name:            spec.Name,
		path:            path,
		parent:          parent,
		children:        map[string]*Queue{},
		guaranteed:      spec.Guaranteed.Clone(),
		max:             spec.Max.Clone(),
		allocated:       resource.Resource{},
		allocating:      resource.Resource{},
		pending:         resource.Resource{},
		serviceAccounts: append([]string(nil), spec.ServiceAccounts...),
		defaultLeaf:     spec.Default,
		maxApplications: spec.MaxApplications,
		limits:          append([]Limit(nil), spec.Limits...),
		submitACL:       ParseACL(spec.SubmitACL),
		adminACL:        ParseACL(spec.AdminACL),
		fence:           spec.Fence,
	}
	m.index[path] = q

	// child.max must not exceed parent.max on any dimension the parent bounds.
	sumGuar := resource.Resource{}
	for _, cs := range spec.Children {
		if _, dup := q.children[cs.Name]; dup {
			return nil, fmt.Errorf("duplicate child %q under %q", cs.Name, path)
		}
		if err := maxWithinParent(cs.Max, q.max); err != nil {
			return nil, fmt.Errorf("child %q under %q: %w", cs.Name, path, err)
		}
		child, err := m.build(cs, q)
		if err != nil {
			return nil, err
		}
		q.children[cs.Name] = child
		sumGuar = resource.Add(sumGuar, child.guaranteed)
	}
	// Σ children.guaranteed ≤ this.guaranteed, on every dimension this bounds.
	if len(q.guaranteed) > 0 {
		for d, g := range q.guaranteed {
			if sumGuar[d] > g {
				return nil, fmt.Errorf("queue %q: Σ children guaranteed %s=%d exceeds %d", path, d, sumGuar[d], g)
			}
		}
	}
	return q, nil
}

// Root returns the tree apex.
func (m *QueueManager) Root() *Queue { return m.root }

// Queue looks up a queue by full dotted path (e.g. "root.team.spark").
func (m *QueueManager) Queue(path string) (*Queue, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	q, ok := m.index[path]
	return q, ok
}

// All returns every queue in the tree sorted by path (introspection/statedump).
func (m *QueueManager) All() []*Queue {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Queue, 0, len(m.index))
	for _, q := range m.index {
		out = append(out, q)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	return out
}

// Leaves returns the leaf queues sorted by path (stable ordering for tests and
// DRF descent seeding).
func (m *QueueManager) Leaves() []*Queue {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*Queue
	for _, q := range m.index {
		if q.IsLeaf() {
			out = append(out, q)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	return out
}

// CanFit reports whether request fits at the leaf without pushing it or any
// ancestor over max. Read-locked and lock-free of the tree's writers.
func (m *QueueManager) CanFit(path string, request resource.Resource) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	q, ok := m.index[path]
	if !ok {
		return false, fmt.Errorf("unknown queue %q", path)
	}
	return q.CanFit(request), nil
}

// FindByNamespaceName locates a Queue CR in the tree by (namespace, bare name),
// returning its path and whether it is a leaf, read atomically. Names are unique
// per namespace, so the match is unambiguous.
func (m *QueueManager) FindByNamespaceName(ns, name string) (path string, isLeaf, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for p, q := range m.index {
		if NamespaceOf(p) == ns && p[strings.LastIndex(p, ".")+1:] == name {
			return p, q.IsLeaf(), true
		}
	}
	return "", false, false
}

// QueueUsedGuaranteed returns a queue's current used (allocated+allocating) and
// its guaranteed floor, read atomically under the tree lock. Used by the reclaim
// controller to decide entitlement (starved queue below guaranteed) and victim
// eligibility (donor queue above guaranteed). ok is false for an unknown path.
func (m *QueueManager) QueueUsedGuaranteed(path string) (used, guaranteed resource.Resource, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	q, exists := m.index[path]
	if !exists {
		return nil, nil, false
	}
	return q.Used(), q.guaranteed.Clone(), true
}

// FenceBoundary returns the path of the nearest preemption fence enclosing the
// given queue (see Queue.fenceBoundary): the top-most queue a preemptor at that
// leaf may reclaim within. Callers filter candidate victims with PathWithinSubtree
// against this boundary. ok is false for an unknown path.
func (m *QueueManager) FenceBoundary(path string) (boundary string, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	q, exists := m.index[path]
	if !exists {
		return "", false
	}
	return q.fenceBoundary().path, true
}

// WithinFence reports whether victimPath lies inside the preemption fence of
// askerPath — i.e. a preemptor at askerPath may consider evicting a pod at
// victimPath without crossing a fence boundary. An unknown askerPath fails closed
// (no reach).
func (m *QueueManager) WithinFence(askerPath, victimPath string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	q, ok := m.index[askerPath]
	if !ok {
		return false
	}
	return PathWithinSubtree(victimPath, q.fenceBoundary().path)
}

// PathWithinSubtree reports whether path is the subtree root itself or one of its
// descendants (dotted-path prefix). Exported for preemption callers that filter
// victims against a fence boundary obtained from FenceBoundary.
func PathWithinSubtree(path, subtreeRoot string) bool {
	return path == subtreeRoot || strings.HasPrefix(path, subtreeRoot+".")
}

// Headroom returns the tightest remaining room per bounded dimension at the leaf.
func (m *QueueManager) Headroom(path string) (resource.Resource, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	q, ok := m.index[path]
	if !ok {
		return nil, fmt.Errorf("unknown queue %q", path)
	}
	return q.PathHeadroom(), nil
}

// WithinMax reports whether the leaf and all ancestors are at or under their max
// (used ≤ max on every bounded dimension). Unlike CanFit it takes no extra
// request — it re-validates that the queue is not already over-committed, used by
// capacity.PreBind to catch headroom drift (e.g. a config rebuild that lowered a
// max) between Reserve and Bind.
func (m *QueueManager) WithinMax(path string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	q, ok := m.index[path]
	if !ok {
		return false, fmt.Errorf("unknown queue %q", path)
	}
	for n := q; n != nil; n = n.parent {
		used := n.Used()
		for d, cap := range n.max {
			if used[d] > cap {
				return false, nil
			}
		}
	}
	return true, nil
}

// Reserve books a two-phase in-flight reservation at the leaf (write-locked).
func (m *QueueManager) Reserve(path string, r resource.Resource) error {
	return m.mutate(path, func(q *Queue) { q.reserve(r) })
}

// Unreserve reverses Reserve (bind failure / Unreserve).
func (m *QueueManager) Unreserve(path string, r resource.Resource) error {
	return m.mutate(path, func(q *Queue) { q.unreserve(r) })
}

// Commit moves a reservation to confirmed allocated on bind observation.
func (m *QueueManager) Commit(path string, r resource.Resource) error {
	return m.mutate(path, func(q *Queue) { q.commit(r) })
}

// Allocate books confirmed usage directly (restart reconcile of bound pods).
func (m *QueueManager) Allocate(path string, r resource.Resource) error {
	return m.mutate(path, func(q *Queue) { q.allocate(r) })
}

// Release frees confirmed usage on a terminal pod.
func (m *QueueManager) Release(path string, r resource.Resource) error {
	return m.mutate(path, func(q *Queue) { q.release(r) })
}

// AddPending / SubPending roll demand up the path.
func (m *QueueManager) AddPending(path string, r resource.Resource) error {
	return m.mutate(path, func(q *Queue) { q.addPending(r) })
}
func (m *QueueManager) SubPending(path string, r resource.Resource) error {
	return m.mutate(path, func(q *Queue) { q.subPending(r) })
}

func (m *QueueManager) mutate(path string, fn func(*Queue)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	q, ok := m.index[path]
	if !ok {
		return fmt.Errorf("unknown queue %q", path)
	}
	if !q.IsLeaf() {
		return fmt.Errorf("queue %q is not a leaf; usage only books at leaves", path)
	}
	fn(q)
	return nil
}

func pathOf(q *Queue) string {
	if q == nil {
		return "<root>"
	}
	return q.path
}

func nonNegative(r resource.Resource) error {
	for d, v := range r {
		if v < 0 {
			return fmt.Errorf("dimension %s is negative (%d)", d, v)
		}
	}
	return nil
}

// maxWithinParent ensures the child does not raise a ceiling the parent bounds:
// for every dimension the parent limits, the child's own limit (if any) must not
// exceed it. A child unbounded on a parent-bounded dimension is fine — it simply
// inherits the parent's tighter cap at fit time (CanFit walks to root).
func maxWithinParent(childMax, parentMax resource.Resource) error {
	for d, pcap := range parentMax {
		if cm, bounded := childMax[d]; bounded && cm > pcap {
			return fmt.Errorf("max %s=%d exceeds parent %d", d, cm, pcap)
		}
	}
	return nil
}
