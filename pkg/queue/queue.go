/*
Package queue is the scheduler's engine: the K8s-agnostic "brain" that owns the
hierarchical queue tree, the leaf→root roll-up of allocated/allocating/pending,
headroom (borrow-up-to-max) math, DRF ordering, placement, gang reservations and
preemption victim selection. The framework plugins are thin adapters that call
into this package; nothing here imports k8s.io/kube-scheduler.

Resource semantics follow package resource: in a usage vector (allocated,
allocating, pending, guaranteed) an absent dimension is 0; in a max vector an
absent dimension is UNLIMITED. That unbounded semantic is applied here (the layer
that knows it is dealing with a limit), never by the resource arithmetic.
*/
package queue

import "slices"

import "github.com/arenadata/ad-scheduler/pkg/resource"

// RootName is the synthetic apex of the tree. Namespaces hang off root as its
// children (path "root.<namespace>"), and SA-mapped leaves off those, matching
// the "<namespace>.<cr-name>" convention with root kept compat-only.
const RootName = "root"

// Queue is one node of the hierarchy. All mutating and reading methods assume
// the owning QueueManager's lock is held — Queue carries no lock of its own, so
// the whole tree moves under one writer or many readers.
type Queue struct {
	name     string // local (last path segment)
	path     string // full dotted path, e.g. "root.team.spark"
	parent   *Queue
	children map[string]*Queue

	guaranteed resource.Resource // min share (DRF weight + preemption floor)
	max        resource.Resource // hard cap; absent dimension = unlimited

	allocated  resource.Resource // confirmed usage (rolled up)
	allocating resource.Resource // in-flight two-phase reservations (rolled up)
	pending    resource.Resource // outstanding demand (rolled up)

	// Placement / authorization, sourced from the Queue CRD (leaf-only for
	// serviceAccounts; ACLs inherit down the tree).
	serviceAccounts []string // SAs routed to this leaf
	defaultLeaf     bool     // namespace default queue (spec.default; ["*"] is an alias)
	maxApplications int32    // cap on distinct running app-ids at this leaf; 0 = unlimited
	limits          []Limit  // per-SA-group sub-caps within this leaf
	submitACL       ACL      // who may submit here; empty = inherit parent
	adminACL        ACL      // who may administer/preempt; empty = inherit parent

	// fence marks this queue as a preemption boundary: a preemptor whose leaf
	// sits at or below this queue may not reclaim victims outside this queue's
	// subtree (spec.preemption.policy=fence). Independently, every top-level
	// queue (a direct child of root) is an implicit fence — the default that
	// keeps reclaim/preemption from ever crossing a namespace/tenant boundary
	// (decision q3). See fenceBoundary.
	fence bool
}

// MaxApplications returns the leaf's running-application cap (0 = unlimited).
func (q *Queue) MaxApplications() int32 { return q.maxApplications }

// Limits returns the leaf's per-SA-group sub-caps (copy).
func (q *Queue) Limits() []Limit { return append([]Limit(nil), q.limits...) }

// ServiceAccounts returns the SA routing list for this (leaf) queue.
func (q *Queue) ServiceAccounts() []string { return append([]string(nil), q.serviceAccounts...) }

// IsDefaultLeaf reports whether this leaf is the namespace default queue (routes
// any otherwise-unmapped SA in its namespace subtree — decision q24 / G9). It is
// set by the explicit spec.default marker or the legacy serviceAccounts ["*"]
// shorthand.
func (q *Queue) IsDefaultLeaf() bool { return q.isDefaultLeaf() }

func (q *Queue) isDefaultLeaf() bool {
	if q.defaultLeaf {
		return true
	}
	return slices.Contains(q.serviceAccounts, "*")
}

// Name returns the local name (last path segment).
func (q *Queue) Name() string { return q.name }

// Path returns the full dotted path.
func (q *Queue) Path() string { return q.path }

// Parent returns the parent queue, or nil for the root.
func (q *Queue) Parent() *Queue { return q.parent }

// IsLeaf reports whether the queue has no children (only leaves take apps).
func (q *Queue) IsLeaf() bool { return len(q.children) == 0 }

// isTopLevel reports whether this queue is a direct child of the synthetic root
// (path root.<segment>) — the default, always-on preemption fence.
func (q *Queue) isTopLevel() bool { return q.parent != nil && q.parent.parent == nil }

// isFence reports whether this queue is a preemption fence: either an explicit
// spec.preemption.policy=fence, or an implicit top-level (per-namespace) fence.
func (q *Queue) isFence() bool { return q.fence || q.isTopLevel() }

// IsFence exposes the effective preemption-fence status (introspection/statedump).
func (q *Queue) IsFence() bool { return q.isFence() }

// fenceBoundary returns the nearest fenced ancestor of this queue (inclusive of
// the queue itself) — the top-most queue a preemptor at this leaf may reach for
// victims. Because top-level queues are always fences, the walk terminates at the
// top-level node at the latest; root is returned only for a queue hung directly
// off root.
func (q *Queue) fenceBoundary() *Queue {
	last := q
	for n := q; n != nil; n = n.parent {
		if n.isFence() {
			return n
		}
		last = n
	}
	return last
}

// Child returns the named direct child, if any.
func (q *Queue) Child(name string) (*Queue, bool) {
	c, ok := q.children[name]
	return c, ok
}

// Guaranteed / Max / Allocated / Allocating / Pending return clones so callers
// cannot mutate the tree's internal vectors.
func (q *Queue) Guaranteed() resource.Resource { return q.guaranteed.Clone() }
func (q *Queue) Max() resource.Resource        { return q.max.Clone() }
func (q *Queue) Allocated() resource.Resource  { return q.allocated.Clone() }
func (q *Queue) Allocating() resource.Resource { return q.allocating.Clone() }
func (q *Queue) Pending() resource.Resource    { return q.pending.Clone() }

// Used is what counts against max: confirmed allocated plus in-flight
// allocating. Headroom and the fit gate are computed against this.
func (q *Queue) Used() resource.Resource { return resource.Add(q.allocated, q.allocating) }

// walkUp applies fn to this queue and every ancestor up to the root. It is the
// single primitive behind every roll-up (reserve/commit/release/pending).
func (q *Queue) walkUp(fn func(*Queue)) {
	for n := q; n != nil; n = n.parent {
		fn(n)
	}
}

// reserve rolls an in-flight (two-phase) reservation up the path: allocating += r
// on the leaf and every ancestor. Pair with commit (on bind) or unreserve (on
// rollback).
func (q *Queue) reserve(r resource.Resource) {
	q.walkUp(func(n *Queue) { n.allocating = resource.Add(n.allocating, r) })
}

// unreserve reverses reserve (bind failure / Unreserve).
func (q *Queue) unreserve(r resource.Resource) {
	q.walkUp(func(n *Queue) { n.allocating = resource.Sub(n.allocating, r) })
}

// commit moves a reservation from allocating to allocated on bind observation,
// along the whole path — the used total is unchanged, so no fit re-check races.
func (q *Queue) commit(r resource.Resource) {
	q.walkUp(func(n *Queue) {
		n.allocating = resource.Sub(n.allocating, r)
		n.allocated = resource.Add(n.allocated, r)
	})
}

// allocate books confirmed usage directly (used on restart reconcile from
// already-bound pods, where there was no reserve phase to commit).
func (q *Queue) allocate(r resource.Resource) {
	q.walkUp(func(n *Queue) { n.allocated = resource.Add(n.allocated, r) })
}

// release frees confirmed usage on a terminal pod, along the path.
func (q *Queue) release(r resource.Resource) {
	q.walkUp(func(n *Queue) { n.allocated = resource.Sub(n.allocated, r) })
}

// addPending / subPending roll demand up the path (drives autoscaler visibility
// and DRF pending-share).
func (q *Queue) addPending(r resource.Resource) {
	q.walkUp(func(n *Queue) { n.pending = resource.Add(n.pending, r) })
}
func (q *Queue) subPending(r resource.Resource) {
	q.walkUp(func(n *Queue) { n.pending = resource.Sub(n.pending, r) })
}

// slack is this queue's own remaining room toward its max, per bounded
// dimension. Every dimension present in max gets an entry, clamped at 0 when the
// queue is at or over cap — a saturated dimension must stay visible (value 0) so
// PathHeadroom's min is not fooled into reporting an ancestor's larger slack.
// Unbounded dimensions (absent in max) are omitted. This is one node's
// contribution; the path-wide limit is PathHeadroom.
func (q *Queue) slack() resource.Resource {
	used := q.Used()
	out := make(resource.Resource, len(q.max))
	for d, cap := range q.max {
		free := max(cap-used[d], 0)
		out[d] = free
	}
	return out
}

// CanFit reports whether request can be admitted at this queue without pushing
// this queue or any ancestor over its max — the borrow-up-to-max gate. A
// dimension unbounded all the way to root imposes no constraint on that
// dimension (this is why we cannot express the gate as resource.FitIn against a
// single headroom vector: absent means "unlimited" here, but "zero" in FitIn).
func (q *Queue) CanFit(request resource.Resource) bool {
	for n := q; n != nil; n = n.parent {
		if len(n.max) == 0 {
			continue
		}
		used := n.Used()
		for d, need := range request {
			cap, bounded := n.max[d]
			if bounded && need > cap-used[d] {
				return false
			}
		}
	}
	return true
}

// fitsMax reports whether request alone is within max at this queue and every
// ancestor, ignoring current usage. Unlike CanFit (a transient headroom check),
// false here means the request can NEVER fit — a permanent misconfiguration
// (e.g. a gang whose minResources exceed the queue ceiling).
func (q *Queue) fitsMax(request resource.Resource) bool {
	for n := q; n != nil; n = n.parent {
		for d, need := range request {
			if cap, bounded := n.max[d]; bounded && need > cap {
				return false
			}
		}
	}
	return true
}

// PathHeadroom is the minimum slack per dimension over this queue and all
// ancestors — the tightest bound on how much more this leaf can consume. A
// dimension bounded by no ancestor is absent (unlimited); a dimension bounded
// somewhere carries the smallest remaining room along the path. Intended for
// observability and DRF; the admission gate is CanFit, which handles the
// absent=unlimited asymmetry that a plain vector cannot.
func (q *Queue) PathHeadroom() resource.Resource {
	out := resource.Resource{}
	for n := q; n != nil; n = n.parent {
		for d, free := range n.slack() {
			if cur, ok := out[d]; !ok || free < cur {
				out[d] = free
			}
		}
	}
	return out
}
