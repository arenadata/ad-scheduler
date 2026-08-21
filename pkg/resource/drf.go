package resource

// DRF (Dominant Resource Fairness) primitives.
//
// The dominant share of a usage vector relative to a capacity vector is the
// maximum, across dimensions, of usage[d]/capacity[d]. Fairness compares
// queues/apps by dominant share: the one with the smaller dominant share is
// "less satisfied" and wins the next allocation. Comp is the single ordering
// primitive the queue/app sorts and DRF Score build on.

// DominantShare returns the dominant resource share of usage relative to
// capacity, in [0, +Inf). A dimension with zero capacity contributes:
//
//   - 0 when usage is also 0 in that dimension (nothing asked, nothing to
//     divide), and
//   - +Inf when usage is > 0 (asking for a resource the capacity does not
//     provide — infinitely over its share),
//
// so a request that names a resource the cluster/queue lacks is always ranked
// last. Negative usage in a dimension is clamped to 0 (a share is never
// negative). An empty usage vector has share 0.
func DominantShare(usage, capacity Resource) float64 {
	var dominant float64
	for k, u := range usage {
		if u <= 0 {
			continue
		}
		c := capacity[k]
		if c <= 0 {
			return posInf
		}
		if share := float64(u) / float64(c); share > dominant {
			dominant = share
		}
	}
	return dominant
}

// Comp orders l and r by their dominant share relative to capacity. It returns
//
//	-1 if l is less satisfied than r (l should be served first),
//	 0 if they are equally satisfied,
//	+1 if l is more satisfied than r.
//
// It is a strict weak ordering suitable for a heap/sort comparator; ties
// (equal dominant share, including two empties) return 0 and must be broken by
// a stable secondary key (enqueue time, name) at the call site.
func Comp(capacity, l, r Resource) int {
	ls := DominantShare(l, capacity)
	rs := DominantShare(r, capacity)
	switch {
	case ls < rs:
		return -1
	case ls > rs:
		return 1
	default:
		return 0
	}
}

// posInf is +Inf without importing math for a single constant; any finite
// share compares below it, which is exactly the "over capacity" ranking we
// want.
var posInf = func() float64 {
	var zero float64
	return 1 / zero
}()
