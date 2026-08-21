/*
Package resource models multi-dimensional cluster resources as an int64-per-
dimension vector. Any vendor key is first-class — `memory`, `vcore`,
`nvidia.com/gpu`, `dra/<deviceClassName>`, hugepages, arbitrary extended
resources — so the engine never special-cases a resource name.

Semantics of an absent dimension are deliberately context-dependent and NOT
baked into the arithmetic here:

  - In a usage/request/allocated vector an absent dimension means 0.
  - In a max/capacity vector an absent dimension means "unlimited"; that
    unbounded semantic is applied by the queue/headroom layer (which knows it
    is dealing with a limit), never by this package. The functions below treat
    a missing key as 0 so that arithmetic stays total and associative.
*/
package resource

import "maps"

// Resource is a multi-dimensional resource vector keyed by resource name.
// A nil Resource is a valid empty vector.
type Resource map[string]int64

// New returns a Resource from the given dimensions. It is a convenience for
// tests and call-sites that build a literal.
func New(dims map[string]int64) Resource {
	if dims == nil {
		return Resource{}
	}
	out := make(Resource, len(dims))
	maps.Copy(out, dims)
	return out
}

// Clone returns a deep copy. Cloning nil yields a non-nil empty Resource so
// callers can safely write into the result.
func (r Resource) Clone() Resource {
	out := make(Resource, len(r))
	maps.Copy(out, r)
	return out
}

// IsEmpty reports whether every dimension is zero (an absent dimension counts
// as zero). A nil or empty map is empty.
func (r Resource) IsEmpty() bool {
	for _, v := range r {
		if v != 0 {
			return false
		}
	}
	return true
}

// StrictlyGtZero reports whether the vector represents a strictly positive
// request: at least one dimension is > 0 and no dimension is < 0. It is the
// guard used to reject empty or negative asks before admission.
func (r Resource) StrictlyGtZero() bool {
	any := false
	for _, v := range r {
		if v < 0 {
			return false
		}
		if v > 0 {
			any = true
		}
	}
	return any
}

// Equal reports whether two vectors are equal dimension-by-dimension, treating
// an absent dimension as zero (so {cpu:0} equals {}).
func Equal(a, b Resource) bool {
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	for k, v := range b {
		if a[k] != v {
			return false
		}
	}
	return true
}

// Add returns the element-wise sum a+b over the union of dimensions. Neither
// input is mutated. Zero-valued result dimensions are pruned so the result is
// canonical (Equal-comparable by value).
func Add(a, b Resource) Resource {
	out := make(Resource, len(a)+len(b))
	for k, v := range a {
		out[k] += v
	}
	for k, v := range b {
		out[k] += v
	}
	prune(out)
	return out
}

// Sub returns the element-wise difference a-b over the union of dimensions.
// Result dimensions may be negative (e.g. over-allocation); callers that need
// a floor use FitIn/StrictlyGtZero to check. Neither input is mutated.
func Sub(a, b Resource) Resource {
	out := make(Resource, len(a)+len(b))
	for k, v := range a {
		out[k] += v
	}
	for k, v := range b {
		out[k] -= v
	}
	prune(out)
	return out
}

// Multiply scales every dimension by factor. Neither input is mutated.
func (r Resource) Multiply(factor int64) Resource {
	out := make(Resource, len(r))
	for k, v := range r {
		if p := v * factor; p != 0 {
			out[k] = p
		}
	}
	return out
}

// FitIn reports whether request fits within available: for every dimension the
// request needs, request[d] <= available[d] (an absent available dimension is
// treated as 0, so asking for a resource the target does not have never fits).
// This is the lock-free predicate capacity.Filter runs against
// min(headroom, nodeAvailable).
func FitIn(request, available Resource) bool {
	for k, need := range request {
		if need > available[k] {
			return false
		}
	}
	return true
}

// Max returns the element-wise maximum of a and b. Used to compute a pod's
// effective request across init containers (max, not sum). Neither input is
// mutated.
func Max(a, b Resource) Resource {
	out := a.Clone()
	for k, v := range b {
		if v > out[k] {
			out[k] = v
		}
	}
	prune(out)
	return out
}

// prune removes zero-valued dimensions in place so equal vectors share one
// canonical representation ({cpu:0} collapses to {}).
func prune(r Resource) {
	for k, v := range r {
		if v == 0 {
			delete(r, k)
		}
	}
}
