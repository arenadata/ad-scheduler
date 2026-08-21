package queue

import (
	"slices"
	"sort"

	"github.com/arenadata/ad-scheduler/pkg/resource"
)

// Preemption victim selection is the pure core of M5 / decision q3. The
// preemption controller (called from PostFilter when no node passed Filter)
// gathers candidate victims and their protection status from the tree, then asks
// SelectVictims for a small set whose eviction frees enough for the incoming
// pod. The guaranteed-protection invariant (never drop a queue below its
// guaranteed), fence boundaries and PDB blocks are all folded into the
// Preemptable flag by the caller, so this function stays pure and testable and
// never needs the tree or a clock.

// Victim is a candidate pod for eviction.
type Victim struct {
	UID      string
	Priority int32             // lower is cheaper to evict
	Request  resource.Resource // freed if evicted
	// Preemptable is false when evicting this pod is forbidden: its queue is at
	// or below guaranteed, it is fenced off from the reclaiming queue, a PDB
	// blocks it, or its priority is >= the incoming pod's. The caller computes
	// this; SelectVictims never evicts a non-preemptable pod.
	Preemptable bool
}

// SelectVictims returns a minimal-ish set of preemptable victims whose combined
// request covers need, preferring to evict the lowest-priority pods and the
// fewest of them. It returns nil when need is empty (nothing to reclaim) or when
// the preemptable pool cannot cover need (infeasible — the caller reports
// Unschedulable rather than evicting pods for nothing).
//
// The set is a greedy approximation (exact minimum is subset-sum / NP-hard, as
// in every real scheduler): victims are taken lowest-priority-first, largest
// contribution first, then a trim pass drops any now-redundant higher-priority
// victim so we spare as much high-priority work as possible.
func SelectVictims(need resource.Resource, candidates []Victim) []Victim {
	if need.IsEmpty() {
		return nil
	}
	pool := make([]Victim, 0, len(candidates))
	for _, v := range candidates {
		if v.Preemptable && contribution(need, v.Request) > 0 {
			pool = append(pool, v)
		}
	}
	// lowest priority first (cheapest to evict); ties -> largest contribution
	// first (fewer victims); ties -> stable UID for determinism.
	sort.SliceStable(pool, func(i, j int) bool {
		if pool[i].Priority != pool[j].Priority {
			return pool[i].Priority < pool[j].Priority
		}
		ci, cj := contribution(need, pool[i].Request), contribution(need, pool[j].Request)
		if ci != cj {
			return ci > cj
		}
		return pool[i].UID < pool[j].UID
	})

	freed := resource.Resource{}
	var chosen []Victim
	for _, v := range pool {
		if resource.FitIn(need, freed) {
			break
		}
		freed = resource.Add(freed, v.Request)
		chosen = append(chosen, v)
	}
	if !resource.FitIn(need, freed) {
		return nil // infeasible: cannot reclaim enough
	}
	// Trim: walk from the last-added (highest-priority / smallest) and drop any
	// victim whose removal still covers need — spare the most valuable pods.
	for i, c := range slices.Backward(chosen) {
		if without := resource.Sub(freed, c.Request); resource.FitIn(need, without) {
			freed = without
			chosen = append(chosen[:i], chosen[i+1:]...)
		}
	}
	return chosen
}

// contribution is how much a victim's request helps toward the needed
// dimensions — the sum over need's dimensions of what the victim frees there. A
// victim that frees nothing we need contributes 0 and is skipped.
func contribution(need, req resource.Resource) int64 {
	var s int64
	for d := range need {
		s += req[d]
	}
	return s
}
