package queue

import "github.com/arenadata/ad-scheduler/pkg/resource"

// NodeSortPolicy selects how nodes are scored once they pass Filter: spread the
// load or pack it. It is the engine-side of the nodesort plugin's Score.
type NodeSortPolicy int

const (
	// NodeSpread favours the least-utilised node (even load, resilience).
	NodeSpread NodeSortPolicy = iota
	// NodeBinpack favours the most-utilised node (dense packing, scale-down).
	NodeBinpack
)

// ParseNodeSortPolicy maps the config string to a policy; unknown/empty ->
// spread (the safer default).
func ParseNodeSortPolicy(s string) NodeSortPolicy {
	if s == "binpacking" || s == "binpack" {
		return NodeBinpack
	}
	return NodeSpread
}

// MaxNodeScore is the top of the score range (mirrors the framework's 0..100).
const MaxNodeScore = 100

// NodeScore scores a node in [0, MaxNodeScore] for the given policy, based on
// the node's dominant utilisation after the candidate pod is placed on it:
// utilisation = dominantShare(used+request, capacity), clamped to [0,1]. Spread
// rewards low utilisation, binpack rewards high. A node with no capacity (or one
// the request overflows) scores 0 under spread and MaxNodeScore under binpack,
// which the Filter gate should already have excluded — Score only ranks the
// survivors.
func NodeScore(policy NodeSortPolicy, used, request, capacity resource.Resource) int64 {
	util := resource.DominantShare(resource.Add(used, request), capacity)
	if util < 0 {
		util = 0
	}
	if util > 1 {
		util = 1
	}
	if policy == NodeBinpack {
		return int64(util * MaxNodeScore)
	}
	return int64((1 - util) * MaxNodeScore)
}
