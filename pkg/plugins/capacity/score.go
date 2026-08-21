package capacity

import (
	"context"

	v1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"

	"github.com/arenadata/ad-scheduler/pkg/queue"
	"github.com/arenadata/ad-scheduler/pkg/resource"
)

// Score ranks feasible nodes by DRF dominant utilisation after placing the pod,
// per the configured NodeSortPolicy: "spread" favours the least-utilised node,
// "binpacking" the most (decision q3 / M3). Scores are already in
// [0, MaxNodeScore], so no NormalizeScore pass is needed.
func (c *Capacity) Score(_ context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	req := resource.EffectiveRequest(pod)
	if s, err := readState(state); err == nil {
		req = s.req // reuse PreFilter's computed request (init-peak, sidecars, DRA)
	}
	capacity := fwkResource(nodeInfo.GetAllocatable())
	used := fwkResource(nodeInfo.GetRequested())
	// Score only on node-backed dimensions: DRA (dra/<class>) and any request
	// dimension the node does not advertise are not node capacity, so including
	// them would saturate the dominant share and flatten the ranking.
	scoreReq := resource.Resource{}
	for d, v := range req {
		if _, ok := capacity[d]; ok {
			scoreReq[d] = v
		}
	}
	return queue.NodeScore(c.nodeSortPolicy, used, scoreReq, capacity), nil
}

// ScoreExtensions returns nil: NodeScore already yields scores in [0, MaxNodeScore].
func (c *Capacity) ScoreExtensions() fwk.ScoreExtensions { return nil }
