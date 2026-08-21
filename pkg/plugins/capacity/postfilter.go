package capacity

import (
	"context"
	"sort"
	"time"

	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"

	"github.com/arenadata/ad-scheduler/pkg/queue"
	"github.com/arenadata/ad-scheduler/pkg/queue/placement"
	"github.com/arenadata/ad-scheduler/pkg/resource"
	"github.com/arenadata/ad-scheduler/pkg/util"
)

// PostFilter is ad-scheduler's node-level preemption (decision q3 / M5). It runs
// only when a pod that IS admissible to its queue (PreFilter passed) still found
// no feasible node (the pool is physically full). It picks a single pool node
// where evicting a minimal set of eligible victims frees enough room, evicts
// them, and nominates the node so the pod lands there on retry.
//
// Victim eligibility mirrors the queue-level reclaim controller so guarantees are
// never breached: only our pods, strictly lower priority than the incoming pod,
// from queues currently over their guaranteed floor, and only up to each donor's
// reclaimable budget. Gangs, DRA pods, and queue-over-max rejects are out of
// scope here (handled by gang admission / reclaim). PDB-aware selection is a
// documented follow-up.
func (c *Capacity) PostFilter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, m fwk.NodeToStatusReader) (*fwk.PostFilterResult, *fwk.Status) {
	st, err := readState(state)
	if err != nil {
		// PreFilter did not admit the pod to its queue (over-max / unmapped):
		// node-level eviction cannot help — let reclaim/gang handle it.
		return nil, fwk.NewStatus(fwk.Unschedulable, "ad-scheduler: pod not admissible to its queue")
	}
	if st.gang {
		// Gang-level preemption is limited to the LAST unplaced member: only preempt
		// once the rest of the gang is already assembled, so we never evict victims
		// for a gang still far from placing (wasted disruption; the gang would
		// Hard-timeout anyway). Full atomic multi-node gang preemption is a follow-up.
		gangName := pod.Annotations[util.PodGroupAnnotation]
		placed, minMember, ok := c.engine.Coordinator().GangProgress(pod.Namespace, gangName)
		if !ok || minMember <= 1 || placed < int(minMember)-1 {
			return nil, fwk.NewStatus(fwk.Unschedulable, "ad-scheduler: gang not at its last member; no node-level preemption")
		}
	}
	// Delay-gate (decision q3): do not evict for a pod that only just became
	// unschedulable — give it ReservationDelay to be placed by normal scheduling
	// (a transient burst may clear on its own). This also throttles preemption and
	// dampens cascades, since each eviction round waits out the delay.
	if d := c.engine.Config().ReservationDelay; d > 0 && unschedulableFor(pod) < d {
		return nil, fwk.NewStatus(fwk.Unschedulable, "ad-scheduler: preemption delayed (waiting out reservation-delay)")
	}
	tree := c.engine.Tree()
	if tree == nil {
		return nil, fwk.NewStatus(fwk.Unschedulable, "ad-scheduler: queue tree not built")
	}
	lister := c.handle.SnapshotSharedLister().NodeInfos()
	// Only nodes that failed with Unschedulable (had a resource shortfall) are
	// preemption candidates; UnschedulableAndUnresolvable (off-pool, taint) are not.
	candidates, err := m.NodesForStatusCode(lister, fwk.Unschedulable)
	if err != nil {
		return nil, fwk.AsStatus(err)
	}

	incomingPrio := priorityOf(pod)
	schedName := c.engine.Config().SchedulerName
	var bestNode string
	var bestVictims []queue.Victim
	var bestByUID map[string]*v1.Pod
	for _, ni := range candidates {
		node := ni.Node()
		if node == nil || !c.poolSelector.Matches(labels.Set(node.Labels)) {
			continue // pool nodes only
		}
		deficit := clampNonNeg(resource.Sub(st.req, nodeAvailable(ni)))
		if deficit.IsEmpty() {
			continue // node already has room on the needed dimensions
		}
		victims, byUID := c.eligibleVictims(tree, ni, incomingPrio, schedName, st.leaf)
		victims = c.filterPDBProtected(victims, byUID)
		selected := queue.SelectVictims(deficit, victims)
		if len(selected) == 0 {
			continue // cannot free enough here without breaching a guarantee
		}
		if bestNode == "" || len(selected) < len(bestVictims) {
			bestNode, bestVictims, bestByUID = node.Name, selected, byUID
		}
	}
	if bestNode == "" {
		return nil, fwk.NewStatus(fwk.Unschedulable, "ad-scheduler: no node-level preemption candidate")
	}

	cs := c.handle.ClientSet()
	rec := c.handle.EventRecorder()
	for _, v := range bestVictims {
		p := bestByUID[v.UID]
		if p == nil {
			continue
		}
		if err := cs.CoreV1().Pods(p.Namespace).Delete(ctx, p.Name, metav1.DeleteOptions{}); err != nil {
			klog.ErrorS(err, "ad-scheduler: node-preemption eviction failed", "victim", p.Namespace+"/"+p.Name)
			continue
		}
		c.engine.IncReclaimEvictions()
		klog.InfoS("ad-scheduler: node-level preemption",
			"victim", p.Namespace+"/"+p.Name, "node", bestNode, "for", pod.Namespace+"/"+pod.Name)
		if rec != nil {
			rec.Eventf(p, pod, v1.EventTypeNormal, "Preempted", "Preempting",
				"Preempted by %s/%s which needed room on node %s", pod.Namespace, pod.Name, bestNode)
		}
	}
	return &fwk.PostFilterResult{NominatingInfo: &fwk.NominatingInfo{
		NominatedNodeName: bestNode, NominatingMode: fwk.ModeOverride,
	}}, fwk.NewStatus(fwk.Success)
}

// eligibleVictims collects preemptable pods on one node, grouped by their queue
// and bounded by each donor queue's reclaimable budget (used − guaranteed), so
// eviction never drops a donor below its guarantee. Pods at or above the incoming
// pod's priority are spared.
func (c *Capacity) eligibleVictims(tree queueTree, ni fwk.NodeInfo, incomingPrio int32, schedName, askerLeaf string) ([]queue.Victim, map[string]*v1.Pod) {
	// Fence (decision q3): the incoming pod may only preempt within the nearest
	// fence enclosing its leaf — by default its top-level (namespace) subtree — so
	// node-level preemption never evicts across a tenant boundary.
	boundary, _ := tree.FenceBoundary(askerLeaf)
	byLeaf := map[string][]*v1.Pod{}
	for _, pi := range ni.GetPods() {
		p := pi.GetPod()
		if p == nil || p.Spec.SchedulerName != schedName || p.DeletionTimestamp != nil {
			continue
		}
		if p.Annotations[util.PodGroupAnnotation] != "" {
			continue // spare gang members: evicting one breaks the gang's all-or-nothing
		}
		if priorityOf(p) >= incomingPrio {
			continue // spare same-or-higher priority work
		}
		leaf, err := placement.Resolve(tree, util.PlacementKey(p))
		if err != nil {
			continue
		}
		if !queue.PathWithinSubtree(leaf, boundary) {
			continue // fenced off from the incoming pod's queue
		}
		byLeaf[leaf] = append(byLeaf[leaf], p)
	}
	var victims []queue.Victim
	byUID := map[string]*v1.Pod{}
	for leaf, pods := range byLeaf {
		used, guar, ok := tree.QueueUsedGuaranteed(leaf)
		if !ok {
			continue
		}
		budget := reclaimableBudget(used, guar)
		if budget.IsEmpty() {
			continue // donor at/below its guaranteed floor: protected
		}
		sort.Slice(pods, func(i, j int) bool { return priorityOf(pods[i]) < priorityOf(pods[j]) })
		evicted := resource.Resource{}
		for _, p := range pods {
			req := resource.EffectiveRequest(p)
			cand := resource.Add(evicted, req)
			if !resource.FitIn(cand, budget) {
				continue // evicting this would breach the donor's guarantee
			}
			evicted = cand
			victims = append(victims, queue.Victim{UID: string(p.UID), Priority: priorityOf(p), Request: req, Preemptable: true})
			byUID[string(p.UID)] = p
		}
	}
	return victims, byUID
}

// filterPDBProtected drops victims whose eviction would breach a
// PodDisruptionBudget. It spends each PDB's remaining disruption budget on the
// lowest-priority candidates first, so a PDB that allows N disruptions yields at
// most N victims. A pod covered by any exhausted PDB is spared.
func (c *Capacity) filterPDBProtected(victims []queue.Victim, byUID map[string]*v1.Pod) []queue.Victim {
	if c.pdbLister == nil || len(victims) == 0 {
		return victims
	}
	// Spend budget cheapest-first (lowest priority) for a stable, minimal outcome.
	sort.SliceStable(victims, func(i, j int) bool { return victims[i].Priority < victims[j].Priority })
	remaining := map[string]int32{} // namespace/name -> disruptions still allowed
	var kept []queue.Victim
	for _, v := range victims {
		p := byUID[v.UID]
		if p == nil {
			continue
		}
		pdbs := c.matchingPDBs(p)
		blocked := false
		for _, pdb := range pdbs {
			key := pdb.Namespace + "/" + pdb.Name
			if _, seen := remaining[key]; !seen {
				remaining[key] = pdb.Status.DisruptionsAllowed
			}
			if remaining[key] <= 0 {
				blocked = true
				break
			}
		}
		if blocked {
			continue // a covering PDB has no disruption budget left: spare this pod
		}
		for _, pdb := range pdbs {
			remaining[pdb.Namespace+"/"+pdb.Name]--
		}
		kept = append(kept, v)
	}
	return kept
}

// matchingPDBs returns the PodDisruptionBudgets in the pod's namespace whose
// selector matches it.
func (c *Capacity) matchingPDBs(pod *v1.Pod) []*policyv1.PodDisruptionBudget {
	all, err := c.pdbLister.PodDisruptionBudgets(pod.Namespace).List(labels.Everything())
	if err != nil {
		return nil
	}
	var out []*policyv1.PodDisruptionBudget
	for _, pdb := range all {
		if pdb.Spec.Selector == nil {
			continue
		}
		sel, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
		if err != nil {
			continue
		}
		if sel.Matches(labels.Set(pod.Labels)) {
			out = append(out, pdb)
		}
	}
	return out
}

// queueTree is the subset of the tree that placement + eligibility need (keeps
// the plugin decoupled from the concrete QueueManager for testability).
type queueTree = *queue.QueueManager

func priorityOf(p *v1.Pod) int32 {
	if p.Spec.Priority != nil {
		return *p.Spec.Priority
	}
	return 0
}

// unschedulableFor reports how long the pod has been unschedulable, read from the
// PodScheduled=False condition (LastTransitionTime is stamped when it first went
// unschedulable and is stable across retries). Zero if not yet marked — i.e. it
// just entered scheduling, so preemption should wait out the delay.
func unschedulableFor(pod *v1.Pod) time.Duration {
	for i := range pod.Status.Conditions {
		c := &pod.Status.Conditions[i]
		if c.Type == v1.PodScheduled && c.Status == v1.ConditionFalse {
			return time.Since(c.LastTransitionTime.Time)
		}
	}
	return 0
}

// nodeAvailable is the node's Allocatable minus what is already requested on it,
// clamped so no dimension is negative, in the engine's unit convention.
func nodeAvailable(ni fwk.NodeInfo) resource.Resource {
	return clampNonNeg(resource.Sub(fwkResource(ni.GetAllocatable()), fwkResource(ni.GetRequested())))
}

// fwkResource converts a framework Resource to the engine's Resource vector (CPU
// in millicores, memory/ephemeral-storage/scalars as their integer value).
func fwkResource(r fwk.Resource) resource.Resource {
	out := resource.Resource{}
	if v := r.GetMilliCPU(); v != 0 {
		out["cpu"] = v
	}
	if v := r.GetMemory(); v != 0 {
		out["memory"] = v
	}
	if v := r.GetEphemeralStorage(); v != 0 {
		out["ephemeral-storage"] = v
	}
	for name, v := range r.GetScalarResources() {
		if v != 0 {
			out[string(name)] = v
		}
	}
	return out
}

// reclaimableBudget is max(0, used − guaranteed) per dimension.
func reclaimableBudget(used, guaranteed resource.Resource) resource.Resource {
	out := resource.Resource{}
	for d, u := range used {
		if free := u - guaranteed[d]; free > 0 {
			out[d] = free
		}
	}
	return out
}

func clampNonNeg(r resource.Resource) resource.Resource {
	out := resource.Resource{}
	for d, v := range r {
		if v > 0 {
			out[d] = v
		}
	}
	return out
}
