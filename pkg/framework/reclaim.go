package framework

import (
	"context"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/arenadata/ad-scheduler/pkg/queue"
	"github.com/arenadata/ad-scheduler/pkg/queue/placement"
	"github.com/arenadata/ad-scheduler/pkg/resource"
	"github.com/arenadata/ad-scheduler/pkg/util"
)

// reclaimInterval is how often the reclaim controller scans for starved pods.
// Reclaim is queue-level (a below-guaranteed queue reclaims capacity borrowed by
// over-guaranteed siblings), which does not map onto the framework's node-level
// PostFilter (that never runs for a PreFilter queue-over-max reject), so it lives
// in its own loop (decision q3, like YuniKorn/Volcano).
const reclaimInterval = 5 * time.Second

func (c *Coordinator) reclaimLoop(ctx context.Context) {
	t := time.NewTicker(reclaimInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.syncQueueStatuses(ctx, c.engine.Tree()) // status + drain finalizer
			c.syncGangStatuses(ctx)                   // persist gang reservations (restart recovery)
			c.checkGangTimeouts(ctx)                  // Hard/members-ready timeout: fail stuck gangs
			c.syncPodGroupPhases(ctx)                 // PodGroup lifecycle phase (Pending→Running→…)
			c.syncCountQuotas(ctx)                    // opt-in count-only DoS-guard quota
			c.evictStalePending(ctx)                  // opt-in pending-TTL evictor
			c.syncProvisioningRequests(ctx)           // opt-in CA gang-demand publisher
			c.reclaimOnce(ctx)
		}
	}
}

// reclaimOnce performs at most one reclaim: it finds the most-deserving starved
// pod (a pod whose queue is below its guaranteed but cannot fit because siblings
// borrowed the headroom) and evicts the minimal set of over-guaranteed victims
// that frees enough for it, then re-queues gated pods.
func (c *Coordinator) reclaimOnce(ctx context.Context) {
	cs, _ := c.client.Load().(kubernetes.Interface)
	if cs == nil {
		return
	}
	tree := c.engine.Tree()
	if tree == nil {
		return
	}
	name := c.engine.Config().SchedulerName

	var pending, bound []*corev1.Pod
	for _, o := range c.podInf.GetStore().List() {
		p, ok := o.(*corev1.Pod)
		if !ok || p.Spec.SchedulerName != name || p.DeletionTimestamp != nil {
			continue
		}
		if p.Annotations[util.PodGroupAnnotation] != "" {
			continue // gang-aware preemption is out of scope here (deferred)
		}
		if p.Spec.NodeName == "" {
			pending = append(pending, p)
		} else {
			bound = append(bound, p)
		}
	}
	if len(pending) == 0 {
		return
	}
	// Serve the most-deserving starved pod first: priority desc, then oldest.
	sort.Slice(pending, func(i, j int) bool {
		if pi, pj := podPriority(pending[i]), podPriority(pending[j]); pi != pj {
			return pi > pj
		}
		return pending[i].CreationTimestamp.Before(&pending[j].CreationTimestamp)
	})

	for _, p := range pending {
		leaf, err := placement.Resolve(tree, util.PlacementKey(p))
		if err != nil {
			continue
		}
		need := resource.EffectiveRequest(p)
		if ok, _ := tree.CanFit(leaf, need); ok {
			continue // already fits; the scheduler will place it without reclaim
		}
		if !entitledToReclaim(tree, leaf, need) {
			continue // queue is not below its guaranteed: no reclaim right
		}
		victims, byUID := c.buildVictims(tree, bound, leaf, need, podPriority(p))
		selected := queue.SelectVictims(need, victims)
		if len(selected) == 0 {
			continue
		}
		c.evictVictims(ctx, cs, selected, byUID, p)
		c.activatePending()
		return // one reclaim per tick; terminating victims are excluded next tick
	}
}

// entitledToReclaim reports whether placing need at leaf keeps it within its own
// guaranteed floor — i.e., the queue is claiming capacity it is guaranteed but
// cannot get. Only such queues may reclaim.
func entitledToReclaim(tree *queue.QueueManager, leaf string, need resource.Resource) bool {
	used, guar, ok := tree.QueueUsedGuaranteed(leaf)
	if !ok || guar.IsEmpty() {
		return false
	}
	for d, n := range need {
		if used[d]+n > guar[d] {
			return false
		}
	}
	return true
}

// buildVictims collects preemptable pods from queues that are over their
// guaranteed floor (donors), lowest-priority-first and only up to each donor's
// reclaimable budget (used − guaranteed) so eviction never drops a donor below
// its guarantee (the guaranteed-protection invariant). Pods at or above the
// starved pod's priority are spared.
func (c *Coordinator) buildVictims(tree *queue.QueueManager, bound []*corev1.Pod, starvedLeaf string, need resource.Resource, starvedPrio int32) ([]queue.Victim, map[string]*corev1.Pod) {
	// Fence (decision q3): a preemptor at starvedLeaf may only reclaim within the
	// nearest fence enclosing it — by default its top-level (namespace) subtree,
	// so reclaim never crosses a tenant boundary. Donors outside the fence are off
	// limits regardless of how far over guaranteed they run.
	boundary, _ := tree.FenceBoundary(starvedLeaf)
	byLeaf := map[string][]*corev1.Pod{}
	for _, p := range bound {
		leaf, err := placement.Resolve(tree, util.PlacementKey(p))
		if err != nil || leaf == starvedLeaf {
			continue
		}
		if !queue.PathWithinSubtree(leaf, boundary) {
			continue // fenced off from the starved queue
		}
		byLeaf[leaf] = append(byLeaf[leaf], p)
	}
	var victims []queue.Victim
	byUID := map[string]*corev1.Pod{}
	for leaf, pods := range byLeaf {
		used, guar, ok := tree.QueueUsedGuaranteed(leaf)
		if !ok {
			continue
		}
		budget := reclaimable(used, guar)
		if budget.IsEmpty() {
			continue // donor is at/below its guaranteed: protected
		}
		sort.Slice(pods, func(i, j int) bool { return podPriority(pods[i]) < podPriority(pods[j]) })
		evicted := resource.Resource{}
		for _, p := range pods {
			if podPriority(p) > starvedPrio {
				continue // spare strictly-higher-priority work (victim priority ≤ ask)
			}
			req := resource.EffectiveRequest(p)
			cand := resource.Add(evicted, req)
			if !resource.FitIn(cand, budget) {
				continue // evicting this would exceed the donor's reclaimable budget
			}
			evicted = cand
			victims = append(victims, queue.Victim{UID: string(p.UID), Priority: podPriority(p), Request: req, Preemptable: true})
			byUID[string(p.UID)] = p
		}
	}
	return victims, byUID
}

func (c *Coordinator) evictVictims(ctx context.Context, cs kubernetes.Interface, selected []queue.Victim, byUID map[string]*corev1.Pod, beneficiary *corev1.Pod) {
	for _, v := range selected {
		p := byUID[v.UID]
		if p == nil {
			continue
		}
		klog.InfoS("ad-scheduler: reclaim eviction",
			"victim", p.Namespace+"/"+p.Name, "freed", v.Request,
			"for", beneficiary.Namespace+"/"+beneficiary.Name)
		if err := cs.CoreV1().Pods(p.Namespace).Delete(ctx, p.Name, metav1.DeleteOptions{}); err != nil {
			klog.ErrorS(err, "ad-scheduler: reclaim eviction failed", "victim", p.Namespace+"/"+p.Name)
			continue
		}
		c.engine.IncReclaimEvictions()
		// eventbridge: surface the decision on both pods for kubectl describe.
		c.emitEvent(p, beneficiary, corev1.EventTypeNormal, "Preempted", "Reclaim",
			"Evicted to reclaim queue guarantee for %s/%s", beneficiary.Namespace, beneficiary.Name)
		c.emitEvent(beneficiary, p, corev1.EventTypeNormal, "Reclaimed", "Reclaim",
			"Reclaimed capacity by evicting borrower %s/%s", p.Namespace, p.Name)
	}
}

// reclaimable is max(0, used − guaranteed) per dimension — how much a donor queue
// can give back without breaching its guarantee.
func reclaimable(used, guaranteed resource.Resource) resource.Resource {
	out := resource.Resource{}
	for d, u := range used {
		if free := u - guaranteed[d]; free > 0 {
			out[d] = free
		}
	}
	return out
}

func podPriority(p *corev1.Pod) int32 {
	if p.Spec.Priority != nil {
		return *p.Spec.Priority
	}
	return 0
}
