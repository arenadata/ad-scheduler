package framework

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// Node lifecycle reaction (decision q20). A pool node's capacity and the pods
// bound to it are only as good as the node itself: when a node is deleted its
// pods are orphaned and will be garbage-collected, but until then their queue
// bookings keep headroom pinned — so a queue can look full while its capacity is
// stranded on a dead node. releaseNodePods frees those bookings as soon as the
// node object disappears, letting the freed headroom be rescheduled immediately
// instead of after the ~5-minute pod GC. It is idempotent: the eventual real pod
// delete finds the pod already untracked and does nothing.
//
// NotReady / cordon are transient (the node may recover), so they are NOT
// released here — the in-tree NodeUnschedulable/taint filters already keep new
// pods off such nodes, and a genuine failure ends in a node delete handled above.
func (c *Coordinator) releaseNodePods(nodeName string) {
	name := c.engine.Config().SchedulerName
	n := 0
	for _, o := range c.podInf.GetStore().List() {
		p, ok := o.(*corev1.Pod)
		if !ok || p.Spec.NodeName != nodeName || p.Spec.SchedulerName != name {
			continue
		}
		c.onPodDelete(p) // idempotent release + untrack (activates gated pods)
		n++
	}
	if n > 0 {
		klog.V(2).InfoS("ad-scheduler: released bookings for pods on deleted node", "node", nodeName, "pods", n)
	}
}
