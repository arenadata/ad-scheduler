package framework

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// evictStalePending is an opt-in DoS/stuck-pod backstop (AD_PENDING_TTL): it
// deletes our pods that have stayed unschedulable longer than the TTL, freeing
// the queue demand they hold and stopping an unschedulable flood from lingering.
// Off by default (TTL 0) — evicting Pending pods is an opinionated policy, so the
// admin opts in. Runs on the reconciler leader only.
func (c *Coordinator) evictStalePending(ctx context.Context) {
	ttl := c.engine.Config().PendingTTL
	if ttl <= 0 {
		return
	}
	cs, _ := c.client.Load().(kubernetes.Interface)
	if cs == nil {
		return
	}
	name := c.engine.Config().SchedulerName
	now := time.Now()
	for _, o := range c.podInf.GetStore().List() {
		p, ok := o.(*corev1.Pod)
		if !ok || p.Spec.SchedulerName != name || p.Spec.NodeName != "" || p.DeletionTimestamp != nil {
			continue
		}
		var pendingFor time.Duration
		for i := range p.Status.Conditions {
			cd := &p.Status.Conditions[i]
			if cd.Type == corev1.PodScheduled && cd.Status == corev1.ConditionFalse {
				pendingFor = now.Sub(cd.LastTransitionTime.Time)
			}
		}
		if pendingFor <= ttl {
			continue
		}
		if err := cs.CoreV1().Pods(p.Namespace).Delete(ctx, p.Name, metav1.DeleteOptions{}); err != nil {
			klog.V(4).ErrorS(err, "ad-scheduler: pending-TTL evict", "pod", p.Namespace+"/"+p.Name)
			continue
		}
		klog.InfoS("ad-scheduler: evicted stale-pending pod", "pod", p.Namespace+"/"+p.Name, "pendingFor", pendingFor.Truncate(time.Second))
		c.emitEvent(p, nil, corev1.EventTypeWarning, "PendingTTLExceeded", "DoSGuard",
			"evicted after staying unschedulable longer than %s", ttl)
	}
}
