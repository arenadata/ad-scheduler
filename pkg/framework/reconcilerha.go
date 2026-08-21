package framework

import (
	"context"
	"os"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
)

// reconcilerLeaseName is the Lease coordinating the mutating reconciler loop
// (reclaim eviction, queue/gang status, gang Hard-timeout) across replicas.
const reconcilerLeaseName = "ad-scheduler-reconciler"

// startReconcilerLE runs the reclaim loop under its own leader election so the
// mutating reconciler is highly available yet single-writer (decision q30): with
// more than one scheduler replica, exactly one runs reclaim/status/timeout at a
// time — a failure-domain independent of the scheduler's own leader election, and
// on a single replica it simply always leads. Read-only tree maintenance (the
// rebuild loop) keeps running in every replica. Idempotent (runs once).
func (c *Coordinator) startReconcilerLE() {
	c.leOnce.Do(func() {
		cs, _ := c.client.Load().(kubernetes.Interface)
		if cs == nil || c.ctx == nil {
			return
		}
		id, err := os.Hostname()
		if err != nil || id == "" {
			id = "ad-scheduler"
		}
		ns := os.Getenv("POD_NAMESPACE")
		if ns == "" {
			ns = "ad-system"
		}
		lock := &resourcelock.LeaseLock{
			LeaseMeta:  metav1.ObjectMeta{Name: reconcilerLeaseName, Namespace: ns},
			Client:     cs.CoordinationV1(),
			LockConfig: resourcelock.ResourceLockConfig{Identity: id},
		}
		go leaderelection.RunOrDie(c.ctx, leaderelection.LeaderElectionConfig{
			Lock:            lock,
			ReleaseOnCancel: true,
			LeaseDuration:   15 * time.Second,
			RenewDeadline:   10 * time.Second,
			RetryPeriod:     2 * time.Second,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(leCtx context.Context) {
					klog.InfoS("ad-scheduler: reconciler leading — running reclaim/status/timeout loops", "id", id)
					c.reclaimLoop(leCtx) // blocks until leadership is lost (leCtx cancelled)
				},
				OnStoppedLeading: func() {
					klog.InfoS("ad-scheduler: reconciler lost leadership — mutating loops paused", "id", id)
				},
			},
		})
	})
}
