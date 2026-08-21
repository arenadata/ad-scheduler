package framework

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/arenadata/ad-scheduler/pkg/queue"
)

// countGuardName is the scheduler-managed count-only ResourceQuota (a DoS
// backstop on pod count), distinct from the admin-owned resource envelope (q27).
const countGuardName = "ad-scheduler-count-guard"

// syncCountQuotas ensures a count-only ResourceQuota exists in every participating
// namespace when AD_AUTO_COUNT_QUOTA is on (off by default — the cluster-admin
// owns quotas; this is an opt-in convenience). It only ever creates its own named
// quota and never touches others, so the admin's envelope RQ is untouched.
// Create-if-absent (AlreadyExists ignored), so it is safe to run repeatedly and
// from the reconciler leader only.
func (c *Coordinator) syncCountQuotas(ctx context.Context) {
	cfg := c.engine.Config()
	if !cfg.AutoCountQuota {
		return
	}
	cs, _ := c.client.Load().(kubernetes.Interface)
	if cs == nil {
		return
	}
	tree := c.engine.Tree()
	if tree == nil {
		return
	}
	seen := map[string]bool{}
	for _, q := range tree.All() {
		ns := queue.NamespaceOf(q.Path())
		if ns == "" || seen[ns] {
			continue
		}
		seen[ns] = true
		rq := &corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{Name: countGuardName, Namespace: ns},
			Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{
				"count/pods": *apiresource.NewQuantity(cfg.CountPodsLimit, apiresource.DecimalSI),
			}},
		}
		if _, err := cs.CoreV1().ResourceQuotas(ns).Create(ctx, rq, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			klog.V(4).ErrorS(err, "ad-scheduler: count-guard quota create", "namespace", ns)
		}
	}
}
