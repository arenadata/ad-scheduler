package e2e

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// These specs require deploy/admission.yaml (the ValidatingAdmissionPolicy) to
// be applied to the cluster — the in-tree admission gate (decision q22).
var _ = Describe("ad-scheduler M6 admission policy (VAP)", func() {
	ctx := context.Background()

	createQueueExpectDeny := func(name string, spec map[string]any) error {
		q := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "scheduling.arenadata.io/v1alpha1",
			"kind":       "Queue",
			"metadata":   map[string]any{"name": name, "namespace": tenantNS},
			"spec":       spec,
		}}
		_, err := dyn.Resource(queueGVR).Namespace(tenantNS).Create(ctx, q, metav1.CreateOptions{})
		if err == nil {
			_ = dyn.Resource(queueGVR).Namespace(tenantNS).Delete(ctx, name, metav1.DeleteOptions{})
		}
		return err
	}

	It("rejects a self-parent Queue", func() {
		err := createQueueExpectDeny("vap-selfp", map[string]any{"parent": "vap-selfp"})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("own parent"))
	})

	It("rejects mapping the default ServiceAccount to a queue (q21)", func() {
		err := createQueueExpectDeny("vap-defsa", map[string]any{"serviceAccounts": []any{"default"}})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("default ServiceAccount"))
	})

	It("admits a valid Queue", func() {
		err := createQueueExpectDeny("vap-ok", map[string]any{
			"serviceAccounts": []any{"vap-ok-sa"},
			"max":             map[string]any{"cpu": "100m"},
		})
		Expect(err).NotTo(HaveOccurred())
	})

	// The MutatingAdmissionPolicy (G7) stamps the pool nodeSelector + toleration
	// onto any pod that targets this scheduler (schedulerName: ad-scheduler) — no
	// separate opt-in label. A submitter chooses the scheduler and gets the pool
	// placement for free.
	It("injects pool placement onto a pod targeting ad-scheduler (MAP)", func() {
		defer func() {
			_ = k8s.CoreV1().Pods(tenantNS).Delete(ctx, "map-inject",
				metav1.DeleteOptions{GracePeriodSeconds: new(int64(0))})
		}()
		// schedulerName: ad-scheduler is set by makePod; strip the pool placement
		// so we can observe MAP adding it back.
		makePod(ctx, "map-inject", tenantSA, "50m", withoutPoolPlacement())

		Eventually(func() map[string]string {
			g, err := k8s.CoreV1().Pods(tenantNS).Get(ctx, "map-inject", metav1.GetOptions{})
			if err != nil {
				return nil
			}
			return g.Spec.NodeSelector
		}, 10*time.Second, time.Second).Should(HaveKeyWithValue(poolLabel, poolValue), "MAP should stamp the pool nodeSelector")

		Eventually(func() string { return podNodeName(ctx, "map-inject") }, 60*time.Second, time.Second).
			ShouldNot(BeEmpty(), "the pod targeting ad-scheduler should schedule on the pool")
	})
})
