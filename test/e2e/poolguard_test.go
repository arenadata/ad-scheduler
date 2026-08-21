package e2e

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const dedicatedTaintKey = "scheduler.arenadata.io/dedicated"

// The pool-guard VAP (deploy/admission.yaml, ad-scheduler-pool-guard) makes the
// dedicated pool single-scheduler: a pod may pin the pool nodeSelector or tolerate
// the dedicated taint ONLY if it targets ad-scheduler. A foreign pod (default
// scheduler) that does so is rejected at admission, so it cannot sneak onto the
// pool outside our accounting. Our own pods (which the MAP stamps) always pass.
var _ = Describe("ad-scheduler M6 pool-guard (single-scheduler dedicated pool)", func() {
	ctx := context.Background()
	AfterEach(func() { cleanupPods(ctx) })

	It("denies a non-ad-scheduler pod that pins the pool nodeSelector", func() {
		_, err := tryCreatePod(ctx, "guard-foreign-selector", func(p *corev1.Pod) {
			p.Spec.SchedulerName = "default-scheduler"
			p.Spec.NodeSelector = map[string]string{poolLabel: poolValue}
		})
		Expect(err).To(HaveOccurred(), "a foreign pod pinning the pool nodeSelector must be denied")
		Expect(err.Error()).To(ContainSubstring("must set spec.schedulerName"), "denied by the pool-guard VAP")
	})

	It("denies a non-ad-scheduler pod that tolerates the dedicated taint", func() {
		_, err := tryCreatePod(ctx, "guard-foreign-toleration", func(p *corev1.Pod) {
			p.Spec.SchedulerName = "default-scheduler"
			p.Spec.Tolerations = []corev1.Toleration{{Key: dedicatedTaintKey, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}}
		})
		Expect(err).To(HaveOccurred(), "a foreign pod tolerating the dedicated taint must be denied")
	})

	It("admits an ad-scheduler pod carrying the pool binding", func() {
		_, err := tryCreatePod(ctx, "guard-our-pod", func(p *corev1.Pod) {
			p.Spec.SchedulerName = schedulerName
			p.Spec.ServiceAccountName = tenantSA
			p.Spec.NodeSelector = map[string]string{poolLabel: poolValue}
			p.Spec.Tolerations = []corev1.Toleration{{Key: dedicatedTaintKey, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}}
		})
		Expect(err).NotTo(HaveOccurred(), "our own pod (schedulerName ad-scheduler) must pass the guard")
	})

	// named-SA guard (G1-G2): a pod scheduled by ad-scheduler must use a named SA.
	It("denies an ad-scheduler pod that uses the default ServiceAccount", func() {
		_, err := tryCreatePod(ctx, "guard-default-sa", func(p *corev1.Pod) {
			p.Spec.SchedulerName = schedulerName
			p.Spec.ServiceAccountName = "default"
		})
		Expect(err).To(HaveOccurred(), "a default-SA pod targeting ad-scheduler must be denied")
		Expect(err.Error()).To(ContainSubstring("named ServiceAccount"), "denied by the named-SA guard VAP")
	})
})

// tryCreatePod builds a minimal tenant pod, applies mutate, and returns the raw
// Create result and error (unlike makePod it does not assert success — the caller
// checks whether admission allowed or denied the pod).
func tryCreatePod(ctx context.Context, name string, mutate func(*corev1.Pod)) (*corev1.Pod, error) {
	q := apiresource.MustParse("50m")
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: tenantNS, Labels: map[string]string{"e2e": "true"}},
		Spec: corev1.PodSpec{
			SchedulerName:                 schedulerName,
			TerminationGracePeriodSeconds: new(int64(1)),
			Containers: []corev1.Container{{
				Name:      "c",
				Image:     "registry.k8s.io/pause:3.10",
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: q}, Limits: corev1.ResourceList{corev1.ResourceCPU: q}},
			}},
		},
	}
	mutate(pod)
	return k8s.CoreV1().Pods(tenantNS).Create(ctx, pod, metav1.CreateOptions{})
}
