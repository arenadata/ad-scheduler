package e2e

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const gangLeafNS = "team-d"

// gangLeafTree (team-d): two leaves, one per ServiceAccount, each with room for
// the gang:
//
//	main  (max cpu 1, SA spark)
//	other (max cpu 1, SA batch)
//
// A gang whose members route to DIFFERENT leaves (member A via SA spark → main,
// member B via SA batch → other) violates one-leaf-per-gang: the engine binds the
// gang to whichever member is admitted first and rejects the other, so minMember
// is never met and the gang cannot assemble. The positive control is the M4 gang
// spec (g-ok), whose members share one SA (→ one leaf) and DO assemble.
var _ = Describe("ad-scheduler M4 one leaf per gang", func() {
	ctx := context.Background()

	BeforeEach(func() {
		ensureNamespace(ctx, gangLeafNS)
		ensureServiceAccount(ctx, gangLeafNS, "spark")
		ensureServiceAccount(ctx, gangLeafNS, "batch")
		ensureQueueRaw(ctx, gangLeafNS, "main", map[string]any{
			"serviceAccounts": []any{"spark"}, "max": map[string]any{"cpu": "1"},
		})
		ensureQueueRaw(ctx, gangLeafNS, "other", map[string]any{
			"serviceAccounts": []any{"batch"}, "max": map[string]any{"cpu": "1"},
		})
		cleanupNS(ctx, gangLeafNS)
	})
	AfterEach(func() {
		deletePodGroup(ctx, gangLeafNS, "g-split")
		cleanupNS(ctx, gangLeafNS)
	})

	It("gates a gang whose members route to different leaves (heterogeneous SA)", func() {
		createPodGroup(ctx, gangLeafNS, "g-split", 2, "200m")
		// member A -> leaf main (SA spark); member B -> leaf other (SA batch).
		makeGangPodNS(ctx, gangLeafNS, "g-split-a", "spark", "100m", "g-split")
		makeGangPodNS(ctx, gangLeafNS, "g-split-b", "batch", "100m", "g-split")

		// The gang binds to whichever member is admitted first; the other routes to
		// a different leaf and is rejected, so minMember=2 is never met. Neither
		// member should ever be placed.
		Consistently(func() bool {
			return podNode(ctx, gangLeafNS, "g-split-a") == "" && podNode(ctx, gangLeafNS, "g-split-b") == ""
		}, 25*time.Second, 2*time.Second).Should(BeTrue(),
			"a gang split across two leaves must not assemble — no member should schedule")
	})
})

// makeGangPodNS creates a gang-member pod (carrying the pod-group annotation) for
// our scheduler in an arbitrary namespace.
func makeGangPodNS(ctx context.Context, ns, name, sa, cpu, gang string) *corev1.Pod {
	q := apiresource.MustParse(cpu)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: ns,
			Labels:      map[string]string{"e2e": "true"},
			Annotations: map[string]string{podGroupAnnotation: gang},
		},
		Spec: corev1.PodSpec{
			SchedulerName:                 schedulerName,
			ServiceAccountName:            sa,
			TerminationGracePeriodSeconds: new(int64(1)),
			Containers: []corev1.Container{{
				Name:      "c",
				Image:     "registry.k8s.io/pause:3.10",
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: q}, Limits: corev1.ResourceList{corev1.ResourceCPU: q}},
			}},
		},
	}
	created, err := k8s.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred())
	return created
}
