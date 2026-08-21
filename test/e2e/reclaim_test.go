package e2e

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const reclaimNS = "team-b"

// reclaimTree:
//
//	team-b: Queue "team" (max cpu 500m)
//	  ├── main  (guaranteed cpu 300m, SA spark)
//	  └── batch (SA batch)  <- borrows team's headroom beyond its guarantee (0)
var _ = Describe("ad-scheduler M5 reclaim (queue preemption)", func() {
	ctx := context.Background()

	BeforeEach(func() {
		ensureNamespace(ctx, reclaimNS)
		ensureServiceAccount(ctx, reclaimNS, "spark")
		ensureServiceAccount(ctx, reclaimNS, "batch")
		ensureQueueRaw(ctx, reclaimNS, "team", map[string]any{"max": map[string]any{"cpu": "500m"}})
		ensureQueueRaw(ctx, reclaimNS, "main", map[string]any{
			"parent": "team", "serviceAccounts": []any{"spark"},
			"guaranteed": map[string]any{"cpu": "300m"},
		})
		ensureQueueRaw(ctx, reclaimNS, "batch", map[string]any{
			"parent": "team", "serviceAccounts": []any{"batch"},
		})
		cleanupNS(ctx, reclaimNS)
	})
	AfterEach(func() { cleanupNS(ctx, reclaimNS) })

	It("reclaims a borrowing sibling's capacity for a below-guaranteed queue", func() {
		// batch borrows the whole 500m headroom of team (its guarantee is 0).
		makePodNS(ctx, reclaimNS, "batch-hog", "batch", "300m")
		Eventually(func() string { return podNode(ctx, reclaimNS, "batch-hog") }, 60*time.Second, time.Second).
			ShouldNot(BeEmpty(), "batch-hog should schedule first")

		// main is guaranteed 300m but cannot fit (team is full). The reclaim
		// controller must evict batch-hog and let main in.
		makePodNS(ctx, reclaimNS, "main-app", "spark", "300m")
		Eventually(func() string { return podNode(ctx, reclaimNS, "main-app") }, 90*time.Second, 2*time.Second).
			ShouldNot(BeEmpty(), "main-app should schedule after reclaim evicts the borrower")

		// batch-hog was the reclaim victim: it should be gone.
		Eventually(func() bool {
			_, err := k8s.CoreV1().Pods(reclaimNS).Get(ctx, "batch-hog", metav1.GetOptions{})
			return apierrors.IsNotFound(err)
		}, 30*time.Second, time.Second).Should(BeTrue(), "batch-hog should have been evicted")
	})
})

// ensureQueueRaw creates a Queue with an arbitrary spec (idempotent).
func ensureQueueRaw(ctx context.Context, ns, name string, spec map[string]any) {
	q := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "scheduling.arenadata.io/v1alpha1",
		"kind":       "Queue",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"spec":       spec,
	}}
	_, err := dyn.Resource(queueGVR).Namespace(ns).Create(ctx, q, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return
	}
	Expect(err).NotTo(HaveOccurred())
}

// makePodNS creates a scheduler pod in an arbitrary namespace.
func makePodNS(ctx context.Context, ns, name, sa, cpu string) *corev1.Pod {
	q := apiresource.MustParse(cpu)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: map[string]string{"e2e": "true"}},
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

func podNode(ctx context.Context, ns, name string) string {
	p, err := k8s.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return ""
	}
	return p.Spec.NodeName
}

func cleanupNS(ctx context.Context, ns string) {
	_ = k8s.CoreV1().Pods(ns).DeleteCollection(ctx,
		metav1.DeleteOptions{GracePeriodSeconds: new(int64(0))},
		metav1.ListOptions{LabelSelector: "e2e=true"})
	Eventually(func() int {
		l, err := k8s.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: "e2e=true"})
		if err != nil {
			return -1
		}
		return len(l.Items)
	}, 60*time.Second, time.Second).Should(Equal(0))
	time.Sleep(2 * time.Second) // let releases settle
}
