package e2e

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("ad-scheduler M2 scheduling", func() {
	ctx := context.Background()

	BeforeEach(func() { cleanupPods(ctx) })
	AfterEach(func() { cleanupPods(ctx) })

	It("places a pod with a mapped ServiceAccount onto a pool node", func() {
		makePod(ctx, "p-mapped", tenantSA, "100m")
		Eventually(func() string { return podNodeName(ctx, "p-mapped") }, 60*time.Second, time.Second).
			ShouldNot(BeEmpty(), "mapped pod should be scheduled")

		node, err := k8s.CoreV1().Nodes().Get(ctx, podNodeName(ctx, "p-mapped"), metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(node.Labels).To(HaveKeyWithValue(poolLabel, poolValue), "must land on a pool node")
	})

	It("rejects a node outside the dedicated pool (off-pool)", func() {
		// Force the only candidate to be the control-plane node, which is NOT in
		// the pool, and tolerate its taint. The pod stays Pending: the MAP-stamped
		// pool nodeSelector excludes the off-pool node, and AdCapacity's off-pool
		// Filter is the in-scheduler backstop (unit-tested in TestFilterOffPool).
		makePod(ctx, "p-offpool", tenantSA, "100m", withNodeName("kind-control-plane"))
		Consistently(func() string { return podNodeName(ctx, "p-offpool") }, 15*time.Second, 2*time.Second).
			Should(BeEmpty(), "off-pool node must be rejected — pod stays Pending")
	})

	It("enforces queue max and releases it when a pod is deleted", func() {
		// queue main has cpu max 500m; two 300m pods cannot both fit.
		makePod(ctx, "cap-a", tenantSA, "300m")
		makePod(ctx, "cap-b", tenantSA, "300m")

		Eventually(func() int { return scheduledCount(ctx, "cap-a", "cap-b") }, 60*time.Second, time.Second).
			Should(Equal(1), "exactly one of the two over-cap pods should schedule")
		Consistently(func() int { return scheduledCount(ctx, "cap-a", "cap-b") }, 8*time.Second, 2*time.Second).
			Should(Equal(1), "the second pod must stay Pending while the queue is full")

		// delete whichever got scheduled; the other must now fit.
		scheduled, pending := "cap-a", "cap-b"
		if podNodeName(ctx, "cap-a") == "" {
			scheduled, pending = "cap-b", "cap-a"
		}
		Expect(k8s.CoreV1().Pods(tenantNS).Delete(ctx, scheduled,
			metav1.DeleteOptions{GracePeriodSeconds: new(int64(0))})).To(Succeed())
		// the pending pod requeues on the Pod/Delete event and retries on backoff
		// once the release propagates to the engine.
		Eventually(func() string { return podNodeName(ctx, pending) }, 90*time.Second, 2*time.Second).
			ShouldNot(BeEmpty(), "the pending pod should schedule once the queue frees up")
	})

	It("holds a pod whose ServiceAccount maps to no queue (fail-closed)", func() {
		// A named ServiceAccount that no Queue routes: placement fail-closes it to
		// Pending. (The `default` SA is instead rejected earlier, at admission, by
		// the named-SA guard VAP — see the admission-guard spec.)
		ensureServiceAccount(ctx, tenantNS, "guest")
		makePod(ctx, "p-nomap", "guest", "100m")
		Consistently(func() string { return podNodeName(ctx, "p-nomap") }, 15*time.Second, 2*time.Second).
			Should(BeEmpty(), "unmapped-SA pod must stay Pending (fail-closed)")
	})
})

// scheduledCount returns how many of the named pods have been assigned a node.
func scheduledCount(ctx context.Context, names ...string) int {
	n := 0
	for _, name := range names {
		if podNodeName(ctx, name) != "" {
			n++
		}
	}
	return n
}
