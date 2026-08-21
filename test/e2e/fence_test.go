package e2e

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const fenceNS = "team-c"

// fenceTree (team-c):
//
//	org (max cpu 500m)
//	  ├── protected (guaranteed cpu 300m, SA spark, preemption.policy=fence)
//	  └── batch     (SA batch)  <- borrows org's headroom beyond its guarantee (0)
//
// This is the M5 reclaim setup (team-b) with one change: an explicit
// preemption.policy=fence on the below-guaranteed leaf. The fence confines the
// preemptor to its own subtree, so it may NOT reclaim the borrowing sibling that
// sits outside the fence — the eviction the un-fenced team-b spec performs must
// NOT happen here. Together the two specs isolate the fence as the cause.
var _ = Describe("ad-scheduler M5 fence (preemption boundary)", func() {
	ctx := context.Background()

	BeforeEach(func() {
		ensureNamespace(ctx, fenceNS)
		ensureServiceAccount(ctx, fenceNS, "spark")
		ensureServiceAccount(ctx, fenceNS, "batch")
		ensureQueueRaw(ctx, fenceNS, "org", map[string]any{"max": map[string]any{"cpu": "500m"}})
		ensureQueueRaw(ctx, fenceNS, "protected", map[string]any{
			"parent": "org", "serviceAccounts": []any{"spark"},
			"guaranteed": map[string]any{"cpu": "300m"},
			"preemption": map[string]any{"policy": "fence"},
		})
		ensureQueueRaw(ctx, fenceNS, "batch", map[string]any{
			"parent": "org", "serviceAccounts": []any{"batch"},
		})
		cleanupNS(ctx, fenceNS)
	})
	AfterEach(func() { cleanupNS(ctx, fenceNS) })

	It("does not reclaim a borrower outside the preemptor's fence", func() {
		// batch borrows the whole 500m headroom of org (its guarantee is 0).
		makePodNS(ctx, fenceNS, "batch-hog", "batch", "300m")
		Eventually(func() string { return podNode(ctx, fenceNS, "batch-hog") }, 60*time.Second, time.Second).
			ShouldNot(BeEmpty(), "batch-hog should schedule first")

		// protected/main is below its 300m guarantee and cannot fit (org is full).
		// Without the fence this would reclaim batch-hog (see the team-b spec); with
		// the fence, batch sits outside protected's boundary and is off-limits.
		makePodNS(ctx, fenceNS, "main-app", "spark", "300m")

		// main-app must stay Pending — the fence blocks the only reclaim candidate.
		Consistently(func() string { return podNode(ctx, fenceNS, "main-app") }, 25*time.Second, 2*time.Second).
			Should(BeEmpty(), "fenced preemptor must not schedule by reclaiming across the fence")

		// and batch-hog must survive: it was never an eligible victim.
		_, err := k8s.CoreV1().Pods(fenceNS).Get(ctx, "batch-hog", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred(), "batch-hog must not be evicted — it is fenced off from the preemptor")
	})
})
