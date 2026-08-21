package e2e

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ad-scheduler M4 gang scheduling", func() {
	ctx := context.Background()

	BeforeEach(func() { cleanupPods(ctx) })
	AfterEach(func() {
		cleanupPods(ctx)
		deletePodGroup(ctx, tenantNS, "g-ok")
		deletePodGroup(ctx, tenantNS, "g-partial")
		// give the coordinator a moment to release the gang reservation before
		// the next spec measures queue headroom.
		time.Sleep(2 * time.Second)
	})

	It("admits a complete gang all-or-nothing (3/3 members schedule together)", func() {
		createPodGroup(ctx, tenantNS, "g-ok", 3, "300m") // 3 x 100m fits queue max 500m
		for _, n := range []string{"g-ok-1", "g-ok-2", "g-ok-3"} {
			makePod(ctx, n, tenantSA, "100m", withGang("g-ok"))
		}
		// the Permit barrier releases all three together once the third is assumed.
		Eventually(func() int { return scheduledCount(ctx, "g-ok-1", "g-ok-2", "g-ok-3") },
			90*time.Second, 2*time.Second).
			Should(Equal(3), "a complete gang should schedule all members")
	})

	It("holds an incomplete gang (2 of minMember=3 stay Pending)", func() {
		createPodGroup(ctx, tenantNS, "g-partial", 3, "300m")
		makePod(ctx, "g-partial-1", tenantSA, "100m", withGang("g-partial"))
		makePod(ctx, "g-partial-2", tenantSA, "100m", withGang("g-partial"))
		// only 2 of 3 members exist: the Permit barrier is never met, so neither
		// binds (all-or-nothing) — they stay Pending at the barrier.
		Consistently(func() int { return scheduledCount(ctx, "g-partial-1", "g-partial-2") },
			20*time.Second, 3*time.Second).
			Should(Equal(0), "an incomplete gang must not place any member")
	})
})
