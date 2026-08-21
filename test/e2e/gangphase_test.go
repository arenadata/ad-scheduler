package e2e

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Gang lifecycle in team-a: the status controller phases (Pending→Scheduling→
// Running) and the membersReadyTimeoutSeconds materialization deadline.
var _ = Describe("ad-scheduler M4 gang lifecycle (phase + membersReady)", func() {
	ctx := context.Background()
	AfterEach(func() {
		deletePodGroup(ctx, tenantNS, "g-phase")
		deletePodGroup(ctx, tenantNS, "g-notready")
		cleanupPods(ctx)
	})

	It("reports Running once minMember members are placed", func() {
		createPodGroupFull(ctx, tenantNS, "g-phase", 2, "200m", 120, 0)
		makePod(ctx, "g-phase-1", tenantSA, "100m", withGang("g-phase"))
		makePod(ctx, "g-phase-2", tenantSA, "100m", withGang("g-phase"))
		// both members assemble and bind; the PodGroup status controller marks the
		// gang Running and counts the placed members.
		Eventually(func() string { return podGroupPhase(ctx, tenantNS, "g-phase") }, 60*time.Second, 2*time.Second).
			Should(Equal("Running"), "a gang with minMember placed must report phase Running")
		Expect(podGroupScheduled(ctx, tenantNS, "g-phase")).To(BeNumerically(">=", int64(2)),
			"status.scheduled must count the placed members")
	})

	It("fails a gang whose members do not all materialize (membersReadyTimeoutSeconds)", func() {
		// A short members-ready deadline with a long schedule deadline: the only
		// member is created, the second never is, so the failure can come only from
		// the membersReady timeout — well before the 300s schedule timeout.
		createPodGroupFull(ctx, tenantNS, "g-notready", 2, "200m", 300, 10)
		makePod(ctx, "g-notready-1", tenantSA, "100m", withGang("g-notready")) // only 1 of 2
		Eventually(func() string { return podGroupPhase(ctx, tenantNS, "g-notready") }, 60*time.Second, 2*time.Second).
			Should(Equal("Failed"), "a gang missing members past membersReadyTimeoutSeconds must fail early")
	})
})

// createPodGroupFull creates a PodGroup with explicit schedule and (optional,
// when > 0) members-ready timeouts.
func createPodGroupFull(ctx context.Context, ns, name string, minMember int32, minCPU string, scheduleTO, membersReadyTO int32) {
	spec := map[string]any{
		"minMember":              int64(minMember),
		"minResources":           map[string]any{"cpu": minCPU},
		"scheduleTimeoutSeconds": int64(scheduleTO),
	}
	if membersReadyTO > 0 {
		spec["membersReadyTimeoutSeconds"] = int64(membersReadyTO)
	}
	pg := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "scheduling.arenadata.io/v1alpha1",
		"kind":       "PodGroup",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"spec":       spec,
	}}
	_, err := dyn.Resource(podGroupGVR).Namespace(ns).Create(ctx, pg, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return
	}
	Expect(err).NotTo(HaveOccurred())
}

func podGroupPhase(ctx context.Context, ns, name string) string {
	u, err := dyn.Resource(podGroupGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return ""
	}
	phase, _, _ := unstructured.NestedString(u.Object, "status", "phase")
	return phase
}

func podGroupScheduled(ctx context.Context, ns, name string) int64 {
	u, err := dyn.Resource(podGroupGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return -1
	}
	n, _, _ := unstructured.NestedInt64(u.Object, "status", "scheduled")
	return n
}
