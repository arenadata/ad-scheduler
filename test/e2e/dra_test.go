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
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// DRA quota e2e: a queue caps dra/<deviceClassName> below the physical device
// count, proving ad-scheduler enforces the device quota at the queue level
// (decision q11), distinct from DynamicResources' physical device-fit. Requires
// kwok (fake node) — the spec skips itself if the node does not become Ready.
var (
	deviceClassGVR = schema.GroupVersionResource{Group: "resource.k8s.io", Version: "v1", Resource: "deviceclasses"}
	sliceGVR       = schema.GroupVersionResource{Group: "resource.k8s.io", Version: "v1", Resource: "resourceslices"}
	templateGVR    = schema.GroupVersionResource{Group: "resource.k8s.io", Version: "v1", Resource: "resourceclaimtemplates"}
)

const (
	draNS    = "team-dra"
	draSA    = "dra-sa"
	draClass = "example.e2e"
	draNode  = "dra-e2e-node"
)

var _ = Describe("ad-scheduler M2/M3 DRA device quota", func() {
	ctx := context.Background()

	BeforeEach(func() {
		if !draSetup(ctx) {
			Skip("kwok fake node not Ready — install kwok to run the DRA quota spec")
		}
		cleanupNS(ctx, draNS)
	})
	AfterEach(func() { cleanupNS(ctx, draNS) })

	It("caps dra/<class> at the queue max below the physical device count", func() {
		// queue max dra/example.e2e = 2; the ResourceSlice advertises 4 devices.
		for _, n := range []string{"dra-1", "dra-2", "dra-3"} {
			makeDRAPod(ctx, n)
		}
		// exactly two fit the queue's device quota; the third is Pending on OUR
		// quota, not on physical availability (4 devices exist).
		draScheduled := func() int {
			n := 0
			for _, name := range []string{"dra-1", "dra-2", "dra-3"} {
				if podNode(ctx, draNS, name) != "" {
					n++
				}
			}
			return n
		}
		Eventually(draScheduled, 60*time.Second, 2*time.Second).
			Should(Equal(2), "two device-claiming pods should schedule within the dra quota")
		Consistently(draScheduled, 10*time.Second, 2*time.Second).
			Should(Equal(2), "the third must stay Pending on the queue's dra/<class> quota")
	})
})

// draSetup ensures the DRA cluster objects + tenant exist and the kwok node is
// Ready. Returns false if kwok is unavailable (so the spec is skipped).
func draSetup(ctx context.Context) bool {
	// cluster-scoped: DeviceClass (matches any device) + a kwok pool node.
	applyUnstructured(ctx, deviceClassGVR, "", &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "resource.k8s.io/v1", "kind": "DeviceClass",
		"metadata": map[string]any{"name": draClass}, "spec": map[string]any{},
	}})
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        draNode,
			Labels:      map[string]string{"type": "kwok", poolLabel: poolValue, "kubernetes.io/hostname": draNode},
			Annotations: map[string]string{"kwok.x-k8s.io/node": "fake"},
		},
		Spec: corev1.NodeSpec{Taints: []corev1.Taint{{Key: "kwok.x-k8s.io/node", Value: "fake", Effect: corev1.TaintEffectNoSchedule}}},
	}
	if _, err := k8s.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return false
	}
	n, err := k8s.CoreV1().Nodes().Get(ctx, draNode, metav1.GetOptions{})
	if err != nil {
		return false
	}
	cap := corev1.ResourceList{corev1.ResourceCPU: apiresource.MustParse("16"), corev1.ResourceMemory: apiresource.MustParse("64Gi"), corev1.ResourcePods: apiresource.MustParse("64")}
	n.Status.Capacity, n.Status.Allocatable = cap, cap
	_, _ = k8s.CoreV1().Nodes().UpdateStatus(ctx, n, metav1.UpdateOptions{})

	// a ResourceSlice advertising 4 devices on the node.
	applyUnstructured(ctx, sliceGVR, "", &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "resource.k8s.io/v1", "kind": "ResourceSlice",
		"metadata": map[string]any{"name": draNode + "-pool"},
		"spec": map[string]any{
			"driver": draClass, "nodeName": draNode,
			"pool":    map[string]any{"name": draNode, "generation": int64(1), "resourceSliceCount": int64(1)},
			"devices": []any{map[string]any{"name": "d0"}, map[string]any{"name": "d1"}, map[string]any{"name": "d2"}, map[string]any{"name": "d3"}},
		},
	}})

	// tenant: namespace, SA, queue capped at dra/<class>=2, and a 1-device template.
	ensureNamespace(ctx, draNS)
	ensureServiceAccount(ctx, draNS, draSA)
	ensureQueueRaw(ctx, draNS, "gpu", map[string]any{
		"serviceAccounts": []any{draSA},
		"max":             map[string]any{"dra/" + draClass: "2"},
	})
	applyUnstructured(ctx, templateGVR, draNS, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "resource.k8s.io/v1", "kind": "ResourceClaimTemplate",
		"metadata": map[string]any{"name": "one-dev", "namespace": draNS},
		"spec": map[string]any{"spec": map[string]any{"devices": map[string]any{"requests": []any{
			map[string]any{"name": "dev", "exactly": map[string]any{"deviceClassName": draClass, "allocationMode": "ExactCount", "count": int64(1)}},
		}}}},
	}})
	time.Sleep(3 * time.Second) // let the tree rebuild pick up the queue

	// wait for the kwok node to be Ready (kwok present).
	for range 15 {
		nn, err := k8s.CoreV1().Nodes().Get(ctx, draNode, metav1.GetOptions{})
		if err == nil {
			for _, c := range nn.Status.Conditions {
				if c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue {
					return true
				}
			}
		}
		time.Sleep(2 * time.Second)
	}
	return false
}

func makeDRAPod(ctx context.Context, name string) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: draNS, Labels: map[string]string{"e2e": "true"}},
		Spec: corev1.PodSpec{
			SchedulerName:                 schedulerName,
			ServiceAccountName:            draSA,
			TerminationGracePeriodSeconds: new(int64(1)),
			Tolerations:                   []corev1.Toleration{{Key: "kwok.x-k8s.io/node", Operator: corev1.TolerationOpExists}},
			ResourceClaims:                []corev1.PodResourceClaim{{Name: "dev", ResourceClaimTemplateName: new("one-dev")}},
			Containers: []corev1.Container{{
				Name: "c", Image: "registry.k8s.io/pause:3.10",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: apiresource.MustParse("50m")},
					Claims:   []corev1.ResourceClaim{{Name: "dev"}},
				},
			}},
		},
	}
	_, err := k8s.CoreV1().Pods(draNS).Create(ctx, pod, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred())
}

// applyUnstructured creates the object if absent (namespace "" = cluster-scoped).
func applyUnstructured(ctx context.Context, gvr schema.GroupVersionResource, ns string, obj *unstructured.Unstructured) {
	ri := dyn.Resource(gvr)
	var err error
	if ns == "" {
		_, err = ri.Create(ctx, obj, metav1.CreateOptions{})
	} else {
		_, err = ri.Namespace(ns).Create(ctx, obj, metav1.CreateOptions{})
	}
	if err != nil && !apierrors.IsAlreadyExists(err) {
		Expect(err).NotTo(HaveOccurred())
	}
}
