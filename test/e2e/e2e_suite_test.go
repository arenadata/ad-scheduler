// Package e2e is ad-scheduler's end-to-end suite: it drives a real cluster
// (kind) through client-go and asserts the scheduler's observable behaviour —
// placement, queue max-cap, off-pool rejection, fail-closed, lifecycle release.
//
// Run against the current kubeconfig context (kind-kind by default):
//
//	go test ./test/e2e/... -timeout 10m
//	# or: ginkgo -v ./test/e2e
//
// The suite is hermetic: BeforeSuite ensures the tenant (namespace team-a, SA
// spark, Queue main) exists and cleans test pods between specs, so it is safe to
// re-run. It assumes the scheduler Deployment, CRDs and RBAC are already applied
// and the pool label is on the worker nodes.
package e2e

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	schedulerName = "ad-scheduler"
	tenantNS      = "team-a"
	tenantSA      = "spark"
	poolLabel     = "scheduler.arenadata.io/name"
	poolValue     = "ad-scheduler"
	cpTaint       = "node-role.kubernetes.io/control-plane"
)

const podGroupAnnotation = "arenadata.io/pod-group"

var (
	queueGVR    = schema.GroupVersionResource{Group: "scheduling.arenadata.io", Version: "v1alpha1", Resource: "queues"}
	podGroupGVR = schema.GroupVersionResource{Group: "scheduling.arenadata.io", Version: "v1alpha1", Resource: "podgroups"}
)

var (
	k8s kubernetes.Interface
	dyn dynamic.Interface
)

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ad-scheduler e2e")
}

var _ = BeforeSuite(func() {
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{}).ClientConfig()
	Expect(err).NotTo(HaveOccurred(), "load kubeconfig (is a cluster reachable?)")
	k8s, err = kubernetes.NewForConfig(cfg)
	Expect(err).NotTo(HaveOccurred())
	dyn, err = dynamic.NewForConfig(cfg)
	Expect(err).NotTo(HaveOccurred())

	ctx := context.Background()
	ensureNamespace(ctx, tenantNS)
	ensureServiceAccount(ctx, tenantNS, tenantSA)
	ensureQueue(ctx, tenantNS, "main", tenantSA, "500m")

	// the scheduler must be Ready before we assert on its decisions.
	Eventually(func() bool { return schedulerReady(ctx) }, 90*time.Second, 2*time.Second).
		Should(BeTrue(), "ad-scheduler Deployment did not become Ready")
})

func schedulerReady(ctx context.Context) bool {
	d, err := k8s.AppsV1().Deployments("ad-system").Get(ctx, "ad-scheduler", metav1.GetOptions{})
	if err != nil {
		return false
	}
	return d.Status.ReadyReplicas >= 1
}

func ensureNamespace(ctx context.Context, name string) {
	_, err := k8s.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return
	}
	Expect(err).NotTo(HaveOccurred())
}

func ensureServiceAccount(ctx context.Context, ns, name string) {
	_, err := k8s.CoreV1().ServiceAccounts(ns).Create(ctx, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
	}, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return
	}
	Expect(err).NotTo(HaveOccurred())
}

// ensureQueue creates a leaf Queue routing sa -> this queue with the given cpu
// max, or leaves an existing one in place.
func ensureQueue(ctx context.Context, ns, name, sa, cpuMax string) {
	q := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "scheduling.arenadata.io/v1alpha1",
		"kind":       "Queue",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"spec": map[string]any{
			"serviceAccounts": []any{sa},
			"submitACL":       ns,
			"max":             map[string]any{"cpu": cpuMax},
		},
	}}
	_, err := dyn.Resource(queueGVR).Namespace(ns).Create(ctx, q, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return
	}
	Expect(err).NotTo(HaveOccurred())
}

// --- pod helpers -----------------------------------------------------------

type podOpt func(*corev1.Pod)

func withNodeName(hostname string) podOpt {
	return func(p *corev1.Pod) {
		if p.Spec.NodeSelector == nil {
			p.Spec.NodeSelector = map[string]string{}
		}
		p.Spec.NodeSelector["kubernetes.io/hostname"] = hostname
		p.Spec.Tolerations = append(p.Spec.Tolerations, corev1.Toleration{
			Key: cpTaint, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule,
		})
	}
}

func withPriority(p int32) podOpt {
	return func(pod *corev1.Pod) { pod.Spec.Priority = &p }
}

// withoutPoolPlacement strips the pool nodeSelector + toleration a submitter
// might add by hand, leaving only schedulerName: ad-scheduler — so a test can
// observe the MutatingAdmissionPolicy stamping them back on (its trigger is
// schedulerName alone; there is no separate opt-in marker).
func withoutPoolPlacement() podOpt {
	return func(pod *corev1.Pod) {
		delete(pod.Spec.NodeSelector, poolLabel)
		pod.Spec.Tolerations = nil
	}
}

// withGang marks the pod as a member of the named PodGroup (gang).
func withGang(name string) podOpt {
	return func(pod *corev1.Pod) {
		if pod.Annotations == nil {
			pod.Annotations = map[string]string{}
		}
		pod.Annotations[podGroupAnnotation] = name
	}
}

// createPodGroup creates (or leaves in place) a PodGroup gang spec.
func createPodGroup(ctx context.Context, ns, name string, minMember int32, minCPU string) {
	pg := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "scheduling.arenadata.io/v1alpha1",
		"kind":       "PodGroup",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"spec": map[string]any{
			"minMember":              int64(minMember),
			"minResources":           map[string]any{"cpu": minCPU},
			"scheduleTimeoutSeconds": int64(120),
		},
	}}
	_, err := dyn.Resource(podGroupGVR).Namespace(ns).Create(ctx, pg, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return
	}
	Expect(err).NotTo(HaveOccurred())
}

func deletePodGroup(ctx context.Context, ns, name string) {
	_ = dyn.Resource(podGroupGVR).Namespace(ns).Delete(ctx, name, metav1.DeleteOptions{})
}

// makePod creates a pod for our scheduler in the tenant namespace requesting cpu.
func makePod(ctx context.Context, name, sa, cpu string, opts ...podOpt) *corev1.Pod {
	q := apiresource.MustParse(cpu)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: tenantNS, Labels: map[string]string{"e2e": "true"}},
		Spec: corev1.PodSpec{
			SchedulerName:                 schedulerName,
			ServiceAccountName:            sa,
			TerminationGracePeriodSeconds: new(int64(1)), // fast teardown for tests/reclaim
			Containers: []corev1.Container{{
				Name:  "c",
				Image: "registry.k8s.io/pause:3.10",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: q},
					Limits:   corev1.ResourceList{corev1.ResourceCPU: q},
				},
			}},
		},
	}
	for _, o := range opts {
		o(pod)
	}
	created, err := k8s.CoreV1().Pods(tenantNS).Create(ctx, pod, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred())
	return created
}

func podNodeName(ctx context.Context, name string) string {
	p, err := k8s.CoreV1().Pods(tenantNS).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return ""
	}
	return p.Spec.NodeName
}

// cleanupPods deletes every e2e pod and waits for them to disappear so specs do
// not leak queue allocation into each other.
func cleanupPods(ctx context.Context) {
	_ = k8s.CoreV1().Pods(tenantNS).DeleteCollection(ctx,
		metav1.DeleteOptions{GracePeriodSeconds: new(int64(0))},
		metav1.ListOptions{LabelSelector: "e2e=true"})
	Eventually(func() int {
		l, err := k8s.CoreV1().Pods(tenantNS).List(ctx, metav1.ListOptions{LabelSelector: "e2e=true"})
		if err != nil {
			return -1
		}
		return len(l.Items)
	}, 60*time.Second, time.Second).Should(Equal(0), "test pods did not clean up")
	// Pod deletion releases queue allocation asynchronously (informer -> engine);
	// let that settle so the next spec starts from clean headroom.
	time.Sleep(2 * time.Second)
}

//go:fix inline
func ptr[T any](v T) *T { return new(v) }
