//go:build perf

// Package perf is ad-scheduler's scheduling throughput/latency harness. It is
// build-tagged (`perf`) so it never runs in the normal unit/e2e suite; run it
// against a cluster with kwok installed (fake nodes, no kubelet) so thousands of
// nodes/pods fit on one machine:
//
//	# install kwok once (https://kwok.sigs.k8s.io):
//	kubectl apply -f https://github.com/kubernetes-sigs/kwok/releases/latest/download/kwok.yaml
//	kubectl apply -f https://github.com/kubernetes-sigs/kwok/releases/latest/download/stage-fast.yaml
//	# then, with ad-scheduler deployed:
//	PERF_NODES=1000 PERF_PODS=20000 go test -tags perf ./test/perf/ -run TestPerfSchedule -timeout 30m -v
//
// It creates PERF_NODES kwok nodes in the pool, a queue with an unbounded max,
// and PERF_PODS pods for ad-scheduler, then measures how fast they are all
// assigned a node (scheduling throughput) plus a coarse per-pod latency. The
// design's M7 SLOs (P99 cycle < 100ms, throughput ≥ 200 pods/s) are asserted
// loosely so the harness reports rather than flakes; tighten per environment.
package perf

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	perfNS    = "perf"
	perfSA    = "perf-sa"
	schedName = "ad-scheduler"
	poolLabel = "scheduler.arenadata.io/name"
	poolValue = "ad-scheduler"
)

var queueGVR = schema.GroupVersionResource{Group: "scheduling.arenadata.io", Version: "v1alpha1", Resource: "queues"}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func TestPerfSchedule(t *testing.T) {
	nodes := envInt("PERF_NODES", 100)
	pods := envInt("PERF_PODS", 1000)
	ctx := context.Background()

	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{}).ClientConfig()
	must(t, err, "kubeconfig")
	cfg.QPS, cfg.Burst = 2000, 4000 // don't let the client throttle the load generator
	k8s, err := kubernetes.NewForConfig(cfg)
	must(t, err, "clientset")
	dyn, err := dynamic.NewForConfig(cfg)
	must(t, err, "dynamic client")

	setupTenant(ctx, t, k8s, dyn)
	t.Cleanup(func() { teardown(context.Background(), k8s, dyn, nodes) })

	t.Logf("creating %d kwok nodes…", nodes)
	createNodes(ctx, t, k8s, nodes)
	waitNodesReady(ctx, t, k8s, nodes)

	// Scheduler decision counter before the run (the pure scheduling rate,
	// decoupled from the apiserver/etcd bind ceiling that dominates wall-clock).
	attempts0 := schedulerDecisions(ctx, k8s)

	t.Logf("creating %d pods…", pods)
	start := time.Now()
	createPods(ctx, t, k8s, pods)

	// Poll until every perf pod has a node assigned.
	scheduled := 0
	deadline := time.Now().Add(20 * time.Minute)
	for scheduled < pods && time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		scheduled = countScheduled(ctx, k8s)
		t.Logf("  scheduled %d/%d (%.0fs)", scheduled, pods, time.Since(start).Seconds())
	}
	elapsed := time.Since(start)
	if scheduled < pods {
		t.Fatalf("only %d/%d pods scheduled in %s", scheduled, pods, elapsed)
	}

	attempts1 := schedulerDecisions(ctx, k8s)
	throughput := float64(pods) / elapsed.Seconds()
	decisionsPerSec := (attempts1 - attempts0) / elapsed.Seconds()
	p50, p99 := latencyPercentiles(ctx, k8s)
	t.Logf("=== ad-scheduler perf ===")
	t.Logf("nodes=%d pods=%d wall=%.1fs", nodes, pods, elapsed.Seconds())
	t.Logf("wall throughput  = %.0f pods/s (bind-bound: includes apiserver/etcd writes)", throughput)
	t.Logf("SCHEDULER decisions = %.0f/s (%.0f attempts over the window) — the engine's true rate", decisionsPerSec, attempts1-attempts0)
	t.Logf("per-pod schedule latency: P50=%s P99=%s", p50, p99)
	t.Logf("(report-oriented; compare against §8 SLOs on a representative control plane.)")

	// Only a total-breakage floor — real SLO judgement is against the report on a
	// representative cluster, not this laptop kind+kwok run.
	if throughput < 5 {
		t.Errorf("throughput %.0f pods/s indicates a scheduling stall", throughput)
	}
}

func setupTenant(ctx context.Context, t *testing.T, k8s kubernetes.Interface, dyn dynamic.Interface) {
	createIgnoreExists(k8s.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: perfNS}}, metav1.CreateOptions{}))
	createIgnoreExists(k8s.CoreV1().ServiceAccounts(perfNS).Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: perfSA, Namespace: perfNS}}, metav1.CreateOptions{}))
	q := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "scheduling.arenadata.io/v1alpha1", "kind": "Queue",
		"metadata": map[string]interface{}{"name": "perf", "namespace": perfNS},
		"spec":     map[string]interface{}{"serviceAccounts": []interface{}{perfSA}}, // unbounded max
	}}
	_, err := dyn.Resource(queueGVR).Namespace(perfNS).Create(ctx, q, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create queue: %v", err)
	}
	time.Sleep(3 * time.Second) // let the tree rebuild pick up the queue
}

func createNodes(ctx context.Context, t *testing.T, k8s kubernetes.Interface, n int) {
	cap := corev1.ResourceList{
		corev1.ResourceCPU:    apiresource.MustParse("32"),
		corev1.ResourceMemory: apiresource.MustParse("128Gi"),
		corev1.ResourcePods:   apiresource.MustParse("256"),
	}
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("kwok-%05d", i)
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
				Labels: map[string]string{
					"type": "kwok", poolLabel: poolValue, "kubernetes.io/hostname": name,
				},
				Annotations: map[string]string{"kwok.x-k8s.io/node": "fake", "node.alpha.kubernetes.io/ttl": "0"},
			},
			Spec: corev1.NodeSpec{Taints: []corev1.Taint{{Key: "kwok.x-k8s.io/node", Value: "fake", Effect: corev1.TaintEffectNoSchedule}}},
		}
		created, err := k8s.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatalf("create node %s: %v", name, err)
		}
		if created == nil {
			continue
		}
		created.Status.Capacity, created.Status.Allocatable = cap, cap
		created.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue, Reason: "KubeletReady"}}
		_, _ = k8s.CoreV1().Nodes().UpdateStatus(ctx, created, metav1.UpdateOptions{})
	}
}

func waitNodesReady(ctx context.Context, t *testing.T, k8s kubernetes.Interface, n int) {
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		list, err := k8s.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: "type=kwok"})
		must(t, err, "list nodes")
		ready := 0
		for i := range list.Items {
			for _, c := range list.Items[i].Status.Conditions {
				if c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue {
					ready++
				}
			}
		}
		if ready >= n {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatal("kwok nodes did not become Ready — is kwok installed?")
}

// createPods submits n pods concurrently (a worker pool) so the load generator,
// not the scheduler, is never the bottleneck — otherwise serial API creates
// dominate the wall time and understate scheduling throughput.
func createPods(ctx context.Context, t *testing.T, k8s kubernetes.Interface, n int) {
	req := corev1.ResourceList{corev1.ResourceCPU: apiresource.MustParse("100m"), corev1.ResourceMemory: apiresource.MustParse("64Mi")}
	const workers = 64
	idx := make(chan int, n)
	for i := 0; i < n; i++ {
		idx <- i
	}
	close(idx)
	var wg sync.WaitGroup
	var failed atomic.Int64
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range idx {
				pod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("perf-%06d", i), Namespace: perfNS, Labels: map[string]string{"perf": "true"}},
					Spec: corev1.PodSpec{
						SchedulerName:      schedName,
						ServiceAccountName: perfSA,
						Tolerations: []corev1.Toleration{
							{Key: "kwok.x-k8s.io/node", Operator: corev1.TolerationOpExists},
							{Key: "node.kubernetes.io/not-ready", Operator: corev1.TolerationOpExists},
						},
						Containers: []corev1.Container{{Name: "c", Image: "registry.k8s.io/pause:3.10", Resources: corev1.ResourceRequirements{Requests: req}}},
					},
				}
				if _, err := k8s.CoreV1().Pods(perfNS).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
					failed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if f := failed.Load(); f > 0 {
		t.Fatalf("%d/%d pod creates failed", f, n)
	}
}

func countScheduled(ctx context.Context, k8s kubernetes.Interface) int {
	list, err := k8s.CoreV1().Pods(perfNS).List(ctx, metav1.ListOptions{LabelSelector: "perf=true"})
	if err != nil {
		return 0
	}
	n := 0
	for i := range list.Items {
		if list.Items[i].Spec.NodeName != "" {
			n++
		}
	}
	return n
}

// latencyPercentiles derives per-pod scheduling latency from the PodScheduled
// condition timestamp minus creation (second-resolution — a coarse indicator).
func latencyPercentiles(ctx context.Context, k8s kubernetes.Interface) (p50, p99 time.Duration) {
	list, err := k8s.CoreV1().Pods(perfNS).List(ctx, metav1.ListOptions{LabelSelector: "perf=true"})
	if err != nil {
		return 0, 0
	}
	var lat []time.Duration
	for i := range list.Items {
		p := &list.Items[i]
		for _, c := range p.Status.Conditions {
			if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionTrue {
				lat = append(lat, c.LastTransitionTime.Sub(p.CreationTimestamp.Time))
			}
		}
	}
	if len(lat) == 0 {
		return 0, 0
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	return lat[len(lat)*50/100], lat[min(len(lat)*99/100, len(lat)-1)]
}

// schedulerDecisions reads scheduler_schedule_attempts_total{result="scheduled"}
// from the scheduler's debug server (:8089/scheduler-metrics, unauthenticated)
// via the apiserver pod proxy. This counts scheduling-cycle completions (the
// engine's decision rate), independent of async bind latency.
func schedulerDecisions(ctx context.Context, k8s kubernetes.Interface) float64 {
	pods, err := k8s.CoreV1().Pods("ad-system").List(ctx, metav1.ListOptions{})
	if err != nil || len(pods.Items) == 0 {
		return 0
	}
	var pod string
	for i := range pods.Items {
		if pods.Items[i].Status.Phase == corev1.PodRunning {
			pod = pods.Items[i].Name
			break
		}
	}
	if pod == "" {
		return 0
	}
	raw, err := k8s.CoreV1().RESTClient().Get().
		Namespace("ad-system").Resource("pods").Name(pod + ":8089").
		SubResource("proxy").Suffix("scheduler-metrics").DoRaw(ctx)
	if err != nil {
		return 0
	}
	var total float64
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "scheduler_schedule_attempts_total{") && strings.Contains(line, `result="scheduled"`) {
			f := strings.Fields(line)
			if v, err := strconv.ParseFloat(f[len(f)-1], 64); err == nil {
				total += v
			}
		}
	}
	return total
}

func teardown(ctx context.Context, k8s kubernetes.Interface, dyn dynamic.Interface, nodes int) {
	_ = k8s.CoreV1().Pods(perfNS).DeleteCollection(ctx, metav1.DeleteOptions{GracePeriodSeconds: ptr(int64(0))}, metav1.ListOptions{LabelSelector: "perf=true"})
	_ = k8s.CoreV1().Nodes().DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: "type=kwok"})
}

func createIgnoreExists[T any](_ T, err error) {
	if err != nil && !apierrors.IsAlreadyExists(err) {
		panic(err)
	}
}

func must(t *testing.T, err error, what string) {
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

func ptr[T any](v T) *T { return &v }
