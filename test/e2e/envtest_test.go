package e2e

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"
)

// envtest mode (the default): the suite runs against a local kube-apiserver +
// etcd from setup-envtest instead of a real cluster. There is no kubelet,
// controller-manager or scheduler Deployment there, so the suite brings up the
// minimum itself:
//
//   - the CRDs (deploy/crds.yaml) and admission policies (deploy/admission.yaml);
//   - fake Nodes: one pool worker and an off-pool "kind-control-plane";
//   - a fake kubelet that marks bound pods Running/Ready and finishes graceful
//     deletions (evictions), which no kubelet would otherwise complete;
//   - the ad-scheduler binary, built from ./cmd/scheduler and run as a local
//     process with the scheduler config embedded in deploy/deployment.yaml.
//
// Set USE_EXISTING_CLUSTER=true to run against the current kubeconfig context
// instead (a cluster with the scheduler already deployed, e.g. kind).

const (
	repoRoot   = "../.."
	poolWorker = "ad-worker-1"
	offPool    = "kind-control-plane"
	taintKey   = "scheduler.arenadata.io/dedicated"
)

var (
	testEnv     *envtest.Environment
	envtestMode bool
	stopLocal   context.CancelFunc
	schedDone   chan error
)

// startTestEnv starts envtest, or attaches to the current kubeconfig context
// when USE_EXISTING_CLUSTER=true.
func startTestEnv() {
	existing := os.Getenv("USE_EXISTING_CLUSTER") == "true"
	envtestMode = !existing
	testEnv = &envtest.Environment{UseExistingCluster: &existing}
	if envtestMode {
		testEnv.CRDDirectoryPaths = []string{filepath.Join(repoRoot, "deploy", "crds.yaml")}
		testEnv.ErrorIfCRDPathMissing = true
	}
	_, err := testEnv.Start()
	Expect(err).NotTo(HaveOccurred(),
		"start envtest (run via `make e2e`, which sets KUBEBUILDER_ASSETS; or set USE_EXISTING_CLUSTER=true)")
}

// bootstrapEnvtest installs everything a real cluster would provide and starts
// the scheduler. Must run after the k8s/dyn clients are set.
func bootstrapEnvtest(ctx context.Context) {
	ensureNamespace(ctx, "ad-system")
	applyManifest(ctx, filepath.Join(repoRoot, "deploy", "admission.yaml"))

	createFakeNode(ctx, poolWorker, map[string]string{poolLabel: poolValue},
		[]corev1.Taint{{Key: taintKey, Value: poolValue, Effect: corev1.TaintEffectNoSchedule}})
	createFakeNode(ctx, offPool, map[string]string{"node-role.kubernetes.io/control-plane": ""},
		[]corev1.Taint{{Key: cpTaint, Effect: corev1.TaintEffectNoSchedule}})

	runCtx, cancel := context.WithCancel(context.Background())
	stopLocal = cancel
	startFakeKubelet(runCtx)
	startScheduler(runCtx)
	waitAdmissionPolicies(ctx)
}

// stopTestEnv tears down the local scheduler + kubelet and the control plane.
func stopTestEnv() {
	if stopLocal != nil {
		stopLocal()
		if schedDone != nil {
			select {
			case <-schedDone:
			case <-time.After(10 * time.Second):
			}
		}
	}
	if testEnv != nil {
		Expect(testEnv.Stop()).To(Succeed())
	}
}

// applyManifest creates every object of a multi-document YAML file.
func applyManifest(ctx context.Context, path string) {
	data, err := os.ReadFile(path)
	Expect(err).NotTo(HaveOccurred())
	groups, err := restmapper.GetAPIGroupResources(k8s.Discovery())
	Expect(err).NotTo(HaveOccurred())
	mapper := restmapper.NewDiscoveryRESTMapper(groups)

	dec := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	for {
		obj := &unstructured.Unstructured{}
		if err := dec.Decode(&obj.Object); err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			Expect(err).NotTo(HaveOccurred(), "decode %s", path)
		}
		if len(obj.Object) == 0 {
			continue
		}
		gvk := obj.GroupVersionKind()
		m, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		Expect(err).NotTo(HaveOccurred(), "map %s", gvk)
		ri := dyn.Resource(m.Resource)
		if ns := obj.GetNamespace(); ns != "" {
			_, err = ri.Namespace(ns).Create(ctx, obj, metav1.CreateOptions{})
		} else {
			_, err = ri.Create(ctx, obj, metav1.CreateOptions{})
		}
		if !apierrors.IsAlreadyExists(err) {
			Expect(err).NotTo(HaveOccurred(), "create %s %s", gvk.Kind, obj.GetName())
		}
	}
}

// createFakeNode registers a Ready node with fixed capacity. Nothing runs on it;
// the fake kubelet only reflects pod status.
func createFakeNode(ctx context.Context, name string, labels map[string]string, taints []corev1.Taint) {
	l := map[string]string{"kubernetes.io/hostname": name, "kubernetes.io/os": "linux"}
	for k, v := range labels {
		l[k] = v
	}
	_, err := k8s.CoreV1().Nodes().Create(ctx, &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: l},
		Spec:       corev1.NodeSpec{Taints: taints},
	}, metav1.CreateOptions{})
	if !apierrors.IsAlreadyExists(err) {
		Expect(err).NotTo(HaveOccurred())
	}
	n, err := k8s.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
	Expect(err).NotTo(HaveOccurred())
	capacity := corev1.ResourceList{
		corev1.ResourceCPU:    apiresource.MustParse("4"),
		corev1.ResourceMemory: apiresource.MustParse("16Gi"),
		corev1.ResourcePods:   apiresource.MustParse("110"),
	}
	n.Status.Capacity, n.Status.Allocatable = capacity, capacity
	n.Status.Conditions = []corev1.NodeCondition{{
		Type: corev1.NodeReady, Status: corev1.ConditionTrue, Reason: "KubeletReady",
		LastHeartbeatTime: metav1.Now(), LastTransitionTime: metav1.Now(),
	}}
	n, err = k8s.CoreV1().Nodes().UpdateStatus(ctx, n, metav1.UpdateOptions{})
	Expect(err).NotTo(HaveOccurred())

	// The apiserver (TaintNodesByCondition) taints every new node not-ready:NoSchedule;
	// the node-lifecycle controller lifts it once the node is Ready, and envtest
	// has no controllers — so lift it here.
	var kept []corev1.Taint
	for _, t := range n.Spec.Taints {
		if t.Key != corev1.TaintNodeNotReady {
			kept = append(kept, t)
		}
	}
	n.Spec.Taints = kept
	_, err = k8s.CoreV1().Nodes().Update(ctx, n, metav1.UpdateOptions{})
	Expect(err).NotTo(HaveOccurred())
}

// startFakeKubelet stands in for the kubelet: a bound pod becomes Running and
// Ready, and a bound pod being deleted gracefully (incl. eviction) is removed.
func startFakeKubelet(ctx context.Context) {
	f := informers.NewSharedInformerFactory(k8s, 0)
	inf := f.Core().V1().Pods().Informer()
	handle := func(obj any) {
		p, ok := obj.(*corev1.Pod)
		if !ok || p.Spec.NodeName == "" {
			return
		}
		if p.DeletionTimestamp != nil {
			uid := p.UID
			_ = k8s.CoreV1().Pods(p.Namespace).Delete(ctx, p.Name, metav1.DeleteOptions{
				GracePeriodSeconds: new(int64(0)), Preconditions: &metav1.Preconditions{UID: &uid},
			})
			return
		}
		if p.Status.Phase != corev1.PodPending && p.Status.Phase != "" {
			return
		}
		now := metav1.Now()
		p = p.DeepCopy()
		p.Status.Phase = corev1.PodRunning
		p.Status.StartTime = &now
		p.Status.Conditions = nil
		for _, t := range []corev1.PodConditionType{corev1.PodScheduled, corev1.PodInitialized, corev1.ContainersReady, corev1.PodReady} {
			p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{Type: t, Status: corev1.ConditionTrue, LastTransitionTime: now})
		}
		_, _ = k8s.CoreV1().Pods(p.Namespace).UpdateStatus(ctx, p, metav1.UpdateOptions{})
	}
	_, err := inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    handle,
		UpdateFunc: func(_, n any) { handle(n) },
	})
	Expect(err).NotTo(HaveOccurred())
	f.Start(ctx.Done())
	Expect(cache.WaitForCacheSync(ctx.Done(), inf.HasSynced)).To(BeTrue())
}

// startScheduler builds (unless E2E_SCHEDULER_BIN is set) and launches the
// scheduler against envtest, then waits for /readyz.
func startScheduler(ctx context.Context) {
	dir, err := os.MkdirTemp("", "ad-e2e-")
	Expect(err).NotTo(HaveOccurred())

	bin := os.Getenv("E2E_SCHEDULER_BIN")
	if bin == "" {
		bin = filepath.Join(dir, "ad-scheduler")
		build := exec.Command("go", "build", "-o", bin, "./cmd/scheduler")
		build.Dir = repoRoot
		out, err := build.CombinedOutput()
		Expect(err).NotTo(HaveOccurred(), "build scheduler: %s", out)
	}

	kubeconfig := filepath.Join(dir, "kubeconfig")
	Expect(os.WriteFile(kubeconfig, testEnv.KubeConfig, 0o600)).To(Succeed())
	config := filepath.Join(dir, "config.yaml")
	Expect(os.WriteFile(config, schedulerConfig(kubeconfig), 0o600)).To(Succeed())

	logPath := filepath.Join(dir, "ad-scheduler.log")
	logFile, err := os.Create(logPath)
	Expect(err).NotTo(HaveOccurred())
	fmt.Fprintf(GinkgoWriter, "ad-scheduler log: %s\n", logPath)

	securePort := freePort()
	cmd := exec.CommandContext(ctx, bin,
		"--config="+config,
		"--bind-address=127.0.0.1",
		"--secure-port="+strconv.Itoa(securePort),
		"-v=2",
	)
	cmd.Env = append(os.Environ(),
		"AD_SCHEDULER_NAME="+schedulerName,
		"AD_NODE_POOL_LABEL_SELECTOR="+poolLabel+"="+poolValue,
		"AD_NODE_POOL_TAINT_KEY="+taintKey,
		"AD_RESERVATION_DELAY=2s",
		"AD_DEBUG_ADDR=127.0.0.1:"+strconv.Itoa(freePort()),
		"POD_NAMESPACE=ad-system",
	)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	Expect(cmd.Start()).To(Succeed())
	schedDone = make(chan error, 1)
	go func() { schedDone <- cmd.Wait(); _ = logFile.Close() }()

	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // self-signed serving cert
	}}
	url := fmt.Sprintf("https://127.0.0.1:%d/readyz", securePort)
	Eventually(func() error {
		select {
		case err := <-schedDone:
			schedDone <- err
			return StopTrying(fmt.Sprintf("ad-scheduler exited: %v (log: %s)", err, logPath))
		default:
		}
		resp, err := client.Get(url)
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("readyz: %s", resp.Status)
		}
		return nil
	}, 60*time.Second, 500*time.Millisecond).Should(Succeed(), "ad-scheduler did not become ready (log: %s)", logPath)
}

// schedulerConfig derives the KubeSchedulerConfiguration from the ConfigMap in
// deploy/deployment.yaml (so the tested profile is the shipped one), pointed at
// envtest and without leader election.
func schedulerConfig(kubeconfig string) []byte {
	data, err := os.ReadFile(filepath.Join(repoRoot, "deploy", "deployment.yaml"))
	Expect(err).NotTo(HaveOccurred())
	dec := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	for {
		var cm corev1.ConfigMap
		err := dec.Decode(&cm)
		if errors.Is(err, io.EOF) {
			break
		}
		Expect(err).NotTo(HaveOccurred())
		raw, ok := cm.Data["config.yaml"]
		if cm.Kind != "ConfigMap" || !ok {
			continue
		}
		cfg := map[string]any{}
		Expect(yaml.Unmarshal([]byte(raw), &cfg)).To(Succeed())
		cfg["leaderElection"] = map[string]any{"leaderElect": false}
		cc, _ := cfg["clientConnection"].(map[string]any)
		if cc == nil {
			cc = map[string]any{}
		}
		cc["kubeconfig"] = kubeconfig
		cfg["clientConnection"] = cc
		out, err := yaml.Marshal(cfg)
		Expect(err).NotTo(HaveOccurred())
		return out
	}
	Fail("no scheduler ConfigMap (data.config.yaml) in deploy/deployment.yaml")
	return nil
}

// waitAdmissionPolicies blocks until the VAP and MAP are enforced (policies are
// picked up asynchronously after creation). Uses server-side dry-run only.
func waitAdmissionPolicies(ctx context.Context) {
	ensureNamespace(ctx, tenantNS)
	dryRun := []string{metav1.DryRunAll}
	Eventually(func() error {
		q := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "scheduling.arenadata.io/v1alpha1", "kind": "Queue",
			"metadata": map[string]any{"name": "vap-probe", "namespace": tenantNS},
			"spec":     map[string]any{"parent": "vap-probe"},
		}}
		if _, err := dyn.Resource(queueGVR).Namespace(tenantNS).Create(ctx, q, metav1.CreateOptions{DryRun: dryRun}); err == nil {
			return errors.New("queue VAP not enforced yet")
		}
		p, err := k8s.CoreV1().Pods(tenantNS).Create(ctx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "map-probe"},
			Spec: corev1.PodSpec{
				SchedulerName: schedulerName, ServiceAccountName: tenantSA,
				Containers: []corev1.Container{{Name: "c", Image: "registry.k8s.io/pause:3.10"}},
			},
		}, metav1.CreateOptions{DryRun: dryRun})
		if err != nil {
			return err
		}
		if p.Spec.NodeSelector[poolLabel] != poolValue {
			return errors.New("MAP not enforced yet")
		}
		return nil
	}, 30*time.Second, 500*time.Millisecond).Should(Succeed(), "admission policies did not take effect")
}

func freePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// On a failed spec, dump the pods and nodes the scheduler saw — the first thing
// needed to tell an admission, taint or capacity problem apart.
var _ = JustAfterEach(func() {
	if !CurrentSpecReport().Failed() {
		return
	}
	ctx := context.Background()
	nodes, _ := k8s.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	for _, n := range nodes.Items {
		fmt.Fprintf(GinkgoWriter, "node %s labels=%v taints=%v\n", n.Name, n.Labels, n.Spec.Taints)
	}
	pods, _ := k8s.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{LabelSelector: "e2e=true"})
	for _, p := range pods.Items {
		fmt.Fprintf(GinkgoWriter, "pod %s/%s node=%q phase=%s nodeSelector=%v tolerations=%v\n",
			p.Namespace, p.Name, p.Spec.NodeName, p.Status.Phase, p.Spec.NodeSelector, p.Spec.Tolerations)
	}
})

// TestServeEnvtest keeps a fully bootstrapped envtest cluster (nodes, fake
// kubelet, admission policies, scheduler) alive for external tooling — notably
// smoke.sh. It is a no-op unless E2E_SERVE_KUBECONFIG names the file to write the
// admin kubeconfig to; it runs until SIGINT/SIGTERM.
func TestServeEnvtest(t *testing.T) {
	path := os.Getenv("E2E_SERVE_KUBECONFIG")
	if path == "" {
		t.Skip("E2E_SERVE_KUBECONFIG not set")
	}
	RegisterFailHandler(func(message string, _ ...int) { panic(message) })
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("serve envtest: %v", r)
		}
	}()

	startTestEnv()
	defer stopTestEnv()
	var err error
	k8s, err = kubernetes.NewForConfig(testEnv.Config)
	Expect(err).NotTo(HaveOccurred())
	dyn, err = dynamic.NewForConfig(testEnv.Config)
	Expect(err).NotTo(HaveOccurred())
	bootstrapEnvtest(context.Background())

	// Written last and atomically: its appearance is the "ready" signal.
	Expect(os.WriteFile(path+".tmp", testEnv.KubeConfig, 0o600)).To(Succeed())
	Expect(os.Rename(path+".tmp", path)).To(Succeed())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
}
