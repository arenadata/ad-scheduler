package resource

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func rl(cpu, mem string, extra map[string]string) corev1.ResourceList {
	out := corev1.ResourceList{}
	if cpu != "" {
		out[corev1.ResourceCPU] = resource.MustParse(cpu)
	}
	if mem != "" {
		out[corev1.ResourceMemory] = resource.MustParse(mem)
	}
	for k, v := range extra {
		out[corev1.ResourceName(k)] = resource.MustParse(v)
	}
	return out
}

func ctr(cpu, mem string) corev1.Container {
	return corev1.Container{Resources: corev1.ResourceRequirements{Requests: rl(cpu, mem, nil)}}
}

func TestToQuantityMapRoundTrip(t *testing.T) {
	// ToQuantityMap must invert FromQuantityMap under the cpu-millivalue convention.
	orig := Resource{"cpu": 1500, "memory": 256 * 1024 * 1024, "nvidia.com/gpu": 2}
	qm := ToQuantityMap(orig)
	// cpu is rendered from millicores: 1500m.
	cpuQ := qm["cpu"]
	if got := cpuQ.MilliValue(); got != 1500 {
		t.Errorf("cpu quantity = %dm, want 1500m", got)
	}
	back := FromQuantityMap(qm)
	if !Equal(back, orig) {
		t.Errorf("round-trip: FromQuantityMap(ToQuantityMap(%v)) = %v", orig, back)
	}
}

func TestFromResourceList_UnitConvention(t *testing.T) {
	got := FromResourceList(rl("500m", "128Mi", map[string]string{"nvidia.com/gpu": "2"}))
	want := Resource{"cpu": 500, "memory": 128 * 1024 * 1024, "nvidia.com/gpu": 2}
	if !Equal(got, want) {
		t.Fatalf("FromResourceList = %v, want %v", got, want)
	}
}

func TestEffectiveRequest_ContainersSum(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{ctr("500m", "256Mi"), ctr("1", "256Mi")}}}
	got := EffectiveRequest(pod)
	if !Equal(got, Resource{"cpu": 1500, "memory": 512 * 1024 * 1024}) {
		t.Fatalf("containers should sum: %v", got)
	}
}

func TestEffectiveRequest_InitIsMaxNotSum(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{
		Containers: []corev1.Container{ctr("500m", "100Mi")},
		InitContainers: []corev1.Container{
			ctr("2", "50Mi"),   // ordinary init, big cpu
			ctr("100m", "1Gi"), // ordinary init, big mem
		},
	}}
	got := EffectiveRequest(pod)
	// max(containers=500m/100Mi, initPeak = max(2cpu/50Mi, 100m/1Gi)) componentwise
	// cpu: max(500, 2000, 100) = 2000; mem: max(100Mi, 50Mi, 1Gi) = 1Gi
	want := Resource{"cpu": 2000, "memory": 1024 * 1024 * 1024}
	if !Equal(got, want) {
		t.Fatalf("init should be per-container max, got %v want %v", got, want)
	}
}

func TestEffectiveRequest_NativeSidecarSums(t *testing.T) {
	always := corev1.ContainerRestartPolicyAlways
	sidecar := ctr("500m", "128Mi")
	sidecar.RestartPolicy = &always
	pod := &corev1.Pod{Spec: corev1.PodSpec{
		Containers:     []corev1.Container{ctr("1", "256Mi")},
		InitContainers: []corev1.Container{sidecar, ctr("2", "64Mi")}, // sidecar + ordinary init
	}}
	got := EffectiveRequest(pod)
	// containers+sidecar = 1500m/384Mi; initPeak = max(sidecar 500m/128Mi, sidecar+init 2500m/192Mi) = 2500m/192Mi
	// eff = max(1500m/384Mi, 2500m/192Mi) = 2500m/384Mi
	want := Resource{"cpu": 2500, "memory": 384 * 1024 * 1024}
	if !Equal(got, want) {
		t.Fatalf("native sidecar handling wrong: %v want %v", got, want)
	}
}

func TestEffectiveRequest_Overhead(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{
		Containers: []corev1.Container{ctr("1", "256Mi")},
		Overhead:   rl("100m", "32Mi", nil),
	}}
	got := EffectiveRequest(pod)
	if !Equal(got, Resource{"cpu": 1100, "memory": (256 + 32) * 1024 * 1024}) {
		t.Fatalf("overhead should be added: %v", got)
	}
}
