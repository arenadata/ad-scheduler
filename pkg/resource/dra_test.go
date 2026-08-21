package resource

import (
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func exactReq(name, class string, mode resourceapi.DeviceAllocationMode, count int64) resourceapi.DeviceRequest {
	return resourceapi.DeviceRequest{Name: name, Exactly: &resourceapi.ExactDeviceRequest{DeviceClassName: class, AllocationMode: mode, Count: count}}
}

func claimWith(reqs ...resourceapi.DeviceRequest) *resourceapi.ResourceClaim {
	return &resourceapi.ResourceClaim{Spec: resourceapi.ResourceClaimSpec{Devices: resourceapi.DeviceClaim{Requests: reqs}}}
}

func podWithClaims(names ...corev1.PodResourceClaim) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns"}, Spec: corev1.PodSpec{ResourceClaims: names}}
}

//go:fix inline
func strptr(s string) *string { return new(s) }

func TestPodDeviceRequestsNamedClaim(t *testing.T) {
	pod := podWithClaims(corev1.PodResourceClaim{Name: "gpus", ResourceClaimName: new("my-claim")})
	claims := map[string]*resourceapi.ResourceClaim{
		"my-claim": claimWith(exactReq("r", "gpu.example.com", resourceapi.DeviceAllocationModeExactCount, 2)),
	}
	lookup := func(_, name string) (*resourceapi.ResourceClaim, error) {
		if c, ok := claims[name]; ok {
			return c, nil
		}
		return nil, fmt.Errorf("not found")
	}
	got, err := PodDeviceRequests(pod, lookup, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if !Equal(got, Resource{"dra/gpu.example.com": 2}) {
		t.Fatalf("named claim = %v, want dra/gpu.example.com:2", got)
	}
}

func TestPodDeviceRequestsTemplateAndDefaultCount(t *testing.T) {
	pod := podWithClaims(corev1.PodResourceClaim{Name: "acc", ResourceClaimTemplateName: new("tmpl")})
	tmpl := &resourceapi.ResourceClaimTemplate{
		Spec: resourceapi.ResourceClaimTemplateSpec{Spec: resourceapi.ResourceClaimSpec{
			Devices: resourceapi.DeviceClaim{Requests: []resourceapi.DeviceRequest{
				exactReq("r", "fpga.example.com", "", 0), // default mode ExactCount, default count 1
			}},
		}},
	}
	tl := func(_, _ string) (*resourceapi.ResourceClaimTemplate, error) { return tmpl, nil }
	got, err := PodDeviceRequests(pod, nil, tl, false)
	if err != nil {
		t.Fatal(err)
	}
	if !Equal(got, Resource{"dra/fpga.example.com": 1}) {
		t.Fatalf("template = %v, want dra/fpga.example.com:1 (default count)", got)
	}
}

func TestPodDeviceRequestsAllModeFailOpenVsClosed(t *testing.T) {
	pod := podWithClaims(corev1.PodResourceClaim{Name: "all", ResourceClaimName: new("c")})
	lookup := func(_, _ string) (*resourceapi.ResourceClaim, error) {
		return claimWith(exactReq("r", "gpu", resourceapi.DeviceAllocationModeAll, 0)), nil
	}
	// fail-open: All is not statically countable -> skipped (empty)
	got, err := PodDeviceRequests(pod, lookup, nil, false)
	if err != nil || !got.IsEmpty() {
		t.Fatalf("fail-open All should skip: got=%v err=%v", got, err)
	}
	// fail-closed: All -> error
	if _, err := PodDeviceRequests(pod, lookup, nil, true); err == nil {
		t.Fatal("fail-closed All must error")
	}
}

func TestPodDeviceRequestsSumsMultiple(t *testing.T) {
	pod := podWithClaims(
		corev1.PodResourceClaim{Name: "a", ResourceClaimName: new("c1")},
		corev1.PodResourceClaim{Name: "b", ResourceClaimName: new("c2")},
	)
	claims := map[string]*resourceapi.ResourceClaim{
		"c1": claimWith(exactReq("r", "gpu", resourceapi.DeviceAllocationModeExactCount, 1)),
		"c2": claimWith(
			exactReq("r1", "gpu", resourceapi.DeviceAllocationModeExactCount, 2),
			exactReq("r2", "tpu", resourceapi.DeviceAllocationModeExactCount, 3),
		),
	}
	lookup := func(_, name string) (*resourceapi.ResourceClaim, error) { return claims[name], nil }
	got, err := PodDeviceRequests(pod, lookup, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if !Equal(got, Resource{"dra/gpu": 3, "dra/tpu": 3}) {
		t.Fatalf("multi-claim sum = %v, want dra/gpu:3 dra/tpu:3", got)
	}
}

func TestPodDeviceRequestsNoClaims(t *testing.T) {
	got, err := PodDeviceRequests(podWithClaims(), nil, nil, false)
	if err != nil || !got.IsEmpty() {
		t.Fatalf("no claims -> empty, got %v err %v", got, err)
	}
}
