package cache

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/arenadata/ad-scheduler/pkg/resource"
)

func node(name string, inPool bool, cpu, mem string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status:     corev1.NodeStatus{Allocatable: corev1.ResourceList{corev1.ResourceCPU: apiresource.MustParse(cpu), corev1.ResourceMemory: apiresource.MustParse(mem)}},
	}
}

func pod(name, nodeName, sched, cpu string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Spec: corev1.PodSpec{
			NodeName:      nodeName,
			SchedulerName: sched,
			Containers:    []corev1.Container{{Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: apiresource.MustParse(cpu)}}}},
		},
	}
}

func TestCacheOwnershipAccounting(t *testing.T) {
	c := New("ad-scheduler")
	c.UpsertNode(node("n1", true, "4", "8Gi"), true)

	c.OnPodBound(pod("ours", "n1", "ad-scheduler", "1"))           // 1000m ours
	c.OnPodBound(pod("theirs", "n1", "default-scheduler", "500m")) // 500m foreign (e.g. DaemonSet)
	c.OnPodBound(pod("unbound", "", "ad-scheduler", "9"))          // not bound -> ignored
	c.OnPodBound(pod("elsewhere", "n2", "ad-scheduler", "9"))      // unknown node -> ignored

	n, _ := c.Node("n1")
	if !resource.Equal(n.Ours, resource.Resource{"cpu": 1000}) {
		t.Fatalf("ours = %v", n.Ours)
	}
	if !resource.Equal(n.Foreign, resource.Resource{"cpu": 500}) {
		t.Fatalf("foreign = %v", n.Foreign)
	}
	if !resource.Equal(n.Available(), resource.Resource{"cpu": 2500, "memory": 8 << 30}) {
		t.Fatalf("available = %v", n.Available())
	}

	// deleting reverses the right tally
	c.OnPodDeleted(pod("theirs", "n1", "default-scheduler", "500m"))
	if !resource.Equal(n.Foreign, resource.Resource{}) {
		t.Fatalf("foreign after delete = %v", n.Foreign)
	}
}

func TestPoolAvailableOnlyCountsPoolNodes(t *testing.T) {
	c := New("ad-scheduler")
	c.UpsertNode(node("pool", true, "4", "0"), true)
	c.UpsertNode(node("general", false, "8", "0"), false) // not in pool -> excluded
	if got := c.PoolAvailable(); !resource.Equal(got, resource.Resource{"cpu": 4000}) {
		t.Fatalf("PoolAvailable should count only pool nodes: %v", got)
	}
}
