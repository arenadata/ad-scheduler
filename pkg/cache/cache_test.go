package cache

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/arenadata/ad-scheduler/pkg/resource"
	"github.com/arenadata/ad-scheduler/pkg/util"
)

func TestNodeAvailable(t *testing.T) {
	n := &Node{
		Name:        "n1",
		InPool:      true,
		Allocatable: resource.Resource{"cpu": 4000, "memory": 8 << 30},
	}
	n.AddForeign(resource.Resource{"cpu": 500, "memory": 1 << 30}) // a DaemonSet
	n.AddOurs(resource.Resource{"cpu": 1000, "memory": 2 << 30})   // one of our pods

	want := resource.Resource{"cpu": 2500, "memory": 5 << 30}
	if !resource.Equal(n.Available(), want) {
		t.Fatalf("Available = %v, want %v", n.Available(), want)
	}
	// removing our pod frees it back
	n.RemoveOurs(resource.Resource{"cpu": 1000, "memory": 2 << 30})
	if !resource.Equal(n.Available(), resource.Resource{"cpu": 3500, "memory": 7 << 30}) {
		t.Fatalf("Available after remove = %v", n.Available())
	}
}

func TestNodeAvailableClampsOversubscription(t *testing.T) {
	n := &Node{Allocatable: resource.Resource{"cpu": 1000}}
	n.AddForeign(resource.Resource{"cpu": 2000}) // oversubscribed
	if got := n.Available(); !resource.Equal(got, resource.Resource{}) {
		t.Fatalf("oversubscribed dim must clamp to 0, got %v", got)
	}
}

func TestNewNode(t *testing.T) {
	kn := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Status:     corev1.NodeStatus{Allocatable: corev1.ResourceList{corev1.ResourceCPU: apiresource.MustParse("4"), corev1.ResourceMemory: apiresource.MustParse("8Gi")}},
	}
	n := NewNode(kn, true)
	if !n.InPool || !resource.Equal(n.Allocatable, resource.Resource{"cpu": 4000, "memory": 8 << 30}) {
		t.Fatalf("NewNode wrong: inPool=%v alloc=%v", n.InPool, n.Allocatable)
	}
}

func TestTaskFSM(t *testing.T) {
	tk := &Task{Namespace: "ns", Name: "p", state: TaskNew}
	for _, to := range []TaskState{TaskPending, TaskScheduling, TaskAllocated, TaskBound, TaskTerminated} {
		if err := tk.Transition(to); err != nil {
			t.Fatalf("legal transition to %s failed: %v", to, err)
		}
	}
	// illegal: Terminated is terminal
	if err := tk.Transition(TaskPending); err == nil {
		t.Fatal("Terminated -> Pending must be illegal")
	}
	// illegal skip: New -> Bound
	fresh := &Task{state: TaskNew}
	if err := fresh.Transition(TaskBound); err == nil {
		t.Fatal("New -> Bound must be illegal")
	}
	// bind-failure path: Allocated -> Pending is legal
	al := &Task{state: TaskAllocated}
	if err := al.Transition(TaskPending); err != nil {
		t.Fatalf("Allocated -> Pending (bind failure) must be legal: %v", err)
	}
}

func TestAppFSMAndPending(t *testing.T) {
	a := NewApplication("app1", util.Identity{Namespace: "ns", ServiceAccount: "spark"})
	if a.State() != AppNew {
		t.Fatal("fresh app must be New")
	}
	if err := a.Transition(AppAccepted); err != nil {
		t.Fatal(err)
	}
	if err := a.Transition(AppNew); err == nil {
		t.Fatal("Accepted -> New must be illegal")
	}

	a.AddTask(&Task{UID: "u1", Request: resource.Resource{"cpu": 500}, state: TaskPending})
	a.AddTask(&Task{UID: "u2", Request: resource.Resource{"cpu": 1500}, state: TaskBound})
	a.AddTask(&Task{UID: "u3", Request: resource.Resource{"cpu": 9000}, state: TaskTerminated}) // excluded
	if got := a.PendingRequest(); !resource.Equal(got, resource.Resource{"cpu": 2000}) {
		t.Fatalf("PendingRequest excludes terminated: %v", got)
	}
	if _, ok := a.Task("u1"); !ok {
		t.Fatal("Task lookup failed")
	}
}
