package capacity

import (
	"context"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	fwk "k8s.io/kube-scheduler/framework"
	kfwk "k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/arenadata/ad-scheduler/pkg/queue"
	"github.com/arenadata/ad-scheduler/pkg/resource"
)

func nodeInfo(labelsMap map[string]string) *kfwk.NodeInfo {
	ni := kfwk.NewNodeInfo()
	ni.SetNode(&v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1", Labels: labelsMap}})
	return ni
}

func TestFilterOffPool(t *testing.T) {
	sel, _ := labels.Parse("scheduler.arenadata.io/name=ad-scheduler")
	c := &Capacity{poolSelector: sel}

	// node in the pool -> Success (nil status)
	if st := c.Filter(context.Background(), nil, nil, nodeInfo(map[string]string{"scheduler.arenadata.io/name": "ad-scheduler"})); st != nil {
		t.Fatalf("pool node must pass Filter, got %v", st)
	}
	// node without the pool label -> rejected
	st := c.Filter(context.Background(), nil, nil, nodeInfo(map[string]string{"other": "x"}))
	if st == nil || st.Code() != fwk.UnschedulableAndUnresolvable {
		t.Fatalf("off-pool node must be UnschedulableAndUnresolvable, got %v", st)
	}
}

func TestReadStateRoundTrip(t *testing.T) {
	state := kfwk.NewCycleState()
	// missing state -> error
	if _, err := readState(state); err == nil {
		t.Fatal("readState on empty CycleState must error")
	}
	// written by PreFilter -> read back by Reserve
	want := &preFilterState{leaf: "root.team.spark", req: resource.Resource{"cpu": 1000}}
	state.Write(stateKey, want)
	got, err := readState(state)
	if err != nil {
		t.Fatal(err)
	}
	if got.leaf != want.leaf || !resource.Equal(got.req, want.req) {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

func TestUnschedulableFor(t *testing.T) {
	// no PodScheduled condition -> just entered scheduling (zero elapsed).
	if d := unschedulableFor(&v1.Pod{}); d != 0 {
		t.Errorf("no condition should be 0, got %v", d)
	}
	// a PodScheduled=False set an hour ago -> ~1h elapsed.
	pod := &v1.Pod{Status: v1.PodStatus{Conditions: []v1.PodCondition{{
		Type:               v1.PodScheduled,
		Status:             v1.ConditionFalse,
		LastTransitionTime: metav1.NewTime(time.Now().Add(-time.Hour)),
	}}}}
	if d := unschedulableFor(pod); d < 59*time.Minute {
		t.Errorf("unschedulableFor = %v, want >= ~1h", d)
	}
	// PodScheduled=True -> not unschedulable (0).
	pod.Status.Conditions[0].Status = v1.ConditionTrue
	if d := unschedulableFor(pod); d != 0 {
		t.Errorf("scheduled pod should be 0, got %v", d)
	}
}

func TestScoreSpreadVsBinpack(t *testing.T) {
	ni := kfwk.NewNodeInfo()
	ni.SetNode(&v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Status:     v1.NodeStatus{Allocatable: v1.ResourceList{v1.ResourceCPU: apiresource.MustParse("4")}},
	})
	pod := &v1.Pod{Spec: v1.PodSpec{Containers: []v1.Container{{
		Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: apiresource.MustParse("1")}},
	}}}}
	state := kfwk.NewCycleState()
	// used=0, req=1, cap=4 -> dominant share 0.25.
	if s, _ := (&Capacity{nodeSortPolicy: queue.NodeSpread}).Score(context.Background(), state, pod, ni); s != 75 {
		t.Errorf("spread score = %d, want 75 (least-utilised favoured)", s)
	}
	if s, _ := (&Capacity{nodeSortPolicy: queue.NodeBinpack}).Score(context.Background(), state, pod, ni); s != 25 {
		t.Errorf("binpack score = %d, want 25", s)
	}
}

func TestSaInSet(t *testing.T) {
	if !saInSet("spark", []string{"eval", "spark"}) {
		t.Error("exact SA should match")
	}
	if !saInSet("anything", []string{"*"}) {
		t.Error("wildcard should match")
	}
	if saInSet("spark", []string{"eval"}) {
		t.Error("non-member must not match")
	}
}
