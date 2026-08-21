package queue

import (
	"testing"

	"github.com/arenadata/ad-scheduler/pkg/resource"
)

// tree:
//
//	root (max cpu 100, mem 200)
//	├── team (max cpu 60)         <- bounds cpu only
//	│   ├── spark (guaranteed cpu 10)
//	│   └── batch
//	└── infra (max cpu 40)
func testTree(t *testing.T) *QueueManager {
	t.Helper()
	m, err := NewManager(&Spec{
		Max: resource.Resource{"cpu": 100, "memory": 200},
		Children: []*Spec{
			{Name: "team", Max: resource.Resource{"cpu": 60}, Children: []*Spec{
				{Name: "spark", Guaranteed: resource.Resource{"cpu": 10}},
				{Name: "batch"},
			}},
			{Name: "infra", Max: resource.Resource{"cpu": 40}},
		},
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return m
}

func TestRollUpToRoot(t *testing.T) {
	m := testTree(t)
	if err := m.Reserve("root.team.spark", resource.Resource{"cpu": 5, "memory": 10}); err != nil {
		t.Fatal(err)
	}
	// allocating rolled up spark -> team -> root
	for _, p := range []string{"root.team.spark", "root.team", "root"} {
		q, _ := m.Queue(p)
		if !resource.Equal(q.Allocating(), resource.Resource{"cpu": 5, "memory": 10}) {
			t.Fatalf("%s allocating = %v", p, q.Allocating())
		}
	}
	// sibling and its subtree untouched
	if q, _ := m.Queue("root.infra"); !q.Allocating().IsEmpty() {
		t.Fatalf("infra should be untouched: %v", q.Allocating())
	}

	// commit: allocating -> allocated, used unchanged
	if err := m.Commit("root.team.spark", resource.Resource{"cpu": 5, "memory": 10}); err != nil {
		t.Fatal(err)
	}
	root, _ := m.Queue("root")
	if !root.Allocating().IsEmpty() || !resource.Equal(root.Allocated(), resource.Resource{"cpu": 5, "memory": 10}) {
		t.Fatalf("after commit root alloc=%v allocating=%v", root.Allocated(), root.Allocating())
	}

	// release frees it back up the path
	if err := m.Release("root.team.spark", resource.Resource{"cpu": 5, "memory": 10}); err != nil {
		t.Fatal(err)
	}
	if !root.Allocated().IsEmpty() {
		t.Fatalf("after release root allocated = %v", root.Allocated())
	}
}

func TestCanFitBorrowUpToAncestorMax(t *testing.T) {
	m := testTree(t)
	spark, _ := m.Queue("root.team.spark")

	// spark has no own max: it may borrow up to team's cpu=60 and root's mem=200.
	if !spark.CanFit(resource.Resource{"cpu": 60}) {
		t.Fatal("spark should borrow up to team cpu=60")
	}
	if spark.CanFit(resource.Resource{"cpu": 61}) {
		t.Fatal("cpu=61 must be rejected by team's max=60")
	}
	// memory bounded only at root=200
	if !spark.CanFit(resource.Resource{"memory": 200}) {
		t.Fatal("mem=200 fits at root")
	}
	if spark.CanFit(resource.Resource{"memory": 201}) {
		t.Fatal("mem=201 exceeds root max")
	}
	// a dimension bounded nowhere on the path is unlimited
	if !spark.CanFit(resource.Resource{"nvidia.com/gpu": 1 << 40}) {
		t.Fatal("gpu unbounded on path should always fit")
	}
}

func TestCanFitAccountsSiblingUsage(t *testing.T) {
	m := testTree(t)
	// batch takes 50 cpu; team now has only 10 cpu of its 60 left.
	if err := m.Reserve("root.team.batch", resource.Resource{"cpu": 50}); err != nil {
		t.Fatal(err)
	}
	spark, _ := m.Queue("root.team.spark")
	if !spark.CanFit(resource.Resource{"cpu": 10}) {
		t.Fatal("spark should fit within team's remaining 10 cpu")
	}
	if spark.CanFit(resource.Resource{"cpu": 11}) {
		t.Fatal("spark cpu=11 must be rejected: team only has 10 left")
	}
}

func TestPathHeadroomMinAndSaturation(t *testing.T) {
	m := testTree(t)
	// use 55 of team's 60 cpu via batch; spark headroom cpu should be 5 (team),
	// memory should be 200 (root), gpu absent (unbounded).
	if err := m.Reserve("root.team.batch", resource.Resource{"cpu": 55}); err != nil {
		t.Fatal(err)
	}
	spark, _ := m.Queue("root.team.spark")
	hr := spark.PathHeadroom()
	if hr["cpu"] != 5 {
		t.Fatalf("cpu headroom = %d, want 5", hr["cpu"])
	}
	if hr["memory"] != 200 {
		t.Fatalf("mem headroom = %d, want 200", hr["memory"])
	}
	if _, ok := hr["nvidia.com/gpu"]; ok {
		t.Fatalf("gpu must be absent (unbounded), got %v", hr)
	}

	// saturate team cpu fully: spark cpu headroom must read 0, not root's 100.
	if err := m.Reserve("root.team.batch", resource.Resource{"cpu": 5}); err != nil {
		t.Fatal(err)
	}
	if hr := spark.PathHeadroom(); hr["cpu"] != 0 {
		t.Fatalf("saturated team cpu must report 0 headroom, got %d", hr["cpu"])
	}
}

func TestUnreserveReverses(t *testing.T) {
	m := testTree(t)
	r := resource.Resource{"cpu": 7}
	_ = m.Reserve("root.team.spark", r)
	_ = m.Unreserve("root.team.spark", r)
	root, _ := m.Queue("root")
	if !root.Allocating().IsEmpty() {
		t.Fatalf("unreserve should zero allocating, got %v", root.Allocating())
	}
}

func TestMutateRejectsNonLeafAndUnknown(t *testing.T) {
	m := testTree(t)
	if err := m.Reserve("root.team", resource.Resource{"cpu": 1}); err == nil {
		t.Fatal("booking on a non-leaf must error")
	}
	if err := m.Reserve("root.nope", resource.Resource{"cpu": 1}); err == nil {
		t.Fatal("booking on unknown queue must error")
	}
}

func TestBuildValidation(t *testing.T) {
	cases := map[string]*Spec{
		"child max exceeds parent": {
			Max:      resource.Resource{"cpu": 10},
			Children: []*Spec{{Name: "c", Max: resource.Resource{"cpu": 20}}},
		},
		"guaranteed sum exceeds parent": {
			Guaranteed: resource.Resource{"cpu": 10},
			Children: []*Spec{
				{Name: "a", Guaranteed: resource.Resource{"cpu": 6}},
				{Name: "b", Guaranteed: resource.Resource{"cpu": 6}},
			},
		},
		"duplicate sibling": {
			Children: []*Spec{{Name: "dup"}, {Name: "dup"}},
		},
		"dotted name": {
			Children: []*Spec{{Name: "a.b"}},
		},
		"negative dimension": {
			Children: []*Spec{{Name: "c", Max: resource.Resource{"cpu": -1}}},
		},
	}
	for name, spec := range cases {
		if _, err := NewManager(spec); err == nil {
			t.Errorf("%s: expected build error, got nil", name)
		}
	}
}

func TestBuildValidTreeAndLeaves(t *testing.T) {
	m := testTree(t)
	got := []string{}
	for _, l := range m.Leaves() {
		got = append(got, l.Path())
	}
	want := []string{"root.infra", "root.team.batch", "root.team.spark"}
	if len(got) != len(want) {
		t.Fatalf("leaves = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("leaves = %v, want %v", got, want)
		}
	}
}
