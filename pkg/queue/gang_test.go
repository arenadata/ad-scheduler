package queue

import (
	"sort"
	"testing"

	"github.com/arenadata/ad-scheduler/pkg/resource"
)

func gangTree(t *testing.T) *QueueManager {
	t.Helper()
	m, err := NewManager(&Spec{
		Max: resource.Resource{"cpu": 100},
		Children: []*Spec{
			{Name: "team", Max: resource.Resource{"cpu": 30}, Children: []*Spec{{Name: "gang"}}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestAdmitGangAtomicCAS(t *testing.T) {
	m := gangTree(t)
	leaf := "root.team.gang"

	// gang A (cpu 20) fits team's 30 -> admitted, reservation rolled up.
	ok, err := m.AdmitGang(leaf, "A", resource.Resource{"cpu": 20})
	if err != nil || !ok {
		t.Fatalf("A should admit: ok=%v err=%v", ok, err)
	}
	team, _ := m.Queue("root.team")
	if !resource.Equal(team.Allocating(), resource.Resource{"cpu": 20}) {
		t.Fatalf("team allocating after A = %v, want cpu:20", team.Allocating())
	}

	// gang B (cpu 20) would push team to 40 > 30 -> rejected, nothing booked.
	ok, _ = m.AdmitGang(leaf, "B", resource.Resource{"cpu": 20})
	if ok {
		t.Fatal("B must be rejected: whole-gang-or-nothing over headroom")
	}
	if resource.Equal(team.Allocating(), resource.Resource{"cpu": 40}) {
		t.Fatal("rejected gang must book nothing")
	}
	if m.IsGangAdmitted("B") {
		t.Fatal("rejected gang must not be recorded")
	}

	// gang C (cpu 10) exactly fills remaining headroom.
	if ok, _ := m.AdmitGang(leaf, "C", resource.Resource{"cpu": 10}); !ok {
		t.Fatal("C (cpu 10) should fit the remaining 10")
	}
}

func TestAdmitGangIdempotent(t *testing.T) {
	m := gangTree(t)
	leaf := "root.team.gang"
	_, _ = m.AdmitGang(leaf, "A", resource.Resource{"cpu": 20})
	// re-admit same gang: idempotent, no double booking.
	ok, _ := m.AdmitGang(leaf, "A", resource.Resource{"cpu": 20})
	if !ok {
		t.Fatal("re-admit should return true")
	}
	team, _ := m.Queue("root.team")
	if !resource.Equal(team.Allocating(), resource.Resource{"cpu": 20}) {
		t.Fatalf("re-admit must not double-book: %v", team.Allocating())
	}
}

func TestReleaseGangIdempotent(t *testing.T) {
	m := gangTree(t)
	leaf := "root.team.gang"
	_, _ = m.AdmitGang(leaf, "A", resource.Resource{"cpu": 20})

	if err := m.ReleaseGang("A"); err != nil {
		t.Fatal(err)
	}
	root, _ := m.Queue("root")
	if !root.Allocating().IsEmpty() {
		t.Fatalf("release should free the reservation up to root: %v", root.Allocating())
	}
	// releasing again, or an unknown gang, is a harmless no-op.
	if err := m.ReleaseGang("A"); err != nil {
		t.Fatal(err)
	}
	if err := m.ReleaseGang("never"); err != nil {
		t.Fatal(err)
	}
	if m.IsGangAdmitted("A") {
		t.Fatal("A should be gone after release")
	}
}

func TestAdmitGangRejectsBadInput(t *testing.T) {
	m := gangTree(t)
	if _, err := m.AdmitGang("root.team.gang", "", resource.Resource{"cpu": 1}); err == nil {
		t.Fatal("empty gangID must error")
	}
	if _, err := m.AdmitGang("root.team.gang", "z", resource.Resource{}); err == nil {
		t.Fatal("empty minResources must error")
	}
	if _, err := m.AdmitGang("root.team", "z", resource.Resource{"cpu": 1}); err == nil {
		t.Fatal("non-leaf target must error")
	}
}

func TestAdmitGangExceedsMaxIsPermanent(t *testing.T) {
	m := gangTree(t) // team max cpu 30
	leaf := "root.team.gang"
	// A gang asking more than the queue ceiling can NEVER fit: it must surface an
	// error (a permanent misconfiguration), not gate quietly as transient (false,nil).
	ok, err := m.AdmitGang(leaf, "toobig", resource.Resource{"cpu": 50})
	if ok {
		t.Fatal("over-max gang must not admit")
	}
	if err == nil {
		t.Fatal("over-max gang must return an error (never fits), not a silent transient gate")
	}
	if m.IsGangAdmitted("toobig") {
		t.Fatal("rejected gang must not be recorded")
	}
	// A gang within max but currently lacking headroom stays a transient (false,nil).
	if _, _ = m.AdmitGang(leaf, "fill", resource.Resource{"cpu": 30}); !m.IsGangAdmitted("fill") {
		t.Fatal("fill gang (cpu 30) should admit and fill the queue")
	}
	ok, err = m.AdmitGang(leaf, "wait", resource.Resource{"cpu": 10})
	if ok || err != nil {
		t.Fatalf("within-max but no-headroom gang must be transient (false,nil): ok=%v err=%v", ok, err)
	}
}

func TestWoundWaitOrdering(t *testing.T) {
	gangs := []WoundWaitKey{
		{EffectivePriority: 1, Age: 100, UID: "old-lowprio"},
		{EffectivePriority: 5, Age: 10, UID: "new-hiprio"},
		{EffectivePriority: 5, Age: 50, UID: "old-hiprio"},
		{EffectivePriority: 5, Age: 50, UID: "aaa-hiprio"},
	}
	sort.SliceStable(gangs, func(i, j int) bool { return WoundWaitLess(gangs[i], gangs[j]) })
	got := []string{gangs[0].UID, gangs[1].UID, gangs[2].UID, gangs[3].UID}
	want := []string{"aaa-hiprio", "old-hiprio", "new-hiprio", "old-lowprio"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("wound-wait order = %v, want %v", got, want)
		}
	}
}
