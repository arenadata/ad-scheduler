package framework

import (
	"testing"

	"github.com/arenadata/ad-scheduler/pkg/config"
	"github.com/arenadata/ad-scheduler/pkg/queue"
	"github.com/arenadata/ad-scheduler/pkg/resource"
)

// A younger gang that WOULD fit is held back while an older/larger gang waits for
// headroom (wound-wait head-of-line), then both proceed in order once capacity
// frees.
func TestAdmitGangWWHeadOfLine(t *testing.T) {
	resetEnginesForTest()
	e := GetOrInitEngine("ad-scheduler", config.Defaults())
	mgr, err := queue.NewManager(&queue.Spec{Children: []*queue.Spec{
		{Name: "team", Children: []*queue.Spec{{Name: "g", Max: resource.Resource{"cpu": 20}}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	e.Rebuild(mgr)
	leaf := "root.team.g"

	// Filler holds 15 of 20 -> 5 free.
	if ok, _ := e.AdmitGang(leaf, "filler", resource.Resource{"cpu": 15}); !ok {
		t.Fatal("filler should admit")
	}
	big := queue.WoundWaitKey{Age: 100, UID: "big"}    // older -> higher rank
	small := queue.WoundWaitKey{Age: 50, UID: "small"} // younger

	// BIG needs 10 (> 5 free) -> does not fit, becomes the head-of-line reservation.
	if ok, _ := e.AdmitGangWW(leaf, "big", resource.Resource{"cpu": 10}, big); ok {
		t.Fatal("big should not fit yet")
	}
	// SMALL needs 5 (would fit the 5 free) but must yield to the older head-of-line BIG.
	if ok, _ := e.AdmitGangWW(leaf, "small", resource.Resource{"cpu": 5}, small); ok || e.IsGangAdmitted("small") {
		t.Fatal("small must be held back by the head-of-line reservation for big")
	}
	// Free the filler -> BIG now fits and releases the head-of-line.
	_ = e.ReleaseGang("filler")
	if ok, _ := e.AdmitGangWW(leaf, "big", resource.Resource{"cpu": 10}, big); !ok {
		t.Fatal("big should admit once headroom frees")
	}
	// SMALL can now proceed (head-of-line cleared).
	if ok, _ := e.AdmitGangWW(leaf, "small", resource.Resource{"cpu": 5}, small); !ok {
		t.Fatal("small should admit after big")
	}
}

// The head-of-line reservation must never block a gang of equal-or-lower rank
// from taking headroom the head-of-line gang is not itself claiming — verified by
// the ordering: a lone gang always admits when it fits.
func TestAdmitGangWWLoneGangAdmits(t *testing.T) {
	resetEnginesForTest()
	e := GetOrInitEngine("ad-scheduler", config.Defaults())
	mgr, _ := queue.NewManager(&queue.Spec{Children: []*queue.Spec{
		{Name: "team", Children: []*queue.Spec{{Name: "g", Max: resource.Resource{"cpu": 20}}}},
	}})
	e.Rebuild(mgr)
	if ok, _ := e.AdmitGangWW("root.team.g", "solo", resource.Resource{"cpu": 5},
		queue.WoundWaitKey{Age: 1, UID: "solo"}); !ok {
		t.Fatal("a lone fitting gang must admit")
	}
}

// One leaf per gang: once a gang is admitted on a leaf, any attempt to admit the
// same gang key on a DIFFERENT leaf (a member whose SA routes it elsewhere) is
// rejected, and the gang stays bound to its original leaf. GangLeaf exposes the
// binding the gang plugin checks each member against.
func TestGangOneLeafPerGang(t *testing.T) {
	resetEnginesForTest()
	e := GetOrInitEngine("ad-scheduler", config.Defaults())
	mgr, err := queue.NewManager(&queue.Spec{Children: []*queue.Spec{
		{Name: "team", Children: []*queue.Spec{
			{Name: "a", Max: resource.Resource{"cpu": 20}},
			{Name: "b", Max: resource.Resource{"cpu": 20}},
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	e.Rebuild(mgr)
	const leafA, leafB, key = "root.team.a", "root.team.b", "g1"

	if ok, err := e.AdmitGang(leafA, key, resource.Resource{"cpu": 5}); !ok || err != nil {
		t.Fatalf("gang should admit on leaf a: ok=%v err=%v", ok, err)
	}
	if got, ok := e.GangLeaf(key); !ok || got != leafA {
		t.Fatalf("GangLeaf = %q,%v; want %q,true", got, ok, leafA)
	}
	// same key, different leaf -> rejected, binding unchanged.
	if ok, err := e.AdmitGang(leafB, key, resource.Resource{"cpu": 5}); ok || err == nil {
		t.Fatalf("admitting the gang on a second leaf must fail: ok=%v err=%v", ok, err)
	}
	if ok, err := e.AdmitGangWW(leafB, key, resource.Resource{"cpu": 5}, queue.WoundWaitKey{UID: key}); ok || err == nil {
		t.Fatalf("AdmitGangWW on a second leaf must fail too: ok=%v err=%v", ok, err)
	}
	if got, _ := e.GangLeaf(key); got != leafA {
		t.Fatalf("gang must stay bound to leaf a, got %q", got)
	}
	// same key, same leaf -> idempotent success.
	if ok, err := e.AdmitGang(leafA, key, resource.Resource{"cpu": 5}); !ok || err != nil {
		t.Fatalf("re-admitting on the same leaf must be idempotent: ok=%v err=%v", ok, err)
	}
	// after release, no binding.
	_ = e.ReleaseGang(key)
	if got, ok := e.GangLeaf(key); ok {
		t.Fatalf("released gang must have no leaf binding, got %q,%v", got, ok)
	}
}
