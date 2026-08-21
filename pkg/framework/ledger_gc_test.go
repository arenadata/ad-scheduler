package framework

import (
	"testing"

	"k8s.io/apimachinery/pkg/types"

	"github.com/arenadata/ad-scheduler/pkg/config"
	"github.com/arenadata/ad-scheduler/pkg/queue"
	"github.com/arenadata/ad-scheduler/pkg/resource"
)

func gcTestEngine(t *testing.T) *Engine {
	t.Helper()
	resetEnginesForTest()
	e := GetOrInitEngine("ad-scheduler", config.Defaults())
	mgr, err := queue.NewManager(&queue.Spec{Children: []*queue.Spec{
		{Name: "team", Children: []*queue.Spec{{Name: "g", Max: resource.Resource{"cpu": 100}}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	e.Rebuild(mgr)
	return e
}

// A booking whose pod has vanished from the informer (missed delete) is released
// by GCBookings — allocating for a still-reserved one, allocated for a committed
// one — while a live pod's booking is untouched.
func TestGCBookingsReleasesVanishedPods(t *testing.T) {
	e := gcTestEngine(t)
	const leaf = "root.team.g"
	live, gone := types.UID("live"), types.UID("gone")

	e.Reserve(live, leaf, resource.Resource{"cpu": 10})
	e.Commit(live)                                      // -> allocated 10
	e.Reserve(gone, leaf, resource.Resource{"cpu": 30}) // -> allocating 30

	q, _ := e.Tree().Queue(leaf)
	if used := q.Used()["cpu"]; used != 40 {
		t.Fatalf("used before GC = %d; want 40", used)
	}
	if n := e.GCBookings(map[types.UID]bool{live: true}); n != 1 {
		t.Fatalf("GCBookings released %d; want 1", n)
	}
	q, _ = e.Tree().Queue(leaf)
	if used := q.Used()["cpu"]; used != 10 {
		t.Fatalf("used after GC = %d; want 10 (only the live commit remains)", used)
	}
	// idempotent: nothing more to free with the same live set.
	if n := e.GCBookings(map[types.UID]bool{live: true}); n != 0 {
		t.Fatalf("second GCBookings released %d; want 0", n)
	}
	// the live booking is intact and still releases normally.
	e.ObserveDeleted(live)
	q, _ = e.Tree().Queue(leaf)
	if used := q.Used()["cpu"]; used != 0 {
		t.Fatalf("used after releasing live = %d; want 0", used)
	}
}

// A gang reservation whose PodGroup has vanished (missed delete) is released by
// GCGangs; a gang whose PodGroup is still present is kept.
func TestGCGangsReleasesVanishedPodGroups(t *testing.T) {
	e := gcTestEngine(t)
	const leaf = "root.team.g"
	if ok, _ := e.AdmitGang(leaf, "ns/live", resource.Resource{"cpu": 10}); !ok {
		t.Fatal("live gang should admit")
	}
	if ok, _ := e.AdmitGang(leaf, "ns/gone", resource.Resource{"cpu": 20}); !ok {
		t.Fatal("gone gang should admit")
	}
	if n := e.GCGangs(map[string]bool{"ns/live": true}); n != 1 {
		t.Fatalf("GCGangs released %d; want 1", n)
	}
	if _, ok := e.GangLeaf("ns/gone"); ok {
		t.Fatal("ns/gone reservation must be released")
	}
	if got, ok := e.GangLeaf("ns/live"); !ok || got != leaf {
		t.Fatalf("ns/live must remain bound to %q, got %q,%v", leaf, got, ok)
	}
	if n := e.GCGangs(map[string]bool{"ns/live": true}); n != 0 {
		t.Fatalf("second GCGangs released %d; want 0", n)
	}
}
