package framework

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/types"

	"github.com/arenadata/ad-scheduler/pkg/config"
	"github.com/arenadata/ad-scheduler/pkg/queue"
	"github.com/arenadata/ad-scheduler/pkg/resource"
)

func TestStateDumpReflectsTreeAndBookings(t *testing.T) {
	resetEnginesForTest()
	e := GetOrInitEngine("ad-scheduler", config.Defaults())

	mgr, err := queue.NewManager(&queue.Spec{
		Max:      resource.Resource{"cpu": 1000},
		Children: []*queue.Spec{{Name: "team", Children: []*queue.Spec{{Name: "spark", ServiceAccounts: []string{"sa"}}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	e.Rebuild(mgr)

	// book a pod into the leaf and admit a gang.
	e.Reserve(types.UID("p1"), "root.team.spark", resource.Resource{"cpu": 200})
	e.Commit(types.UID("p1"))
	if _, err := e.AdmitGang("root.team.spark", "team-a/g1", resource.Resource{"cpu": 300}); err != nil {
		t.Fatal(err)
	}

	sd := e.StateDump()
	if !sd.Ready || sd.Generation == 0 {
		t.Fatalf("dump should be ready with a generation: %+v", sd)
	}
	// find the leaf
	var leaf *QueueDump
	for i := range sd.Queues {
		if sd.Queues[i].Path == "root.team.spark" {
			leaf = &sd.Queues[i]
		}
	}
	if leaf == nil {
		t.Fatal("leaf queue missing from dump")
	}
	if leaf.Allocated["cpu"] != 200 {
		t.Fatalf("leaf allocated cpu = %d, want 200", leaf.Allocated["cpu"])
	}
	if leaf.Allocating["cpu"] != 300 { // the gang reservation
		t.Fatalf("leaf allocating cpu = %d, want 300 (gang)", leaf.Allocating["cpu"])
	}
	if len(sd.Gangs) != 1 || sd.Gangs[0].Key != "team-a/g1" {
		t.Fatalf("gang dump = %+v", sd.Gangs)
	}

	// root rolls up both.
	var root *QueueDump
	for i := range sd.Queues {
		if sd.Queues[i].Path == "root" {
			root = &sd.Queues[i]
		}
	}
	if root.Allocated["cpu"] != 200 || root.Allocating["cpu"] != 300 {
		t.Fatalf("root roll-up wrong: alloc=%v allocating=%v", root.Allocated, root.Allocating)
	}

	// metrics text mentions the queue series.
	m := e.metricsText()
	if !strings.Contains(m, `ad_queue_allocated{queue="root.team.spark",dim="cpu"} 200`) {
		t.Fatalf("metrics missing allocated series:\n%s", m)
	}
	if !strings.Contains(m, "ad_scheduler_ready 1") {
		t.Fatal("metrics missing ready gauge")
	}
	// root: used (alloc 200 + allocating 300) = 500 of max 1000 -> dominant share 0.5.
	if !strings.Contains(m, `ad_queue_dominant_share{queue="root"} 0.5`) {
		t.Fatalf("metrics missing/incorrect dominant-share series:\n%s", m)
	}

	// AdmittedGangs snapshot feeds durable PodGroup.status recovery.
	gangs := e.AdmittedGangs()
	g, ok := gangs["team-a/g1"]
	if !ok {
		t.Fatalf("AdmittedGangs missing team-a/g1: %v", gangs)
	}
	if g.Leaf != "root.team.spark" || g.MinRes["cpu"] != 300 {
		t.Fatalf("AdmittedGangs snapshot wrong: leaf=%q minRes=%v", g.Leaf, g.MinRes)
	}
}

func TestMetricAllowlist(t *testing.T) {
	resetEnginesForTest()
	cfg := config.Defaults()
	cfg.MetricDimensionAllowlist = []string{"cpu"}
	e := GetOrInitEngine("allowlist", cfg)
	mgr, err := queue.NewManager(&queue.Spec{
		Max:      resource.Resource{"cpu": 1000, "memory": 1 << 30},
		Children: []*queue.Spec{{Name: "t", ServiceAccounts: []string{"sa"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	e.Rebuild(mgr)
	e.Reserve(types.UID("p"), "root.t", resource.Resource{"cpu": 100, "memory": 1 << 20})
	e.Commit(types.UID("p"))
	m := e.metricsText()
	if !strings.Contains(m, `dim="cpu"`) {
		t.Error("cpu dim should be present (allowed)")
	}
	if strings.Contains(m, `dim="memory"`) {
		t.Error("memory dim must be filtered out (not in allowlist)")
	}
}
