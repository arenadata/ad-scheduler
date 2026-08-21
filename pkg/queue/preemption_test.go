package queue

import (
	"testing"

	"github.com/arenadata/ad-scheduler/pkg/resource"
)

func uids(vs []Victim) map[string]bool {
	m := map[string]bool{}
	for _, v := range vs {
		m[v.UID] = true
	}
	return m
}

func TestSelectVictimsGuaranteedProtection(t *testing.T) {
	need := resource.Resource{"cpu": 10}
	// the only pod big enough is protected (its queue is at guaranteed) -> no
	// feasible set, even though a smaller preemptable pod exists.
	got := SelectVictims(need, []Victim{
		{UID: "protected", Priority: 0, Request: resource.Resource{"cpu": 20}, Preemptable: false},
		{UID: "small", Priority: 0, Request: resource.Resource{"cpu": 3}, Preemptable: true},
	})
	if got != nil {
		t.Fatalf("must be infeasible without touching protected pod, got %v", uids(got))
	}
}

func TestSelectVictimsPrefersLowPriority(t *testing.T) {
	need := resource.Resource{"cpu": 5}
	got := SelectVictims(need, []Victim{
		{UID: "hi", Priority: 100, Request: resource.Resource{"cpu": 5}, Preemptable: true},
		{UID: "lo", Priority: 1, Request: resource.Resource{"cpu": 5}, Preemptable: true},
	})
	if len(got) != 1 || got[0].UID != "lo" {
		t.Fatalf("should evict the lowest-priority victim, got %v", uids(got))
	}
}

func TestSelectVictimsTrimsRedundant(t *testing.T) {
	need := resource.Resource{"cpu": 10}
	// lowest-priority 'a' is tried first but is too small alone; 'b' must be
	// evicted anyway and alone covers need, so 'a' is trimmed.
	got := SelectVictims(need, []Victim{
		{UID: "a", Priority: 1, Request: resource.Resource{"cpu": 2}, Preemptable: true},
		{UID: "b", Priority: 2, Request: resource.Resource{"cpu": 10}, Preemptable: true},
	})
	if len(got) != 1 || got[0].UID != "b" {
		t.Fatalf("redundant low-prio victim should be trimmed, got %v", uids(got))
	}
}

func TestSelectVictimsMultiDimension(t *testing.T) {
	need := resource.Resource{"cpu": 4, "memory": 4}
	// need both dims; a frees only cpu, b frees only memory -> both required.
	got := SelectVictims(need, []Victim{
		{UID: "cpuonly", Priority: 1, Request: resource.Resource{"cpu": 4}, Preemptable: true},
		{UID: "memonly", Priority: 1, Request: resource.Resource{"memory": 4}, Preemptable: true},
		{UID: "irrelevant", Priority: 0, Request: resource.Resource{"nvidia.com/gpu": 8}, Preemptable: true},
	})
	got2 := uids(got)
	if len(got) != 2 || !got2["cpuonly"] || !got2["memonly"] {
		t.Fatalf("both contributing victims required, irrelevant skipped; got %v", got2)
	}
}

func TestSelectVictimsInfeasibleAndEmpty(t *testing.T) {
	if got := SelectVictims(resource.Resource{}, []Victim{{UID: "x", Request: resource.Resource{"cpu": 9}, Preemptable: true}}); got != nil {
		t.Fatal("empty need reclaims nothing")
	}
	if got := SelectVictims(resource.Resource{"cpu": 100}, []Victim{{UID: "x", Request: resource.Resource{"cpu": 9}, Preemptable: true}}); got != nil {
		t.Fatal("insufficient pool must be infeasible (nil)")
	}
}
