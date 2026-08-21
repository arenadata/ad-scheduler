package queue

import (
	"testing"

	"github.com/arenadata/ad-scheduler/pkg/resource"
)

func TestNodeScoreSpreadVsBinpack(t *testing.T) {
	cap := resource.Resource{"cpu": 1000, "memory": 1000}
	// node A lightly used (200/1000 = 0.2), node B heavily used (800/1000 = 0.8),
	// placing a request of 100m cpu.
	req := resource.Resource{"cpu": 100}
	aUsed := resource.Resource{"cpu": 200}
	bUsed := resource.Resource{"cpu": 800}

	// spread: lighter node scores higher
	if NodeScore(NodeSpread, aUsed, req, cap) <= NodeScore(NodeSpread, bUsed, req, cap) {
		t.Fatal("spread should prefer the lightly-used node")
	}
	// binpack: heavier node scores higher
	if NodeScore(NodeBinpack, bUsed, req, cap) <= NodeScore(NodeBinpack, aUsed, req, cap) {
		t.Fatal("binpack should prefer the heavily-used node")
	}

	// exact values: A util after req = 300/1000 = 0.3
	if got := NodeScore(NodeSpread, aUsed, req, cap); got != 70 {
		t.Errorf("spread score = %d, want 70", got)
	}
	if got := NodeScore(NodeBinpack, aUsed, req, cap); got != 30 {
		t.Errorf("binpack score = %d, want 30", got)
	}
}

func TestNodeScoreClampsOverfull(t *testing.T) {
	cap := resource.Resource{"cpu": 100}
	// request overflows capacity -> util clamps to 1
	if got := NodeScore(NodeSpread, resource.Resource{"cpu": 50}, resource.Resource{"cpu": 200}, cap); got != 0 {
		t.Errorf("overfull spread score = %d, want 0", got)
	}
	if got := NodeScore(NodeBinpack, resource.Resource{"cpu": 50}, resource.Resource{"cpu": 200}, cap); got != MaxNodeScore {
		t.Errorf("overfull binpack score = %d, want %d", got, MaxNodeScore)
	}
}

func TestParseNodeSortPolicy(t *testing.T) {
	for in, want := range map[string]NodeSortPolicy{
		"binpacking": NodeBinpack, "binpack": NodeBinpack, "fair": NodeSpread, "": NodeSpread,
	} {
		if got := ParseNodeSortPolicy(in); got != want {
			t.Errorf("ParseNodeSortPolicy(%q) = %v, want %v", in, got, want)
		}
	}
}
