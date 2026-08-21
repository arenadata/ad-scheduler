package queue

import (
	"sort"
	"testing"
)

// order sorts the units with Less and returns their UIDs in order.
func order(policy SortPolicy, units []AppOrder) []string {
	sort.SliceStable(units, func(i, j int) bool { return Less(policy, units[i], units[j]) })
	out := make([]string, len(units))
	for i, u := range units {
		out[i] = u.UID
	}
	return out
}

func eq(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
}

func TestFIFOBySubmitTimeThenPriorityFallback(t *testing.T) {
	units := []AppOrder{
		{UID: "b", SubmitNanos: 200, Priority: 5},
		{UID: "a", SubmitNanos: 100, Priority: 1},
		{UID: "c", SubmitNanos: 300, Priority: 9},
	}
	eq(t, order(SortFIFO, units), []string{"a", "b", "c"}) // pure submit order

	// equal submit -> higher priority first
	tie := []AppOrder{
		{UID: "low", SubmitNanos: 100, Priority: 1},
		{UID: "high", SubmitNanos: 100, Priority: 9},
	}
	eq(t, order(SortFIFO, tie), []string{"high", "low"})
}

func TestFairByDominantShare(t *testing.T) {
	units := []AppOrder{
		{UID: "satisfied", DominantShare: 0.9, SubmitNanos: 100},
		{UID: "starved", DominantShare: 0.1, SubmitNanos: 200},
		{UID: "mid", DominantShare: 0.5, SubmitNanos: 50},
	}
	// least-satisfied first, regardless of submit time
	eq(t, order(SortFair, units), []string{"starved", "mid", "satisfied"})
}

func TestGangMembersClusterContiguously(t *testing.T) {
	// gang G submitted at 150 (between solo a@100 and solo z@200); its two
	// members must stay adjacent as one block ordered at the gang's submit time.
	units := []AppOrder{
		{UID: "z", SubmitNanos: 200},
		{UID: "g2", GangID: "G", GangSubmitNanos: 150, SubmitNanos: 160},
		{UID: "a", SubmitNanos: 100},
		{UID: "g1", GangID: "G", GangSubmitNanos: 150, SubmitNanos: 155},
	}
	got := order(SortFIFO, units)
	eq(t, got, []string{"a", "g1", "g2", "z"})
}

func TestDeterministicStableTiebreak(t *testing.T) {
	// identical keys -> ordered by UID, and the order is total (no ties that
	// would make sort unstable / non-deterministic).
	units := []AppOrder{
		{UID: "y", SubmitNanos: 100, Priority: 1},
		{UID: "x", SubmitNanos: 100, Priority: 1},
	}
	eq(t, order(SortFIFO, units), []string{"x", "y"})
}

func TestParseSortPolicy(t *testing.T) {
	for in, want := range map[string]SortPolicy{
		"fair": SortFair, "stateaware": SortStateAware, "fifo": SortFIFO, "": SortFIFO, "junk": SortFIFO,
	} {
		if got := ParseSortPolicy(in); got != want {
			t.Errorf("ParseSortPolicy(%q) = %v, want %v", in, got, want)
		}
	}
}
