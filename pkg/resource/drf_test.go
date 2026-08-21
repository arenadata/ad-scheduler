package resource

import (
	"math"
	"testing"
)

func TestDominantShare(t *testing.T) {
	cap := Resource{"memory": 1000, "vcore": 100, "nvidia.com/gpu": 8}
	cases := []struct {
		name  string
		usage Resource
		want  float64
	}{
		{"empty", Resource{}, 0},
		{"memory-dominant", Resource{"memory": 500, "vcore": 10}, 0.5}, // max(0.5, 0.1)
		{"vcore-dominant", Resource{"memory": 100, "vcore": 50}, 0.5},  // max(0.1, 0.5)
		{"gpu-dominant", Resource{"nvidia.com/gpu": 4}, 0.5},           // 4/8
		{"over-capacity", Resource{"memory": 2000}, 2.0},
		{"absent-capacity-dim", Resource{"dra/gpu": 1}, math.Inf(1)}, // capacity lacks the dim
		{"negative-ignored", Resource{"memory": -100, "vcore": 10}, 0.1},
	}
	for _, c := range cases {
		if got := DominantShare(c.usage, cap); !almostEqual(got, c.want) {
			t.Errorf("%s: DominantShare(%v) = %v, want %v", c.name, c.usage, got, c.want)
		}
	}
}

func TestComp(t *testing.T) {
	cap := Resource{"memory": 1000, "vcore": 100}
	less := Resource{"memory": 100}   // share 0.1
	more := Resource{"memory": 500}   // share 0.5
	equalA := Resource{"vcore": 50}   // share 0.5
	equalB := Resource{"memory": 500} // share 0.5

	if Comp(cap, less, more) != -1 {
		t.Error("less-satisfied should compare -1 (served first)")
	}
	if Comp(cap, more, less) != 1 {
		t.Error("more-satisfied should compare +1")
	}
	if Comp(cap, equalA, equalB) != 0 {
		t.Error("equal dominant share should compare 0 (tie -> secondary key)")
	}
	if Comp(cap, Resource{}, Resource{}) != 0 {
		t.Error("two empties are equal")
	}
	// a request for an absent-capacity dim ranks last (share +Inf)
	if Comp(cap, more, Resource{"dra/gpu": 1}) != -1 {
		t.Error("finite share must rank before over-capacity (+Inf)")
	}
}

func TestCompIsAntisymmetric(t *testing.T) {
	cap := Resource{"memory": 1000, "vcore": 100}
	vs := []Resource{
		{}, {"memory": 100}, {"vcore": 50}, {"memory": 900, "vcore": 90}, {"dra/gpu": 1},
	}
	for _, a := range vs {
		for _, b := range vs {
			if Comp(cap, a, b) != -Comp(cap, b, a) {
				t.Errorf("Comp not antisymmetric for %v vs %v", a, b)
			}
		}
	}
}
