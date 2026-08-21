package resource

import (
	"math"
	"testing"
)

func TestAddSub(t *testing.T) {
	a := Resource{"memory": 100, "vcore": 4000}
	b := Resource{"memory": 50, "nvidia.com/gpu": 2}

	if got := Add(a, b); !Equal(got, Resource{"memory": 150, "vcore": 4000, "nvidia.com/gpu": 2}) {
		t.Fatalf("Add union wrong: %v", got)
	}
	if got := Sub(a, b); !Equal(got, Resource{"memory": 50, "vcore": 4000, "nvidia.com/gpu": -2}) {
		t.Fatalf("Sub can go negative: %v", got)
	}
	// inputs must not be mutated
	if !Equal(a, Resource{"memory": 100, "vcore": 4000}) || !Equal(b, Resource{"memory": 50, "nvidia.com/gpu": 2}) {
		t.Fatalf("Add/Sub mutated inputs: a=%v b=%v", a, b)
	}
}

func TestAddPrunesZeros(t *testing.T) {
	got := Add(Resource{"cpu": 5}, Resource{"cpu": -5})
	if len(got) != 0 {
		t.Fatalf("zero dimension must be pruned, got %v", got)
	}
	if !Equal(got, Resource{}) || !Equal(got, nil) {
		t.Fatalf("pruned result should equal empty and nil: %v", got)
	}
}

func TestEqualTreatsAbsentAsZero(t *testing.T) {
	if !Equal(Resource{"cpu": 0}, Resource{}) {
		t.Fatal("{cpu:0} must equal {}")
	}
	if Equal(Resource{"cpu": 1}, Resource{}) {
		t.Fatal("{cpu:1} must not equal {}")
	}
}

func TestMultiply(t *testing.T) {
	got := Resource{"memory": 10, "vcore": 2}.Multiply(16)
	if !Equal(got, Resource{"memory": 160, "vcore": 32}) {
		t.Fatalf("Multiply wrong: %v", got)
	}
	if !(Resource{"memory": 5}).Multiply(0).IsEmpty() {
		t.Fatal("Multiply by 0 must be empty")
	}
}

func TestStrictlyGtZero(t *testing.T) {
	cases := []struct {
		r    Resource
		want bool
	}{
		{Resource{}, false},
		{nil, false},
		{Resource{"cpu": 0}, false},
		{Resource{"cpu": 1}, true},
		{Resource{"cpu": 1, "gpu": 0}, true},
		{Resource{"cpu": -1}, false}, // any negative => not a valid positive ask
		{Resource{"cpu": 1, "gpu": -1}, false},
	}
	for _, c := range cases {
		if got := c.r.StrictlyGtZero(); got != c.want {
			t.Errorf("StrictlyGtZero(%v) = %v, want %v", c.r, got, c.want)
		}
	}
}

func TestFitIn(t *testing.T) {
	avail := Resource{"memory": 100, "vcore": 4000, "nvidia.com/gpu": 2}
	cases := []struct {
		req  Resource
		want bool
	}{
		{Resource{"memory": 100, "vcore": 4000}, true}, // exact on named dims
		{Resource{"memory": 101}, false},               // over one dim
		{Resource{}, true},                             // empty always fits
		{Resource{"nvidia.com/gpu": 2}, true},          // exact gpu
		{Resource{"nvidia.com/gpu": 3}, false},         // over gpu
		{Resource{"dra/gpu-a100": 1}, false},           // asks a dim available lacks (absent => 0)
		{Resource{"memory": 50, "vcore": 2000, "nvidia.com/gpu": 1}, true},
	}
	for _, c := range cases {
		if got := FitIn(c.req, avail); got != c.want {
			t.Errorf("FitIn(%v, %v) = %v, want %v", c.req, avail, got, c.want)
		}
	}
}

func TestMax(t *testing.T) {
	// effective request across init containers is the componentwise max
	got := Max(Resource{"memory": 100, "vcore": 2000}, Resource{"memory": 50, "vcore": 4000, "nvidia.com/gpu": 1})
	if !Equal(got, Resource{"memory": 100, "vcore": 4000, "nvidia.com/gpu": 1}) {
		t.Fatalf("Max wrong: %v", got)
	}
}

func TestCloneIsolation(t *testing.T) {
	a := Resource{"cpu": 1}
	c := a.Clone()
	c["cpu"] = 99
	if a["cpu"] != 1 {
		t.Fatal("Clone did not isolate")
	}
	if got := Resource(nil).Clone(); got == nil {
		t.Fatal("Clone(nil) should be non-nil empty")
	}
}

func almostEqual(a, b float64) bool {
	if math.IsInf(a, 1) && math.IsInf(b, 1) {
		return true
	}
	return math.Abs(a-b) < 1e-9
}
