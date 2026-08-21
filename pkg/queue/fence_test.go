package queue

import "testing"

// leaf builds a childless Spec node.
func fenceLeaf(name string) *Spec { return &Spec{Name: name} }

// twoNamespaceTree is the default case: root → {a → {x,y}, b → {z}}. Top-level
// queues a and b are implicit fences, so preemption may not cross between them.
func twoNamespaceTree(t *testing.T) *QueueManager {
	t.Helper()
	m, err := NewManager(&Spec{Name: "root", Children: []*Spec{
		{Name: "a", Children: []*Spec{fenceLeaf("x"), fenceLeaf("y")}},
		{Name: "b", Children: []*Spec{fenceLeaf("z")}},
	}})
	if err != nil {
		t.Fatalf("build tree: %v", err)
	}
	return m
}

func TestFenceBoundaryDefaultsToTopLevel(t *testing.T) {
	m := twoNamespaceTree(t)
	for leaf, want := range map[string]string{
		"root.a.x": "root.a",
		"root.a.y": "root.a",
		"root.b.z": "root.b",
	} {
		got, ok := m.FenceBoundary(leaf)
		if !ok || got != want {
			t.Fatalf("FenceBoundary(%q) = %q,%v; want %q", leaf, got, ok, want)
		}
	}
	if _, ok := m.FenceBoundary("root.nope"); ok {
		t.Fatalf("FenceBoundary of unknown path must report ok=false")
	}
}

func TestFenceBlocksCrossNamespaceReclaim(t *testing.T) {
	m := twoNamespaceTree(t)
	// within the same top-level (namespace) subtree: reachable.
	if !m.WithinFence("root.a.x", "root.a.y") {
		t.Fatalf("sibling leaf in the same namespace must be within fence")
	}
	// across the top-level fence: not reachable in either direction.
	if m.WithinFence("root.a.x", "root.b.z") {
		t.Fatalf("cross-namespace victim must be fenced off (a → b)")
	}
	if m.WithinFence("root.b.z", "root.a.x") {
		t.Fatalf("cross-namespace victim must be fenced off (b → a)")
	}
	// unknown asker fails closed.
	if m.WithinFence("root.nope", "root.a.y") {
		t.Fatalf("unknown asker must have no reach")
	}
}

// TestExplicitFenceTightensBoundary checks that an explicit
// preemption.policy=fence on a sub-tree narrows a preemptor's reach below the
// top-level default, and documents the intended asymmetry: the fence constrains
// where an ask may look for victims, it does not independently protect victims
// from asks originating above the fence.
func TestExplicitFenceTightensBoundary(t *testing.T) {
	m, err := NewManager(&Spec{Name: "root", Children: []*Spec{
		{Name: "a", Children: []*Spec{
			{Name: "team1", Fence: true, Children: []*Spec{fenceLeaf("p"), fenceLeaf("q")}},
			{Name: "team2", Children: []*Spec{fenceLeaf("r")}},
		}},
	}})
	if err != nil {
		t.Fatalf("build tree: %v", err)
	}

	// team1's explicit fence is nearer than the top-level default → tighter boundary.
	if got, _ := m.FenceBoundary("root.a.team1.p"); got != "root.a.team1" {
		t.Fatalf("FenceBoundary(team1.p) = %q; want root.a.team1", got)
	}
	// team2 has no explicit fence → its boundary is the top-level namespace node.
	if got, _ := m.FenceBoundary("root.a.team2.r"); got != "root.a" {
		t.Fatalf("FenceBoundary(team2.r) = %q; want root.a", got)
	}

	// A preemptor inside the fenced team1 stays within team1.
	if !m.WithinFence("root.a.team1.p", "root.a.team1.q") {
		t.Fatalf("within the fenced subtree must be reachable")
	}
	if m.WithinFence("root.a.team1.p", "root.a.team2.r") {
		t.Fatalf("team1 (fenced) must not reach team2")
	}
	// team2 (unfenced) reaches up to the namespace fence, so it CAN reach into
	// team1 — fence bounds the asker, not the victim.
	if !m.WithinFence("root.a.team2.r", "root.a.team1.p") {
		t.Fatalf("unfenced team2 reaches the whole namespace subtree, including team1")
	}
}

func TestPathWithinSubtree(t *testing.T) {
	cases := []struct {
		path, root string
		want       bool
	}{
		{"root.a", "root.a", true},     // self
		{"root.a.x", "root.a", true},   // descendant
		{"root.ab.x", "root.a", false}, // prefix but not a path boundary
		{"root.b.z", "root.a", false},  // sibling subtree
		{"root", "root.a", false},      // ancestor is not within
	}
	for _, c := range cases {
		if got := PathWithinSubtree(c.path, c.root); got != c.want {
			t.Fatalf("PathWithinSubtree(%q,%q) = %v; want %v", c.path, c.root, got, c.want)
		}
	}
}
