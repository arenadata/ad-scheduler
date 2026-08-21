package queue

import "testing"

func TestACLParseAndAllows(t *testing.T) {
	a := ParseACL("teamA, teamB")
	if !a.Allows("teamA") || !a.Allows("teamB") || a.Allows("teamC") {
		t.Fatalf("explicit ACL match wrong: %+v", a)
	}
	if a.IsEmpty() {
		t.Fatal("non-empty ACL reported empty")
	}
	star := ParseACL("*")
	if !star.Allows("anything") {
		t.Fatal("wildcard should allow any namespace")
	}
	if !ParseACL("   ").IsEmpty() || !ParseACL("").IsEmpty() {
		t.Fatal("blank ACL must be empty (inherit)")
	}
}

func TestACLInheritance(t *testing.T) {
	// root submits only teamA; child leaf has no ACL -> inherits; grandchild
	// overrides with teamB.
	m, err := NewManager(&Spec{
		SubmitACL: "teamA",
		Children: []*Spec{
			{Name: "inherits"}, // empty -> inherit root
			{Name: "overrides", SubmitACL: "teamB"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	inh, _ := m.Queue("root.inherits")
	if !inh.CanSubmit("teamA") || inh.CanSubmit("teamB") {
		t.Fatalf("inheriting leaf should honour root ACL (teamA only)")
	}
	ovr, _ := m.Queue("root.overrides")
	if !ovr.CanSubmit("teamB") || ovr.CanSubmit("teamA") {
		t.Fatalf("overriding leaf should use its own ACL (teamB only)")
	}
}

func TestPlacementValidation(t *testing.T) {
	cases := map[string]*Spec{
		"SA on non-leaf": {
			Children: []*Spec{
				{Name: "team", ServiceAccounts: []string{"x"}, Children: []*Spec{{Name: "c"}}},
			},
		},
		"same SA two leaves": {
			Children: []*Spec{{Name: "team", Children: []*Spec{
				{Name: "a", ServiceAccounts: []string{"dup"}},
				{Name: "b", ServiceAccounts: []string{"dup"}},
			}}},
		},
		"two default leaves": {
			Children: []*Spec{{Name: "team", Children: []*Spec{
				{Name: "a", ServiceAccounts: []string{"*"}},
				{Name: "b", ServiceAccounts: []string{"*"}},
			}}},
		},
		"default marker + star alias collide": {
			Children: []*Spec{{Name: "team", Children: []*Spec{
				{Name: "a", Default: true},
				{Name: "b", ServiceAccounts: []string{"*"}},
			}}},
		},
		"default on non-leaf": {
			Children: []*Spec{
				{Name: "team", Default: true, Children: []*Spec{{Name: "c"}}},
			},
		},
	}
	for name, spec := range cases {
		if _, err := NewManager(spec); err == nil {
			t.Errorf("%s: expected build error", name)
		}
	}
	// valid: distinct SAs + one default under one namespace
	if _, err := NewManager(&Spec{Children: []*Spec{{Name: "team", Children: []*Spec{
		{Name: "a", ServiceAccounts: []string{"sa1"}},
		{Name: "b", ServiceAccounts: []string{"sa2", "*"}},
	}}}}); err != nil {
		t.Errorf("valid placement rejected: %v", err)
	}
	// valid: the explicit spec.default marker (with its own SA) is a default queue;
	// a distinct namespace may also have its own default — one per namespace.
	if _, err := NewManager(&Spec{Children: []*Spec{
		{Name: "team", Children: []*Spec{
			{Name: "a", ServiceAccounts: []string{"sa1"}},
			{Name: "b", ServiceAccounts: []string{"sa2"}, Default: true},
		}},
		{Name: "other", Children: []*Spec{{Name: "d", Default: true}}},
	}}); err != nil {
		t.Errorf("valid explicit-default placement rejected: %v", err)
	}
	// valid: the same SA listed twice on ONE leaf is a harmless duplicate (it
	// still routes to exactly one queue), not a cross-leaf conflict.
	if _, err := NewManager(&Spec{Children: []*Spec{{Name: "team", Children: []*Spec{
		{Name: "a", ServiceAccounts: []string{"dup-sa", "dup-sa"}},
	}}}}); err != nil {
		t.Errorf("duplicate SA within one leaf rejected: %v", err)
	}
}
