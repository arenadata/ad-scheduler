package queue

import (
	"testing"

	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/arenadata/ad-scheduler/pkg/apis/scheduling/v1alpha1"
	"github.com/arenadata/ad-scheduler/pkg/queue"
	"github.com/arenadata/ad-scheduler/pkg/resource"
)

func cr(ns, name, parent string) v1alpha1.Queue {
	return v1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       v1alpha1.QueueSpec{Parent: parent},
	}
}

func TestBuildSpecTreeAndConversion(t *testing.T) {
	q1 := cr("team", "root-q", "")
	q1.Spec.Max = v1alpha1.ResourceList{"cpu": apiresource.MustParse("4"), "memory": apiresource.MustParse("8Gi")}
	q1.Spec.MaxApplications = 5
	q2 := cr("team", "spark", "root-q")
	q2.Spec.ServiceAccounts = []string{"spark-sa"}
	q2.Spec.SubmitACL = "team"
	q2.Spec.Limits = []v1alpha1.QueueLimit{{ServiceAccounts: []string{"spark-sa"}, MaxApplications: 3, MaxResources: v1alpha1.ResourceList{"cpu": apiresource.MustParse("2")}}}
	q3 := cr("infra", "ops", "")
	q3.Spec.Default = true // the infra namespace default queue

	spec, err := BuildSpec([]v1alpha1.Queue{q2, q1, q3}, map[string]resource.Resource{
		"team": {"cpu": 10000},
	})
	if err != nil {
		t.Fatalf("BuildSpec: %v", err)
	}

	// The built spec must construct a valid engine tree with the right paths.
	m, err := queue.NewManager(spec)
	if err != nil {
		t.Fatalf("NewManager(builtSpec): %v", err)
	}
	for _, path := range []string{"root.team", "root.team.root-q", "root.team.root-q.spark", "root.infra", "root.infra.ops"} {
		if _, ok := m.Queue(path); !ok {
			t.Errorf("expected queue %q in built tree", path)
		}
	}

	// namespace node carries the envelope; cpu 10000m = 10 cores.
	teamNode, _ := m.Queue("root.team")
	if !resource.Equal(teamNode.Max(), resource.Resource{"cpu": 10000}) {
		t.Errorf("team envelope max = %v", teamNode.Max())
	}
	// CR max converted (cpu->millicores, memory->bytes).
	rq, _ := m.Queue("root.team.root-q")
	if !resource.Equal(rq.Max(), resource.Resource{"cpu": 4000, "memory": 8 << 30}) {
		t.Errorf("root-q max = %v", rq.Max())
	}
	if rq.MaxApplications() != 5 {
		t.Errorf("root-q maxApplications = %d, want 5", rq.MaxApplications())
	}
	// placement fields carried through to the leaf.
	spark, _ := m.Queue("root.team.root-q.spark")
	if sas := spark.ServiceAccounts(); len(sas) != 1 || sas[0] != "spark-sa" {
		t.Errorf("spark serviceAccounts = %v", sas)
	}
	if lims := spark.Limits(); len(lims) != 1 || lims[0].MaxApplications != 3 || lims[0].MaxResources["cpu"] != 2000 {
		t.Errorf("spark limits = %+v", spark.Limits())
	}
	if !spark.CanSubmit("team") {
		t.Error("spark should honour its submitACL for namespace team")
	}
	// the explicit spec.default marker carries through to the leaf.
	if ops, _ := m.Queue("root.infra.ops"); ops == nil || !ops.IsDefaultLeaf() {
		t.Errorf("root.infra.ops should be the namespace default queue (spec.default)")
	}
}

func TestBuildSpecErrors(t *testing.T) {
	cases := map[string][]v1alpha1.Queue{
		"unknown parent": {cr("ns", "a", "ghost")},
		"duplicate name": {cr("ns", "dup", ""), cr("ns", "dup", "")},
		"two-node cycle": {cr("ns", "a", "b"), cr("ns", "b", "a")},
	}
	for name, qs := range cases {
		if _, err := BuildSpec(qs, nil); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

// A Queue that violates an admission invariant (self-parent) is excluded from the
// tree fail-closed — the build still succeeds and the valid siblings remain.
func TestBuildSpecExcludesInvalidCR(t *testing.T) {
	spec, err := BuildSpec([]v1alpha1.Queue{cr("ns", "a", "a"), cr("ns", "good", "")}, nil)
	if err != nil {
		t.Fatalf("build should succeed excluding the invalid CR: %v", err)
	}
	m, err := queue.NewManager(spec)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if _, ok := m.Queue("root.ns.a"); ok {
		t.Error("self-parent CR must be excluded from the tree")
	}
	if _, ok := m.Queue("root.ns.good"); !ok {
		t.Error("the valid CR must remain in the tree")
	}
}

func TestBuildSpecEmpty(t *testing.T) {
	spec, err := BuildSpec(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	m, err := queue.NewManager(spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := m.Queue("root"); !ok {
		t.Fatal("root must exist even for an empty tree")
	}
	for _, l := range m.Leaves() {
		if queue.NamespaceOf(l.Path()) != "" {
			t.Fatalf("empty build should have no namespace leaves, got %s", l.Path())
		}
	}
}
