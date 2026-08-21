package admission

import (
	"os"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/arenadata/ad-scheduler/pkg/apis/scheduling/v1alpha1"
	"github.com/arenadata/ad-scheduler/pkg/config"
)

func goodQueue() *v1alpha1.Queue {
	return &v1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{Name: "q", Namespace: "ns"},
		Spec:       v1alpha1.QueueSpec{Parent: "root-q", ServiceAccounts: []string{"spark"}},
	}
}

func TestValidateQueue(t *testing.T) {
	if err := ValidateQueue(goodQueue()); err != nil {
		t.Fatalf("valid queue rejected: %v", err)
	}
	cases := []struct {
		name string
		mut  func(*v1alpha1.Queue)
	}{
		{"self-parent", func(q *v1alpha1.Queue) { q.Spec.Parent = q.Name }},
		{"dotted-parent", func(q *v1alpha1.Queue) { q.Spec.Parent = "a.b" }},
		{"absolute-capacity", func(q *v1alpha1.Queue) { q.Spec.CapacityMode = "percentage" }},
		{"no-default-sa", func(q *v1alpha1.Queue) { q.Spec.ServiceAccounts = []string{"spark", "default"} }},
	}
	for _, c := range cases {
		q := goodQueue()
		c.mut(q)
		if err := ValidateQueue(q); err == nil {
			t.Errorf("%s: expected rejection, got nil", c.name)
		}
	}
}

// TestVAPInSyncWithRules is the codegen contract: the shipped VAP must carry
// exactly the CEL these rules define, so the single source and the deployed
// policy never drift (regenerate deploy/admission.yaml from GenerateVAPValidations
// if this fails).
func TestVAPInSyncWithRules(t *testing.T) {
	data, err := os.ReadFile("../../deploy/admission.yaml")
	if err != nil {
		t.Skipf("admission.yaml not readable: %v", err)
	}
	s := string(data)
	for _, r := range QueueRules {
		if !strings.Contains(s, r.CEL) {
			t.Errorf("deploy/admission.yaml out of sync: missing CEL for rule %q:\n  %s", r.Name, r.CEL)
		}
		if !strings.Contains(s, r.Message) {
			t.Errorf("deploy/admission.yaml out of sync: missing message for rule %q", r.Name)
		}
	}
}

// collapseWS folds every run of whitespace (incl. newlines/indentation) to a
// single space so a multi-line CEL block in the YAML compares equal to the
// generator's raw output regardless of how the folded YAML scalar is indented.
func collapseWS(s string) string { return strings.Join(strings.Fields(s), " ") }

// TestMAPInSyncWithConfig is the MAP codegen contract, symmetric to
// TestVAPInSyncWithRules: the shipped MAP must carry EXACTLY the trigger and the
// full nodeSelector+toleration mutation that GenerateMAP renders from the
// node-pool config (single source) — not merely the right fragments — so the
// policy cannot structurally drift from the engine's own pool label / taint.
func TestMAPInSyncWithConfig(t *testing.T) {
	data, err := os.ReadFile("../../deploy/admission.yaml")
	if err != nil {
		t.Skipf("admission.yaml not readable: %v", err)
	}
	fileWS := collapseWS(string(data))
	cfg := config.Defaults()
	label, value, ok := cfg.NodePool.PoolLabelValue()
	if !ok {
		t.Fatalf("default pool selector %q is not a key=value pair", cfg.NodePool.LabelSelector)
	}
	map0 := GenerateMAP(cfg.SchedulerName, label, value, cfg.NodePool.TaintKey)

	// exact trigger (single line) and the WHOLE mutation (whitespace-normalized).
	if !strings.Contains(fileWS, collapseWS(map0.Trigger)) {
		t.Errorf("MAP out of sync: shipped trigger differs from GenerateMAP\n  want: %s", map0.Trigger)
	}
	if !strings.Contains(fileWS, collapseWS(map0.Mutation)) {
		t.Errorf("MAP out of sync: shipped mutation differs from GenerateMAP (regenerate the MAP block)\n  want:\n%s", map0.Mutation)
	}
}

// TestGenerateMAP checks the generator threads the config-derived label, value
// and taint into both the escaped nodeSelector key and the toleration.
func TestGenerateMAP(t *testing.T) {
	m := GenerateMAP("ad-scheduler", "pool.example.com/name", "team-x", "pool.example.com/dedicated")
	if !strings.Contains(m.Trigger, "ad-scheduler") {
		t.Errorf("trigger missing schedulerName: %s", m.Trigger)
	}
	for _, frag := range []string{
		jsonPatchEscape("pool.example.com/name"), // escaped nodeSelector key
		`"team-x"`,                               // pool value
		"pool.example.com/dedicated",             // taint key
	} {
		if !strings.Contains(m.Mutation, frag) {
			t.Errorf("mutation missing %q:\n%s", frag, m.Mutation)
		}
	}
}

// TestPoolGuardInSyncWithConfig is the pool-guard codegen contract: the shipped
// companion VAP must carry exactly the wantsPool variable, the schedulerName
// validation, its message and the DaemonSet exemption that GeneratePoolGuard
// renders from the node-pool config, so the guard cannot drift from the MAP it
// mirrors or the engine's pool label / taint.
func TestPoolGuardInSyncWithConfig(t *testing.T) {
	data, err := os.ReadFile("../../deploy/admission.yaml")
	if err != nil {
		t.Skipf("admission.yaml not readable: %v", err)
	}
	fileWS := collapseWS(string(data))
	cfg := config.Defaults()
	label, value, ok := cfg.NodePool.PoolLabelValue()
	if !ok {
		t.Fatalf("default pool selector %q is not a key=value pair", cfg.NodePool.LabelSelector)
	}
	pg := GeneratePoolGuard(cfg.SchedulerName, label, value, cfg.NodePool.TaintKey)
	for name, frag := range map[string]string{
		"wantsPool variable":  pg.WantsPool,
		"validation":          pg.Validation,
		"daemonset exemption": pg.ExemptDaemonSet,
		"message":             pg.Message,
	} {
		if !strings.Contains(fileWS, collapseWS(frag)) {
			t.Errorf("pool-guard out of sync: shipped VAP missing %s (regenerate the block)\n  want: %s", name, frag)
		}
	}
}

// TestGeneratePoolGuard checks the guard threads the config label/value/taint and
// scheduler name into the wantsPool detection and the schedulerName requirement.
func TestGeneratePoolGuard(t *testing.T) {
	pg := GeneratePoolGuard("ad-scheduler", "pool.example.com/name", "team-x", "pool.example.com/dedicated")
	for _, frag := range []string{
		"pool.example.com/name",      // nodeSelector key
		"'team-x'",                   // pool value
		"pool.example.com/dedicated", // taint key
	} {
		if !strings.Contains(pg.WantsPool, frag) {
			t.Errorf("wantsPool missing %q:\n%s", frag, pg.WantsPool)
		}
	}
	if !strings.Contains(pg.Validation, "'ad-scheduler'") {
		t.Errorf("validation missing schedulerName: %s", pg.Validation)
	}
	// A universal Exists toleration (no key) must NOT trip the guard — the CEL
	// only matches the dedicated taint key, so it is has()-guarded.
	if !strings.Contains(pg.WantsPool, "has(t.key)") {
		t.Errorf("wantsPool must has()-guard the toleration key: %s", pg.WantsPool)
	}
}

// TestSAGuardInSyncWithConfig is the named-SA guard codegen contract: the shipped
// VAP must carry exactly the validation + message GenerateSAGuard renders (the
// trigger is the shared schedulerName gate, checked elsewhere), so the guard
// cannot drift.
func TestSAGuardInSyncWithConfig(t *testing.T) {
	data, err := os.ReadFile("../../deploy/admission.yaml")
	if err != nil {
		t.Skipf("admission.yaml not readable: %v", err)
	}
	fileWS := collapseWS(string(data))
	cfg := config.Defaults()
	sg := GenerateSAGuard(cfg.SchedulerName)
	for name, frag := range map[string]string{
		"validation": sg.Validation,
		"message":    sg.Message,
		"trigger":    sg.Trigger,
	} {
		if !strings.Contains(fileWS, collapseWS(frag)) {
			t.Errorf("named-SA guard out of sync: shipped VAP missing %s (regenerate the block)\n  want: %s", name, frag)
		}
	}
}

// TestGenerateSAGuard checks the guard rejects the default (and empty) SA and
// names the scheduler in its message.
func TestGenerateSAGuard(t *testing.T) {
	sg := GenerateSAGuard("ad-scheduler")
	for _, frag := range []string{"'default'", "!= ''", "serviceAccountName"} {
		if !strings.Contains(sg.Validation, frag) {
			t.Errorf("validation missing %q: %s", frag, sg.Validation)
		}
	}
	if !strings.Contains(sg.Trigger, "'ad-scheduler'") {
		t.Errorf("trigger must gate on schedulerName: %s", sg.Trigger)
	}
	if !strings.Contains(sg.Message, "named ServiceAccount") {
		t.Errorf("message should explain the requirement: %s", sg.Message)
	}
}

func TestGenerateVAPValidations(t *testing.T) {
	out := GenerateVAPValidations()
	for _, r := range QueueRules {
		if !strings.Contains(out, r.CEL) {
			t.Errorf("generated VAP missing rule %q", r.Name)
		}
	}
}
