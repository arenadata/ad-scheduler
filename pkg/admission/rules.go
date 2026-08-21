/*
Package admission is the single source of truth for the Queue admission
invariants (decision q22/q27). Each rule carries BOTH its CEL expression (what
the in-tree ValidatingAdmissionPolicy enforces at apply time) AND an equivalent
Go predicate (what the engine enforces fail-closed while assembling the tree), so
the two can never drift: the VAP YAML is generated from these rules and the
engine rejects exactly what the VAP would. The design contract is
VAP-accept ⊆ engine-accept — the engine is the source of truth and never admits
what the VAP would reject.
*/
package admission

import (
	"fmt"
	"slices"
	"strings"

	"github.com/arenadata/ad-scheduler/pkg/apis/scheduling/v1alpha1"
)

// QueueRule is one Queue invariant, expressed once as a CEL expression (for the
// generated VAP) and as a Go predicate (for the engine). Valid returns true when
// the Queue satisfies the rule.
type QueueRule struct {
	Name    string
	CEL     string // VAP validation expression over `object`
	Message string
	Valid   func(*v1alpha1.Queue) bool
}

// QueueRules is the canonical invariant set. Adding a rule here updates both the
// generated VAP (GenerateVAP) and the engine's pre-check (ValidateQueue).
var QueueRules = []QueueRule{
	{
		Name:    "self-parent",
		CEL:     "!has(object.spec.parent) || object.spec.parent != object.metadata.name",
		Message: "a Queue cannot be its own parent",
		Valid:   func(q *v1alpha1.Queue) bool { return q.Spec.Parent == "" || q.Spec.Parent != q.Name },
	},
	{
		Name:    "dotted-parent",
		CEL:     "!has(object.spec.parent) || !object.spec.parent.contains('.')",
		Message: "spec.parent must be a bare Queue name in the same namespace (no dots)",
		Valid:   func(q *v1alpha1.Queue) bool { return !strings.Contains(q.Spec.Parent, ".") },
	},
	{
		Name:    "absolute-capacity",
		CEL:     "!has(object.spec.capacityMode) || object.spec.capacityMode == 'absolute'",
		Message: "only capacityMode=absolute is supported in the MVP (decision q13)",
		Valid:   func(q *v1alpha1.Queue) bool { return q.Spec.CapacityMode == "" || q.Spec.CapacityMode == "absolute" },
	},
	{
		Name:    "no-default-sa",
		CEL:     "!has(object.spec.serviceAccounts) || object.spec.serviceAccounts.all(sa, sa != 'default')",
		Message: "the default ServiceAccount must not be mapped to a queue (decision q21)",
		Valid: func(q *v1alpha1.Queue) bool {
			return !slices.Contains(q.Spec.ServiceAccounts, "default")
		},
	},
}

// ValidateQueue runs the engine-side (Go) predicates, returning the first
// violated rule. controller/queue calls it so an invalid Queue is excluded from
// the tree fail-closed even if the VAP was not installed or was bypassed.
func ValidateQueue(q *v1alpha1.Queue) error {
	for _, r := range QueueRules {
		if !r.Valid(q) {
			return fmt.Errorf("queue %s/%s violates %q: %s", q.Namespace, q.Name, r.Name, r.Message)
		}
	}
	return nil
}

// GenerateVAPValidations renders the CEL validations block (spec.validations of
// the ValidatingAdmissionPolicy) from QueueRules, so deploy/admission.yaml is a
// build artifact of this single source rather than hand-maintained.
func GenerateVAPValidations() string {
	var b strings.Builder
	b.WriteString("  validations:\n")
	for _, r := range QueueRules {
		fmt.Fprintf(&b, "    - expression: %q\n", r.CEL)
		fmt.Fprintf(&b, "      message: %q\n", r.Message)
	}
	return b.String()
}
