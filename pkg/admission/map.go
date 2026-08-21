package admission

import (
	"fmt"
	"strings"
)

// The MutatingAdmissionPolicy is, like the VAP, a build artifact of a single
// source — here the node-pool config (label + dedicated taint) — so the policy,
// the engine's off-pool Filter (same pool label) and the pool provisioning cannot
// drift. These renderers emit the pieces of deploy/admission.yaml's MAP; the
// TestMAPInSyncWithConfig test fails if the shipped policy diverges.

// jsonPatchEscape escapes a JSON-pointer path segment (RFC 6901): '~' -> '~0',
// '/' -> '~1'. A label like scheduler.arenadata.io/name becomes
// scheduler.arenadata.io~1name so it addresses the nodeSelector sub-key.
func jsonPatchEscape(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "~", "~0"), "/", "~1")
}

// MAPTrigger is the matchCondition CEL that fires the MAP: a pod that targets
// this scheduler by name (there is no separate opt-in label — choosing the
// scheduler is the opt-in, decision q22/G7).
func MAPTrigger(schedulerName string) string {
	return fmt.Sprintf("has(object.spec.schedulerName) && object.spec.schedulerName == '%s'", schedulerName)
}

// MAPArtifacts is the complete generated MutatingAdmissionPolicy content derived
// from the node-pool config: the matchCondition Trigger (which pods get the pool
// binding — the pod allow-list) and the JSONPatch Mutation (the pool nodeSelector
// + dedicated toleration it stamps). deploy/admission.yaml carries exactly these,
// verified by TestMAPInSyncWithConfig.
type MAPArtifacts struct {
	Trigger  string // matchCondition CEL: the allow-list of pods to mutate
	Mutation string // JSONPatch CEL: the stamped nodeSelector + toleration
}

// GenerateMAP renders the full MAP content from the node-pool config so the
// shipped policy is a build artifact of a single source (like the VAP): the
// trigger allow-list plus the nodeSelector/toleration mutation. The engine's
// off-pool Filter matches the same pool label, so policy and enforcement cannot
// drift.
func GenerateMAP(schedulerName, poolLabel, poolValue, taintKey string) MAPArtifacts {
	return MAPArtifacts{
		Trigger:  MAPTrigger(schedulerName),
		Mutation: GenerateMAPMutation(poolLabel, poolValue, taintKey),
	}
}

// GenerateMAPMutation renders the JSONPatch CEL that stamps the pool nodeSelector
// (non-destructive merge via the ~1-escaped key) and the dedicated-pool toleration
// (appended only if absent, idempotent) from the node-pool label/value/taint.
func GenerateMAPMutation(poolLabel, poolValue, taintKey string) string {
	esc := jsonPatchEscape(poolLabel)
	return fmt.Sprintf(`(
  has(object.spec.nodeSelector)
    ? [JSONPatch{op: "add", path: "/spec/nodeSelector/%s", value: "%s"}]
    : [JSONPatch{op: "add", path: "/spec/nodeSelector", value: {"%s": "%s"}}]
) + (
  has(object.spec.tolerations)
    ? (object.spec.tolerations.exists(t, t.key == "%s" && t.operator == "Exists" && t.effect == "NoSchedule")
        ? []
        : [JSONPatch{op: "add", path: "/spec/tolerations/-", value: {"key": "%s", "operator": "Exists", "effect": "NoSchedule"}}])
    : [JSONPatch{op: "add", path: "/spec/tolerations", value: [{"key": "%s", "operator": "Exists", "effect": "NoSchedule"}]}]
)`, esc, poolValue, poolLabel, poolValue, taintKey, taintKey, taintKey)
}
