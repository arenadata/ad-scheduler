package admission

import "fmt"

/*
The named-SA guard is a companion ValidatingAdmissionPolicy (decision q21 / gaps
G1-G2): a pod scheduled by ad-scheduler must use a NAMED ServiceAccount. The
`default` SA maps to no queue (the Queue VAP bans it from spec.serviceAccounts),
so such a pod could never be admitted to a queue — placement fail-closes it to
Pending. This guard rejects it at admission instead, for immediate, clear
operator feedback rather than a silently-stuck pod.

Scope: like the MAP and pool-guard it is gated by the schedulerName opt-in
(decision G7) — it fires only for pods a submitter aimed at ad-scheduler, so
non-batch pods that legitimately use the default SA elsewhere are untouched. A
pod that FORGETS schedulerName leaks past this (and past the MAP) — but it is then
not our pod (it goes to the default scheduler), consistent with the per-pod
opt-in model. Generated from the config (single source; TestSAGuardInSyncWithConfig).
*/

// SAGuard is the generated content of the named-SA VAP.
type SAGuard struct {
	Trigger    string // matchCondition CEL: pods that opted into this scheduler
	Validation string // validation CEL: a named (non-default) ServiceAccount
	Message    string
}

// GenerateSAGuard renders the named-SA VAP pieces from the scheduler name.
func GenerateSAGuard(schedulerName string) SAGuard {
	return SAGuard{
		Trigger:    MAPTrigger(schedulerName),
		Validation: `object.spec.serviceAccountName != 'default' && object.spec.serviceAccountName != ''`,
		Message: fmt.Sprintf(
			"a pod scheduled by %s must use a named ServiceAccount — the default SA maps to no queue (decision q21); set spec.serviceAccountName",
			schedulerName),
	}
}
