package admission

import "fmt"

/*
The pool-guard is a companion ValidatingAdmissionPolicy to the MAP (decision
q20/q22): the dedicated pool is single-scheduler, so a pod may carry the pool
nodeSelector or tolerate the dedicated taint ONLY if it is scheduled by
ad-scheduler. Without it a pod bound by the DEFAULT scheduler could pin itself to
pool nodes (its nodeSelector) and tolerate their taint, landing on the dedicated
pool outside our per-queue accounting. The MAP stamps exactly these two fields on
pods that target this scheduler, so our own pods always satisfy the guard; only a
pod that wants the pool WITHOUT targeting us is rejected — closing the "taint /
pool-label ⇒ schedulerName" loop atomically at admission.

Like the MAP it is generated from the node-pool config (single source;
TestPoolGuardInSyncWithConfig fails on drift). DaemonSet pods (CNI/CSI/kube-proxy
legitimately tolerate the NoSchedule taint to run on every node) and system
namespaces are exempt — they are cluster-admin-managed, not tenant bypass. There
is no engine Go-predicate mirror (unlike QueueRules): the guard governs FOREIGN
pods the engine never sees; capacity for any that slip through is still covered by
foreign-pod accounting, so this is a preventive admission layer, not the sole gate.
*/

// PoolGuard is the generated content of the pool-guard VAP, threaded into
// deploy/admission.yaml.
type PoolGuard struct {
	WantsPool       string // variable CEL: the pod is asking to land on the pool
	Validation      string // validation CEL: wantsPool ⇒ scheduled by us
	ExemptDaemonSet string // matchCondition CEL: skip DaemonSet-owned pods
	Message         string
}

// GeneratePoolGuard renders the pool-guard VAP pieces from the node-pool config.
func GeneratePoolGuard(schedulerName, poolLabel, poolValue, taintKey string) PoolGuard {
	return PoolGuard{
		WantsPool:       poolGuardWantsPool(poolLabel, poolValue, taintKey),
		Validation:      poolGuardValidation(schedulerName),
		ExemptDaemonSet: poolGuardExemptDaemonSet(),
		Message:         poolGuardMessage(schedulerName),
	}
}

// poolGuardWantsPool is true when a pod pins the pool nodeSelector value or
// tolerates the dedicated taint — i.e. it is trying to land on the pool. Every
// field access is has()-guarded so the CEL never errors on a pod that omits them.
func poolGuardWantsPool(poolLabel, poolValue, taintKey string) string {
	return fmt.Sprintf(
		`(has(object.spec.nodeSelector) && '%s' in object.spec.nodeSelector && object.spec.nodeSelector['%s'] == '%s') `+
			`|| (has(object.spec.tolerations) && object.spec.tolerations.exists(t, has(t.key) && t.key == '%s'))`,
		poolLabel, poolLabel, poolValue, taintKey)
}

// poolGuardValidation passes unless a pool-seeking pod fails to target this
// scheduler. It reads the wantsPool variable.
func poolGuardValidation(schedulerName string) string {
	return fmt.Sprintf(
		`!variables.wantsPool || (has(object.spec.schedulerName) && object.spec.schedulerName == '%s')`,
		schedulerName)
}

// poolGuardExemptDaemonSet skips DaemonSet-owned pods — they legitimately run on
// every node (incl. the pool) and are admin-managed, not a tenant bypass.
func poolGuardExemptDaemonSet() string {
	return `!(has(object.metadata.ownerReferences) && object.metadata.ownerReferences.exists(o, o.kind == 'DaemonSet'))`
}

func poolGuardMessage(schedulerName string) string {
	return fmt.Sprintf(
		"a pod that targets the ad-scheduler dedicated pool (its nodeSelector or dedicated toleration) must set spec.schedulerName: %s",
		schedulerName)
}
