/*
Package podgroup is the PodGroup (gang) status controller's pure core: it maps a
gang's observed runtime state to its lifecycle Phase, mirroring how
controller/queue is the pure builder for the queue tree. The K8s wiring — reading
the live counts from the informers and patching PodGroup.status — lives in
pkg/framework (the coordinator), so this decision logic stays free of apimachinery
and is unit-testable in isolation.
*/
package podgroup

// Phase values for PodGroup.status.phase — the gang lifecycle surfaced on
// `kubectl get podgroup`.
const (
	// PhasePending: the gang exists but holds no reservation yet (awaiting
	// headroom, or not all members submitted).
	PhasePending = "Pending"
	// PhaseScheduling: admitted (reservation held) and assembling — fewer than
	// minMember members are placed.
	PhaseScheduling = "Scheduling"
	// PhaseRunning: minMember members are placed (the all-or-nothing barrier met).
	PhaseRunning = "Running"
	// PhaseCompleted: minMember members finished successfully — terminal.
	PhaseCompleted = "Completed"
	// PhaseFailed: the gang gave up (materialization / Hard timeout) — terminal.
	PhaseFailed = "Failed"
)

// Observed is the live state of a gang the phase is computed from.
type Observed struct {
	Admitted  bool // holds a quota reservation (passed admission-by-accounting)
	Bound     int  // members placed on a node
	Succeeded int  // members in the Succeeded phase
	MinMember int
	Failed    bool // latched terminal failure (a timeout gave up on the gang)
}

// Phase maps the observed gang state to its lifecycle phase. Failed and Completed
// are terminal; otherwise a gang is Running once minMember members are placed,
// Scheduling while it holds a reservation but is still assembling, and Pending
// before admission. minMember ≤ 0 (a barrier-only or malformed gang) never reaches
// Running/Completed on member counts.
func (o Observed) Phase() string {
	switch {
	case o.Failed:
		return PhaseFailed
	case o.MinMember > 0 && o.Succeeded >= o.MinMember:
		return PhaseCompleted
	case o.MinMember > 0 && o.Bound >= o.MinMember:
		return PhaseRunning
	case o.Admitted:
		return PhaseScheduling
	default:
		return PhasePending
	}
}
