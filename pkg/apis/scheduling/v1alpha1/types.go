/*
Package v1alpha1 defines ad-scheduler's CRD types in group
scheduling.arenadata.io. The queue tree is CRD-only (decision q26): there is no
ConfigMap. Two namespaced kinds:

  - Queue     — one node of the hierarchical queue tree; path is <namespace>.<name>
    (decisions q7/q26). Its per-namespace envelope is a native
    ResourceQuota, not a field here (decision q27).
  - PodGroup  — the canonical gang unit (decision q5/q26).

Resource vectors are modelled as ResourceList (name -> quantity), the same shape
Kubernetes uses, so guaranteed/max/minResources carry familiar units (memory,
vcore, nvidia.com/gpu, dra/<class>, ...).
*/
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ResourceList is a multi-dimensional resource vector keyed by resource name.
type ResourceList map[string]resource.Quantity

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Queue is one node of the hierarchical queue tree (decisions q7/q26). It is
// namespaced; its logical path is <metadata.namespace>.<metadata.name>.
type Queue struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec   QueueSpec   `json:"spec"`
	Status QueueStatus `json:"status"`
}

// QueueSpec is the desired state of a Queue.
type QueueSpec struct {
	// Parent is the bare name of a Queue CR in the same namespace; empty means a
	// direct child of the synthetic namespace root <namespace>. It is immutable
	// (rename = delete+create).
	Parent string `json:"parent,omitempty"`
	// CapacityMode is the capacity discriminator; only "absolute" in MVP
	// (decision q13).
	CapacityMode string `json:"capacityMode,omitempty"`
	// Guaranteed is the floor; sum over children must be <= parent.guaranteed.
	Guaranteed ResourceList `json:"guaranteed,omitempty"`
	// Max is the ceiling; clamped by the engine to parent.max and the namespace
	// ResourceQuota envelope.
	Max ResourceList `json:"max,omitempty"`
	// ApplicationSortPolicy is fifo|fair|stateaware.
	ApplicationSortPolicy string `json:"applicationSortPolicy,omitempty"`
	// Preemption configures fence/delay for reclaim (decision q3).
	Preemption *PreemptionSpec `json:"preemption,omitempty"`
	// MaxApplications caps running applications in this queue.
	MaxApplications int32 `json:"maxApplications,omitempty"`
	// SubmitACL authorizes by namespace; SA is a routing filter, not a boundary.
	SubmitACL string `json:"submitACL,omitempty"`
	// AdminACL grants administration / preemption override.
	AdminACL string `json:"adminACL,omitempty"`
	// ServiceAccounts routes (namespace, SA) pods to this leaf (decision q24).
	ServiceAccounts []string `json:"serviceAccounts,omitempty"`
	// Default marks this leaf as the namespace default queue — analogous to a
	// default StorageClass. Any pod whose (namespace, ServiceAccount) matches no
	// leaf's serviceAccounts lands here (over-max still fails closed to Pending).
	// At most one default queue per namespace; the legacy shorthand
	// serviceAccounts: ["*"] is an equivalent marker (decision q24 / G9).
	Default bool `json:"default,omitempty"`
	// DrainPolicy is graceful|forbid on delete (decision q15).
	DrainPolicy string `json:"drainPolicy,omitempty"`
	// Limits are per-tenant sub-limits within this queue.
	Limits []QueueLimit `json:"limits,omitempty"`
}

// PreemptionSpec configures reclaim behaviour for a queue.
type PreemptionSpec struct {
	// Policy is e.g. "fence".
	Policy string `json:"policy,omitempty"`
	// Delay is the anti-flap grace before victims are selected.
	Delay metav1.Duration `json:"delay"`
}

// QueueLimit is a sub-limit scoped to a set of service accounts.
type QueueLimit struct {
	ServiceAccounts []string     `json:"serviceAccounts,omitempty"`
	MaxApplications int32        `json:"maxApplications,omitempty"`
	MaxResources    ResourceList `json:"maxResources,omitempty"`
}

// QueueStatus is the observed state of a Queue.
type QueueStatus struct {
	// Path is the computed authoritative identity <namespace>.<...>.
	Path string `json:"path,omitempty"`
	// ParentPath is the resolved parent path.
	ParentPath string `json:"parentPath,omitempty"`
	// Phase is Active|Draining|Degraded|Orphaned.
	Phase string `json:"phase,omitempty"`
	// EffectiveMax is max after clamping to envelope/parent.
	EffectiveMax ResourceList `json:"effectiveMax,omitempty"`
	// Allocated / Pending are current usage.
	Allocated ResourceList `json:"allocated,omitempty"`
	Pending   ResourceList `json:"pending,omitempty"`
	// RunningApplications is the current running count.
	RunningApplications int32 `json:"runningApplications,omitempty"`
	// AdmittedToTree reports whether this node is in the live tree (else it is
	// subtree-quarantined, decision q31).
	AdmittedToTree bool `json:"admittedToTree,omitempty"`
	// Leaf reports whether this queue has no children.
	Leaf bool `json:"leaf,omitempty"`
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// QueueList is a list of Queue.
type QueueList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []Queue `json:"items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodGroup is the canonical gang unit (decisions q5/q26). All members resolve to
// one leaf queue and one scheduler profile.
type PodGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec   PodGroupSpec   `json:"spec"`
	Status PodGroupStatus `json:"status"`
}

// PodGroupSpec is the desired state of a gang.
type PodGroupSpec struct {
	// MinMember is the gang floor; strict all-or-nothing (decision q5).
	MinMember int32 `json:"minMember"`
	// MinResources is the aggregate ask validated <= queue.max on admission.
	MinResources ResourceList `json:"minResources,omitempty"`
	// Queue optionally names the target leaf (<namespace>.<cr>); else SA-routing.
	Queue string `json:"queue,omitempty"`
	// ScheduleTimeoutSeconds is the gang TTL (= Permit-Wait timeout).
	ScheduleTimeoutSeconds *int32 `json:"scheduleTimeoutSeconds,omitempty"`
	// MembersReadyTimeoutSeconds bounds lazy materialization of all members.
	MembersReadyTimeoutSeconds *int32 `json:"membersReadyTimeoutSeconds,omitempty"`
	// GangSchedulingStyle is Hard in MVP; Soft is a reserved forward-compat value.
	GangSchedulingStyle string `json:"gangSchedulingStyle,omitempty"`

	// Reserved forward-compat fields for a future partial/soft layer (decision q5):
	Policy  string `json:"policy,omitempty"`
	Desired *int32 `json:"desired,omitempty"`
	Total   *int32 `json:"total,omitempty"`
}

// PodGroupStatus is the observed state of a gang.
type PodGroupStatus struct {
	// Phase is Pending|PreScheduling|Scheduling|Scheduled|Running|Failed.
	Phase string `json:"phase,omitempty"`
	// Admitted latches the quota-CAS admission (decision q1); durable checkpoint.
	Admitted  bool  `json:"admitted,omitempty"`
	Scheduled int32 `json:"scheduled,omitempty"`
	Succeeded int32 `json:"succeeded,omitempty"`
	// ReservedResources is the virtual reservation held for recovery (q1).
	ReservedResources ResourceList `json:"reservedResources,omitempty"`
	// ReservedQueue is the leaf path the reservation is booked on, persisted so a
	// restarted scheduler can restore the gang's headroom hold before scheduling
	// resumes (q1 recovery), without waiting for members to re-enter admission.
	ReservedQueue string `json:"reservedQueue,omitempty"`
	// OccupiedNodes maps node name -> members placed there.
	OccupiedNodes map[string]int32 `json:"occupiedNodes,omitempty"`
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodGroupList is a list of PodGroup.
type PodGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []PodGroup `json:"items"`
}
