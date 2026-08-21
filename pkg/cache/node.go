/*
Package cache holds the scheduler's in-memory view of pods, applications and
nodes layered over the framework cache. M1 provides the flat node accounting and
the application/task state machines; the hierarchical queue tree lands in M2.
*/
package cache

import (
	corev1 "k8s.io/api/core/v1"

	"github.com/arenadata/ad-scheduler/pkg/resource"
)

// Node is the scheduler's accounting view of one dedicated-pool node
// (decision q20). Availability is Allocatable minus everything already on the
// node — both our own assumed/bound pods and foreign pods (DaemonSets, static
// pods, pods with spec.nodeName set, anything the default-scheduler placed):
//
//	Available = Allocatable - Foreign - Ours
//
// Foreign accounting is mandatory (decision q20): without it a shared or
// drifted node would be over-subscribed.
type Node struct {
	Name string
	// InPool reports pool membership (the label-filtered informer, M1). A node
	// that is not in the pool is rejected by capacity.Filter regardless.
	InPool bool
	// Allocatable is the node's capacity.
	Allocatable resource.Resource
	// Foreign is the summed effective request of non-ad-scheduler pods bound here.
	Foreign resource.Resource
	// Ours is the summed effective request of our assumed/bound pods here.
	Ours resource.Resource
}

// NewNode builds a Node from a corev1.Node, marking pool membership.
func NewNode(n *corev1.Node, inPool bool) *Node {
	return &Node{
		Name:        n.Name,
		InPool:      inPool,
		Allocatable: resource.FromResourceList(n.Status.Allocatable),
		Foreign:     resource.Resource{},
		Ours:        resource.Resource{},
	}
}

// Available returns Allocatable - Foreign - Ours, clamped so no dimension is
// negative (an over-subscribed dimension reads as 0 available, so nothing more
// fits there).
func (n *Node) Available() resource.Resource {
	return clampNonNegative(resource.Sub(resource.Sub(n.Allocatable, n.Foreign), n.Ours))
}

// AddForeign / RemoveForeign track a foreign pod's effective request.
func (n *Node) AddForeign(req resource.Resource)    { n.Foreign = resource.Add(n.Foreign, req) }
func (n *Node) RemoveForeign(req resource.Resource) { n.Foreign = resource.Sub(n.Foreign, req) }

// AddOurs / RemoveOurs track one of our pods' effective request.
func (n *Node) AddOurs(req resource.Resource)    { n.Ours = resource.Add(n.Ours, req) }
func (n *Node) RemoveOurs(req resource.Resource) { n.Ours = resource.Sub(n.Ours, req) }

// clampNonNegative floors every dimension at 0.
func clampNonNegative(r resource.Resource) resource.Resource {
	out := make(resource.Resource, len(r))
	for k, v := range r {
		if v > 0 {
			out[k] = v
		}
	}
	return out
}
