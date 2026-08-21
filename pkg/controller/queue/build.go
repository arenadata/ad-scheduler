/*
Package queue (controller) turns the authoritative Queue CRDs into the engine's
K8s-agnostic queue.Spec tree. This file is the pure builder — no informers, no
client — so the tree-assembly logic (grouping by namespace, resolving intra-
namespace parent references, synthesizing the namespace nodes, converting
resource vectors) is unit-testable without a cluster. The informer/reconcile
wrapper that watches Queue objects and atomically swaps the built tree lives
alongside it and is exercised only under cluster integration.

Path convention (decisions q7/q26): a Queue CR named <name> in namespace <ns>
with empty spec.parent becomes root.<ns>.<name>; the synthetic namespace node
root.<ns> is always present (never a CR, never a leaf) and carries the
namespace's ResourceQuota envelope as its max.
*/
package queue

import (
	"fmt"
	"sort"
	"strings"

	"k8s.io/klog/v2"

	"github.com/arenadata/ad-scheduler/pkg/admission"
	"github.com/arenadata/ad-scheduler/pkg/apis/scheduling/v1alpha1"
	"github.com/arenadata/ad-scheduler/pkg/queue"
	"github.com/arenadata/ad-scheduler/pkg/resource"
)

// BuildSpec assembles the root queue.Spec from all Queue CRs across namespaces.
// envelopes optionally maps a namespace to its ResourceQuota-derived max, applied
// to the synthetic namespace node (nil = unbounded). It fails closed: any
// structural error (unknown/cyclic parent, duplicate name) returns an error and
// no partial tree, so the caller keeps the last-good tree.
func BuildSpec(queues []v1alpha1.Queue, envelopes map[string]resource.Resource) (*queue.Spec, error) {
	byNS := map[string][]v1alpha1.Queue{}
	for i := range queues {
		q := queues[i]
		// Engine-side pre-check from the same invariant source as the VAP (q22):
		// a Queue that violates an admission rule is excluded fail-closed, so the
		// tree stays valid even if the VAP was not installed or was bypassed.
		if err := admission.ValidateQueue(&q); err != nil {
			klog.ErrorS(err, "ad-scheduler: excluding invalid Queue from the tree")
			continue
		}
		byNS[q.Namespace] = append(byNS[q.Namespace], q)
	}

	nsNames := make([]string, 0, len(byNS))
	for ns := range byNS {
		nsNames = append(nsNames, ns)
	}
	sort.Strings(nsNames) // deterministic tree order

	root := &queue.Spec{Name: queue.RootName}
	for _, ns := range nsNames {
		nsNode, err := buildNamespace(ns, byNS[ns], envelopes[ns])
		if err != nil {
			return nil, err
		}
		root.Children = append(root.Children, nsNode)
	}
	return root, nil
}

// buildNamespace builds one synthetic namespace node from that namespace's CRs.
func buildNamespace(ns string, crs []v1alpha1.Queue, envelope resource.Resource) (*queue.Spec, error) {
	byName := make(map[string]*v1alpha1.Queue, len(crs))
	childrenOf := map[string][]string{} // parent name ("" = ns root) -> child names
	for i := range crs {
		cr := &crs[i]
		if _, dup := byName[cr.Name]; dup {
			return nil, fmt.Errorf("namespace %q: duplicate Queue %q", ns, cr.Name)
		}
		byName[cr.Name] = cr
	}
	for i := range crs {
		cr := &crs[i]
		if cr.Spec.Parent != "" {
			if _, ok := byName[cr.Spec.Parent]; !ok {
				return nil, fmt.Errorf("namespace %q: Queue %q references unknown parent %q", ns, cr.Name, cr.Spec.Parent)
			}
		}
		childrenOf[cr.Spec.Parent] = append(childrenOf[cr.Spec.Parent], cr.Name)
	}

	nsNode := &queue.Spec{Name: ns, Max: envelope.Clone()}
	visiting := map[string]bool{}
	built := map[string]bool{}
	var build func(name string) (*queue.Spec, error)
	build = func(name string) (*queue.Spec, error) {
		if visiting[name] {
			return nil, fmt.Errorf("namespace %q: parent cycle at Queue %q", ns, name)
		}
		visiting[name] = true
		defer func() { visiting[name] = false }()

		cr := byName[name]
		spec := crToSpec(cr)
		for _, childName := range sortedStrings(childrenOf[name]) {
			child, err := build(childName)
			if err != nil {
				return nil, err
			}
			spec.Children = append(spec.Children, child)
		}
		built[name] = true
		return spec, nil
	}
	for _, topName := range sortedStrings(childrenOf[""]) {
		top, err := build(topName)
		if err != nil {
			return nil, err
		}
		nsNode.Children = append(nsNode.Children, top)
	}
	// A CR whose parent chain never reaches the ns root (only possible via a
	// cycle detached from the roots) would be unbuilt — reject fail-closed.
	if len(built) != len(byName) {
		return nil, fmt.Errorf("namespace %q: %d Queue(s) unreachable from namespace root (parent cycle?)", ns, len(byName)-len(built))
	}
	return nsNode, nil
}

// crToSpec converts one Queue CR (its own node, children filled by the caller).
func crToSpec(cr *v1alpha1.Queue) *queue.Spec {
	return &queue.Spec{
		Name:            cr.Name,
		Guaranteed:      resource.FromQuantityMap(cr.Spec.Guaranteed),
		Max:             resource.FromQuantityMap(cr.Spec.Max),
		ServiceAccounts: append([]string(nil), cr.Spec.ServiceAccounts...),
		Default:         cr.Spec.Default,
		MaxApplications: cr.Spec.MaxApplications,
		Limits:          crLimits(cr.Spec.Limits),
		SubmitACL:       cr.Spec.SubmitACL,
		AdminACL:        cr.Spec.AdminACL,
		Fence:           isFencePolicy(cr.Spec.Preemption),
	}
}

// isFencePolicy reports whether a Queue's preemption spec marks it a fence
// boundary (spec.preemption.policy=fence, case-insensitive). Top-level queues are
// fences implicitly regardless (see queue.Queue.isFence).
func isFencePolicy(p *v1alpha1.PreemptionSpec) bool {
	return p != nil && strings.EqualFold(p.Policy, "fence")
}

// crLimits converts the CRD's per-SA-group limits to the engine's Limit vectors.
func crLimits(in []v1alpha1.QueueLimit) []queue.Limit {
	if len(in) == 0 {
		return nil
	}
	out := make([]queue.Limit, 0, len(in))
	for _, l := range in {
		out = append(out, queue.Limit{
			ServiceAccounts: append([]string(nil), l.ServiceAccounts...),
			MaxApplications: l.MaxApplications,
			MaxResources:    resource.FromQuantityMap(l.MaxResources),
		})
	}
	return out
}

func sortedStrings(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}
