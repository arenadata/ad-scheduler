/*
Package capacity is ad-scheduler's capacity plugin — the thin framework adapter
over the shared engine (pkg/framework, pkg/queue). It owns:

  - PreFilter: resolve the pod's leaf queue by (namespace, ServiceAccount)
    placement, compute its effective request, and gate on queue headroom
    (borrow-up-to-max). Fail-closed: no tree / no queue mapping ⇒ reject.
  - Filter: reject nodes outside the dedicated pool (authoritative backstop to
    the label-filtered node view). Node-level resource fit stays with the
    in-tree NodeResourcesFit plugin.
  - Reserve/Unreserve: book / roll back the leaf's allocating reservation so a
    burst of pods sees each other's demand and queue max is enforced.
  - PostBind: commit the reservation to allocated once the pod is bound.

The factory (New) is where the engine and its informer coordinator are brought
up, once per profile (decisions q26/q28). Framework interface types live in the
staging module k8s.io/kube-scheduler/framework (imported as fwk); CycleState and
NodeInfo are interfaces (risk #1, §10.1).
*/
package capacity

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/labels"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	policyv1listers "k8s.io/client-go/listers/policy/v1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"

	"github.com/arenadata/ad-scheduler/pkg/config"
	adfw "github.com/arenadata/ad-scheduler/pkg/framework"
	"github.com/arenadata/ad-scheduler/pkg/queue"
	"github.com/arenadata/ad-scheduler/pkg/queue/placement"
	"github.com/arenadata/ad-scheduler/pkg/resource"
	"github.com/arenadata/ad-scheduler/pkg/util"
)

// Name is the plugin name used in KubeSchedulerConfiguration.
const Name = "AdCapacity"

// stateKey namespaces this plugin's per-cycle state.
const stateKey fwk.StateKey = "AdCapacity"

// preFilterState carries PreFilter's resolved leaf + effective request forward
// to Reserve/PostBind so they are not recomputed. gang marks a PodGroup member:
// its queue footprint is held by the gang's aggregate reservation (gang plugin),
// so per-pod Reserve/Commit and the per-pod headroom gate are skipped to avoid
// double-counting.
type preFilterState struct {
	leaf string
	req  resource.Resource
	gang bool
}

// Clone is a no-op: the state is immutable after PreFilter writes it.
func (s *preFilterState) Clone() fwk.StateData { return s }

// Capacity is the capacity plugin.
type Capacity struct {
	engine       *adfw.Engine
	handle       fwk.Handle // node snapshot + clientset for PostFilter preemption
	poolSelector labels.Selector

	// DRA device-quota resolution (decision q11): dra/<class> counts from the
	// pod's ResourceClaims, added to its effective request. Nil lookups (DRA
	// informers absent) resolve to no device requests.
	draClaims     resource.ClaimLookup
	draTemplates  resource.TemplateLookup
	draFailClosed bool

	// pdbLister backs PDB-aware node-level preemption: a candidate victim covered
	// by a PodDisruptionBudget with no remaining disruption budget is spared.
	pdbLister policyv1listers.PodDisruptionBudgetLister

	// nodeSortPolicy is how Score ranks feasible nodes (spread | binpack).
	nodeSortPolicy queue.NodeSortPolicy
}

var (
	_ fwk.PreFilterPlugin   = (*Capacity)(nil)
	_ fwk.FilterPlugin      = (*Capacity)(nil)
	_ fwk.PostFilterPlugin  = (*Capacity)(nil)
	_ fwk.ScorePlugin       = (*Capacity)(nil)
	_ fwk.ReservePlugin     = (*Capacity)(nil)
	_ fwk.PreBindPlugin     = (*Capacity)(nil)
	_ fwk.PostBindPlugin    = (*Capacity)(nil)
	_ fwk.EnqueueExtensions = (*Capacity)(nil)
)

// Name returns the plugin name.
func (*Capacity) Name() string { return Name }

// EventsToRegister tells the scheduling queue which cluster events can make a
// pod we rejected schedulable again, so gated pods are retried promptly instead
// of waiting for the periodic flush:
//
//   - Pod delete/completion frees queue headroom (the over-max reject);
//   - a Node added to (or relabelled into) the pool creates a feasible node (the
//     off-pool reject).
func (c *Capacity) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		{Event: fwk.ClusterEvent{Resource: fwk.Pod, ActionType: fwk.Delete}},
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add | fwk.UpdateNodeLabel}},
	}, nil
}

// PreFilter resolves the leaf queue and gates on queue headroom.
func (c *Capacity) PreFilter(_ context.Context, state fwk.CycleState, pod *v1.Pod, _ []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	t := c.engine.Tree()
	if t == nil {
		return nil, fwk.NewStatus(fwk.Unschedulable, "ad-scheduler: queue tree not built yet (fail-closed, q30)")
	}
	id := util.PlacementKey(pod)
	leaf, err := placement.Resolve(t, id)
	if err != nil {
		// unmapped identity: resolvable once a matching Queue/SA mapping exists.
		return nil, fwk.NewStatus(fwk.Unschedulable, "ad-scheduler: "+err.Error())
	}
	req := resource.EffectiveRequest(pod)
	if dra, err := resource.PodDeviceRequests(pod, c.draClaims, c.draTemplates, c.draFailClosed); err != nil {
		return nil, fwk.NewStatus(fwk.Unschedulable, "ad-scheduler: DRA: "+err.Error())
	} else if !dra.IsEmpty() {
		req = resource.Add(req, dra) // dra/<class> counts become extra dimensions
	}
	gang := pod.Annotations[util.PodGroupAnnotation] != ""
	if !gang {
		// maxApplications: cap the distinct running app-ids at the leaf. A pod
		// joining an already-running app is always admitted; a new app is gated
		// once the leaf is full. Only evaluated when the leaf sets the cap.
		if q, ok := t.Queue(leaf); ok {
			if maxApps := q.MaxApplications(); maxApps > 0 {
				apps := c.engine.Coordinator().AppIDsOnLeaf(leaf, pod.UID)
				if _, running := apps[util.AppID(pod)]; !running && len(apps) >= int(maxApps) {
					return nil, fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("ad-scheduler: queue %q at maxApplications=%d", leaf, maxApps))
				}
			}
			// Per-SA-group sub-limits (decision q19): apps + resources within the
			// queue, scoped to the limit's ServiceAccounts.
			for _, lim := range q.Limits() {
				if !saInSet(id.ServiceAccount, lim.ServiceAccounts) {
					continue
				}
				apps, used := c.engine.Coordinator().LeafLimitUsage(leaf, lim.ServiceAccounts, pod.UID)
				if lim.MaxApplications > 0 {
					if _, running := apps[util.AppID(pod)]; !running && len(apps) >= int(lim.MaxApplications) {
						return nil, fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("ad-scheduler: queue %q limit (SAs %v) at maxApplications=%d", leaf, lim.ServiceAccounts, lim.MaxApplications))
					}
				}
				if !lim.MaxResources.IsEmpty() && !resource.FitIn(resource.Add(used, req), lim.MaxResources) {
					return nil, fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("ad-scheduler: queue %q limit (SAs %v) over maxResources", leaf, lim.ServiceAccounts))
				}
			}
		}
		// Non-gang pods gate on queue headroom here. Gang members are gated by
		// the gang plugin's aggregate admission instead (avoids double-counting).
		ok, err := t.CanFit(leaf, req)
		if err != nil {
			return nil, fwk.AsStatus(err)
		}
		if !ok {
			return nil, fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("ad-scheduler: queue %q over max for request %v", leaf, req))
		}
	}
	state.Write(stateKey, &preFilterState{leaf: leaf, req: req, gang: gang})
	return nil, nil
}

// PreFilterExtensions: the capacity gate does not react to per-node add/remove
// simulation, so none.
func (c *Capacity) PreFilterExtensions() fwk.PreFilterExtensions { return nil }

// Filter rejects nodes outside the dedicated pool. Node resource fit is the
// in-tree NodeResourcesFit plugin's job.
func (c *Capacity) Filter(_ context.Context, _ fwk.CycleState, _ *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	node := nodeInfo.Node()
	if node == nil {
		return fwk.AsStatus(fmt.Errorf("ad-scheduler: nil node in Filter"))
	}
	if !c.poolSelector.Matches(labels.Set(node.Labels)) {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "ad-scheduler: node not in dedicated pool")
	}
	return nil
}

// Reserve books the leaf's allocating reservation for this pod. Gang members are
// skipped — their footprint is the gang's aggregate reservation.
func (c *Capacity) Reserve(_ context.Context, state fwk.CycleState, pod *v1.Pod, _ string) *fwk.Status {
	s, err := readState(state)
	if err != nil {
		return fwk.AsStatus(err)
	}
	if s.gang {
		return nil
	}
	c.engine.Reserve(pod.UID, s.leaf, s.req)
	return nil
}

// Unreserve rolls back the reservation on a bind failure / un-assume (keyed and
// idempotent, so it is a no-op for gang members that never reserved).
func (c *Capacity) Unreserve(_ context.Context, _ fwk.CycleState, pod *v1.Pod, _ string) {
	c.engine.Unreserve(pod.UID)
}

// PreBindPreFlight declares whether this plugin runs PreBind for the pod: it
// does for non-gang pods (the re-validate gate), and skips gang members.
func (c *Capacity) PreBindPreFlight(_ context.Context, state fwk.CycleState, _ *v1.Pod, _ string) (*fwk.PreBindPreFlightResult, *fwk.Status) {
	if s, err := readState(state); err != nil || s.gang {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	return nil, fwk.NewStatus(fwk.Success)
}

// PreBind is the final re-validate gate: between Reserve and Bind the queue could
// have drifted over max (e.g. a config rebuild lowered a max). If so, reject so
// the pod is unreserved and re-queued rather than bound over the cap.
func (c *Capacity) PreBind(_ context.Context, state fwk.CycleState, _ *v1.Pod, nodeName string) *fwk.Status {
	s, err := readState(state)
	if err != nil {
		return fwk.AsStatus(err)
	}
	if s.gang {
		return nil
	}
	t := c.engine.Tree()
	if t == nil {
		return fwk.NewStatus(fwk.Unschedulable, "ad-scheduler: queue tree gone before bind")
	}
	within, err := t.WithinMax(s.leaf)
	if err != nil {
		return fwk.AsStatus(err)
	}
	if !within {
		return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("ad-scheduler: queue %q over max at bind (headroom drift)", s.leaf))
	}
	// Shared-pool mode (decision q20): another scheduler can place a foreign pod on
	// this node between Filter and Bind, so re-validate the node still has room
	// (optimistic concurrency). In exclusive mode the pool taint guarantees we own
	// the node, so this check is skipped.
	if c.engine.Config().NodePool.Mode == config.NodePoolShared {
		if n, ok := c.engine.Cache().Node(nodeName); ok && !resource.FitIn(s.req, n.Available()) {
			return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("ad-scheduler: node %q lacks room at bind (shared-pool drift)", nodeName))
		}
	}
	return nil
}

// PostBind commits the reservation to allocated once the pod is bound. Gang
// members are skipped (the gang reservation stands until the gang is released).
func (c *Capacity) PostBind(_ context.Context, state fwk.CycleState, pod *v1.Pod, _ string) {
	if s, err := readState(state); err == nil && s.gang {
		return
	}
	c.engine.Commit(pod.UID)
}

func readState(state fwk.CycleState) (*preFilterState, error) {
	data, err := state.Read(stateKey)
	if err != nil {
		return nil, fmt.Errorf("ad-scheduler: PreFilter state missing: %w", err)
	}
	s, ok := data.(*preFilterState)
	if !ok {
		return nil, fmt.Errorf("ad-scheduler: PreFilter state has unexpected type %T", data)
	}
	return s, nil
}

// New is the PluginFactory registered via app.WithPlugin. It brings up (once per
// profile) the shared engine and its informer coordinator, then returns the
// plugin bound to them.
func New(ctx context.Context, _ k8sruntime.Object, handle fwk.Handle) (fwk.Plugin, error) {
	cfg := config.FromEnv()
	engine := adfw.GetOrInitEngine(cfg.SchedulerName, cfg)

	dynClient, err := dynamic.NewForConfig(handle.KubeConfig())
	if err != nil {
		return nil, fmt.Errorf("ad-scheduler: dynamic client: %w", err)
	}
	podInf := handle.SharedInformerFactory().Core().V1().Pods().Informer()
	nodeInf := handle.SharedInformerFactory().Core().V1().Nodes().Informer()
	rqInf := handle.SharedInformerFactory().Core().V1().ResourceQuotas().Informer()
	coord, err := engine.EnsureCoordinator(ctx, dynClient, podInf, nodeInf, rqInf)
	if err != nil {
		return nil, fmt.Errorf("ad-scheduler: coordinator: %w", err)
	}
	// Let the coordinator re-queue gated pods after it frees queue headroom, so
	// their retry sees the release (defeats the requeue-vs-release race), and give
	// it the clientset so the reclaim controller can evict victims.
	coord.SetActivator(func(pods map[string]*v1.Pod) { handle.Activate(klog.Background(), pods) })
	coord.SetClient(handle.ClientSet())
	coord.SetRecorder(handle.EventRecorder())

	sel, err := labels.Parse(cfg.NodePool.LabelSelector)
	if err != nil {
		return nil, fmt.Errorf("ad-scheduler: node pool selector %q: %w", cfg.NodePool.LabelSelector, err)
	}

	// DRA device-quota (decision q11): wire the ResourceClaim/Template listers so
	// PreFilter can add dra/<class> counts. Registering them starts the informers.
	claimLister := handle.SharedInformerFactory().Resource().V1().ResourceClaims().Lister()
	tmplLister := handle.SharedInformerFactory().Resource().V1().ResourceClaimTemplates().Lister()
	claims := func(ns, name string) (*resourceapi.ResourceClaim, error) {
		return claimLister.ResourceClaims(ns).Get(name)
	}
	templates := func(ns, name string) (*resourceapi.ResourceClaimTemplate, error) {
		return tmplLister.ResourceClaimTemplates(ns).Get(name)
	}
	// PDB lister for PDB-aware preemption (registering it starts the informer).
	pdbLister := handle.SharedInformerFactory().Policy().V1().PodDisruptionBudgets().Lister()

	return &Capacity{
		engine: engine, handle: handle, poolSelector: sel,
		draClaims: claims, draTemplates: templates, draFailClosed: cfg.DRAFailClosed,
		pdbLister: pdbLister, nodeSortPolicy: queue.ParseNodeSortPolicy(cfg.NodeSortPolicy),
	}, nil
}

// saInSet reports whether sa is covered by a limit's ServiceAccounts (exact match
// or the "*" wildcard).
func saInSet(sa string, set []string) bool {
	for _, s := range set {
		if s == "*" || s == sa {
			return true
		}
	}
	return false
}
