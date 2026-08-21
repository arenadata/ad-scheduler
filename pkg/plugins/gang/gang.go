/*
Package gang is ad-scheduler's gang-scheduling plugin — all-or-nothing without
placeholder pods (decisions q1/q5/q9). It combines two mechanisms:

  - Admission by accounting (PreEnqueue): the first time a member of a gang is
    considered, the gang's aggregate minResources are reserved on its leaf
    (Engine.AdmitGang, a whole-gang-or-nothing CAS against headroom). If they do
    not fit, members stay gated (PreEnqueue returns Unschedulable) — invisible to
    the scheduler and consuming nothing — until headroom frees. No pause pods.
  - A Permit barrier: each admitted member that reaches Permit waits until
    minMember members are simultaneously assumed, then all are released together.
    If the barrier is not met within the timeout, members are rejected and
    requeued (all-or-nothing).

The capacity plugin skips its per-pod headroom gate and Reserve for gang members
so their footprint is counted exactly once (the gang reservation).
*/
package gang

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	fwk "k8s.io/kube-scheduler/framework"

	"github.com/arenadata/ad-scheduler/pkg/config"
	adfw "github.com/arenadata/ad-scheduler/pkg/framework"
	"github.com/arenadata/ad-scheduler/pkg/queue"
	"github.com/arenadata/ad-scheduler/pkg/queue/placement"
	"github.com/arenadata/ad-scheduler/pkg/util"
)

// Name is the plugin name used in KubeSchedulerConfiguration.
const Name = "AdGang"

// Gang is the gang-scheduling plugin.
type Gang struct {
	engine         *adfw.Engine
	handle         fwk.Handle
	defaultTimeout time.Duration
}

var (
	_ fwk.PreEnqueuePlugin = (*Gang)(nil)
	_ fwk.PermitPlugin     = (*Gang)(nil)
)

// Name returns the plugin name.
func (*Gang) Name() string { return Name }

// gangKey returns the (key, podGroupName, isMember) for a pod. A member carries
// the pod-group annotation; key is namespace/name.
func gangKey(pod *v1.Pod) (string, string, bool) {
	name := pod.Annotations[util.PodGroupAnnotation]
	if name == "" {
		return "", "", false
	}
	return pod.Namespace + "/" + name, name, true
}

// PreEnqueue admits the gang by accounting before its members enter the queue.
func (g *Gang) PreEnqueue(_ context.Context, pod *v1.Pod) *fwk.Status {
	key, name, member := gangKey(pod)
	if !member {
		return nil // not a gang member: normal enqueue
	}
	info, ok := g.engine.Coordinator().GangInfo(pod.Namespace, name)
	if !ok {
		// PodGroup not observed yet. If the gang is already admitted, let members
		// through (a momentary registry miss must not evict a live reservation);
		// otherwise gate until it is observed.
		if g.engine.IsGangAdmitted(key) {
			return nil
		}
		return fwk.NewStatus(fwk.Unschedulable, "ad-gang: PodGroup "+key+" not observed yet")
	}
	if info.Failed {
		// Hard-timeout already gave up on this gang: keep members gated (do not
		// re-admit) until the PodGroup is recreated. Pods are left untouched (q5).
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "ad-gang: gang "+key+" failed (Hard-timeout)")
	}
	// No quota to reserve (barrier-only gang): no leaf binding to keep consistent,
	// admit trivially.
	if !info.MinResources.StrictlyGtZero() {
		return nil
	}
	leaf, st := g.resolveLeaf(pod, info.Queue)
	if st != nil {
		return st
	}
	// One leaf per gang (decisions q1/q5): the gang's whole reservation lives on a
	// single leaf, so every member must resolve to it. A member whose SA routes it
	// to a different leaf would otherwise ride the wrong queue's reservation. This
	// is a cross-member invariant the apiserver cannot express (a VAP cannot join a
	// pod to its gang's siblings), so the engine is the sole gate: reject the stray
	// member rather than let it schedule against another queue's quota.
	if bound, ok := g.engine.GangLeaf(key); ok {
		if leaf != bound {
			return fwk.NewStatus(fwk.UnschedulableAndUnresolvable,
				fmt.Sprintf("ad-gang: member routes to leaf %q but gang %s is bound to %q; all gang members must map to one leaf", leaf, key, bound))
		}
		return nil // admitted on the matching leaf: let it through
	}
	// Not yet admitted: admit the gang on this member's leaf (binds the leaf).
	// Wound-wait head-of-line: rank the gang by (member priority DESC, gang age
	// ASC, gang key) so an older/larger gang is not starved by younger ones.
	wwkey := queue.WoundWaitKey{EffectivePriority: gangPriority(pod), Age: info.Age, UID: key}
	admitted, err := g.engine.AdmitGangWW(leaf, key, info.MinResources, wwkey)
	if err != nil {
		// Transient (tree not built yet) or a concurrent member just bound the gang
		// to a different leaf — retry; the GangLeaf guard above catches a genuine
		// mismatch on the next attempt with a terminal status.
		return fwk.NewStatus(fwk.Unschedulable, "ad-gang: "+err.Error())
	}
	if !admitted {
		return fwk.NewStatus(fwk.Unschedulable, "ad-gang: awaiting headroom for gang "+key)
	}
	return nil
}

// resolveLeaf picks the gang's leaf: the explicit PodGroup.spec.queue (a
// namespace-qualified path, normalised under root) if set, else the member's SA
// placement.
func (g *Gang) resolveLeaf(pod *v1.Pod, explicit string) (string, *fwk.Status) {
	t := g.engine.Tree()
	if t == nil {
		return "", fwk.NewStatus(fwk.Unschedulable, "ad-gang: queue tree not built yet")
	}
	if explicit != "" {
		path := explicit
		if !hasRootPrefix(path) {
			path = queue.RootName + "." + path
		}
		if _, ok := t.Queue(path); !ok {
			return "", fwk.NewStatus(fwk.Unschedulable, "ad-gang: PodGroup queue "+explicit+" not found")
		}
		return path, nil
	}
	leaf, err := placement.Resolve(t, util.PlacementKey(pod))
	if err != nil {
		return "", fwk.NewStatus(fwk.Unschedulable, "ad-gang: "+err.Error())
	}
	return leaf, nil
}

func hasRootPrefix(path string) bool {
	return len(path) >= len(queue.RootName)+1 && path[:len(queue.RootName)+1] == queue.RootName+"."
}

// gangPriority is the member pod's scheduling priority (gang members share it),
// the effective-priority component of the wound-wait ordering key.
func gangPriority(pod *v1.Pod) int32 {
	if pod.Spec.Priority != nil {
		return *pod.Spec.Priority
	}
	return 0
}

// Permit holds an admitted member until minMember members are simultaneously
// assumed, then releases the whole gang.
func (g *Gang) Permit(_ context.Context, _ fwk.CycleState, pod *v1.Pod, _ string) (*fwk.Status, time.Duration) {
	key, name, member := gangKey(pod)
	if !member {
		return nil, 0 // non-gang: Success, no wait
	}
	info, ok := g.engine.Coordinator().GangInfo(pod.Namespace, name)
	if !ok || info.MinMember <= 1 {
		return nil, 0 // unknown or trivial gang: no barrier
	}

	// Count gang members already waiting at the barrier, plus this one.
	waiting := 1
	g.handle.IterateOverWaitingPods(func(w fwk.WaitingPod) {
		if k, _, m := gangKey(w.GetPod()); m && k == key {
			waiting++
		}
	})
	if int32(waiting) >= info.MinMember {
		// Barrier met: release every waiting member of this gang; self proceeds.
		g.handle.IterateOverWaitingPods(func(w fwk.WaitingPod) {
			if k, _, m := gangKey(w.GetPod()); m && k == key {
				w.Allow(Name)
			}
		})
		return nil, 0
	}
	timeout := g.defaultTimeout
	if info.TimeoutSecs > 0 {
		timeout = time.Duration(info.TimeoutSecs) * time.Second
	}
	return fwk.NewStatus(fwk.Wait, fmt.Sprintf("ad-gang: waiting for %d/%d members of %s", waiting, info.MinMember, key)), timeout
}

// New is the PluginFactory registered via app.WithPlugin. It shares the engine
// and informer coordinator with the capacity plugin.
func New(ctx context.Context, _ k8sruntime.Object, handle fwk.Handle) (fwk.Plugin, error) {
	cfg := config.FromEnv()
	engine := adfw.GetOrInitEngine(cfg.SchedulerName, cfg)

	dynClient, err := dynamic.NewForConfig(handle.KubeConfig())
	if err != nil {
		return nil, fmt.Errorf("ad-gang: dynamic client: %w", err)
	}
	podInf := handle.SharedInformerFactory().Core().V1().Pods().Informer()
	nodeInf := handle.SharedInformerFactory().Core().V1().Nodes().Informer()
	rqInf := handle.SharedInformerFactory().Core().V1().ResourceQuotas().Informer()
	if _, err := engine.EnsureCoordinator(ctx, dynClient, podInf, nodeInf, rqInf); err != nil {
		return nil, fmt.Errorf("ad-gang: coordinator: %w", err)
	}
	return &Gang{engine: engine, handle: handle, defaultTimeout: cfg.GangScheduleTimeout}, nil
}
