package framework

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"github.com/arenadata/ad-scheduler/pkg/apis/scheduling/v1alpha1"
	ctrlpodgroup "github.com/arenadata/ad-scheduler/pkg/controller/podgroup"
	"github.com/arenadata/ad-scheduler/pkg/queue"
	"github.com/arenadata/ad-scheduler/pkg/resource"
	"github.com/arenadata/ad-scheduler/pkg/util"
)

// podGroupPhaseFailed is the terminal phase for a gang that gave up (Hard-timeout).
const podGroupPhaseFailed = "Failed"

// Gang admission-by-accounting keeps its reservations in an in-memory ledger, so
// a scheduler restart would lose them until members re-enter admission — and in
// that window another queue could grab the freed headroom, starving the gang
// (decision q1). To close that gap the coordinator persists each admitted gang's
// reservation into PodGroup.status and, on rebuild, restores any reservation the
// (empty, freshly-started) ledger is missing before scheduling resumes.

// recoverGangs restores gang reservations from PodGroup.status into the ledger.
// It runs on every rebuild and is idempotent: a gang already in the ledger (the
// normal case) is skipped, so only a fresh-start (empty ledger) actually
// re-admits. Best-effort — a reservation that no longer fits the current tree
// (capacity shrank) is logged and left for the members to re-contend.
func (c *Coordinator) recoverGangs(tree *queue.QueueManager) {
	if tree == nil {
		return
	}
	for _, o := range c.podGroupInf.GetStore().List() {
		u, ok := o.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		var pg v1alpha1.PodGroup
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &pg); err != nil {
			continue
		}
		if pg.Status.Phase == podGroupPhaseFailed {
			continue // a gang that gave up is not restored
		}
		if !pg.Status.Admitted || pg.Status.ReservedQueue == "" {
			continue
		}
		key := pg.Namespace + "/" + pg.Name
		if c.engine.IsGangAdmitted(key) {
			continue // ledger already holds it (normal rebuild) — nothing to restore
		}
		minRes := resource.FromQuantityMap(pg.Status.ReservedResources)
		if !minRes.StrictlyGtZero() {
			continue
		}
		if _, ok := tree.Queue(pg.Status.ReservedQueue); !ok {
			continue // its leaf is gone from the tree — cannot restore
		}
		admitted, err := c.engine.AdmitGang(pg.Status.ReservedQueue, key, minRes)
		if err != nil || !admitted {
			klog.V(2).InfoS("ad-scheduler: gang reservation not restored (capacity changed?)",
				"gang", key, "leaf", pg.Status.ReservedQueue, "admitted", admitted, "err", err)
			continue
		}
		klog.V(2).InfoS("ad-scheduler: restored gang reservation after restart", "gang", key, "leaf", pg.Status.ReservedQueue)
	}
}

// syncGangStatuses writes the live gang ledger back to PodGroup.status so it is
// durable across restarts. It stamps admitted gangs with their leaf + reservation
// and clears PodGroups no longer holding one. Best-effort; runs alongside the
// queue-status sync in the reclaim loop.
func (c *Coordinator) syncGangStatuses(ctx context.Context) {
	admitted := c.engine.AdmittedGangs()
	// Track when each reservation was first seen (the Hard-timeout clock) and drop
	// timestamps for gangs no longer holding one.
	now := time.Now()
	c.mu.Lock()
	for key := range admitted {
		if _, ok := c.gangAdmittedAt[key]; !ok {
			c.gangAdmittedAt[key] = now
		}
	}
	for key := range c.gangAdmittedAt {
		if _, ok := admitted[key]; !ok {
			delete(c.gangAdmittedAt, key)
		}
	}
	c.mu.Unlock()
	for _, o := range c.podGroupInf.GetStore().List() {
		u, ok := o.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		ns, name := u.GetNamespace(), u.GetName()
		res, isAdmitted := admitted[ns+"/"+name]

		curAdmitted, _, _ := unstructured.NestedBool(u.Object, "status", "admitted")
		curLeaf, _, _ := unstructured.NestedString(u.Object, "status", "reservedQueue")
		if curAdmitted == isAdmitted && (!isAdmitted || curLeaf == res.Leaf) {
			continue // no observable change; avoid a needless write
		}
		c.patchGangStatus(ctx, ns, name, isAdmitted, res)
		// eventbridge: surface admission/release on the PodGroup for kubectl describe.
		if curAdmitted != isAdmitted {
			if isAdmitted {
				c.emitEvent(u, nil, corev1.EventTypeNormal, "GangAdmitted", "Gang",
					"Admitted: reserved %v on %s", res.MinRes, res.Leaf)
			} else {
				c.emitEvent(u, nil, corev1.EventTypeNormal, "GangReleased", "Gang", "Gang reservation released")
			}
		}
	}
}

func (c *Coordinator) patchGangStatus(ctx context.Context, ns, name string, admitted bool, res GangReservation) {
	status := map[string]any{"admitted": admitted}
	if admitted {
		status["reservedQueue"] = res.Leaf
		status["reservedResources"] = resource.ToQuantityMap(res.MinRes)
	} else {
		status["reservedQueue"] = ""
		status["reservedResources"] = nil
	}
	body, err := json.Marshal(map[string]any{"status": status})
	if err != nil {
		return
	}
	if _, err := c.dynClient.Resource(podGroupGVR).Namespace(ns).
		Patch(ctx, name, types.MergePatchType, body, metav1.PatchOptions{}, "status"); err != nil {
		klog.V(4).ErrorS(err, "ad-scheduler: gang status patch", "gang", ns+"/"+name)
	}
}

// gangTimeoutVerdict decides whether an admitted gang has given up, and why. Two
// independent deadlines run from admission: membersReadyTO bounds lazy
// materialization (all minMember members must EXIST as pods), scheduleTO is the
// Hard timeout (minMember must be BOUND). members-ready is checked first because
// it is the earlier, cheaper failure (a gang whose members never all show up can
// never assemble); a gang that materialized/assembled in time is spared. A zero
// membersReadyTO disables that check.
func gangTimeoutVerdict(elapsed, membersReadyTO, scheduleTO time.Duration, created, bound, minMember int) (fail bool, reason string) {
	if membersReadyTO > 0 && elapsed >= membersReadyTO && created < minMember {
		return true, "members not materialized within membersReadyTimeoutSeconds"
	}
	if elapsed >= scheduleTO && bound < minMember {
		return true, "members not assembled within scheduleTimeoutSeconds"
	}
	return false, ""
}

// checkGangTimeouts enforces the gang timeouts (decision q5): an admitted gang
// that does not materialize all members (membersReadyTimeoutSeconds) or assemble
// minMember bound members (scheduleTimeoutSeconds) gives up — its reservation is
// released and the PodGroup is marked Failed so PreEnqueue stops re-admitting it.
// Placed members (0 under the all-or-nothing Permit barrier) are left untouched.
// Runs in the reclaim loop after syncGangStatuses has stamped the admission clock.
func (c *Coordinator) checkGangTimeouts(ctx context.Context) {
	now := time.Now()
	defaultTO := c.engine.Config().GangScheduleTimeout
	admitted := c.engine.AdmittedGangs()

	// Collect gangs whose earliest deadline has elapsed under the lock (cheap), then
	// evaluate the live member counts outside it (a pod-store scan per candidate).
	type cand struct {
		key, ns, name                       string
		minMember                           int
		elapsed, membersReadyTO, scheduleTO time.Duration
	}
	var cands []cand
	c.mu.Lock()
	for key := range admitted {
		at, ok := c.gangAdmittedAt[key]
		if !ok {
			continue // clock not stamped yet (set next sync)
		}
		info := c.gangs[key]
		if info.Failed {
			continue
		}
		scheduleTO := defaultTO
		if info.TimeoutSecs > 0 {
			scheduleTO = time.Duration(info.TimeoutSecs) * time.Second
		}
		membersReadyTO := time.Duration(info.MembersReadyTimeoutSecs) * time.Second
		elapsed := now.Sub(at)
		if elapsed < scheduleTO && (membersReadyTO == 0 || elapsed < membersReadyTO) {
			continue // no deadline reached yet
		}
		ns, name := splitGangKey(key)
		cands = append(cands, cand{key, ns, name, int(info.MinMember), elapsed, membersReadyTO, scheduleTO})
	}
	c.mu.Unlock()

	for _, t := range cands {
		fail, reason := gangTimeoutVerdict(t.elapsed, t.membersReadyTO, t.scheduleTO,
			c.createdGangMembers(t.ns, t.name), c.boundGangMembers(t.ns, t.name), t.minMember)
		if !fail {
			continue // materialized/assembled after all — not a timeout
		}
		// Latch Failed immediately (durable via the status patch below) so members
		// stop re-admitting, release the reservation, and free its headroom.
		c.mu.Lock()
		if info, ok := c.gangs[t.key]; ok {
			info.Failed = true
			c.gangs[t.key] = info
		}
		delete(c.gangAdmittedAt, t.key)
		c.mu.Unlock()
		_ = c.engine.ReleaseGang(t.key)
		c.patchGangFailed(ctx, t.ns, t.name)
		c.activatePending() // others may now use the freed headroom
		if o, ok, _ := c.podGroupInf.GetStore().GetByKey(t.key); ok {
			if u, ok := o.(*unstructured.Unstructured); ok {
				c.emitEvent(u, nil, corev1.EventTypeWarning, "GangTimedOut", "Gang",
					"Gang gave up (%s); reservation released", reason)
			}
		}
		klog.InfoS("ad-scheduler: gang timeout — marked Failed, reservation released", "gang", t.key, "reason", reason)
	}
}

// GangProgress reports how many of a gang's members are already placed and its
// minMember, so node-level preemption can limit itself to the last unplaced
// member (avoiding evictions for a gang still far from assembling). ok is false
// for an unknown gang.
func (c *Coordinator) GangProgress(ns, name string) (placed int, minMember int32, ok bool) {
	c.mu.Lock()
	info, exists := c.gangs[ns+"/"+name]
	c.mu.Unlock()
	if !exists {
		return 0, 0, false
	}
	return c.boundGangMembers(ns, name), info.MinMember, true
}

// boundGangMembers counts this gang's members already placed on a node.
func (c *Coordinator) boundGangMembers(ns, name string) int {
	n := 0
	for _, o := range c.podInf.GetStore().List() {
		p, ok := o.(*corev1.Pod)
		if !ok || p.Namespace != ns || p.Spec.NodeName == "" {
			continue
		}
		if p.Annotations[util.PodGroupAnnotation] == name {
			n++
		}
	}
	return n
}

// createdGangMembers counts this gang's members that currently exist as pods
// (materialized), regardless of whether they are bound — the membersReady check.
// Pods being deleted no longer count toward materialization.
func (c *Coordinator) createdGangMembers(ns, name string) int {
	n := 0
	for _, o := range c.podInf.GetStore().List() {
		p, ok := o.(*corev1.Pod)
		if !ok || p.Namespace != ns || p.DeletionTimestamp != nil {
			continue
		}
		if p.Annotations[util.PodGroupAnnotation] == name {
			n++
		}
	}
	return n
}

// succeededGangMembers counts this gang's members in the Succeeded phase (drives
// the Completed lifecycle phase and the status.succeeded count).
func (c *Coordinator) succeededGangMembers(ns, name string) int {
	n := 0
	for _, o := range c.podInf.GetStore().List() {
		p, ok := o.(*corev1.Pod)
		if !ok || p.Namespace != ns || p.Status.Phase != corev1.PodSucceeded {
			continue
		}
		if p.Annotations[util.PodGroupAnnotation] == name {
			n++
		}
	}
	return n
}

// syncPodGroupPhases reconciles PodGroup.status.phase (+ scheduled/succeeded
// counts) from the live gang state — the dedicated PodGroup status controller
// (its pure decision is controller/podgroup.Observed.Phase). It gives
// `kubectl get podgroup` the gang lifecycle: Pending → Scheduling → Running →
// Completed, or Failed. Runs in the reclaim loop after checkGangTimeouts, so a
// gang already latched Failed is left terminal; only a changed phase/count is
// patched (no status-write feedback loop).
func (c *Coordinator) syncPodGroupPhases(ctx context.Context) {
	admitted := c.engine.AdmittedGangs()
	for _, o := range c.podGroupInf.GetStore().List() {
		u, ok := o.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		ns, name := u.GetNamespace(), u.GetName()
		key := ns + "/" + name
		c.mu.Lock()
		info, known := c.gangs[key]
		c.mu.Unlock()
		if !known {
			continue
		}
		curPhase, _, _ := unstructured.NestedString(u.Object, "status", "phase")
		if curPhase == podGroupPhaseFailed {
			continue // terminal — the timeout path owns it
		}
		bound := c.boundGangMembers(ns, name)
		succeeded := c.succeededGangMembers(ns, name)
		_, isAdmitted := admitted[key]
		phase := ctrlpodgroup.Observed{
			Admitted:  isAdmitted,
			Bound:     bound,
			Succeeded: succeeded,
			MinMember: int(info.MinMember),
			Failed:    info.Failed,
		}.Phase()

		curScheduled, _, _ := unstructured.NestedInt64(u.Object, "status", "scheduled")
		curSucceeded, _, _ := unstructured.NestedInt64(u.Object, "status", "succeeded")
		if curPhase == phase && curScheduled == int64(bound) && curSucceeded == int64(succeeded) {
			continue // no observable change; avoid a needless write
		}
		c.patchPodGroupPhase(ctx, ns, name, phase, bound, succeeded)
	}
}

func (c *Coordinator) patchPodGroupPhase(ctx context.Context, ns, name, phase string, scheduled, succeeded int) {
	body, _ := json.Marshal(map[string]any{"status": map[string]any{
		"phase":     phase,
		"scheduled": int64(scheduled),
		"succeeded": int64(succeeded),
	}})
	if _, err := c.dynClient.Resource(podGroupGVR).Namespace(ns).
		Patch(ctx, name, types.MergePatchType, body, metav1.PatchOptions{}, "status"); err != nil {
		klog.V(4).ErrorS(err, "ad-scheduler: podgroup phase patch", "gang", ns+"/"+name)
	}
}

// patchGangFailed marks the PodGroup terminally Failed and clears its reservation.
func (c *Coordinator) patchGangFailed(ctx context.Context, ns, name string) {
	body, _ := json.Marshal(map[string]any{"status": map[string]any{
		"phase": podGroupPhaseFailed, "admitted": false, "reservedQueue": "", "reservedResources": nil,
	}})
	if _, err := c.dynClient.Resource(podGroupGVR).Namespace(ns).
		Patch(ctx, name, types.MergePatchType, body, metav1.PatchOptions{}, "status"); err != nil {
		klog.V(4).ErrorS(err, "ad-scheduler: gang failed patch", "gang", ns+"/"+name)
	}
}

func splitGangKey(key string) (ns, name string) {
	if before, after, ok := strings.Cut(key, "/"); ok {
		return before, after
	}
	return "", key
}
