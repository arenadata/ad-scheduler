package framework

import (
	"context"
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/klog/v2"

	"github.com/arenadata/ad-scheduler/pkg/queue"
)

// drainFinalizer blocks deletion of a Queue with live allocation (decision q15).
const drainFinalizer = "scheduling.arenadata.io/queue-drain"

// syncQueueStatuses reconciles every Queue CR against the live tree: it writes
// the observable status (path/phase/leaf/admittedToTree) and enforces the drain
// finalizer — a Queue with live allocation cannot be deleted until it drains
// (phase Draining). Best-effort; per-object errors are logged, not fatal.
func (c *Coordinator) syncQueueStatuses(ctx context.Context, tree *queue.QueueManager) {
	if tree == nil {
		return
	}
	for _, o := range c.queueInf.GetStore().List() {
		u, ok := o.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		c.reconcileQueue(ctx, tree, u)
	}
}

func (c *Coordinator) reconcileQueue(ctx context.Context, tree *queue.QueueManager, u *unstructured.Unstructured) {
	ns, name := u.GetNamespace(), u.GetName()
	path, leaf, inTree := tree.FindByNamespaceName(ns, name)
	live := false
	if inTree {
		if used, _, ok := tree.QueueUsedGuaranteed(path); ok {
			live = !used.IsEmpty()
		}
	}
	deleting := u.GetDeletionTimestamp() != nil
	hasFin := hasString(u.GetFinalizers(), drainFinalizer)

	// Finalizer lifecycle: add on a live queue; remove once a deleting queue has
	// drained so the API can garbage-collect it.
	switch {
	case !deleting && !hasFin:
		c.updateFinalizers(ctx, u, append(u.GetFinalizers(), drainFinalizer))
		return
	case deleting && hasFin && !live:
		c.updateFinalizers(ctx, u, without(u.GetFinalizers(), drainFinalizer))
		return
	}

	phase := "Active"
	switch {
	case deleting:
		phase = "Draining"
	case !inTree:
		phase = "Degraded" // quarantined / not admitted to the live tree
	}
	c.patchQueueStatus(ctx, u, path, phase, inTree, leaf)
}

func (c *Coordinator) updateFinalizers(ctx context.Context, u *unstructured.Unstructured, finalizers []string) {
	cp := u.DeepCopy()
	cp.SetFinalizers(finalizers)
	if _, err := c.dynClient.Resource(queueGVR).Namespace(u.GetNamespace()).Update(ctx, cp, metav1.UpdateOptions{}); err != nil {
		klog.V(4).ErrorS(err, "ad-scheduler: queue finalizer update", "queue", u.GetNamespace()+"/"+u.GetName())
	}
}

func (c *Coordinator) patchQueueStatus(ctx context.Context, u *unstructured.Unstructured, path, phase string, inTree, leaf bool) {
	cur, _, _ := unstructured.NestedString(u.Object, "status", "path")
	curPhase, _, _ := unstructured.NestedString(u.Object, "status", "phase")
	if cur == path && curPhase == phase {
		return // no change; avoid a needless write
	}
	cp := u.DeepCopy()
	_ = unstructured.SetNestedField(cp.Object, path, "status", "path")
	_ = unstructured.SetNestedField(cp.Object, phase, "status", "phase")
	_ = unstructured.SetNestedField(cp.Object, inTree, "status", "admittedToTree")
	_ = unstructured.SetNestedField(cp.Object, leaf, "status", "leaf")
	if _, err := c.dynClient.Resource(queueGVR).Namespace(u.GetNamespace()).UpdateStatus(ctx, cp, metav1.UpdateOptions{}); err != nil {
		klog.V(4).ErrorS(err, "ad-scheduler: queue status update", "queue", u.GetNamespace()+"/"+u.GetName())
	}
}

func hasString(ss []string, s string) bool {
	return slices.Contains(ss, s)
}

func without(ss []string, s string) []string {
	out := ss[:0:0]
	for _, x := range ss {
		if x != s {
			out = append(out, x)
		}
	}
	return out
}
