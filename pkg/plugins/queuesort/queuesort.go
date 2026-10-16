/*
Package queuesort is ad-scheduler's single, process-wide QueueSort plugin
(decision: exactly one QueueSort; PrioritySort is disabled in the config). It
orders the scheduling queue by the engine's Less over an AppOrder built from each
pod: priority-desc, then enqueue-time-asc, then a stable UID tiebreak. Gang
clustering keys are wired here for forward-compat but stay inert until the gang
coordinator (M4) supplies stable per-gang keys.
*/
package queuesort

import (
	"context"

	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	fwk "k8s.io/kube-scheduler/framework"

	"github.com/arenadata/ad-scheduler/pkg/queue"
)

// Name is the plugin name used in KubeSchedulerConfiguration.
const Name = "AdQueueSort"

// QueueSort is the ordering plugin.
type QueueSort struct{}

var _ fwk.QueueSortPlugin = (*QueueSort)(nil)

// Name returns the plugin name.
func (*QueueSort) Name() string { return Name }

// Less reports whether entity a should be attempted before entity b. Entities are
// single pods or pod groups (QueuedEntityInfo).
func (*QueueSort) Less(a, b fwk.QueuedEntityInfo) bool {
	return queue.Less(queue.SortFIFO, toOrder(a), toOrder(b))
}

// toOrder projects a queued entity onto the engine's ordering read-model. Fair
// (DRF) share is a per-queue-descent concern, not the flat global QueueSort, so
// it is left zero here; the global order is priority-desc + enqueue-time-asc.
func toOrder(e fwk.QueuedEntityInfo) queue.AppOrder {
	return queue.AppOrder{
		Priority:    e.GetPriority(),
		SubmitNanos: e.GetTimestamp().UnixNano(),
		UID:         entityID(e),
	}
}

// entityID is the stable tiebreak key: the pod UID for a single pod, the group
// key for a pod group. The concrete queue types live in k8s.io/kubernetes, so
// they are matched structurally.
func entityID(e fwk.QueuedEntityInfo) string {
	switch v := e.(type) {
	case interface{ GetPodInfo() fwk.PodInfo }:
		if pi := v.GetPodInfo(); pi != nil && pi.GetPod() != nil {
			return string(pi.GetPod().UID)
		}
	case interface{ GetKey() string }:
		return string(e.Type()) + "/" + v.GetKey()
	}
	return ""
}

// New is the PluginFactory registered via app.WithPlugin.
func New(_ context.Context, _ k8sruntime.Object, _ fwk.Handle) (fwk.Plugin, error) {
	return &QueueSort{}, nil
}
