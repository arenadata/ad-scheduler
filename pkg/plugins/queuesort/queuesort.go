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

// Less reports whether pod a should be attempted before pod b.
func (*QueueSort) Less(a, b fwk.QueuedPodInfo) bool {
	return queue.Less(queue.SortFIFO, toOrder(a), toOrder(b))
}

// toOrder projects a queued pod onto the engine's ordering read-model. Fair
// (DRF) share is a per-queue-descent concern, not the flat global QueueSort, so
// it is left zero here; the global order is priority-desc + enqueue-time-asc.
func toOrder(qpi fwk.QueuedPodInfo) queue.AppOrder {
	pod := qpi.GetPodInfo().GetPod()
	var prio int32
	if pod.Spec.Priority != nil {
		prio = *pod.Spec.Priority
	}
	return queue.AppOrder{
		Priority:    prio,
		SubmitNanos: qpi.GetTimestamp().UnixNano(),
		UID:         string(pod.UID),
	}
}

// New is the PluginFactory registered via app.WithPlugin.
func New(_ context.Context, _ k8sruntime.Object, _ fwk.Handle) (fwk.Plugin, error) {
	return &QueueSort{}, nil
}
