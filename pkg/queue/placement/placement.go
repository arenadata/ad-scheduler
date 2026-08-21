/*
Package placement resolves a pod's identity (namespace, ServiceAccount) to a
leaf queue path, the K8s-agnostic core of decision q24 / G9.

Routing is declarative and read straight off the tree the controller assembled:

  - The namespace is the subtree apex by convention: root.<namespace>. A
    namespace with no such node is not onboarded → Reject (fail-closed).
  - Within that subtree an exact SA match on a leaf's serviceAccounts wins.
  - Otherwise the namespace default queue (a leaf marked spec.default, or the
    legacy serviceAccounts ["*"] alias — analogous to a default StorageClass,
    ≤1 per namespace) catches otherwise-unmapped SAs. This is what lets
    late/indirect pods (Spark executors spawned by a driver, inheriting its SA)
    resolve without a per-pod Queue edit (G9). A pod routed here still fails
    closed to Pending if it would push the queue over max.
  - No exact match and no default queue → Reject (fail-closed): an unmapped
    identity never silently lands in a wrong queue.
*/
package placement

import (
	"errors"
	"fmt"
	"slices"

	"github.com/arenadata/ad-scheduler/pkg/queue"
	"github.com/arenadata/ad-scheduler/pkg/util"
)

// ErrNoQueue is the sentinel for an identity that resolves to no leaf. Callers
// (capacity.PreFilter) turn it into an Unschedulable/Reject status.
var ErrNoQueue = errors.New("no queue for identity")

// Resolve maps id to a leaf queue path, or returns ErrNoQueue (fail-closed).
func Resolve(m *queue.QueueManager, id util.Identity) (string, error) {
	if _, ok := m.Queue(queue.RootName + "." + id.Namespace); !ok {
		return "", fmt.Errorf("%w: namespace %q not onboarded", ErrNoQueue, id.Namespace)
	}
	var defaultLeaf string
	for _, leaf := range m.Leaves() {
		if queue.NamespaceOf(leaf.Path()) != id.Namespace {
			continue
		}
		if slices.Contains(leaf.ServiceAccounts(), id.ServiceAccount) {
			return leaf.Path(), nil // exact match wins over default
		}
		if leaf.IsDefaultLeaf() {
			defaultLeaf = leaf.Path()
		}
	}
	if defaultLeaf != "" {
		return defaultLeaf, nil
	}
	return "", fmt.Errorf("%w: identity %s unmapped and namespace has no default queue", ErrNoQueue, id)
}
