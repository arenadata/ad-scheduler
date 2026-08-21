package cache

import (
	"sync"

	corev1 "k8s.io/api/core/v1"

	"github.com/arenadata/ad-scheduler/pkg/resource"
)

// Cache is the scheduler's flat node/pod accounting (M1). An all-pods informer
// EventHandler drives it via OnPodBound/OnPodDeleted; the classification and
// accounting live here so they are testable without a cluster. The hierarchical
// queue tree is layered on top in M2.
type Cache struct {
	schedulerName string

	mu    sync.RWMutex
	nodes map[string]*Node
}

// New builds an empty Cache for the given scheduler name. Pods whose
// spec.schedulerName equals it are "ours"; everything else bound to a pool node
// is foreign and must still be accounted (decision q20).
func New(schedulerName string) *Cache {
	return &Cache{schedulerName: schedulerName, nodes: map[string]*Node{}}
}

// UpsertNode adds or refreshes a node, preserving its running Foreign/Ours
// tallies (allocatable may change on the underlying Node object).
func (c *Cache) UpsertNode(n *corev1.Node, inPool bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.nodes[n.Name]; ok {
		existing.InPool = inPool
		existing.Allocatable = resource.FromResourceList(n.Status.Allocatable)
		return
	}
	c.nodes[n.Name] = NewNode(n, inPool)
}

// RemoveNode drops a node from the pool view.
func (c *Cache) RemoveNode(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.nodes, name)
}

// Node returns the accounting view of a node.
func (c *Cache) Node(name string) (*Node, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	n, ok := c.nodes[name]
	return n, ok
}

// IsOurs reports whether a pod is scheduled by us (vs a foreign pod we only
// account for on our nodes).
func (c *Cache) IsOurs(pod *corev1.Pod) bool { return pod.Spec.SchedulerName == c.schedulerName }

// OnPodBound accounts a pod that has landed on a node (spec.nodeName set). A pod
// not yet bound, or on a node we do not track, is ignored. Its effective request
// is added to the node's Ours or Foreign tally depending on ownership.
func (c *Cache) OnPodBound(pod *corev1.Pod) {
	if pod.Spec.NodeName == "" {
		return
	}
	req := resource.EffectiveRequest(pod)
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[pod.Spec.NodeName]
	if !ok {
		return
	}
	if c.IsOurs(pod) {
		n.AddOurs(req)
	} else {
		n.AddForeign(req)
	}
}

// OnPodDeleted reverses the accounting OnPodBound applied for a pod.
func (c *Cache) OnPodDeleted(pod *corev1.Pod) {
	if pod.Spec.NodeName == "" {
		return
	}
	req := resource.EffectiveRequest(pod)
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[pod.Spec.NodeName]
	if !ok {
		return
	}
	if c.IsOurs(pod) {
		n.RemoveOurs(req)
	} else {
		n.RemoveForeign(req)
	}
}

// PoolAvailable returns total available capacity across in-pool nodes only —
// the cluster capacity the engine plans against (decision q20).
func (c *Cache) PoolAvailable() resource.Resource {
	c.mu.RLock()
	defer c.mu.RUnlock()
	total := resource.Resource{}
	for _, n := range c.nodes {
		if n.InPool {
			total = resource.Add(total, n.Available())
		}
	}
	return total
}
