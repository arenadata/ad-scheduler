package framework

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	clientcache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/events"
	"k8s.io/klog/v2"

	"github.com/arenadata/ad-scheduler/pkg/admission"
	"github.com/arenadata/ad-scheduler/pkg/apis/scheduling/v1alpha1"
	ctrlqueue "github.com/arenadata/ad-scheduler/pkg/controller/queue"
	"github.com/arenadata/ad-scheduler/pkg/queue"
	"github.com/arenadata/ad-scheduler/pkg/queue/placement"
	"github.com/arenadata/ad-scheduler/pkg/resource"
	"github.com/arenadata/ad-scheduler/pkg/util"
)

// The dynamic resources the coordinator watches.
var (
	queueGVR    = schema.GroupVersionResource{Group: v1alpha1.GroupName, Version: "v1alpha1", Resource: "queues"}
	podGroupGVR = schema.GroupVersionResource{Group: v1alpha1.GroupName, Version: "v1alpha1", Resource: "podgroups"}
)

// trackedPod is the last-known accounting footprint of a bound pod, so pod
// updates are idempotent and deletes reverse exactly what an add applied. gang
// members are not booked per-pod into their queue (the gang's aggregate
// reservation holds their footprint), only accounted on the node.
type trackedPod struct {
	ours     bool
	gang     bool
	identity util.Identity
	req      resource.Resource
}

// GangInfo is the gang admission spec the gang plugin needs, sourced from a
// PodGroup CR. Queue is the optional explicit leaf path (spec.queue); when empty
// the gang plugin resolves the leaf from the member pod's SA placement.
type GangInfo struct {
	MinMember    int32
	MinResources resource.Resource
	Queue        string
	TimeoutSecs  int32
	// MembersReadyTimeoutSecs bounds lazy materialization (spec.
	// membersReadyTimeoutSeconds): all minMember members must EXIST as pods within
	// this window of admission, else the gang gives up early — distinct from
	// TimeoutSecs, which bounds how long assembled members take to BIND. 0 = off.
	MembersReadyTimeoutSecs int32
	// Failed latches a Hard-timeout abort (driven by PodGroup.status.phase), so
	// PreEnqueue refuses to re-admit members of a gang that already gave up.
	Failed bool
	// Age is the wound-wait ordering age: -CreationUnixNano, so an older gang
	// (earlier creation) sorts as "older" (larger Age) and wins head-of-line.
	Age int64
}

// Coordinator drives the Engine from informers: a dynamic Queue informer
// (debounced full rebuild → atomic swap) plus the scheduler's shared pod/node
// informers (foreign-pod accounting and per-queue allocation). It is created
// once per profile by the capacity plugin factory.
type Coordinator struct {
	engine       *Engine
	poolSelector labels.Selector

	dynClient   dynamic.Interface
	dynFactory  dynamicinformer.DynamicSharedInformerFactory
	queueInf    clientcache.SharedIndexInformer
	podGroupInf clientcache.SharedIndexInformer
	podInf      clientcache.SharedIndexInformer
	nodeInf     clientcache.SharedIndexInformer
	rqInf       clientcache.SharedIndexInformer

	mu             sync.Mutex
	tracked        map[types.UID]trackedPod
	gangs          map[string]GangInfo  // gangKey (ns/name) -> spec, from PodGroups
	gangAdmittedAt map[string]time.Time // gangKey -> when its reservation was first seen (Hard-timeout)

	// activate re-queues our pending pods to the active queue. It is the plugin's
	// handle.Activate; set once by a plugin factory. When a pod frees queue
	// headroom, we release the allocation and THEN activate gated pods, so their
	// retry sees the freed headroom (the framework's event-driven requeue would
	// otherwise race the release and leave them stuck under QueueingHints).
	activate atomic.Value // func(map[string]*corev1.Pod)
	// client deletes victims for the reclaim controller; set by a plugin factory.
	client atomic.Value // kubernetes.Interface
	// recorder emits K8s Events for scheduling decisions (reclaim/gang), so they
	// show up in kubectl describe (the eventbridge). Set by a plugin factory.
	recorder atomic.Value // events.EventRecorderLogger

	rebuildCh chan struct{}
	started   sync.Once

	// ctx is captured at Start so SetClient can launch the leader-elected reclaim
	// loop once the clientset arrives (the plugin factory sets it after Start).
	ctx    context.Context
	leOnce sync.Once
}

// SetRecorder wires the K8s EventRecorder for the eventbridge.
func (c *Coordinator) SetRecorder(r events.EventRecorderLogger) {
	if r != nil {
		c.recorder.Store(r)
	}
}

// emitEvent records a K8s Event on regarding (attributed to related), best-effort.
func (c *Coordinator) emitEvent(regarding, related runtime.Object, eventtype, reason, action, note string, args ...any) {
	if r, ok := c.recorder.Load().(events.EventRecorderLogger); ok && r != nil {
		r.Eventf(regarding, related, eventtype, reason, action, note, args...)
	}
}

// SetClient wires the clientset the reclaim controller uses to evict victims. It
// also launches the leader-elected reclaim loop (reconciler HA, decision q30):
// the mutating loops (reclaim eviction, queue/gang status, gang Hard-timeout) run
// only on the reconciler leader, so multiple scheduler replicas never fight.
func (c *Coordinator) SetClient(cs kubernetes.Interface) {
	if cs != nil {
		c.client.Store(cs)
		c.startReconcilerLE()
	}
}

// SetActivator wires the scheduling-queue activator (handle.Activate bound with a
// logger). Idempotent; the last caller wins (all plugins share one handle).
func (c *Coordinator) SetActivator(fn func(map[string]*corev1.Pod)) {
	if fn != nil {
		c.activate.Store(fn)
	}
}

// NewCoordinator wires the informers. kubeCfg builds the dynamic client for the
// Queue CRD; podInf/nodeInf are the scheduler's shared informers from
// handle.SharedInformerFactory().
func NewCoordinator(engine *Engine, dynClient dynamic.Interface, podInf, nodeInf, rqInf clientcache.SharedIndexInformer) (*Coordinator, error) {
	sel, err := labels.Parse(engine.Config().NodePool.LabelSelector)
	if err != nil {
		return nil, err
	}
	factory := dynamicinformer.NewDynamicSharedInformerFactory(dynClient, 0)
	c := &Coordinator{
		engine:         engine,
		poolSelector:   sel,
		dynClient:      dynClient,
		dynFactory:     factory,
		queueInf:       factory.ForResource(queueGVR).Informer(),
		podGroupInf:    factory.ForResource(podGroupGVR).Informer(),
		podInf:         podInf,
		nodeInf:        nodeInf,
		rqInf:          rqInf,
		tracked:        map[types.UID]trackedPod{},
		gangs:          map[string]GangInfo{},
		gangAdmittedAt: map[string]time.Time{},
		rebuildCh:      make(chan struct{}, 1),
	}
	return c, nil
}

// HasSynced gates scheduler readiness: every driving informer has synced.
func (c *Coordinator) HasSynced() bool {
	return c.queueInf.HasSynced() && c.podGroupInf.HasSynced() && c.podInf.HasSynced() &&
		c.nodeInf.HasSynced() && c.rqInf.HasSynced()
}

// GangInfo returns the admission spec for a gang (PodGroup) by namespace/name.
func (c *Coordinator) GangInfo(namespace, name string) (GangInfo, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, ok := c.gangs[namespace+"/"+name]
	return g, ok
}

// LeafLimitUsage returns the distinct app-ids and the summed effective request of
// our non-terminal pods on leaf whose ServiceAccount is in saSet (or saSet has
// "*"), excluding the pod with UID exclude — backing per-SA-group Limits. Only
// computed when a leaf actually declares limits.
func (c *Coordinator) LeafLimitUsage(leaf string, saSet []string, exclude types.UID) (map[string]struct{}, resource.Resource) {
	apps := map[string]struct{}{}
	used := resource.Resource{}
	tree := c.engine.Tree()
	if tree == nil {
		return apps, used
	}
	name := c.engine.Config().SchedulerName
	set := map[string]bool{}
	star := false
	for _, s := range saSet {
		if s == "*" {
			star = true
		}
		set[s] = true
	}
	for _, o := range c.podInf.GetStore().List() {
		p, ok := o.(*corev1.Pod)
		if !ok || p.UID == exclude || p.Spec.SchedulerName != name || p.DeletionTimestamp != nil {
			continue
		}
		if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		id := util.PlacementKey(p)
		if !star && !set[id.ServiceAccount] {
			continue
		}
		if l, err := placement.Resolve(tree, id); err != nil || l != leaf {
			continue
		}
		apps[util.AppID(p)] = struct{}{}
		used = resource.Add(used, resource.EffectiveRequest(p))
	}
	return apps, used
}

// AppIDsOnLeaf returns the distinct application ids of our non-terminal pods
// currently routed to leaf, excluding the pod with UID exclude. It backs the
// capacity gate that enforces a leaf's maxApplications (decision: one app = one
// app-id, per util.AppID). Only computed when a leaf actually sets the cap.
func (c *Coordinator) AppIDsOnLeaf(leaf string, exclude types.UID) map[string]struct{} {
	tree := c.engine.Tree()
	if tree == nil {
		return nil
	}
	name := c.engine.Config().SchedulerName
	apps := map[string]struct{}{}
	for _, o := range c.podInf.GetStore().List() {
		p, ok := o.(*corev1.Pod)
		if !ok || p.UID == exclude || p.Spec.SchedulerName != name || p.DeletionTimestamp != nil {
			continue
		}
		if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		if l, err := placement.Resolve(tree, util.PlacementKey(p)); err == nil && l == leaf {
			apps[util.AppID(p)] = struct{}{}
		}
	}
	return apps
}

// Start registers handlers and runs the dynamic informer + rebuild loop under
// ctx. It is idempotent (safe if several plugin factories call it).
func (c *Coordinator) Start(ctx context.Context) {
	c.started.Do(func() {
		_, _ = c.queueInf.AddEventHandler(clientcache.ResourceEventHandlerFuncs{
			AddFunc: func(any) { c.triggerRebuild() },
			// Rebuild only when the spec changed. With the status subresource on,
			// metadata.generation bumps on spec writes only, so a status-only patch
			// from our own status-writer leaves generation unchanged and must NOT
			// rebuild — otherwise the writer feedback-loops a rebuild every cycle.
			UpdateFunc: func(oldObj, newObj any) {
				if queueSpecChanged(oldObj, newObj) {
					c.triggerRebuild()
				}
			},
			DeleteFunc: func(any) { c.triggerRebuild() },
		})
		_, _ = c.nodeInf.AddEventHandler(clientcache.ResourceEventHandlerFuncs{
			AddFunc:    c.onNodeUpsert,
			UpdateFunc: func(_, obj any) { c.onNodeUpsert(obj) },
			DeleteFunc: c.onNodeDelete,
		})
		_, _ = c.podInf.AddEventHandler(clientcache.ResourceEventHandlerFuncs{
			AddFunc:    c.onPodUpsert,
			UpdateFunc: func(_, obj any) { c.onPodUpsert(obj) },
			DeleteFunc: c.onPodDelete,
		})
		_, _ = c.podGroupInf.AddEventHandler(clientcache.ResourceEventHandlerFuncs{
			AddFunc:    c.onPodGroupUpsert,
			UpdateFunc: func(_, obj any) { c.onPodGroupUpsert(obj) },
			DeleteFunc: c.onPodGroupDelete,
		})
		// ResourceQuota is the per-namespace envelope (q27): a change re-clamps the
		// namespace node's max on the next rebuild. Only the hard limits feed the
		// envelope (envelopes() reads Status/Spec.Hard), so a status.used churn —
		// which fires on every pod create/delete — must NOT trigger a rebuild.
		_, _ = c.rqInf.AddEventHandler(clientcache.ResourceEventHandlerFuncs{
			AddFunc: func(any) { c.triggerRebuild() },
			UpdateFunc: func(oldObj, newObj any) {
				if resourceQuotaHardChanged(oldObj, newObj) {
					c.triggerRebuild()
				}
			},
			DeleteFunc: func(any) { c.triggerRebuild() },
		})
		c.ctx = ctx
		c.dynFactory.Start(ctx.Done())
		c.engine.StartDebugServer(ctx)
		go c.rebuildLoop(ctx)  // read-only tree maintenance runs in every replica
		go c.ledgerGCLoop(ctx) // ledger self-heal runs in every replica (own engine)
		// the mutating reclaim loop is started under leader election by SetClient.
	})
}

func (c *Coordinator) triggerRebuild() {
	select {
	case c.rebuildCh <- struct{}{}:
	default: // a rebuild is already pending; coalesce
	}
}

// queueSpecChanged reports whether a Queue update touched spec (not just status
// or noise). metadata.generation is the reliable spec-change signal under a
// status subresource. Unparseable objects conservatively rebuild (fail-open).
func queueSpecChanged(oldObj, newObj any) bool {
	o, ok1 := oldObj.(*unstructured.Unstructured)
	n, ok2 := newObj.(*unstructured.Unstructured)
	if !ok1 || !ok2 {
		return true
	}
	return o.GetGeneration() != n.GetGeneration()
}

// resourceQuotaHardChanged reports whether a ResourceQuota update changed the
// hard limits that feed the namespace envelope. status.used churns on every pod
// event and does not affect the envelope, so ignoring it avoids a rebuild storm.
func resourceQuotaHardChanged(oldObj, newObj any) bool {
	o, ok1 := oldObj.(*corev1.ResourceQuota)
	n, ok2 := newObj.(*corev1.ResourceQuota)
	if !ok1 || !ok2 {
		return true
	}
	return !resourceListEqual(o.Status.Hard, n.Status.Hard) ||
		!resourceListEqual(o.Spec.Hard, n.Spec.Hard)
}

func resourceListEqual(a, b corev1.ResourceList) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok || av.Cmp(bv) != 0 {
			return false
		}
	}
	return true
}

// rebuildLoop debounces Queue events and rebuilds the tree. It also does an
// initial rebuild once the Queue informer has synced (so an empty tree at boot
// becomes the real tree without waiting for an event).
func (c *Coordinator) rebuildLoop(ctx context.Context) {
	// Recovery barrier (decision q30): do not build the first tree — and therefore
	// do not let PreFilter admit anything (it fails closed until Ready) — until
	// ALL driving informers have synced. Otherwise the first rebuild's
	// reseedQueueAllocation could run before bound pods are in the pod cache and
	// under-count queue usage, opening an over-admission window on cold start.
	if !clientcache.WaitForCacheSync(ctx.Done(),
		c.queueInf.HasSynced, c.podGroupInf.HasSynced,
		c.podInf.HasSynced, c.nodeInf.HasSynced, c.rqInf.HasSynced) {
		return
	}
	c.rebuild()
	debounce := c.engine.Config().ReconcileDebounce
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.rebuildCh:
			timer := time.NewTimer(debounce)
		drain:
			for {
				select {
				case <-c.rebuildCh: // keep coalescing during the debounce window
				case <-timer.C:
					break drain
				case <-ctx.Done():
					timer.Stop()
					return
				}
			}
			c.rebuild()
		}
	}
}

// ledgerGCInterval is how often each replica reconciles its in-memory booking /
// gang ledgers against the informer truth. It is a slow backstop for missed
// delete events (informer gaps), not a hot path — leaks are rare and self-heal
// here rather than surviving until a restart.
const ledgerGCInterval = 60 * time.Second

// ledgerGCLoop periodically releases bookings and gang reservations whose backing
// pod / PodGroup has vanished from the informer without a delete event. It runs in
// EVERY replica because each engine owns its own ledger, and the scheduler leader
// — whose ledger actually gates admission — may live on a different pod than the
// reconciler leader; releasing a stale entry is idempotent, so it never fights a
// real delete. The first sweep waits for the pod/PodGroup caches so it never
// "GC"s live entries against an empty cache.
func (c *Coordinator) ledgerGCLoop(ctx context.Context) {
	if !clientcache.WaitForCacheSync(ctx.Done(), c.podInf.HasSynced, c.podGroupInf.HasSynced) {
		return
	}
	t := time.NewTicker(ledgerGCInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.gcStaleLedgers()
		}
	}
}

// gcStaleLedgers releases ledger entries whose backing object is gone from the
// informer cache. A present, non-terminal pod holds its booking; a present
// PodGroup holds its gang reservation. Everything else is a leak (missed delete)
// and is freed so queue headroom recovers without a restart.
func (c *Coordinator) gcStaleLedgers() {
	liveUIDs := make(map[types.UID]bool)
	for _, o := range c.podInf.GetStore().List() {
		p, ok := o.(*corev1.Pod)
		if !ok {
			continue
		}
		// Terminal pods have already had their accounting released (onPodUpsert);
		// excluding them lets GC reclaim any that lingered without a delete event.
		if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		liveUIDs[p.UID] = true
	}
	freed := 0
	if n := c.engine.GCBookings(liveUIDs); n > 0 {
		klog.V(2).InfoS("ad-scheduler: released stale bookings (missed pod delete?)", "count", n)
		freed += n
	}

	livePGs := make(map[string]bool)
	for _, o := range c.podGroupInf.GetStore().List() {
		if u, ok := o.(*unstructured.Unstructured); ok {
			livePGs[u.GetNamespace()+"/"+u.GetName()] = true
		}
	}
	if n := c.engine.GCGangs(livePGs); n > 0 {
		klog.V(2).InfoS("ad-scheduler: released abandoned gang reservations (missed PodGroup delete?)", "count", n)
		freed += n
	}

	// A GC release frees queue headroom (like a real Pod/Delete or gang release),
	// but no cluster event fires for it — so the framework's QueueingHints would
	// not requeue pods gated on that quota until the ~5m unschedulable flush.
	// Re-activate gated pods here so a gang waiting on the freed headroom retries
	// admission now (the same reactive-requeue path used at timeout / delete).
	if freed > 0 {
		c.activatePending()
	}
}

// rebuild lists the Queue CRs, assembles the tree, swaps it in, and re-seeds
// per-queue allocation from the currently tracked bound pods. On any assembly
// error it logs and keeps the last-good tree (fail-closed, decision q28).
func (c *Coordinator) rebuild() {
	objs := c.queueInf.GetStore().List()
	queues := make([]v1alpha1.Queue, 0, len(objs))
	for _, o := range objs {
		u, ok := o.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		var q v1alpha1.Queue
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &q); err != nil {
			klog.ErrorS(err, "ad-scheduler: skipping malformed Queue", "name", u.GetName(), "namespace", u.GetNamespace())
			continue
		}
		// Config audit: surface an invariant-violating Queue as a Warning event on
		// the object (kubectl describe queue) and exclude it, so a misconfig is
		// visible even if the VAP was not installed (same rules — pkg/admission).
		if verr := admission.ValidateQueue(&q); verr != nil {
			c.emitEvent(u, nil, corev1.EventTypeWarning, "QueueRejected", "Admission", "excluded from the queue tree: %v", verr)
			continue
		}
		queues = append(queues, q)
	}
	spec, err := ctrlqueue.BuildSpec(queues, c.envelopes())
	if err != nil {
		klog.ErrorS(err, "ad-scheduler: queue tree assembly failed — keeping last-good")
		return
	}
	mgr, err := queue.NewManager(spec)
	if err != nil {
		klog.ErrorS(err, "ad-scheduler: queue tree invalid — keeping last-good")
		return
	}
	c.engine.Rebuild(mgr)
	c.reseedQueueAllocation(mgr)
	c.recoverGangs(mgr) // restore gang reservations from PodGroup.status after a restart
	// A tree change can make previously-unschedulable pods placeable (e.g. a Queue
	// was just created for a namespace, or a max was raised). Re-attempt gated pods
	// now instead of waiting for the scheduler's periodic unschedulable flush.
	c.activatePending()
	klog.V(2).InfoS("ad-scheduler: queue tree rebuilt", "queues", len(queues), "generation", c.engine.Snapshot().Generation())
}

// envelopes derives the per-namespace compute envelope from ResourceQuotas
// (decision q27): the tightest cap the namespace's ResourceQuota(s) impose,
// applied to the synthetic namespace node's max. Non-compute quota keys
// (count/*, limits.*) are ignored; requests.<r> and bare <r> map to dimension r.
func (c *Coordinator) envelopes() map[string]resource.Resource {
	out := map[string]resource.Resource{}
	for _, o := range c.rqInf.GetStore().List() {
		rq, ok := o.(*corev1.ResourceQuota)
		if !ok {
			continue
		}
		hard := rq.Status.Hard
		if len(hard) == 0 {
			hard = rq.Spec.Hard
		}
		env := resource.Resource{}
		for name, q := range hard {
			dim := envelopeDim(string(name))
			if dim == "" {
				continue
			}
			v := q.Value()
			if dim == "cpu" {
				v = q.MilliValue()
			}
			if cur, ok := env[dim]; !ok || v < cur { // tightest if keys overlap
				env[dim] = v
			}
		}
		if len(env) == 0 {
			continue
		}
		if cur, ok := out[rq.Namespace]; ok {
			out[rq.Namespace] = minCap(cur, env)
		} else {
			out[rq.Namespace] = env
		}
	}
	return out
}

// envelopeDim maps a ResourceQuota hard key to a Resource dimension, or "" to
// ignore it. Only compute request caps count toward the queue envelope.
func envelopeDim(key string) string {
	switch key {
	case "cpu", "requests.cpu":
		return "cpu"
	case "memory", "requests.memory":
		return "memory"
	}
	if after, ok := strings.CutPrefix(key, "requests."); ok {
		return after
	}
	return ""
}

// minCap returns the tighter of two capacity vectors (absent dim = unbounded, so
// a dim present in only one is kept; shared dims take the min).
func minCap(a, b resource.Resource) resource.Resource {
	out := a.Clone()
	for d, v := range b {
		if cur, ok := out[d]; !ok || v < cur {
			out[d] = v
		}
	}
	return out
}

// reseedQueueAllocation re-derives per-queue allocation from the tracked bound
// pods on the new tree. ObserveBound is idempotent, so pods already carried over
// by Engine.Rebuild are not double-counted; this catches pods bound before the
// first tree and pods whose leaf moved on a config change.
func (c *Coordinator) reseedQueueAllocation(mgr *queue.QueueManager) {
	c.mu.Lock()
	snapshot := make(map[types.UID]trackedPod, len(c.tracked))
	for uid, tp := range c.tracked {
		if tp.ours && !tp.gang {
			snapshot[uid] = tp
		}
	}
	c.mu.Unlock()
	for uid, tp := range snapshot {
		if leaf, err := placement.Resolve(mgr, tp.identity); err == nil {
			c.engine.ObserveBound(uid, leaf, tp.req)
		}
	}
}

func (c *Coordinator) onNodeUpsert(obj any) {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return
	}
	inPool := c.poolSelector.Matches(labels.Set(node.Labels))
	// Pool label/taint drift (q20): UpsertNode re-evaluates inPool on every update,
	// so a node that loses the pool label flips to full-foreign accounting
	// automatically; log the transition for the operator.
	if prev, ok := c.engine.Cache().Node(node.Name); ok && prev.InPool != inPool {
		klog.InfoS("ad-scheduler: node pool membership drifted", "node", node.Name, "inPool", inPool)
	}
	c.engine.Cache().UpsertNode(node, inPool)
}

func (c *Coordinator) onNodeDelete(obj any) {
	if node, ok := toNode(obj); ok {
		c.engine.Cache().RemoveNode(node.Name)
		c.releaseNodePods(node.Name) // node gone: free our pods' bookings now (q20)
	}
}

func (c *Coordinator) onPodUpsert(obj any) {
	pod, ok := obj.(*corev1.Pod)
	if !ok || pod.Spec.NodeName == "" {
		return // unbound pods carry no node/queue accounting yet
	}
	// Lifecycle/app-GC (decision q14): a pod that has reached a terminal phase
	// (Succeeded/Failed) is done — release its accounting now, like a delete, so
	// completed work frees queue headroom without waiting for the object's GC.
	// Idempotent via the tracked map (the later real delete becomes a no-op).
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		c.onPodDelete(pod)
		return
	}
	c.mu.Lock()
	if _, seen := c.tracked[pod.UID]; seen {
		c.mu.Unlock()
		return // already accounted (idempotent)
	}
	tp := trackedPod{
		ours:     c.engine.Cache().IsOurs(pod),
		gang:     pod.Annotations[util.PodGroupAnnotation] != "",
		identity: util.PlacementKey(pod),
		req:      resource.EffectiveRequest(pod),
	}
	c.tracked[pod.UID] = tp
	c.mu.Unlock()

	c.engine.Cache().OnPodBound(pod) // node available -= this pod (ours or foreign)
	if tp.ours && !tp.gang {
		if t := c.engine.Tree(); t != nil {
			if leaf, err := placement.Resolve(t, tp.identity); err == nil {
				c.engine.ObserveBound(pod.UID, leaf, tp.req)
			}
		}
	}
}

func (c *Coordinator) onPodDelete(obj any) {
	pod, ok := toPod(obj)
	if !ok {
		return
	}
	c.mu.Lock()
	tp, tracked := c.tracked[pod.UID]
	delete(c.tracked, pod.UID)
	c.mu.Unlock()
	if !tracked {
		return
	}
	c.engine.Cache().OnPodDeleted(pod)
	if tp.ours && !tp.gang {
		c.engine.ObserveDeleted(pod.UID)
	}
	if tp.ours {
		// This pod freed queue headroom (leaf allocation or gang reservation on
		// release). Activate gated pods AFTER the release so their retry sees the
		// freed capacity, defeating the requeue-vs-release race under QueueingHints.
		c.activatePending()
	}
}

// activatePending pushes every unscheduled pod of ours back to the active queue.
func (c *Coordinator) activatePending() {
	fn, _ := c.activate.Load().(func(map[string]*corev1.Pod))
	if fn == nil {
		return
	}
	name := c.engine.Config().SchedulerName
	pending := map[string]*corev1.Pod{}
	for _, o := range c.podInf.GetStore().List() {
		p, ok := o.(*corev1.Pod)
		if !ok || p.Spec.SchedulerName != name || p.Spec.NodeName != "" || p.DeletionTimestamp != nil {
			continue
		}
		pending[p.Namespace+"/"+p.Name] = p
	}
	if len(pending) > 0 {
		fn(pending)
	}
}

// ReleaseGangAndActivate releases a gang reservation and re-queues gated pods so
// a waiting gang can be admitted into the freed headroom. Used by PodGroup
// delete and gang abort.
func (c *Coordinator) releaseGangAndActivate(key string) {
	_ = c.engine.ReleaseGang(key)
	c.activatePending()
}

// onPodGroupUpsert refreshes the gang spec registry from a PodGroup CR.
func (c *Coordinator) onPodGroupUpsert(obj any) {
	pg, ok := toPodGroup(obj)
	if !ok {
		return
	}
	info := GangInfo{
		MinMember:    pg.Spec.MinMember,
		MinResources: resource.FromQuantityMap(pg.Spec.MinResources),
		Queue:        pg.Spec.Queue,
		Failed:       pg.Status.Phase == podGroupPhaseFailed,
		Age:          -pg.CreationTimestamp.UnixNano(), // older gang => larger Age
	}
	if pg.Spec.ScheduleTimeoutSeconds != nil {
		info.TimeoutSecs = *pg.Spec.ScheduleTimeoutSeconds
	}
	if pg.Spec.MembersReadyTimeoutSeconds != nil {
		info.MembersReadyTimeoutSecs = *pg.Spec.MembersReadyTimeoutSeconds
	}
	c.mu.Lock()
	c.gangs[pg.Namespace+"/"+pg.Name] = info
	c.mu.Unlock()
}

// onPodGroupDelete drops the gang spec and releases its virtual reservation
// (idempotent), so a deleted PodGroup never leaks headroom.
func (c *Coordinator) onPodGroupDelete(obj any) {
	pg, ok := toPodGroup(obj)
	if !ok {
		return
	}
	key := pg.Namespace + "/" + pg.Name
	c.mu.Lock()
	delete(c.gangs, key)
	c.mu.Unlock()
	c.releaseGangAndActivate(key)
}

func toPodGroup(obj any) (*v1alpha1.PodGroup, bool) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		if t, tomb := obj.(clientcache.DeletedFinalStateUnknown); tomb {
			u, ok = t.Obj.(*unstructured.Unstructured)
		}
		if !ok {
			return nil, false
		}
	}
	var pg v1alpha1.PodGroup
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &pg); err != nil {
		klog.ErrorS(err, "ad-scheduler: skipping malformed PodGroup", "name", u.GetName(), "namespace", u.GetNamespace())
		return nil, false
	}
	return &pg, true
}

// toPod / toNode unwrap the object, handling cache.DeletedFinalStateUnknown
// tombstones the informer delivers on delete.
func toPod(obj any) (*corev1.Pod, bool) {
	if p, ok := obj.(*corev1.Pod); ok {
		return p, true
	}
	if t, ok := obj.(clientcache.DeletedFinalStateUnknown); ok {
		p, ok := t.Obj.(*corev1.Pod)
		return p, ok
	}
	return nil, false
}

func toNode(obj any) (*corev1.Node, bool) {
	if n, ok := obj.(*corev1.Node); ok {
		return n, true
	}
	if t, ok := obj.(clientcache.DeletedFinalStateUnknown); ok {
		n, ok := t.Obj.(*corev1.Node)
		return n, ok
	}
	return nil, false
}
