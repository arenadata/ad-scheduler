package framework

import (
	"context"
	"maps"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/arenadata/ad-scheduler/pkg/resource"
	"github.com/arenadata/ad-scheduler/pkg/util"
)

// provReqGVR is the Cluster Autoscaler ProvisioningRequest CRD (autoscaling.x-k8s.io/v1).
var provReqGVR = schema.GroupVersionResource{Group: "autoscaling.x-k8s.io", Version: "v1", Resource: "provisioningrequests"}

const (
	// best-effort-atomic-scale-up: CA provisions the whole PodSet's capacity as
	// one unit (all-or-nothing) — the right semantics for a gang that must land
	// together. (check-capacity, the other class, only reports fit without scaling.)
	provReqClassAtomic = "best-effort-atomic-scale-up.autoscaling.x-k8s.io"
	provReqNamePrefix  = "ad-gang-"
	// consume-provisioning-request tells CA that these pods are served by the
	// named ProvisioningRequest — CA excludes them from ordinary member-by-member
	// scale-up, so the gang's capacity is provisioned all-or-nothing by the
	// best-effort-atomic request instead (matching gang semantics).
	provReqConsumeAnnotation = "autoscaling.x-k8s.io/consume-provisioning-request"
	// labels so we can GC only the objects we own.
	provReqManagedBy = "scheduler.arenadata.io/managed-by"
	provReqGangLabel = "scheduler.arenadata.io/gang"
	provReqOwnerVal  = "ad-scheduler"
)

// syncProvisioningRequests publishes aggregate node demand for admitted-but-
// unplaceable gangs as CA ProvisioningRequests (opt-in AD_AUTOSCALE_DEMAND). A
// gang that has been admitted (queue headroom reserved) but whose members are
// still Unschedulable — no pool node has room — needs N nodes provisioned *at
// once*; publishing a best-effort-atomic ProvisioningRequest lets CA scale the
// block atomically instead of the pool growing member-by-member (which can
// deadlock at the Permit barrier with half the gang's nodes idle).
//
// Each admitted+unplaceable gang gets one PodTemplate (a representative member's
// resource ask + pool nodeSelector/tolerations) and one ProvisioningRequest for
// its still-unplaced count. Fully-assembled or vanished gangs are GC'd. Runs on
// the reconciler leader only (via reclaimLoop).
func (c *Coordinator) syncProvisioningRequests(ctx context.Context) {
	if !c.engine.Config().AutoscaleDemand {
		return
	}
	cs, _ := c.client.Load().(kubernetes.Interface)
	if cs == nil || c.dynClient == nil {
		return
	}

	// Snapshot the gang ledger under the lock, act without it (informer reads +
	// API writes must not hold c.mu).
	c.mu.Lock()
	gangs := make(map[string]GangInfo, len(c.gangs))
	maps.Copy(gangs, c.gangs)
	c.mu.Unlock()

	desired := map[string]bool{} // "ns/prName" we want to keep this tick
	for key, info := range gangs {
		ns, name := splitGangKey(key)
		if info.Failed {
			continue // gave up (Hard-timeout) — no point provisioning
		}
		placed, minMember, ok := c.GangProgress(ns, name)
		if !ok || minMember <= 0 {
			continue
		}
		remaining := int(minMember) - placed
		if remaining <= 0 {
			continue // fully assembled — demand met, will be GC'd below
		}
		members := c.pendingGangMembers(ns, name)
		if len(members) == 0 {
			continue // no unschedulable member to model the ask on
		}
		prName := provReqNamePrefix + name
		if err := c.ensurePodTemplate(ctx, cs, ns, prName, name, members[0]); err != nil {
			klog.V(4).ErrorS(err, "ad-scheduler: provreq podtemplate", "gang", key)
			continue
		}
		if err := c.ensureProvisioningRequest(ctx, ns, prName, name, int64(remaining)); err != nil {
			klog.V(4).ErrorS(err, "ad-scheduler: provreq upsert", "gang", key)
			continue
		}
		c.markMembersConsumeProvReq(ctx, cs, members, prName)
		desired[ns+"/"+prName] = true
	}

	c.gcProvisioningRequests(ctx, cs, desired)
}

// pendingGangMembers returns the gang's Unschedulable, still-pending members —
// the pods whose ask the ProvisioningRequest models and which get the consume
// annotation. Empty if none have hit the Unschedulable condition yet.
func (c *Coordinator) pendingGangMembers(ns, name string) []*corev1.Pod {
	var out []*corev1.Pod
	for _, o := range c.podInf.GetStore().List() {
		p, ok := o.(*corev1.Pod)
		if !ok || p.Namespace != ns || p.Spec.NodeName != "" || p.DeletionTimestamp != nil {
			continue
		}
		if p.Annotations[util.PodGroupAnnotation] != name {
			continue
		}
		if podUnschedulable(p) {
			out = append(out, p)
		}
	}
	return out
}

// markMembersConsumeProvReq stamps the consume-provisioning-request annotation on
// gang members that don't yet carry it, so CA routes their capacity through the
// atomic ProvisioningRequest instead of scaling per-member.
func (c *Coordinator) markMembersConsumeProvReq(ctx context.Context, cs kubernetes.Interface, members []*corev1.Pod, prName string) {
	patch := []byte(`{"metadata":{"annotations":{"` + provReqConsumeAnnotation + `":"` + prName + `"}}}`)
	for _, p := range members {
		if p.Annotations[provReqConsumeAnnotation] == prName {
			continue // already annotated
		}
		if _, err := cs.CoreV1().Pods(p.Namespace).Patch(ctx, p.Name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
			klog.V(4).ErrorS(err, "ad-scheduler: provreq consume annotation", "pod", p.Namespace+"/"+p.Name)
		}
	}
}

// podUnschedulable reports whether the scheduler has marked the pod
// PodScheduled=False (the CA-visible "needs a node" signal).
func podUnschedulable(p *corev1.Pod) bool {
	for i := range p.Status.Conditions {
		cd := &p.Status.Conditions[i]
		if cd.Type == corev1.PodScheduled && cd.Status == corev1.ConditionFalse {
			return true
		}
	}
	return false
}

// ensurePodTemplate upserts the core/v1 PodTemplate the ProvisioningRequest's
// PodSet references. Its single container carries the member's cpu/memory ask and
// the pod's pool nodeSelector + tolerations, so CA's default-scheduler simulation
// places it only on (would-be) pool nodes.
func (c *Coordinator) ensurePodTemplate(ctx context.Context, cs kubernetes.Interface, ns, prName, gang string, rep *corev1.Pod) error {
	spec := corev1.PodSpec{
		NodeSelector: rep.Spec.NodeSelector,
		Affinity:     rep.Spec.Affinity,
		Tolerations:  rep.Spec.Tolerations,
		Containers: []corev1.Container{{
			Name:      "gang-member",
			Image:     "registry.k8s.io/pause:3.10",
			Resources: corev1.ResourceRequirements{Requests: memberRequests(rep)},
		}},
	}
	want := &corev1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      prName,
			Namespace: ns,
			Labels:    map[string]string{provReqManagedBy: provReqOwnerVal, provReqGangLabel: gang},
		},
		Template: corev1.PodTemplateSpec{Spec: spec},
	}
	cur, err := cs.CoreV1().PodTemplates(ns).Get(ctx, prName, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		_, err = cs.CoreV1().PodTemplates(ns).Create(ctx, want, metav1.CreateOptions{})
		return ignoreAlreadyExists(err)
	}
	want.ResourceVersion = cur.ResourceVersion
	_, err = cs.CoreV1().PodTemplates(ns).Update(ctx, want, metav1.UpdateOptions{})
	return err
}

// ensureProvisioningRequest upserts the best-effort-atomic ProvisioningRequest
// for a gang, its PodSet asking for `count` copies of the PodTemplate.
func (c *Coordinator) ensureProvisioningRequest(ctx context.Context, ns, prName, gang string, count int64) error {
	cli := c.dynClient.Resource(provReqGVR).Namespace(ns)
	cur, err := cli.Get(ctx, prName, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		obj := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": provReqGVR.Group + "/" + provReqGVR.Version,
			"kind":       "ProvisioningRequest",
			"metadata": map[string]any{
				"name":      prName,
				"namespace": ns,
				"labels":    map[string]any{provReqManagedBy: provReqOwnerVal, provReqGangLabel: gang},
			},
			"spec": map[string]any{
				"provisioningClassName": provReqClassAtomic,
				"podSets": []any{map[string]any{
					"podTemplateRef": map[string]any{"name": prName},
					"count":          count,
				}},
			},
		}}
		_, err = cli.Create(ctx, obj, metav1.CreateOptions{})
		return ignoreAlreadyExists(err)
	}
	// Keep the requested count in sync with the still-unplaced member count.
	podSets := []any{map[string]any{
		"podTemplateRef": map[string]any{"name": prName},
		"count":          count,
	}}
	if err := unstructured.SetNestedSlice(cur.Object, podSets, "spec", "podSets"); err != nil {
		return err
	}
	_, err = cli.Update(ctx, cur, metav1.UpdateOptions{})
	return err
}

// gcProvisioningRequests deletes ProvisioningRequests and PodTemplates we own
// (managed-by label) whose gang is no longer admitted+unplaceable (assembled,
// failed, or the PodGroup was deleted) — anything not in `desired`.
func (c *Coordinator) gcProvisioningRequests(ctx context.Context, cs kubernetes.Interface, desired map[string]bool) {
	sel := provReqManagedBy + "=" + provReqOwnerVal
	if prs, err := c.dynClient.Resource(provReqGVR).Namespace(metav1.NamespaceAll).
		List(ctx, metav1.ListOptions{LabelSelector: sel}); err == nil {
		for i := range prs.Items {
			pr := &prs.Items[i]
			if desired[pr.GetNamespace()+"/"+pr.GetName()] {
				continue
			}
			_ = c.dynClient.Resource(provReqGVR).Namespace(pr.GetNamespace()).
				Delete(ctx, pr.GetName(), metav1.DeleteOptions{})
		}
	} else {
		klog.V(4).ErrorS(err, "ad-scheduler: provreq list for GC")
	}
	if pts, err := cs.CoreV1().PodTemplates(metav1.NamespaceAll).
		List(ctx, metav1.ListOptions{LabelSelector: sel}); err == nil {
		for i := range pts.Items {
			pt := &pts.Items[i]
			if desired[pt.Namespace+"/"+pt.Name] {
				continue
			}
			_ = cs.CoreV1().PodTemplates(pt.Namespace).Delete(ctx, pt.Name, metav1.DeleteOptions{})
		}
	}
}

// memberRequests renders a gang member's effective cpu/memory ask as a container
// ResourceList. Non-node dimensions (dra/<class>) are excluded — DRA capacity is
// not something CA provisions from a node template.
func memberRequests(p *corev1.Pod) corev1.ResourceList {
	out := corev1.ResourceList{}
	for dim, q := range resource.ToQuantityMap(resource.EffectiveRequest(p)) {
		switch dim {
		case string(corev1.ResourceCPU):
			out[corev1.ResourceCPU] = q
		case string(corev1.ResourceMemory):
			out[corev1.ResourceMemory] = q
		}
	}
	if _, ok := out[corev1.ResourceCPU]; !ok {
		out[corev1.ResourceCPU] = *apiresource.NewMilliQuantity(100, apiresource.DecimalSI)
	}
	return out
}

// ignoreAlreadyExists collapses a lost create/get race into success.
func ignoreAlreadyExists(err error) error {
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}
