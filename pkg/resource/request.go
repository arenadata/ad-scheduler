package resource

import (
	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
)

// quantityToUnit applies the canonical unit convention to one quantity: CPU in
// millicores, everything else its integer Value.
func quantityToUnit(name string, q apiresource.Quantity) int64 {
	if name == string(corev1.ResourceCPU) {
		return q.MilliValue()
	}
	return q.Value()
}

// FromResourceList converts a Kubernetes ResourceList to a Resource vector.
// CPU is stored in millicores (Quantity.MilliValue); every other dimension —
// memory, ephemeral-storage, hugepages, nvidia.com/gpu, dra/*, arbitrary
// extended resources — is stored as its integer Value. This is the one
// canonical unit convention the engine uses everywhere (headroom, DRF, gang).
func FromResourceList(rl corev1.ResourceList) Resource {
	out := make(Resource, len(rl))
	for name, q := range rl {
		if v := quantityToUnit(string(name), q); v != 0 {
			out[string(name)] = v
		}
	}
	return out
}

// FromQuantityMap converts a string-keyed quantity map (the CRD's ResourceList,
// map[string]Quantity) to a Resource under the same unit convention.
func FromQuantityMap(m map[string]apiresource.Quantity) Resource {
	out := make(Resource, len(m))
	for name, q := range m {
		if v := quantityToUnit(name, q); v != 0 {
			out[name] = v
		}
	}
	return out
}

// ToQuantityMap is the inverse of FromQuantityMap: it renders a Resource back to
// a string-keyed quantity map under the canonical unit convention (CPU from
// millicores, everything else from its integer value). Used to persist an engine
// vector — e.g. a gang's reservation — into a CRD status for durable recovery.
func ToQuantityMap(r Resource) map[string]apiresource.Quantity {
	out := make(map[string]apiresource.Quantity, len(r))
	for name, v := range r {
		if name == string(corev1.ResourceCPU) {
			out[name] = *apiresource.NewMilliQuantity(v, apiresource.DecimalSI)
		} else {
			out[name] = *apiresource.NewQuantity(v, apiresource.DecimalSI)
		}
	}
	return out
}

// EffectiveRequest computes a pod's effective resource request the same way
// kube-scheduler does, so headroom accounting matches what the node must
// actually hold:
//
//   - regular containers are summed;
//   - native sidecars (init containers with restartPolicy: Always) run for the
//     whole pod lifetime, so they are summed into the running total too and
//     count toward the peak seen by later init containers;
//   - ordinary init containers run one at a time, so their contribution is the
//     max over init containers of (sidecars-started-so-far + this init's
//     request), not a sum;
//   - the pod's effective request is the componentwise max of the container
//     sum and that init-container peak;
//   - spec.overhead (pod-level runtime overhead) is added on top.
func EffectiveRequest(pod *corev1.Pod) Resource {
	reqs := Resource{}
	for i := range pod.Spec.Containers {
		reqs = Add(reqs, FromResourceList(pod.Spec.Containers[i].Resources.Requests))
	}

	restartableInit := Resource{} // running sum of native-sidecar requests
	initPeak := Resource{}        // max request seen while init containers run
	for i := range pod.Spec.InitContainers {
		c := &pod.Spec.InitContainers[i]
		cr := FromResourceList(c.Resources.Requests)
		if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			// Native sidecar: runs alongside app containers for the pod's life.
			reqs = Add(reqs, cr)
			restartableInit = Add(restartableInit, cr)
			initPeak = Max(initPeak, restartableInit)
		} else {
			// Ordinary init: its peak is its own request plus the sidecars
			// already started before it.
			initPeak = Max(initPeak, Add(restartableInit, cr))
		}
	}

	eff := Max(reqs, initPeak)
	if pod.Spec.Overhead != nil {
		eff = Add(eff, FromResourceList(corev1.ResourceList(pod.Spec.Overhead)))
	}
	return eff
}
