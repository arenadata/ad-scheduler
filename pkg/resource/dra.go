package resource

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
)

// DRADimPrefix namespaces DRA device-class dimensions in a Resource vector: a
// pod requesting N devices of DeviceClass "gpu.example.com" contributes
// dra/gpu.example.com: N (decision q11 — device-class is a first-class dimension
// the engine accounts in headroom/roll-up/DRF/gang like any other resource).
const DRADimPrefix = "dra/"

// ClaimLookup resolves a ResourceClaim by namespace/name (backed by an informer
// lister at runtime, or a fake in tests).
type ClaimLookup func(namespace, name string) (*resourceapi.ResourceClaim, error)

// TemplateLookup resolves a ResourceClaimTemplate by namespace/name.
type TemplateLookup func(namespace, name string) (*resourceapi.ResourceClaimTemplate, error)

// PodDeviceRequests resolves a pod's DRA device requests into a dra/<class>
// count vector. Each pod.spec.resourceClaims entry is resolved to a
// ResourceClaim spec (a named claim or a template) via the lookups, and its
// device requests are summed by device class.
//
// Unresolvable requests — allocationMode: All (count unknown) or a FirstAvailable
// alternative — are governed by failClosed: closed returns an error (the caller
// rejects the pod rather than under-count its quota), open skips the request
// (fail-open, permissive). This is decision q11's fail-open/closed switch.
func PodDeviceRequests(pod *corev1.Pod, claim ClaimLookup, template TemplateLookup, failClosed bool) (Resource, error) {
	out := Resource{}
	for i := range pod.Spec.ResourceClaims {
		spec, err := resolveClaimSpec(pod.Namespace, &pod.Spec.ResourceClaims[i], claim, template)
		if err != nil {
			if failClosed {
				return nil, err
			}
			continue
		}
		if spec == nil {
			continue
		}
		for j := range spec.Devices.Requests {
			if err := addDeviceRequest(out, &spec.Devices.Requests[j], failClosed); err != nil {
				return nil, err
			}
		}
	}
	prune(out)
	return out, nil
}

// resolveClaimSpec turns a PodResourceClaim reference into the referenced
// ResourceClaim's spec (from a standalone claim or a template). Returns nil,nil
// when the reference sets neither name (nothing to account).
func resolveClaimSpec(ns string, prc *corev1.PodResourceClaim, claim ClaimLookup, template TemplateLookup) (*resourceapi.ResourceClaimSpec, error) {
	switch {
	case prc.ResourceClaimName != nil && *prc.ResourceClaimName != "":
		if claim == nil {
			return nil, fmt.Errorf("DRA claim %q referenced but no claim lookup configured", *prc.ResourceClaimName)
		}
		rc, err := claim(ns, *prc.ResourceClaimName)
		if err != nil {
			return nil, fmt.Errorf("resolving ResourceClaim %s/%s: %w", ns, *prc.ResourceClaimName, err)
		}
		return &rc.Spec, nil
	case prc.ResourceClaimTemplateName != nil && *prc.ResourceClaimTemplateName != "":
		if template == nil {
			return nil, fmt.Errorf("DRA template %q referenced but no template lookup configured", *prc.ResourceClaimTemplateName)
		}
		t, err := template(ns, *prc.ResourceClaimTemplateName)
		if err != nil {
			return nil, fmt.Errorf("resolving ResourceClaimTemplate %s/%s: %w", ns, *prc.ResourceClaimTemplateName, err)
		}
		return &t.Spec.Spec, nil
	}
	return nil, nil
}

func addDeviceRequest(out Resource, req *resourceapi.DeviceRequest, failClosed bool) error {
	switch {
	case req.Exactly != nil:
		return addExact(out, req.Exactly.DeviceClassName, req.Exactly.AllocationMode, req.Exactly.Count, req.Name, failClosed)
	case len(req.FirstAvailable) > 0:
		// "one of these" — the allocator picks one at bind time. We conservatively
		// account the first alternative so quota is not silently understated; a
		// fail-closed caller may prefer to reject such requests outright.
		if failClosed {
			return fmt.Errorf("dra request %q uses firstAvailable (alternative unknown ahead of allocation); fail-closed", req.Name)
		}
		sr := req.FirstAvailable[0]
		return addExact(out, sr.DeviceClassName, sr.AllocationMode, sr.Count, req.Name, failClosed)
	}
	return nil
}

func addExact(out Resource, class string, mode resourceapi.DeviceAllocationMode, count int64, reqName string, failClosed bool) error {
	if class == "" {
		return nil
	}
	if mode == resourceapi.DeviceAllocationModeAll {
		if failClosed {
			return fmt.Errorf("dra request %q uses allocationMode=All (count unknown); fail-closed", reqName)
		}
		return nil // fail-open: an "All" request is not statically countable
	}
	if count <= 0 {
		count = 1 // ExactCount default
	}
	out[DRADimPrefix+class] += count
	return nil
}
