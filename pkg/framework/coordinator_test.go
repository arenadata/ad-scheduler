package framework

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func uQueue(gen int64) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "scheduling.arenadata.io/v1alpha1",
		"kind":       "Queue",
	}}
	u.SetGeneration(gen)
	return u
}

func TestQueueSpecChanged(t *testing.T) {
	// status-only patch: generation unchanged -> no rebuild (breaks the feedback loop).
	if queueSpecChanged(uQueue(3), uQueue(3)) {
		t.Error("same generation must not trigger a rebuild")
	}
	// spec write: generation bumped -> rebuild.
	if !queueSpecChanged(uQueue(3), uQueue(4)) {
		t.Error("changed generation must trigger a rebuild")
	}
	// non-unstructured: fail-open (rebuild).
	if !queueSpecChanged("x", uQueue(1)) {
		t.Error("unparseable objects must fail open (rebuild)")
	}
}

func rq(hard, used corev1.ResourceList) *corev1.ResourceQuota {
	return &corev1.ResourceQuota{
		Spec:   corev1.ResourceQuotaSpec{Hard: hard},
		Status: corev1.ResourceQuotaStatus{Hard: hard, Used: used},
	}
}

func TestResourceQuotaHardChanged(t *testing.T) {
	hard := corev1.ResourceList{"cpu": apiresource.MustParse("4")}
	// only status.used moved (every pod event): must NOT rebuild.
	a := rq(hard, corev1.ResourceList{"cpu": apiresource.MustParse("1")})
	b := rq(hard, corev1.ResourceList{"cpu": apiresource.MustParse("2")})
	if resourceQuotaHardChanged(a, b) {
		t.Error("status.used churn must not trigger a rebuild")
	}
	// hard changed: must rebuild.
	c := rq(corev1.ResourceList{"cpu": apiresource.MustParse("8")}, nil)
	if !resourceQuotaHardChanged(a, c) {
		t.Error("changed hard limit must trigger a rebuild")
	}
	// non-RQ: fail-open.
	if !resourceQuotaHardChanged("x", b) {
		t.Error("unparseable objects must fail open (rebuild)")
	}
}

func TestSplitGangKey(t *testing.T) {
	cases := []struct{ key, ns, name string }{
		{"team-a/g1", "team-a", "g1"},
		{"ns/with-dash", "ns", "with-dash"},
		{"noslash", "", "noslash"},
	}
	for _, c := range cases {
		ns, name := splitGangKey(c.key)
		if ns != c.ns || name != c.name {
			t.Errorf("splitGangKey(%q) = (%q,%q), want (%q,%q)", c.key, ns, name, c.ns, c.name)
		}
	}
}

func TestResourceListEqual(t *testing.T) {
	q := func(s string) apiresource.Quantity { return apiresource.MustParse(s) }
	cases := []struct {
		a, b corev1.ResourceList
		want bool
	}{
		{corev1.ResourceList{"cpu": q("1")}, corev1.ResourceList{"cpu": q("1")}, true},
		{corev1.ResourceList{"cpu": q("1000m")}, corev1.ResourceList{"cpu": q("1")}, true}, // equal by value
		{corev1.ResourceList{"cpu": q("1")}, corev1.ResourceList{"cpu": q("2")}, false},
		{corev1.ResourceList{"cpu": q("1")}, corev1.ResourceList{"memory": q("1")}, false},
		{corev1.ResourceList{"cpu": q("1")}, corev1.ResourceList{}, false},
	}
	for i, c := range cases {
		if got := resourceListEqual(c.a, c.b); got != c.want {
			t.Errorf("case %d: resourceListEqual = %v, want %v", i, got, c.want)
		}
	}
}
