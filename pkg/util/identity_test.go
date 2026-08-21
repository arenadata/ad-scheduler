package util

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPlacementKeyAndDefault(t *testing.T) {
	named := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "spark-prod"}, Spec: corev1.PodSpec{ServiceAccountName: "spark"}}
	id := PlacementKey(named)
	if id.Namespace != "spark-prod" || id.ServiceAccount != "spark" || id.IsDefault() {
		t.Fatalf("named SA key wrong: %+v", id)
	}
	if id.String() != "spark-prod/spark" {
		t.Fatalf("String = %q", id.String())
	}

	// empty serviceAccountName resolves to "default" and is flagged
	bare := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns"}}
	id2 := PlacementKey(bare)
	if id2.ServiceAccount != "default" || !id2.IsDefault() {
		t.Fatalf("empty SA must resolve to default and be flagged: %+v", id2)
	}
}

func TestAppID(t *testing.T) {
	cases := []struct {
		name string
		pod  *corev1.Pod
		want string
	}{
		{"annotation wins", &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{AppIDAnnotation: "job-1"},
			Labels:      map[string]string{"applicationId": "label-1"},
		}}, "job-1"},
		{"applicationId label", &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"applicationId": "app-2"}}}, "app-2"},
		{"spark-app-selector", &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"spark-app-selector": "spark-3"}}}, "spark-3"},
		{"autogen", &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", UID: "abc"}}, "ad-ns-abc"},
	}
	for _, c := range cases {
		if got := AppID(c.pod); got != c.want {
			t.Errorf("%s: AppID = %q, want %q", c.name, got, c.want)
		}
	}
}
