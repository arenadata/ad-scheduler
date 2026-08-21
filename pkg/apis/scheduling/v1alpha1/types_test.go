package v1alpha1

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func sampleQueue() *Queue {
	return &Queue{
		TypeMeta:   metav1.TypeMeta{APIVersion: SchemeGroupVersion.String(), Kind: "Queue"},
		ObjectMeta: metav1.ObjectMeta{Name: "batch", Namespace: "spark-prod", Labels: map[string]string{"team": "ml"}},
		Spec: QueueSpec{
			Parent:                "",
			CapacityMode:          "absolute",
			Guaranteed:            ResourceList{"memory": resource.MustParse("64Gi"), "vcore": resource.MustParse("20")},
			Max:                   ResourceList{"memory": resource.MustParse("128Gi")},
			ApplicationSortPolicy: "fifo",
			Preemption:            &PreemptionSpec{Policy: "fence", Delay: metav1.Duration{}},
			ServiceAccounts:       []string{"spark", "eval"},
			DrainPolicy:           "graceful",
			Limits: []QueueLimit{
				{ServiceAccounts: []string{"*"}, MaxApplications: 20, MaxResources: ResourceList{"memory": resource.MustParse("100Gi")}},
			},
		},
		Status: QueueStatus{
			Path:       "spark-prod.batch",
			Phase:      "Active",
			Allocated:  ResourceList{"memory": resource.MustParse("32Gi")},
			Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Built"}},
		},
	}
}

func TestQueueDeepCopyIsolation(t *testing.T) {
	orig := sampleQueue()
	cp := orig.DeepCopy()

	// mutate every reference-typed field of the copy
	cp.Labels["team"] = "changed"
	cp.Spec.Guaranteed["memory"] = resource.MustParse("1Gi")
	cp.Spec.ServiceAccounts[0] = "changed"
	cp.Spec.Limits[0].MaxResources["memory"] = resource.MustParse("1Gi")
	cp.Spec.Preemption.Policy = "changed"
	cp.Status.Conditions[0].Reason = "changed"

	if orig.Labels["team"] != "ml" {
		t.Error("labels not isolated")
	}
	if got := orig.Spec.Guaranteed["memory"]; got.String() != "64Gi" {
		t.Errorf("guaranteed not isolated: %s", got.String())
	}
	if orig.Spec.ServiceAccounts[0] != "spark" {
		t.Error("serviceAccounts not isolated")
	}
	if got := orig.Spec.Limits[0].MaxResources["memory"]; got.String() != "100Gi" {
		t.Errorf("limit resources not isolated: %s", got.String())
	}
	if orig.Spec.Preemption.Policy != "fence" {
		t.Error("preemption pointer not isolated")
	}
	if orig.Status.Conditions[0].Reason != "Built" {
		t.Error("conditions not isolated")
	}
}

func TestPodGroupDeepCopyIsolation(t *testing.T) {
	tos := int32(300)
	orig := &PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "spark-pi", Namespace: "spark-prod"},
		Spec: PodGroupSpec{
			MinMember:              4,
			MinResources:           ResourceList{"vcore": resource.MustParse("8")},
			Queue:                  "spark-prod.batch",
			ScheduleTimeoutSeconds: &tos,
			GangSchedulingStyle:    "Hard",
		},
		Status: PodGroupStatus{Phase: "Pending", OccupiedNodes: map[string]int32{"node1": 2}},
	}
	cp := orig.DeepCopy()
	*cp.Spec.ScheduleTimeoutSeconds = 999
	cp.Spec.MinResources["vcore"] = resource.MustParse("1")
	cp.Status.OccupiedNodes["node1"] = 99

	if *orig.Spec.ScheduleTimeoutSeconds != 300 {
		t.Error("timeout pointer not isolated")
	}
	if got := orig.Spec.MinResources["vcore"]; got.String() != "8" {
		t.Errorf("minResources not isolated: %s", got.String())
	}
	if orig.Status.OccupiedNodes["node1"] != 2 {
		t.Error("occupiedNodes not isolated")
	}
}

func TestDeepCopyObjectImplementsRuntimeObject(t *testing.T) {
	var _ runtime.Object = &Queue{}
	var _ runtime.Object = &QueueList{}
	var _ runtime.Object = &PodGroup{}
	var _ runtime.Object = &PodGroupList{}

	obj := sampleQueue().DeepCopyObject()
	if _, ok := obj.(*Queue); !ok {
		t.Fatalf("DeepCopyObject returned %T, want *Queue", obj)
	}
}

func TestAddToScheme(t *testing.T) {
	s := runtime.NewScheme()
	if err := AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	if !s.Recognizes(SchemeGroupVersion.WithKind("Queue")) {
		t.Error("scheme does not recognize Queue")
	}
	if !s.Recognizes(SchemeGroupVersion.WithKind("PodGroup")) {
		t.Error("scheme does not recognize PodGroup")
	}
	if got := Resource("queues"); got.Group != GroupName || got.Resource != "queues" {
		t.Errorf("Resource() = %v", got)
	}
}
