package cache

import (
	"fmt"

	"k8s.io/apimachinery/pkg/types"

	"github.com/arenadata/ad-scheduler/pkg/resource"
	"github.com/arenadata/ad-scheduler/pkg/util"
)

// TaskState is a task (== one pod) lifecycle state.
type TaskState string

const (
	TaskNew        TaskState = "New"
	TaskPending    TaskState = "Pending"
	TaskScheduling TaskState = "Scheduling"
	TaskAllocated  TaskState = "Allocated" // reserved (assumed) on a node
	TaskBound      TaskState = "Bound"
	TaskTerminated TaskState = "Terminated"
)

// ApplicationState is an application (a group of tasks) lifecycle state.
type ApplicationState string

const (
	AppNew        ApplicationState = "New"
	AppAccepted   ApplicationState = "Accepted"
	AppRunning    ApplicationState = "Running"
	AppCompleting ApplicationState = "Completing"
	AppCompleted  ApplicationState = "Completed"
	AppFailed     ApplicationState = "Failed"
)

// Allowed transitions. Kept as explicit tables (a small, dependency-free FSM);
// can move to looplab/fsm later without changing call sites.
var (
	taskTransitions = map[TaskState]map[TaskState]bool{
		TaskNew:        {TaskPending: true, TaskTerminated: true},
		TaskPending:    {TaskScheduling: true, TaskTerminated: true},
		TaskScheduling: {TaskAllocated: true, TaskPending: true, TaskTerminated: true}, // back to Pending on Unreserve
		TaskAllocated:  {TaskBound: true, TaskPending: true, TaskTerminated: true},     // back to Pending on bind failure
		TaskBound:      {TaskTerminated: true},
		TaskTerminated: {},
	}
	appTransitions = map[ApplicationState]map[ApplicationState]bool{
		AppNew:        {AppAccepted: true, AppFailed: true},
		AppAccepted:   {AppRunning: true, AppFailed: true, AppCompleted: true},
		AppRunning:    {AppCompleting: true, AppFailed: true},
		AppCompleting: {AppCompleted: true, AppFailed: true},
		AppCompleted:  {},
		AppFailed:     {},
	}
)

// CanTransitionTask reports whether from->to is an allowed task transition.
func CanTransitionTask(from, to TaskState) bool { return taskTransitions[from][to] }

// CanTransitionApp reports whether from->to is an allowed application transition.
func CanTransitionApp(from, to ApplicationState) bool { return appTransitions[from][to] }

// Task is one pod tracked by the scheduler.
type Task struct {
	UID       types.UID
	Namespace string
	Name      string
	NodeName  string // set once Allocated/Bound
	Request   resource.Resource
	state     TaskState
}

// State returns the task's current state.
func (t *Task) State() TaskState { return t.state }

// Transition moves the task to to, erroring on an illegal transition.
func (t *Task) Transition(to TaskState) error {
	if !CanTransitionTask(t.state, to) {
		return fmt.Errorf("task %s/%s: illegal transition %s -> %s", t.Namespace, t.Name, t.state, to)
	}
	t.state = to
	return nil
}

// Application groups a set of tasks under one app id and identity/queue.
type Application struct {
	ID       string
	Identity util.Identity // (namespace, serviceAccount) — the placement key
	Queue    string        // resolved leaf path; empty until placement (M2)
	tasks    map[types.UID]*Task
	state    ApplicationState
}

// NewApplication starts a fresh application in the New state.
func NewApplication(id string, identity util.Identity) *Application {
	return &Application{ID: id, Identity: identity, tasks: map[types.UID]*Task{}, state: AppNew}
}

// State returns the application's current state.
func (a *Application) State() ApplicationState { return a.state }

// Transition moves the application to to, erroring on an illegal transition.
func (a *Application) Transition(to ApplicationState) error {
	if !CanTransitionApp(a.state, to) {
		return fmt.Errorf("application %s: illegal transition %s -> %s", a.ID, a.state, to)
	}
	a.state = to
	return nil
}

// AddTask registers a task under the application.
func (a *Application) AddTask(t *Task) { a.tasks[t.UID] = t }

// Task returns a tracked task by UID.
func (a *Application) Task(uid types.UID) (*Task, bool) { t, ok := a.tasks[uid]; return t, ok }

// PendingRequest sums the effective request of tasks not yet terminated —
// the application's outstanding demand.
func (a *Application) PendingRequest() resource.Resource {
	total := resource.Resource{}
	for _, t := range a.tasks {
		if t.state != TaskTerminated {
			total = resource.Add(total, t.Request)
		}
	}
	return total
}
