/*
Command ad-scheduler is the scheduler binary: the kube-scheduler scheduling
framework with ad-scheduler's out-of-tree plugins registered via WithPlugin (no
fork). M0 registers only the no-op AdCapacity plugin so the binary boots and
schedules under the default framework logic; the real plugin set (queuesort,
capacity, drf, nodesort, gang, preemption) is wired across M2–M5.
*/
package main

import (
	"os"

	"k8s.io/component-base/cli"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"

	"github.com/arenadata/ad-scheduler/pkg/plugins/capacity"
	"github.com/arenadata/ad-scheduler/pkg/plugins/gang"
	"github.com/arenadata/ad-scheduler/pkg/plugins/queuesort"
)

func main() {
	command := app.NewSchedulerCommand(
		app.WithPlugin(capacity.Name, capacity.New),
		app.WithPlugin(queuesort.Name, queuesort.New),
		app.WithPlugin(gang.Name, gang.New),
	)
	code := cli.Run(command)
	os.Exit(code)
}
