package local

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/docker/docker/client"
	"github.com/gammadia/alfred/provisioner/internal"
	"github.com/gammadia/alfred/scheduler"
)

type Node struct {
	name        string
	provisioner *Provisioner

	log    *slog.Logger
	ctx    context.Context
	cancel context.CancelFunc
	docker *client.Client

	preparedJobs map[string]bool

	mutex sync.Mutex
}

// Node implements scheduler.Node
var _ scheduler.Node = (*Node)(nil)

func (n *Node) Name() string {
	return n.name
}

func (n *Node) PrepareForTask(task *scheduler.Task) error {
	jobKey := task.Job.FQN()
	log := n.log.With("job", jobKey)
	if _, loaded := n.provisioner.preparedJobs.LoadOrStore(jobKey, nil); loaded {
		return nil
	}

	log.Debug("Bootstrap local environment")

	// We never check that locally built images in available in local, because they can only be so
	isLocallyBuilt := func(image string) bool { return strings.HasPrefix(image, "sha256:") }

	for _, image := range task.Job.Steps {
		if isLocallyBuilt(image) {
			continue
		}
		if err := internal.EnsureNodeHasImage(&n.mutex, n.log, n.docker, nil, image); err != nil {
			return fmt.Errorf("failed to ensure node has image '%s': %w", image, err)
		}
	}
	for _, service := range task.Job.Services {
		if isLocallyBuilt(service.Image) {
			continue
		}
		if err := internal.EnsureNodeHasImage(&n.mutex, n.log, n.docker, nil, service.Image); err != nil {
			return fmt.Errorf("failed to ensure node has image '%s' for service '%s': %w", service.Image, service.Name, err)
		}
	}

	log.Debug("Local environment bootstrapped")

	return nil
}

func (n *Node) RunTask(task *scheduler.Task, runConfig scheduler.RunTaskConfig) (int, error) {
	return internal.RunContainer(n.ctx, n.log, n.docker, task, n.provisioner.fs, runConfig)
}

func (*Node) Terminate() error {
	return nil
}
