package scheduler

import (
	"log/slog"
	"time"

	"github.com/gammadia/alfred/proto"
)

type NodeStatus proto.NodeStatus_Status

const (
	NodeStatusQueued             = NodeStatus(proto.NodeStatus_QUEUED)
	NodeStatusProvisioning       = NodeStatus(proto.NodeStatus_PROVISIONING)
	NodeStatusFailedProvisioning = NodeStatus(proto.NodeStatus_FAILED_PROVISIONING)
	NodeStatusDiscarded          = NodeStatus(proto.NodeStatus_DISCARDED)
	NodeStatusOnline             = NodeStatus(proto.NodeStatus_ONLINE)
	NodeStatusTerminating        = NodeStatus(proto.NodeStatus_TERMINATING)
	NodeStatusFailedTerminating  = NodeStatus(proto.NodeStatus_FAILED_TERMINATING)
	NodeStatusTerminated         = NodeStatus(proto.NodeStatus_TERMINATED)
)

func (ns NodeStatus) AsProto() proto.NodeStatus_Status {
	return proto.NodeStatus_Status(ns)
}

type RunTaskConfig struct {
	ArtifactPreserver ArtifactPreserver
	SecretLoader      SecretLoader
}

type Node interface {
	Name() string
	PrepareForTask(task *Task) error
	RunTask(task *Task, config RunTaskConfig) (int, error) // TODO: we need cancellation
	Terminate() error
}

type nodeState struct {
	scheduler *Scheduler

	node   Node
	status NodeStatus
	tasks  []*Task
	log    *slog.Logger

	nodeName      string
	earliestStart time.Time
}

func (ns *nodeState) UpdateStatus(status NodeStatus) {
	if ns.status != status {
		ns.status = status
		ns.scheduler.broadcast(EventNodeStatusUpdated{Node: ns.nodeName, Status: status})
	}
}
