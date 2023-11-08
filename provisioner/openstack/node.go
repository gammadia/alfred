package openstack

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/docker/docker/client"
	"github.com/gammadia/alfred/provisioner/internal"
	"github.com/gammadia/alfred/scheduler"
	"github.com/gophercloud/gophercloud/openstack/compute/v2/servers"
	"golang.org/x/crypto/ssh"
)

type Node struct {
	name        string
	provisioner *Provisioner

	log    *slog.Logger
	server *servers.Server
	ssh    *ssh.Client
	docker *client.Client
	fs     *fs

	preparedJobs sync.Map
	terminated   atomic.Bool

	mutex sync.Mutex
}

// Node implements scheduler.Node
var _ scheduler.Node = (*Node)(nil)

func (n *Node) Name() string {
	return n.name
}

func (n *Node) connect(server *servers.Server) (err error) {
	n.provisioner.wg.Add(1)
	defer func() {
		if err != nil {
			_ = n.Terminate()
		}
	}()

	openstackClient := n.provisioner.client
	err = servers.WaitForStatus(openstackClient, server.ID, "ACTIVE", 60)
	if err != nil {
		return fmt.Errorf("failed while waiting for server '%s' to become ready: %w", n.name, err)
	}

	pages, err := servers.ListAddresses(openstackClient, server.ID).AllPages()
	if err != nil {
		return fmt.Errorf("failed to get server addresses for '%s': %w", n.name, err)
	}

	allAddresses, err := servers.ExtractAddresses(pages)
	if err != nil {
		return fmt.Errorf("failed to extract server addresses for '%s': %w", n.name, err)
	}

	var nodeAddress string
	for _, addresses := range allAddresses {
		for _, address := range addresses {
			if address.Version == 4 {
				nodeAddress = address.Address
			}
		}
	}
	if nodeAddress == "" {
		return fmt.Errorf("failed to find IPv4 address for server '%s'", n.name)
	}

	// Initialize SSH connection
	ctx, cancel := context.WithTimeout(context.TODO(), 1*time.Minute)
	defer cancel()

	for n.ssh == nil {
		select {
		case <-ctx.Done():
			return fmt.Errorf("failed to connect to server '%s' after 1 minute", n.name)

		default:
			time.Sleep(5 * time.Second)
			n.ssh, err = ssh.Dial("tcp", fmt.Sprintf("%s:22", nodeAddress), &ssh.ClientConfig{
				User:            n.provisioner.config.SshUsername,
				Timeout:         10 * time.Second,
				HostKeyCallback: ssh.InsecureIgnoreHostKey(),
				Auth: []ssh.AuthMethod{
					ssh.PublicKeys(n.provisioner.privateKey),
				},
			})
			if err != nil {
				switch {
				case errors.Is(err, syscall.ECONNREFUSED),
					errors.Is(err, syscall.ETIMEDOUT),
					errors.Is(err, os.ErrDeadlineExceeded):
					n.log.Debug("SSH connection to server refused, retrying in 5 seconds")

				default:
					return fmt.Errorf("failed to connect to server '%s': %w", n.name, err)
				}
			}
		}
	}

	// Initialize Docker client
	n.docker, err = client.NewClientWithOpts(
		client.WithHost(n.provisioner.config.DockerHost),
		client.WithDialContext(func(ctx context.Context, network, addr string) (conn net.Conn, err error) {
			ctx, cancel := context.WithDeadline(ctx, time.Now().Add(2*time.Minute))
			defer cancel()
			for {
				select {
				case <-ctx.Done():
					return nil, fmt.Errorf("failed to dial docker: %w", ctx.Err())

				default:
					if conn, err = n.ssh.Dial(network, addr); err == nil {
						return
					} else {
						n.log.Debug("Connection to Docker daemon refused, retrying in 5 seconds")
						time.Sleep(5 * time.Second)
					}
				}
			}
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to initialize docker client: %w", err)
	}

	// Initialize file system
	n.fs = newFs(n.provisioner.config.Workspace, n.ssh)

	return nil
}

func (n *Node) PrepareForTask(task *scheduler.Task) error {
	jobKey := task.Job.FQN()
	log := n.log.With("job", jobKey, "task", task.Name)
	if _, loaded := n.preparedJobs.LoadOrStore(jobKey, nil); loaded {
		return nil
	}

	log.Debug("Prepare job for task")

	for _, image := range task.Job.Steps {
		if err := internal.EnsureNodeHasImage(&n.mutex, n.log, n.docker, n.ssh, image); err != nil {
			return fmt.Errorf("failed to ensure node has image '%s': %w", image, err)
		}
	}
	for _, service := range task.Job.Services {
		if err := internal.EnsureNodeHasImage(&n.mutex, n.log, n.docker, n.ssh, service.Image); err != nil {
			return fmt.Errorf("failed to ensure node has image '%s' for service '%s': %w", service.Image, service.Name, err)
		}
	}

	log.Debug("Job prepared for task")

	return nil
}

func (n *Node) RunTask(task *scheduler.Task, runConfig scheduler.RunTaskConfig) (int, error) {
	return internal.RunContainer(context.TODO(), n.log, n.docker, task, n.fs, runConfig)
}

func (n *Node) Terminate() error {
	if n.terminated.Load() == true {
		return nil
	}
	n.terminated.Store(true)

	if n.ssh != nil {
		_ = n.ssh.Close()
	}

	if n.docker != nil {
		_ = n.docker.Close()
	}

	err := servers.Delete(n.provisioner.client, n.server.ID).ExtractErr()

	n.provisioner.wg.Done()
	return err
}
