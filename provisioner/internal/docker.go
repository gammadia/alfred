package internal

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/gammadia/alfred/proto"
	"github.com/gammadia/alfred/scheduler"
	"github.com/samber/lo"
	"golang.org/x/crypto/ssh"
)

func EnsureNodeHasImage(
	mutex *sync.Mutex,
	log *slog.Logger,
	docker *client.Client,
	ssh *ssh.Client,
	image string,
) error {
	mutex.Lock()
	defer mutex.Unlock()

	imageLog := log.With("image", image)

	if strings.HasPrefix(image, "sha256:") {
		return ensureNodeHasLocallyBuiltImage(imageLog, docker, ssh, image)
	}

	return ensureNodeHasImageFromRegistry(imageLog, docker, image)
}

func ensureNodeHasLocallyBuiltImage(log *slog.Logger, docker *client.Client, ssh *ssh.Client, image string) error {
	log.Debug("Ensuring node has locally-built image")

	if _, _, err := docker.ImageInspectWithRaw(context.TODO(), image); err == nil {
		log.Debug("Image already on node")
		return nil
	} else if !client.IsErrNotFound(err) {
		return fmt.Errorf("inspect image '%s': %w", image, err)
	}

	log.Info("Image not on node, loading")

	session, err := ssh.NewSession()
	if err != nil {
		return fmt.Errorf("failed to create SSH session: %w", err)
	}
	defer session.Close()

	saveCmd := exec.Command("/bin/bash", "-euo", "pipefail", "-c", fmt.Sprintf("docker save '%s' | zstd --compress --adapt=min=5,max=15", image))
	saveOut := lo.Must(saveCmd.StdoutPipe())
	session.Stdin = saveOut
	session.Stderr = os.Stderr

	if err := saveCmd.Start(); err != nil {
		return fmt.Errorf("failed starting docker save for image '%s': %w", image, err)
	}

	if err := session.Run("zstd --decompress | docker load"); err != nil {
		return fmt.Errorf("failed docker load for image '%s': %w", image, err)
	}

	if err := saveCmd.Wait(); err != nil {
		return fmt.Errorf("failed docker save for iamge '%s': %w", image, err)
	}

	log.Debug("Image loaded on node")
	return nil
}

func ensureNodeHasImageFromRegistry(log *slog.Logger, docker *client.Client, image string) error {
	log.Debug("Ensuring node has image from registry")

	ctx := context.TODO()
	list, err := docker.ImageList(ctx, types.ImageListOptions{
		Filters: filters.NewArgs(filters.Arg("reference", image)),
	})
	if err != nil {
		return fmt.Errorf("failed to list images: %w", err)
	}

	// We only need to check that the list is non-empty, because we filtered by reference
	if len(list) == 0 {
		log.Debug("Pulling image")
		reader, err := docker.ImagePull(ctx, image, types.ImagePullOptions{})
		if err != nil {
			return fmt.Errorf("failed to pull image '%s': %w", image, err)
		}
		defer reader.Close()

		// Wait for the pull to finish
		_, _ = io.Copy(io.Discard, reader)

		// We might not be handling pull error properly, but parsing the JSON response is a pain
		// Let's just assume it worked, and if it didn't, the container create will fail
	}

	return nil
}

func RunContainer(
	ctx context.Context,
	log *slog.Logger,
	docker *client.Client,
	task *scheduler.Task,
	fs WorkspaceFS,
	runConfig scheduler.RunTaskConfig,
) (int, error) {
	cleanup := func(what string, thunk func() error, args ...any) {
		if err := thunk(); err != nil {
			args = append([]any{"error", err}, args...)
			log.Error("Failed to cleanup "+what, args...)
		}
	}

	// Setup network to link main container with services
	networkName := fmt.Sprintf("alfred-%s", task.FQN())
	netResp, err := docker.NetworkCreate(ctx, networkName, types.NetworkCreate{Driver: "bridge"})
	if err != nil {
		return -1, fmt.Errorf("failed to create docker network: %w", err)
	}
	networkId := netResp.ID
	defer cleanup(
		"network",
		func() error { return docker.NetworkRemove(context.Background(), networkId) },
	)

	// Initialize workspace
	taskFs := fs.Scope(task.FQN())

	if err := taskFs.MkDir("/"); err != nil {
		return -1, fmt.Errorf("failed to create workspace: %w", err)
	}
	defer cleanup(
		"workspace",
		func() error { return taskFs.Delete("/") },
	)

	for _, dir := range []string{"output", "shared"} {
		if err := taskFs.MkDir("/" + dir); err != nil {
			return -1, fmt.Errorf("failed to create workspace directory '%s': %w", dir, err)
		}
	}

	// Environment variables for each service
	serviceEnv := map[string][]string{}
	// Container IDs for each service
	serviceContainers := map[string]string{}

	for _, service := range task.Job.Services {
		env := lo.Map(service.Env, func(jobEnv *proto.Job_Env, _ int) string {
			return fmt.Sprintf("%s=%s", jobEnv.Key, jobEnv.Value)
		})
		serviceEnv[service.Name] = env

		resp, err := docker.ContainerCreate(
			ctx,
			&container.Config{
				Image: service.Image,
				Env:   env,
			},
			&container.HostConfig{},
			&network.NetworkingConfig{
				EndpointsConfig: map[string]*network.EndpointSettings{
					networkName: {
						NetworkID: networkId,
						Aliases:   []string{service.Name},
					},
				},
			},
			nil,
			fmt.Sprintf("alfred-%s-%s", task.FQN(), service.Name),
		)
		if err != nil {
			return -1, fmt.Errorf("failed to create docker container for service '%s': %w", service.Name, err)
		}
		defer cleanup(
			"service container",
			func() error {
				return docker.ContainerRemove(context.Background(), resp.ID,
					types.ContainerRemoveOptions{RemoveVolumes: true, Force: true})
			},
			"service", service.Name,
		)
		serviceContainers[service.Name] = resp.ID
	}

	// Start all services
	var wg sync.WaitGroup
	serviceErrors := make(chan error, len(task.Job.Services))

	wg.Add(len(task.Job.Services))
	for _, service := range task.Job.Services {
		go func(service *proto.Job_Service) {
			defer wg.Done()
			serviceLog := log.With("service", service.Name)

			serviceLog.Debug("Starting service container")
			containerId := serviceContainers[service.Name]
			err := docker.ContainerStart(ctx, containerId, types.ContainerStartOptions{})
			if err != nil {
				serviceErrors <- fmt.Errorf("failed to start docker container for service '%s': %w", service.Name, err)
				return
			}

			if service.Health == nil {
				serviceLog.Debug("No health check defined, skipping...")
				return
			}

			interval := lo.Ternary(service.Health.Interval != nil, service.Health.Interval.AsDuration(), 10*time.Second)
			timeout := lo.Ternary(service.Health.Timeout != nil, service.Health.Timeout.AsDuration(), 5*time.Second)
			retries := lo.Ternary(service.Health.Retries != nil, int(*service.Health.Retries), 3)

			for i := 0; i < retries; i++ {
				// Always wait 1 second before running the health check, and potentially more between retries
				time.Sleep(lo.Ternary(i > 0, interval, 1*time.Second))

				healthCheckLog := serviceLog.With(slog.Group("retry", "attempt", i+1, "interval", interval))

				exec, err := docker.ContainerExecCreate(ctx, containerId, types.ExecConfig{
					Cmd:          append([]string{service.Health.Cmd}, service.Health.Args...),
					Env:          serviceEnv[service.Name],
					AttachStdout: true, // We are piping stdout to io.Discard to "wait" for completion
				})
				if err != nil {
					serviceErrors <- fmt.Errorf("failed to create docker exec for service '%s': %w", service.Name, err)
					return
				}

				execCtx, cancel := context.WithTimeout(ctx, timeout)
				defer cancel()
				healthCheckLog.Debug("Running health check")
				attach, err := docker.ContainerExecAttach(execCtx, exec.ID, types.ExecStartCheck{})
				if err != nil {
					serviceErrors <- fmt.Errorf("failed to attach docker exec for service '%s': %w", service.Name, err)
					return
				}

				healthCheckTimedOut := false
				go func() {
					<-execCtx.Done()
					healthCheckTimedOut = true
					attach.Close()
				}()

				if _, err := io.Copy(io.Discard, attach.Reader); err != nil && !healthCheckTimedOut {
					serviceErrors <- fmt.Errorf("failed during docker exec for service '%s': %w", service.Name, err)
					return
				}

				if !healthCheckTimedOut {
					inspect, err := docker.ContainerExecInspect(ctx, exec.ID)
					if err != nil {
						serviceErrors <- fmt.Errorf("failed to inspect docker exec for service '%s': %w", service.Name, err)
						return
					}
					if inspect.ExitCode == 0 {
						healthCheckLog.Debug("Service is ready")
						return
					}

					healthCheckLog.Debug("Service health check failed, retrying...", "exitcode", inspect.ExitCode)
				} else {
					healthCheckLog.Debug("Service health check timed out, retrying...")
				}
			}

			serviceErrors <- fmt.Errorf("failed health check for service '%s'", service.Name)
		}(service)
	}

	// Wait for all services to start
	wg.Wait()
	close(serviceErrors)

	if err := <-serviceErrors; err != nil {
		return -1, fmt.Errorf("some service failed: %w", err)
	}

	// Create and execute steps containers
	var status container.WaitResponse
	for i, image := range task.Job.Steps {
		// Using a func here so that defer are called between each iteration
		if err := func(stepIndex int) error {
			secretEnv := []string{}
			for _, secret := range task.Job.Secrets {
				if runConfig.SecretLoader == nil {
					return fmt.Errorf("no secret loader available")
				}
				secretData, err := runConfig.SecretLoader(secret.Value)
				if err != nil {
					return fmt.Errorf("failed to load secret '%s': %w", secret.Key, err)
				}
				secretEnv = append(secretEnv, fmt.Sprintf("%s=%s", secret.Key, base64.StdEncoding.EncodeToString(secretData)))
			}

			resp, err := docker.ContainerCreate(
				ctx,
				&container.Config{
					Image: image,
					Env: append(
						append(
							lo.Map(task.Job.Env, func(jobEnv *proto.Job_Env, _ int) string {
								return fmt.Sprintf("%s=%s", jobEnv.Key, jobEnv.Value)
							}),
							secretEnv...,
						),
						[]string{
							fmt.Sprintf("ALFRED_TASK=%s", task.Name),
							"ALFRED_SHARED=/alfred/shared",
							"ALFRED_OUTPUT=/alfred/output",
						}...,
					),
				},
				&container.HostConfig{
					AutoRemove: false, // Otherwise this will remove the container before we can get the logs
					Mounts: []mount.Mount{
						{
							Type:   mount.TypeBind,
							Source: taskFs.HostPath("/"),
							Target: "/alfred",
						},
					},
				},
				&network.NetworkingConfig{
					EndpointsConfig: map[string]*network.EndpointSettings{
						networkName: {
							NetworkID: networkId,
						},
					},
				},
				nil,
				fmt.Sprintf("alfred-%s-%d", task.FQN(), stepIndex),
			)
			if err != nil {
				return fmt.Errorf("failed to create docker container for step %d: %w", stepIndex, err)
			}
			defer cleanup(
				"step container",
				func() error {
					return docker.ContainerRemove(context.Background(), resp.ID,
						types.ContainerRemoveOptions{RemoveVolumes: true, Force: true})
				},
			)

			// Start main container
			wait, errChan := docker.ContainerWait(ctx, resp.ID, container.WaitConditionNextExit)
			err = docker.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{})
			if err != nil {
				return fmt.Errorf("failed to start docker container for step %d: %w", stepIndex, err)
			}

			out, err := docker.ContainerLogs(ctx, resp.ID, types.ContainerLogsOptions{ShowStdout: true, ShowStderr: true, Follow: true, Timestamps: true, Details: true})
			if err != nil {
				return fmt.Errorf("failed to get docker container logs for step %d: %w", stepIndex, err)
			}
			defer out.Close()

			_, _ = stdcopy.StdCopy(os.Stdout, os.Stderr, out)

			// Wait for the container to finish
			select {
			case status = <-wait:
				// Container is done
				if status.StatusCode != 0 {
					break
				}
			case err := <-errChan:
				return fmt.Errorf("failed while waiting for docker container for step %d: %w", stepIndex, err)
			}

			return nil
		}(i + 1); err != nil {
			return -1, err
		}
	}

	// Preserve artifacts
	if runConfig.ArtifactPreserver != nil {
		reader, err := taskFs.Archive("/output")
		if err != nil {
			return -1, fmt.Errorf("failed to archive 'output' directory: %w", err)
		}
		defer reader.Close()

		if err := runConfig.ArtifactPreserver(reader, task); err != nil {
			return -1, fmt.Errorf("failed to preserve artifacts: %w", err)
		}
	}

	if status.StatusCode != 0 {
		return int(status.StatusCode), fmt.Errorf("task execution ended with status: %d", status.StatusCode)
	}

	return 0, nil
}
