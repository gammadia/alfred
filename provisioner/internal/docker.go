package internal

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/mount"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/gammadia/alfred/proto"
	"github.com/gammadia/alfred/scheduler"
	"github.com/samber/lo"
)

func RunContainer(
	ctx context.Context,
	docker *client.Client,
	task *scheduler.Task,
	fs WorkspaceFS,
	runConfig scheduler.RunTaskConfig,
) (int, error) {
	tryTo := func(what string, thunk func() error, args ...any) {
		if err := thunk(); err != nil {
			args = append([]any{"error", err}, args...)
			task.Log.Error("Failed to "+what, args...)
		}
	}

	// Setup network to link main container with services
	networkName := fmt.Sprintf("alfred-%s", task.FQN())
	netResp, err := docker.NetworkCreate(ctx, networkName, types.NetworkCreate{Driver: "bridge"})
	if err != nil {
		return -1, fmt.Errorf("failed to create docker network: %w", err)
	}
	networkId := netResp.ID
	defer tryTo(
		"remove Docker network",
		func() error {
			return docker.NetworkRemove(context.Background(), networkId)
		},
	)

	// Initialize workspace
	taskFs := fs.Scope(task.FQN())

	if err := taskFs.MkDir("/"); err != nil {
		return -1, fmt.Errorf("failed to create workspace: %w", err)
	}
	defer tryTo(
		"remove task workspace",
		func() error {
			return taskFs.Delete("/")
		},
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
		serviceLog := task.Log.With("service", service.Name)

		env := lo.Map(service.Env, func(jobEnv *proto.Job_Env, _ int) string {
			return fmt.Sprintf("%s=%s", jobEnv.Key, jobEnv.Value)
		})
		serviceEnv[service.Name] = env

		// Make sure the image has been loaded
		list, err := docker.ImageList(ctx, types.ImageListOptions{
			Filters: filters.NewArgs(filters.Arg("reference", service.Image)),
		})
		if err != nil {
			return -1, fmt.Errorf("failed to list docker images for service '%s': %w", service.Name, err)
		}

		// We only need to check that the list is non-empty, because we filtered by reference
		if len(list) == 0 {
			serviceLog.Debug("Pulling service image")
			reader, err := docker.ImagePull(ctx, service.Image, types.ImagePullOptions{})
			if err != nil {
				return -1, fmt.Errorf("failed to pull docker image for service '%s': %w", service.Name, err)
			}
			defer reader.Close()

			// Wait for the pull to finish
			_, _ = io.Copy(io.Discard, reader)

			// We might not be handling pull error properly, but parsing the JSON response is a pain
			// Let's just assume it worked, and if it didn't, the container create will fail
		} else {
			serviceLog.Debug("Service image already on node")
		}

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
		defer tryTo(
			"remove service container",
			func() error {
				return docker.ContainerRemove(context.Background(), resp.ID, types.ContainerRemoveOptions{RemoveVolumes: true, Force: true})
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
			serviceLog := task.Log.With("service", service.Name)

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
				healthCheckCmd := append([]string{service.Health.Cmd}, service.Health.Args...)

				exec, err := docker.ContainerExecCreate(ctx, containerId, types.ExecConfig{
					Cmd:          healthCheckCmd,
					Env:          serviceEnv[service.Name],
					AttachStdout: true, // We are piping stdout to io.Discard to "wait" for completion
				})
				if err != nil {
					serviceErrors <- fmt.Errorf("failed to create docker exec for service '%s': %w", service.Name, err)
					return
				}

				execCtx, cancel := context.WithTimeout(ctx, timeout)
				defer cancel()
				healthCheckLog.Debug("Running health check", "cmd", healthCheckCmd)
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

					healthCheckLog.Debug("Service health check unsuccessful, retrying...", "exitcode", inspect.ExitCode)
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
	var stepError error
	for i, image := range task.Job.Steps {
		// Using a func here so that defer are called between each iteration
		stepError = func(stepIndex int) error {
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
							fmt.Sprintf("ALFRED_TASK_FQN=%s", task.FQN()),
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
			defer tryTo(
				"remove step container",
				func() error {
					return docker.ContainerRemove(context.Background(), resp.ID, types.ContainerRemoveOptions{RemoveVolumes: true, Force: true})
				},
				"step", stepIndex,
			)

			// Start main container
			wait, errChan := docker.ContainerWait(ctx, resp.ID, container.WaitConditionNextExit)
			err = docker.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{})
			if err != nil {
				return fmt.Errorf("failed to start docker container for step %d: %w", stepIndex, err)
			}

			// Wait for the container to finish
			select {
			case status = <-wait:
				tryTo(
					"save container logs",
					func() error {
						return taskFs.SaveContainerLogs(resp.ID, fmt.Sprintf("/output/step-%d.log", stepIndex))
					},
				)

				// Container is done
				if status.StatusCode != 0 {
					return fmt.Errorf("step %d failed with status: %d", stepIndex, status.StatusCode)
				}
			case err := <-errChan:
				return fmt.Errorf("failed while waiting for docker container for step %d: %w", stepIndex, err)
			}

			return nil
		}(i + 1)

		// There's no point in executing further steps if one of them failed
		if stepError != nil {
			break
		}
	}

	// Here we don't use defer because the task workspace is removed in a defer statement already
	tryTo(
		"preserve artifact",
		func() error {
			if runConfig.ArtifactPreserver != nil {
				task.Log.Debug("Preserve artifact")

				reader, err := taskFs.Archive("/output")
				if err != nil {
					return fmt.Errorf("failed to archive 'output' directory: %w", err)
				}
				defer reader.Close()

				if err := runConfig.ArtifactPreserver(reader, task); err != nil {
					return fmt.Errorf("failed to preserve artifacts: %w", err)
				}
			}
			return nil
		},
	)

	if stepError != nil {
		return lo.Ternary(status.StatusCode != 0, int(status.StatusCode), -1), fmt.Errorf("task execution ended with error: %w", stepError)
	}

	return 0, nil
}
