package utils

import (
	"context"
	"fmt"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

func RunDockerContainer(image, containerName string, ports map[string]string, envVars []string) (string, error) {
	ctx := context.Background()

	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return "", fmt.Errorf("failed to create Docker client: %w", err)
	}

	// Map ports
	portBindings := nat.PortMap{}
	for containerPort, hostPort := range ports {
		portBindings[nat.Port(containerPort+"/tcp")] = []nat.PortBinding{
			{HostIP: "0.0.0.0", HostPort: hostPort},
		}
	}

	// Create container
	resp, err := cli.ContainerCreate(
		ctx,
		&container.Config{
			Image: image,
			Env:   envVars,
			ExposedPorts: nat.PortSet{
				nat.Port("60080/tcp"): struct{}{}, // Example port
			},
		},
		&container.HostConfig{
			PortBindings: portBindings,
		},
		nil,
		nil,
		containerName,
	)
	if err != nil {
		return "", fmt.Errorf("failed to create Docker container: %w", err)
	}

	// Start container
	if err := cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return "", fmt.Errorf("failed to start Docker container: %w", err)
	}

	return resp.ID, nil
}
