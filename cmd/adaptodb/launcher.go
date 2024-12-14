package main

import (
	"adaptodb/pkg/schema"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/docker/go-connections/nat"
	"golang.org/x/crypto/ssh"
)

func connectSSH(spec *NodeSpec) (*ssh.Client, error) {
	key, err := os.ReadFile(spec.info.SSHKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read SSH key: %v", err)
	}

	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SSH key: %v", err)
	}

	config := &ssh.ClientConfig{
		User: spec.info.User,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	return ssh.Dial("tcp", spec.info.Address, config)
}

type Launcher struct {
	localProcesses map[uint64]*exec.Cmd
	sshSessions    map[uint64]*ssh.Client
	ctrlAddress    string
	networkName    string
	networkIsSetup bool
}

func NewLauncher(ctrlAddress string) *Launcher {
	return &Launcher{
		localProcesses: make(map[uint64]*exec.Cmd),
		sshSessions:    make(map[uint64]*ssh.Client),
		ctrlAddress:    ctrlAddress,
		networkName:    "adaptodb-net",
	}
}

func IsLocalAddress(addr string) bool {
	return strings.HasPrefix(addr, "127.0.0.1") ||
		strings.HasPrefix(addr, "localhost") ||
		addr == "::1"
}

func (l *Launcher) Launch(spec NodeSpec, members []schema.NodeInfo, keyRanges []schema.KeyRange) error {
	if IsLocalAddress(spec.info.Address) {
		return l.launchLocal(spec, members, keyRanges)
	}
	return l.launchRemote(spec, members, keyRanges)
}

func (l *Launcher) setupNetwork() error {
	// Create docker network
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return fmt.Errorf("failed to create Docker client: %v", err)
	}

	_, err = cli.NetworkCreate(context.Background(), l.networkName, network.CreateOptions{
		Driver: "bridge",
	})
	if err != nil {
		if errdefs.IsConflict(err) {
			// Ignore the error if the network already exists
			return nil
		}
		return fmt.Errorf("failed to create Docker network: %v", err)
	}

	return nil
}

func (l *Launcher) launchLocal(spec NodeSpec, members []schema.NodeInfo, keyRanges []schema.KeyRange) error {
	if !l.networkIsSetup {
		if err := l.setupNetwork(); err != nil {
			return fmt.Errorf("failed to setup docker network: %v", err)
		}
		l.networkIsSetup = true
	}

	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return fmt.Errorf("failed to create Docker client: %v", err)
	}
	defer cli.Close()

	// Define the command and arguments to pass to the container
	cmd := []string{
		// "./bin/release/node",  // use this in production
		"./bin/debug/node", // use this in development
		"--id", fmt.Sprintf("%d", spec.info.ID),
		"--group-id", fmt.Sprintf("%d", spec.GroupID),
		"--address", spec.info.Address,
		"--name", spec.info.Name,
		"--data-dir", spec.DataDir,
		"--wal-dir", spec.WalDir,
		"--members", getDragonBoatMembers(members),
		"--keyrange", schema.KeyRangeToString(keyRanges),
		"--ctrl-address", l.ctrlAddress,
	}

	// Define log configuration to redirect stdout and stderr to a file
	logConfig := container.LogConfig{
		Type: "json-file",
		Config: map[string]string{
			"max-size": "10m",
			"max-file": "3",
		},
	}

	// Define port bindings
	portBindings := nat.PortMap{
		nat.Port(fmt.Sprintf("%d/tcp", schema.NodeDebugPort)): []nat.PortBinding{{HostPort: fmt.Sprintf("%d", schema.NodeDebugPort+spec.info.ID)}},
		nat.Port(fmt.Sprintf("%d/tcp", schema.NodeGrpcPort)):  []nat.PortBinding{{HostPort: fmt.Sprintf("%d", schema.NodeGrpcPort+spec.info.ID)}},
		nat.Port(fmt.Sprintf("%d/tcp", schema.NodeHttpPort)):  []nat.PortBinding{{HostPort: fmt.Sprintf("%d", schema.NodeHttpPort+spec.info.ID)}},
		nat.Port(fmt.Sprintf("%d/tcp", schema.NodeStatsPort)): []nat.PortBinding{{HostPort: fmt.Sprintf("%d", schema.NodeStatsPort+spec.info.ID)}},
	}

	log.Printf("Port bindings: %v", portBindings)

	// Run the existing container with the specified arguments
	resp, err := cli.ContainerCreate(context.Background(), &container.Config{
		Image:    "adaptodb-node", // Replace with your Docker image
		Cmd:      cmd,
		Hostname: spec.info.Name,
	}, &container.HostConfig{
		NetworkMode:  container.NetworkMode(l.networkName),
		PortBindings: portBindings,
		LogConfig:    logConfig,
	}, &network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			l.networkName: {},
		},
	}, nil, spec.info.Name)

	if err != nil {
		return fmt.Errorf("failed to create Docker container: %v", err)
	}

	// Start the container
	if err := cli.ContainerStart(context.Background(), resp.ID, container.StartOptions{}); err != nil {
		return fmt.Errorf("failed to start Docker container: %v", err)
	}

	return nil
}

// dockerCleanup stops and removes all running containers and networks
func (l *Launcher) dockerCleanup() error {
	ctx := context.Background()
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return fmt.Errorf("failed to create Docker client: %v", err)
	}
	defer cli.Close()

	containers, err := cli.ContainerList(ctx, container.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list Docker containers: %v", err)
	}

	// Stop and remove containers
	for _, cntner := range containers {
		fmt.Printf("Stopping container %s (%s)...\n", cntner.ID[:10], cntner.Names[0])
		if err := cli.ContainerStop(ctx, cntner.ID, container.StopOptions{}); err != nil {
			fmt.Printf("Warning: failed to stop container: %v\n", err)
			continue
		}

		// Remove container and its volumes
		fmt.Printf("Removing container %s and its volumes...\n", cntner.ID[:10])
		if err := cli.ContainerRemove(ctx, cntner.ID, container.RemoveOptions{
			RemoveVolumes: true,
			Force:         true,
		}); err != nil {
			fmt.Printf("Warning: failed to remove container: %v\n", err)
			continue
		}
	}
	fmt.Println("Containers cleanup succeed")

	// Remove docker network
	networks, err := cli.NetworkList(ctx, network.ListOptions{
		Filters: filters.NewArgs(
			filters.Arg("name", l.networkName),
		),
	})
	if err != nil {
		return fmt.Errorf("failed to list networks: %v", err)
	}

	for _, nw := range networks {
		fmt.Printf("Removing network %s...\n", nw.Name)
		if err := cli.NetworkRemove(ctx, nw.ID); err != nil {
			if !errdefs.IsNotFound(err) {
				fmt.Printf("Warning: failed to remove network: %v\n", err)
			}
			continue
		}
	}
	fmt.Println("Network cleanup succeed")

	return nil
}

func (l *Launcher) launchRemote(spec NodeSpec, members []schema.NodeInfo, keyRanges []schema.KeyRange) error {
	client, err := connectSSH(&spec)
	if err != nil {
		return fmt.Errorf("failed to establish SSH connection: %v", err)
	}

	l.sshSessions[spec.info.ID] = client

	session, err := client.NewSession()
	if err != nil {
		return fmt.Errorf("failed to create SSH session: %v", err)
	}
	defer session.Close()

	cmd := fmt.Sprintf("./node -id %d -addr %s -group %d -data-dir %s -wal-dir %s -members '%s'",
		spec.info.ID, spec.info.Address, spec.GroupID, spec.DataDir, spec.WalDir, getDragonBoatMembers(members))

	if err := session.Start(cmd); err != nil {
		return fmt.Errorf("failed to start remote command: %v", err)
	}

	return nil
}

func (l *Launcher) Stop() error {
	// Stop local processes
	l.dockerCleanup()

	// for _, proc := range l.localProcesses {
	// 	if err := proc.Process.Kill(); err != nil {
	// 		return fmt.Errorf("failed to kill local process: %v", err)
	// 	}
	// 	proc.Wait()
	// }

	// Close SSH sessions
	for _, client := range l.sshSessions {
		client.Close()
	}

	return nil
}

func getDragonBoatMembers(mapping []schema.NodeInfo) string {
	var members []string
	for _, node := range mapping {
		members = append(members, fmt.Sprintf("%d=%s", node.ID, schema.GetDragronboatAddr(node.Name)))
	}
	return strings.Join(members, ",")
}
