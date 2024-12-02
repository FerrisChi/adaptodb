package main

import (
	"adaptodb/pkg/schema"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/docker/go-connections/nat"
	"golang.org/x/crypto/ssh"
)

func connectSSH(conf *SSHConfig) (*ssh.Client, error) {
	key, err := os.ReadFile(conf.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read SSH key: %v", err)
	}

	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SSH key: %v", err)
	}

	config := &ssh.ClientConfig{
		User: conf.User,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	return ssh.Dial("tcp", conf.Host, config)
}

type Launcher struct {
	localProcesses map[uint64]*exec.Cmd
	sshSessions    map[uint64]*ssh.Client
	ctrlAddress    string
	networkName string
	networkIsSetup bool
}

func NewLauncher(ctrlAddress string) *Launcher {
	return &Launcher{
		localProcesses: make(map[uint64]*exec.Cmd),
		sshSessions:    make(map[uint64]*ssh.Client),
		ctrlAddress:    ctrlAddress,
		networkName: "adaptodb-net",
	}
}

func IsLocalAddress(addr string) bool {
	return strings.HasPrefix(addr, "127.0.0.1") ||
		strings.HasPrefix(addr, "localhost") ||
		addr == "::1"
}

func (l *Launcher) Launch(spec NodeSpec, members map[uint64]string, keyRanges []schema.KeyRange) error {
	if IsLocalAddress(spec.GrpcAddress) {
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

func (l *Launcher) launchLocal(spec NodeSpec, members map[uint64]string, keyRanges []schema.KeyRange) error {
	if !l.networkIsSetup {
		if err := l.setupNetwork(); err != nil {
			return fmt.Errorf("failed to setup docker network: %v", err)
		}
		l.networkIsSetup = true
	}

	port, _ := strconv.Atoi(spec.RaftAddress[strings.LastIndex(spec.RaftAddress, ":")+1:])
	_debugPort := port + 100
	fmt.Println("debug port: ", _debugPort)

    cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
    if err != nil {
		return fmt.Errorf("failed to create Docker client: %v", err)
    }
	defer cli.Close()

    // Define the command and arguments to pass to the container
    cmd := []string{
		// "./bin/release/node",  // use this in production
		"./bin/debug/node",    // use this in development
        "--id", fmt.Sprintf("%d", spec.ID),
        "--group-id", fmt.Sprintf("%d", spec.GroupID),
        "--address", spec.RaftAddress,
        "--data-dir", spec.DataDir,
        "--wal-dir", spec.WalDir,
        "--members", formatMembers(members),
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
		// nat.Port(fmt.Sprintf("%d/tcp", port)): []nat.PortBinding{{HostPort: fmt.Sprintf("%d", port)}},
        nat.Port(fmt.Sprintf("%d/tcp", _debugPort)): []nat.PortBinding{{HostPort: fmt.Sprintf("%d", _debugPort)}},
        nat.Port(fmt.Sprintf("%d/tcp", 51000+spec.ID)): []nat.PortBinding{{HostPort: fmt.Sprintf("%d", 51000+spec.ID)}},
        nat.Port(fmt.Sprintf("%d/tcp", 52000+spec.ID)): []nat.PortBinding{{HostPort: fmt.Sprintf("%d", 52000+spec.ID)}},
        nat.Port(fmt.Sprintf("%d/tcp", 53000+spec.ID)): []nat.PortBinding{{HostPort: fmt.Sprintf("%d", 53000+spec.ID)}},
    }

	log.Printf("Port bindings: %v", portBindings)
	
	// Read current hostname from `/etc/hostname`
	// This is needed to set the hostname of the container because
	// for some magical reason, dragonboat expects the container's hostname
	// to be the same as the host's hostname
	hostname, err := os.ReadFile("/etc/hostname")
	if err != nil {
		return fmt.Errorf("failed to read hostname: %v", err)
	}

    // Run the existing container with the specified arguments
    resp, err := cli.ContainerCreate(context.Background(), &container.Config{
        Image: "adaptodb-node", // Replace with your Docker image
        Cmd:   cmd,
		Hostname: string(hostname),
    }, &container.HostConfig{
        NetworkMode: container.NetworkMode(l.networkName),
		PortBindings: portBindings,
		LogConfig: logConfig,
    }, &network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			l.networkName: {},
		},
	}, nil, fmt.Sprintf("node-%d", spec.ID))

    if err != nil {
        return fmt.Errorf("failed to create Docker container: %v", err)
    }

    // Start the container
    if err := cli.ContainerStart(context.Background(), resp.ID, container.StartOptions{}); err != nil {
        return fmt.Errorf("failed to start Docker container: %v", err)
    }

    return nil
	
	// Create docker container
	// cmd := exec.Command("docker", "run",
	// 	"-d",
	// 	"--name", fmt.Sprintf("node%d", spec.ID),
	// 	"--network", l.networkName,
	// 	"-p", fmt.Sprintf("%d:%d", _debugPort, _debugPort),
	// 	"-p", fmt.Sprintf("%d:%d", 51000+spec.ID, 51000+spec.ID),
	// 	"-p", fmt.Sprintf("%d:%d", 52000+spec.ID, 52000+spec.ID),
	// 	"-p", fmt.Sprintf("%d:%d", 53000+spec.ID, 53000+spec.ID),
	// 	"adaptodb-node",
	// 	"--id", fmt.Sprintf("%d", spec.ID),
	// 	"--group-id", fmt.Sprintf("%d", spec.GroupID),
	// 	"--address", spec.RaftAddress,
	// 	"--data-dir", spec.DataDir,
	// 	"--wal-dir", spec.WalDir,
	// 	"--members", formatMembers(members),
	// 	"--keyrange", schema.KeyRangeToString(keyRanges),
	// 	"--ctrl-address", l.ctrlAddress,
	// )

	// cmd := exec.Command("./bin/release/node", // use this in production
	// 	// cmd := exec.Command("dlv", "exec", "./bin/debug/node", "--headless", fmt.Sprintf("--listen=:%d", debugPort), "--api-version=2", "--", // use this in development
	// 	"--id", fmt.Sprintf("%d", spec.ID),
	// 	"--group-id", fmt.Sprintf("%d", spec.GroupID),
	// 	"--address", spec.RaftAddress,
	// 	"--data-dir", spec.DataDir,
	// 	"--wal-dir", spec.WalDir,
	// 	"--members", formatMembers(members),
	// 	"--keyrange", schema.KeyRangeToString(keyRanges),
	// 	"--ctrl-address", l.ctrlAddress,
	// )

	// cmd.Stdout = os.Stdout
	// cmd.Stderr = os.Stderr

	// if err := cmd.Start(); err != nil {
	// 	return fmt.Errorf("failed to start local node process: %v", err)
	// }

	// l.localProcesses[spec.ID] = cmd
	// return nil
}

func (l *Launcher) dockerCleanup() error {
	// Stop and remove all containers
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

	for _, cntner := range containers {
		fmt.Print("Stopping container ", cntner.ID[:10], "... ")
		if err := cli.ContainerStop(ctx, cntner.ID, container.StopOptions{}); err != nil {
			return fmt.Errorf("failed to stop Docker container: %v", err)
		}
		fmt.Println("Success")

		fmt.Print("Removing container ", cntner.ID[:10], "... ")
		if err := cli.ContainerRemove(ctx, cntner.ID, container.RemoveOptions{}); err != nil {
			return fmt.Errorf("failed to remove Docker container: %v", err)
		}
		fmt.Println("Success")
	}
	// cmd := exec.Command("docker", "ps", "-q", "-f", "name=node-*")
    // output, err := cmd.Output()
    // if err != nil {
    //     return fmt.Errorf("failed to list containers: %v", err)
    // }

	// if len(output) > 0 {
	// 	cmd = exec.Command("docker", "stop", string(output))
	// 	if err := cmd.Run(); err != nil {
	// 		return fmt.Errorf("failed to stop docker containers: %v", err)
	// 	}
	// 	cmd = exec.Command("docker", "rm", string(output))
	// 	if err := cmd.Run(); err != nil {
	// 		return fmt.Errorf("failed to remove docker containers: %v", err)
	// 	}
	// }

	// Remove docker network
    err = cli.NetworkRemove(context.Background(), l.networkName)
    if err != nil {
		if errdefs.IsNotFound(err) {
            // Ignore the error if the network does not exist
            return nil
        }
        return fmt.Errorf("failed to remove Docker network: %v", err)
    }

    return nil
}

func (l *Launcher) launchRemote(spec NodeSpec, members map[uint64]string, keyRanges []schema.KeyRange) error {
	client, err := connectSSH(spec.SSH)
	if err != nil {
		return fmt.Errorf("failed to establish SSH connection: %v", err)
	}

	l.sshSessions[spec.ID] = client

	session, err := client.NewSession()
	if err != nil {
		return fmt.Errorf("failed to create SSH session: %v", err)
	}
	defer session.Close()

	cmd := fmt.Sprintf("./node -id %d -addr %s -group %d -data-dir %s -wal-dir %s -members '%s'",
		spec.ID, spec.RaftAddress, spec.GroupID, spec.DataDir, spec.WalDir, formatMembers(members))

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

func formatMembers(members map[uint64]string) string {
	result := ""
	for id, addr := range members {
		if result != "" {
			result += ","
		}
		result += fmt.Sprintf("%d=%s", id, addr)
	}
	return result
}
