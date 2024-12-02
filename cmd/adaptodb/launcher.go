package main

import (
	"adaptodb/pkg/schema"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

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
}

func NewLauncher(ctrlAddress string) *Launcher {
	return &Launcher{
		localProcesses: make(map[uint64]*exec.Cmd),
		sshSessions:    make(map[uint64]*ssh.Client),
		ctrlAddress:    ctrlAddress,
	}
}

func IsLocalAddress(addr string) bool {
	return strings.HasPrefix(addr, "127.0.0.1") ||
		strings.HasPrefix(addr, "localhost") ||
		addr == "::1"
}

func (l *Launcher) Launch(spec NodeSpec, members map[uint64]string, keyRanges []schema.KeyRange) error {
	if IsLocalAddress(spec.RpcAddress) {
		return l.launchLocal(spec, members, keyRanges)
	}
	return l.launchRemote(spec, members, keyRanges)
}

func (l *Launcher) launchLocal(spec NodeSpec, members map[uint64]string, keyRanges []schema.KeyRange) error {
	port, _ := strconv.Atoi(spec.RaftAddress[strings.LastIndex(spec.RaftAddress, ":")+1:])
	_debugPort := port + 100
	fmt.Println("debug port: ", _debugPort)

	cmd := exec.Command("./bin/release/node", // use this in production
		// cmd := exec.Command("dlv", "exec", "./bin/debug/node", "--headless", fmt.Sprintf("--listen=:%d", debugPort), "--api-version=2", "--", // use this in development
		"--id", fmt.Sprintf("%d", spec.ID),
		"--group-id", fmt.Sprintf("%d", spec.GroupID),
		"--address", spec.RaftAddress,
		"--data-dir", spec.DataDir,
		"--wal-dir", spec.WalDir,
		"--members", formatMembers(members),
		"--keyrange", schema.KeyRangeToString(keyRanges),
		"--ctrl-address", l.ctrlAddress,
	)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start local node process: %v", err)
	}

	l.localProcesses[spec.ID] = cmd
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
	for _, proc := range l.localProcesses {
		if err := proc.Process.Kill(); err != nil {
			return fmt.Errorf("failed to kill local process: %v", err)
		}
		proc.Wait()
	}

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
