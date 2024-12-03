package main

import (
	"adaptodb/pkg/balancer"
	"adaptodb/pkg/controller"
	"adaptodb/pkg/metadata"
	"adaptodb/pkg/router"
	"adaptodb/pkg/schema"
	"adaptodb/pkg/utils"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"gopkg.in/yaml.v3"

	// Custom imports for AdaptoDB components (hypothetical paths)

	pb "adaptodb/pkg/proto/proto"
)

type SSHConfig struct {
	Host    string // hostname:port or IP:port
	User    string
	KeyPath string // path to private key
}
type NodeSpec struct {
	ID          uint64
	GrpcAddress string     // RPC Address
	RaftAddress string     // Raft Address
	SSH         *SSHConfig // nil for local nodes
	GroupID     uint64
	DataDir     string
	WalDir      string
}

func main() {
	logger := setupLogger()
	config := loadConfig(logger)

	handleDarwinSignal()

	metadataManager := initializeMetadataManager(config, logger)
	launcher := initializeNodes(config, metadataManager, logger)
	startRaftStabilization(metadataManager, logger)

	controllerAddress := "localhost:60082"
	controller := initializeController(metadataManager, controllerAddress, logger)
	startHTTPServer(metadataManager, logger)
	startRouterGRPCServer(metadataManager, logger)
	startBalancer(controllerAddress, metadataManager, logger)

	waitForShutdown(logger, launcher, controller)
	// waitForShutdown(logger, nil, controller)
}

// setupLogger configures the logger
func setupLogger() struct {
	Logf   func(format string, v ...interface{})
	Fatalf func(format string, v ...interface{})
} {
	logger := utils.NamedLogger("AdaptoDB Main")
	logFile, err := os.OpenFile("adaptodb.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		logger.Fatalf("Failed to open log file: %v", err)
	}
	log.SetOutput(logFile)
	log.SetFlags(log.Ltime | log.Lshortfile)

	return logger
}

// loadConfig reads and parses the configuration file
func loadConfig(logger struct {
	Logf   func(format string, v ...interface{})
	Fatalf func(format string, v ...interface{})
},
) schema.Config {
	data, err := os.ReadFile("config.yaml")
	if err != nil {
		logger.Fatalf("Failed to read configuration file: %v", err)
	}

	var config schema.Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		logger.Fatalf("Failed to load configuration: %v", err)
	}

	logger.Logf("Configuration loaded: %+v", config)
	return config
}

// handleDarwinSignal addresses a macOS-specific issue: https://github.com/golang/go/issues/17393
func handleDarwinSignal() {
	if runtime.GOOS == "darwin" {
		signal.Ignore(syscall.Signal(0xd))
	}
}

// initializeMetadataManager sets up the metadata manager.
func initializeMetadataManager(config schema.Config, logger struct {
	Logf   func(format string, v ...interface{})
	Fatalf func(format string, v ...interface{})
},
) *metadata.Metadata {
	metadataManager, err := metadata.NewMetadata(&config)
	if err != nil {
		logger.Fatalf("Failed to initialize Metadata Manager: %v", err)
	}
	return metadataManager
}

// initializeNodes launches the Raft nodes.
func initializeNodes(config schema.Config, metadataManager *metadata.Metadata, logger struct {
	Logf   func(format string, v ...interface{})
	Fatalf func(format string, v ...interface{})
},
) *Launcher {
	controllerAddress := "localhost:60082"
	launcher := NewLauncher(controllerAddress)

	for _, group := range config.RaftGroups {
		launchNodes(group, metadataManager, launcher, logger)
	}
	return launcher
}

// iniitalize docker container nodes
func initializeDockerNodes(config schema.Config, metadataManager *metadata.Metadata, logger struct {
	Logf   func(format string, v ...interface{})
	Fatalf func(format string, v ...interface{})
},
) {
	controllerAddress := "localhost:60082"

	for _, group := range config.RaftGroups {
		for _, nodeInfo := range group.Members {
			nodePorts := map[string]string{
				"60080": fmt.Sprintf("%d", 60080+nodeInfo.ID), // HTTP Port
				"60081": fmt.Sprintf("%d", 60081+nodeInfo.ID), // gRPC Port
			}

			envVars := []string{
				fmt.Sprintf("--id=%d", nodeInfo.ID),
				fmt.Sprintf("--group-id=%d", group.ShardID),
        fmt.Sprintf("--address=%s", nodeInfo.RaftAddress),
        fmt.Sprintf("--data-dir=%s", ),
        fmt.Sprintf("--members=%s")
				fmt.Sprintf("DATA_DIR=/data/node%d", nodeInfo.ID),
				fmt.Sprintf("WAL_DIR=/wal/node%d", nodeInfo.ID),
				fmt.Sprintf("CONTROLER_ADDRESS=%s", controllerAddress),
			}

			containerName := fmt.Sprintf("adaptodb_node_%d", nodeInfo.ID)
			containerID, err := utils.RunDockerContainer("adaptodb-node-image", containerName, nodePorts, envVars)
			if err != nil {
				logger.Fatalf("Failed to launch node %d in Docker: %v", nodeInfo.ID, err)
			}

			logger.Logf("Node %d launched in Docker (Container ID: %s)", nodeInfo.ID, containerID)
		}
	}
}

func launchNodes(group schema.RaftGroup, metadataManager *metadata.Metadata, launcher *Launcher, logger struct {
	Logf   func(format string, v ...interface{})
	Fatalf func(format string, v ...interface{})
},
) {
	groupMembers := make(map[uint64]string)
	for _, nodeInfo := range group.Members {
		groupMembers[nodeInfo.ID] = nodeInfo.RaftAddress
	}
	keyRanges := metadataManager.GetShardKeyRanges(group.ShardID)

	for _, nodeInfo := range group.Members {
		spec := createNodeSpec(nodeInfo, group.ShardID)
		if err := launcher.Launch(spec, groupMembers, keyRanges); err != nil {
			logger.Fatalf("Failed to launch node %d: %v", nodeInfo.ID, err)
		}
	}
	logger.Logf("All nodes in group %d launched.", group.ShardID)
}

func createNodeSpec(nodeInfo schema.NodeInfo, groupID uint64) NodeSpec {
	var ssh *SSHConfig
	if !IsLocalAddress(nodeInfo.GrpcAddress) {
		ssh = &SSHConfig{
			Host:    nodeInfo.GrpcAddress,
			User:    nodeInfo.User,
			KeyPath: nodeInfo.SSHKeyPath,
		}
	}
	return NodeSpec{
		ID:          nodeInfo.ID,
		GrpcAddress: nodeInfo.GrpcAddress,
		RaftAddress: nodeInfo.RaftAddress,
		SSH:         ssh,
		GroupID:     groupID,
		DataDir:     fmt.Sprintf("tmp/data/node%d", nodeInfo.ID),
		WalDir:      fmt.Sprintf("tmp/wal/node%d", nodeInfo.ID),
	}
}

// startRaftStabilization waits for Raft to stabilize.
func startRaftStabilization(metadataManager *metadata.Metadata, logger struct {
	Logf   func(format string, v ...interface{})
	Fatalf func(format string, v ...interface{})
},
) {
	logger.Logf("Waiting for Raft to stabilize...")
	time.Sleep(schema.NODE_LAUNCH_TIMEOUT)
	metadataManager.UpdateKeyRangeFromNode()
	logger.Logf("Raft stabilized.")
}

// initializeController sets up the shard controller.
func initializeController(metadataManager *metadata.Metadata, address string, logger struct {
	Logf   func(format string, v ...interface{})
	Fatalf func(format string, v ...interface{})
},
) *controller.Controller {
	controller := controller.NewController(metadataManager)
	grpcServer := grpc.NewServer()
	pb.RegisterControllerServer(grpcServer, controller)
	reflection.Register(grpcServer)

	go startGRPCServer(grpcServer, address, logger)
	return controller
}

// startHTTPServer starts the HTTP server for the router.
func startHTTPServer(metadataManager *metadata.Metadata, logger struct {
	Logf   func(format string, v ...interface{})
	Fatalf func(format string, v ...interface{})
},
) {
	router := router.NewRouter(metadataManager)
	go func() {
		http.HandleFunc("/", router.HandleRequest)
		http.HandleFunc("/config", router.HandleConfigRequest)
		logger.Logf("HTTP server listening on localhost:60080")
		if err := http.ListenAndServe("localhost:60080", nil); err != nil {
			logger.Fatalf("Failed to start HTTP server: %v", err)
		}
	}()
}

// startRouterGRPCServer starts the gRPC server for the router.
func startRouterGRPCServer(metadataManager *metadata.Metadata, logger struct {
	Logf   func(format string, v ...interface{})
	Fatalf func(format string, v ...interface{})
},
) {
	router := router.NewRouter(metadataManager)
	grpcServer := grpc.NewServer()
	pb.RegisterShardRouterServer(grpcServer, router)
	reflection.Register(grpcServer)

	address := "localhost:60081"
	go startGRPCServer(grpcServer, address, logger)
}

// startGRPCServer starts a generic gRPC server.
func startGRPCServer(server *grpc.Server, address string, logger struct {
	Logf   func(format string, v ...interface{})
	Fatalf func(format string, v ...interface{})
},
) {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		logger.Fatalf("Failed to listen on %s: %v", address, err)
	}
	logger.Logf("gRPC server listening on %s", address)
	if err := server.Serve(lis); err != nil {
		logger.Fatalf("Failed to serve gRPC server: %v", err)
	}
}

// startBalancer sets up and starts the balancer.
func startBalancer(controllerAddress string, metadataManager *metadata.Metadata, logger struct {
	Logf   func(format string, v ...interface{})
	Fatalf func(format string, v ...interface{})
},
) {
	analyzer := balancer.NewDefaultAnalyzer("default", metadataManager)
	balancer, err := balancer.NewBalancer(controllerAddress, analyzer)
	if err != nil {
		logger.Fatalf("Failed to initialize Balance Watcher: %v", err)
	}
	go balancer.StartMonitoring()
}

// waitForShutdown handles graceful shutdown.
func waitForShutdown(logger struct {
	Logf   func(format string, v ...interface{})
	Fatalf func(format string, v ...interface{})
}, launcher *Launcher, controller *controller.Controller,
) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	logger.Logf("AdaptoDB is running and awaiting requests...")
	<-sigChan
	logger.Logf("Shutdown signal received. Shutting down AdaptoDB...")

	for _, process := range launcher.localProcesses {
		process.Process.Signal(syscall.SIGTERM)
	}
	for _, session := range launcher.sshSessions {
		session.Close()
	}

	controller.Stop()
	logger.Logf("AdaptoDB has shut down gracefully.")
}
