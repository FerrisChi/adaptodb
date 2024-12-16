package main

import (
	"adaptodb/pkg/balancer"
	"adaptodb/pkg/controller"
	"adaptodb/pkg/metadata"
	"adaptodb/pkg/router"
	"adaptodb/pkg/schema"
	"adaptodb/pkg/utils"
	"flag"
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

type NodeSpec struct {
	info    schema.NodeInfo
	GroupID uint64
	DataDir string
	WalDir  string
}

func main() {
	// Define CLI flags to choose load balancer algorithm
	var algoStr string
	var algoParam float64
	flag.StringVar(&algoStr, "algo", "Relative", "ImbalanceAlgorithm to use: Relative, Percentile, or Statistical")
	flag.Float64Var(&algoParam, "algoParam", 10.0, "Paramter for the chosen algorithm (e.g. threshold factor, percentile, or stddev factor)")
	flag.Parse()

	logger := setupLogger()
	config := loadConfig(logger)

	handleDarwinSignal()

	logger.Logf("Starting AdaptoDB with algorithm %s and parameter %f\n", algoStr, algoParam)

	metadataManager := initializeMetadataManager(config, logger)
	controllerAddress := "host.docker.internal:60082"

	// Initialize nodes
	launcher := NewLauncher(controllerAddress)
	for _, group := range config.RaftGroups {
		launchNodes(group, metadataManager, launcher, logger)
	}

	startRaftStabilization(metadataManager, logger)

	controller := initializeController(metadataManager, "127.0.0.1:60082", logger)
	startHTTPServer(metadataManager, logger)
	startRouterGRPCServer(metadataManager, logger)

	// Convert algoStr to an ImbalanceAlgorithm
	algo, err := balancer.ParseAlgo(algoStr)
	if err != nil {
		logger.Fatalf("Invalid algorithm specified: %v", err)
	}

	balancer := startBalancer("127.0.0.1:60082", metadataManager, logger, algo, algoParam)

	waitForShutdown(logger, launcher, controller, balancer)
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

func launchNodes(group schema.RaftGroup, metadataManager *metadata.Metadata, launcher *Launcher, logger struct {
	Logf   func(format string, v ...interface{})
	Fatalf func(format string, v ...interface{})
},
) {
	for i := 0; i < len(group.Members); i++ {
		nodeInfo := &group.Members[i]
		if nodeInfo.Name == "" {
			nodeInfo.Name = fmt.Sprintf("node-%d", nodeInfo.ID)
			metadataManager.UpdateNodeInfo(nodeInfo.ID, *nodeInfo)
		}
	}
	keyRanges := metadataManager.GetShardKeyRanges(group.ShardID)

	for _, nodeInfo := range group.Members {
		spec := NodeSpec{
			info:    nodeInfo,
			GroupID: group.ShardID,
			DataDir: fmt.Sprintf("tmp/data/node%d", nodeInfo.ID),
			WalDir:  fmt.Sprintf("tmp/wal/node%d", nodeInfo.ID),
		}
		if err := launcher.Launch(spec, group.Members, keyRanges); err != nil {
			logger.Fatalf("Failed to launch node %d: %v", nodeInfo.ID, err)
		}
	}
	logger.Logf("All nodes in group %d launched.", group.ShardID)
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
		logger.Logf("HTTP server listening on 127.0.0.1:60080")
		if err := http.ListenAndServe("127.0.0.1:60080", nil); err != nil {
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

	address := "127.0.0.1:60081"
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
func startBalancer(
	ctrlLocalAddress string,
	metadataManager *metadata.Metadata,
	logger struct {
		Logf   func(format string, v ...interface{})
		Fatalf func(format string, v ...interface{})
	},
	algo balancer.ImbalanceAlgorithm,
	algoParam float64,
) *balancer.Balancer {
	analyzer := balancer.NewDefaultAnalyzer("default", metadataManager)
	balancer, err := balancer.NewBalancer(ctrlLocalAddress, analyzer)
	if err != nil {
		logger.Fatalf("Failed to initialize Balance Watcher: %v", err)
	}
	go balancer.StartMonitoring(algo, algoParam)
	return balancer
}

// waitForShutdown handles graceful shutdown.
func waitForShutdown(logger struct {
	Logf   func(format string, v ...interface{})
	Fatalf func(format string, v ...interface{})
}, launcher *Launcher, controller *controller.Controller, balancer *balancer.Balancer,
) {
	sigChan := make(chan os.Signal, 1)
	defer close(sigChan)
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

	launcher.Stop()
	controller.Stop()
	balancer.Stop()
	logger.Logf("AdaptoDB has shut down gracefully.")
}
