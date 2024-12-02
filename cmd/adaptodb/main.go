package main

import (
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
	"adaptodb/pkg/balancer"
	"adaptodb/pkg/controller"
	"adaptodb/pkg/metadata"
	pb "adaptodb/pkg/proto/proto"
	"adaptodb/pkg/router"
	"adaptodb/pkg/schema"
)

type SSHConfig struct {
	Host    string // hostname:port or IP:port
	User    string
	KeyPath string // path to private key
}
type NodeSpec struct {
	ID          uint64
	RpcAddress  string     // RPC Address
	RaftAddress string     // Raft Address
	SSH         *SSHConfig // nil for local nodes
	GroupID     uint64
	DataDir     string
	WalDir      string
}

func main() {

	logFile, err := os.OpenFile("adaptodb.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("failed to open log file: %v", err)
	}
	log.SetOutput(logFile)
	log.SetFlags(log.Ltime | log.Lshortfile)

	// Load configuration
	config := schema.Config{}
	data, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Fatalf("failed to read configuration file: %v", err)
	}
	print(string(data))

	err = yaml.Unmarshal(data, &config)
	if err != nil {
		log.Fatalf("failed to load configuration: %v", err)
	}

	log.Print("Configuration: ", config)

	// https://github.com/golang/go/issues/17393
	if runtime.GOOS == "darwin" {
		signal.Ignore(syscall.Signal(0xd))
	}

	// Initialize Metadata Manager
	metadata, err := metadata.NewMetadata(&config)
	if err != nil {
		log.Fatalf("failed to initialize Metadata Manager: %v", err)
	}

	// Initialize Dragonboat Nodes
	controllerAddress := "localhost:60082"
	launcher := NewLauncher(controllerAddress)
	defer launcher.Stop()

	for _, group := range config.RaftGroups {
		groupMembers := make(map[uint64]string)
		for _, nodeInfo := range group.Members {
			groupMembers[nodeInfo.ID] = nodeInfo.RaftAddress
		}
		keyRanges := metadata.GetShardKeyRanges(group.ShardID)

		for _, nodeInfo := range group.Members {
			var ssh *SSHConfig
			if IsLocalAddress(nodeInfo.RpcAddress) {
				ssh = nil
			} else {
				ssh = &SSHConfig{
					Host:    nodeInfo.RpcAddress,
					User:    nodeInfo.User,
					KeyPath: nodeInfo.SSHKeyPath,
				}
			}
			spec := NodeSpec{
				ID:          nodeInfo.ID,
				RpcAddress:  nodeInfo.RpcAddress,
				RaftAddress: nodeInfo.RaftAddress,
				SSH:         ssh,
				GroupID:     group.ShardID,
				DataDir:     fmt.Sprintf("tmp/data/node%d", nodeInfo.ID),
				WalDir:      fmt.Sprintf("tmp/wal/node%d", nodeInfo.ID),
			}

			if err := launcher.Launch(spec, groupMembers, keyRanges); err != nil {
				log.Fatalf("failed to launch node %d: %v", nodeInfo.ID, err)
			}
		}
	}
	log.Println("All nodes have been launched. Waiting for Raft to stabilize...")
	time.Sleep(schema.NODE_LAUNCH_TIMEOUT)
	metadata.UpdateKeyRangeFromNode()

	// Initialize Shard Controller and gRPC server
	controller := controller.NewController(metadata)
	defer controller.Stop()

	controllerGrpcServer := grpc.NewServer()
	defer controllerGrpcServer.GracefulStop()
	pb.RegisterControllerServer(controllerGrpcServer, controller)
	reflection.Register(controllerGrpcServer)

	lis, err := net.Listen("tcp", controllerAddress)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", controllerAddress, err)
	}
	log.Println("gRPC server listening on", controllerAddress)

	go func() {
		if err := controllerGrpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// Initialize Balance Watcher
	analyzer := balancer.NewDefaultAnalyzer("default", metadata)
	balancer, err := balancer.NewBalancer(controllerAddress, analyzer)
	if err != nil {
		log.Fatalf("failed to initialize Balance Watcher: %v", err)
	}
	defer balancer.Stop()

	// Initialize Request Router and HTTP server
	router := router.NewRouter(metadata)

	go func() {
		http.HandleFunc("/", router.HandleRequest)
		http.HandleFunc("/config", router.HandleConfigRequest)
		log.Printf("HTTP server listening on localhost:60080")
		if err := http.ListenAndServe("localhost:60080", nil); err != nil {
			log.Fatalf("failed to start HTTP server: %v", err)
		}
	}()

	// Initialize gRPC server
	routerGrpcServer := grpc.NewServer()
	defer routerGrpcServer.GracefulStop()
	pb.RegisterShardRouterServer(routerGrpcServer, router)
	reflection.Register(routerGrpcServer)

	routerGrpcAddress := "localhost:60081"
	lis, err = net.Listen("tcp", routerGrpcAddress)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", routerGrpcAddress, err)
	}
	log.Println("gRPC server listening on", routerGrpcAddress)

	go func() {
		if err := routerGrpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// Start Balance Watcher
	go balancer.StartMonitoring()

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	defer close(sigChan)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("AdaptoDB is running and awaiting requests...")

	// Wait for termination signal
	<-sigChan
	log.Println("Shutdown signal received. Shutting down AdaptoDB...")

	// Close all processes and sessions
	for _, process := range launcher.localProcesses {
		process.Process.Signal(syscall.SIGTERM)
	}
	for _, session := range launcher.sshSessions {
		session.Close()
	}

	// Shutdown (deferred) servers
	log.Println("AdaptoDB has shut down gracefully.")
}
