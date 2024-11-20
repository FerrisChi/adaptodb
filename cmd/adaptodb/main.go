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

	"github.com/lni/dragonboat/v3"
	dconfig "github.com/lni/dragonboat/v3/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"gopkg.in/yaml.v3"

	// Custom imports for AdaptoDB components (hypothetical paths)
	"adaptodb/pkg/balancer"
	"adaptodb/pkg/controller"
	"adaptodb/pkg/metadata"
	"adaptodb/pkg/router"
	"adaptodb/pkg/router/proto"
	"adaptodb/pkg/schema"
	"adaptodb/pkg/sm"

	protoController "adaptodb/pkg/controller/proto"
)

// raftGroups:
//   - shardID: 1
//     members:
//       1: "localhost:63001"
//       2: "localhost:63002"
//       3: "localhost:63003"
//   - shardID: 2
//     members:
//       4: "localhost:64001"
//       5: "localhost:64002"
//       6: "localhost:64003"

func main() {

	logFile, err := os.OpenFile("adaptodb.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("failed to open log file: %v", err)
	}
	log.SetOutput(logFile)

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

	nodeHostList := make([]*dragonboat.NodeHost, 0)
	// Initialize each Raft group based on config
	for _, group := range config.RaftGroups {
		// Start each node in the group
		for nodeID, address := range group.Members {
			log.Printf("Starting Raft node %d with address %s in group %d", nodeID, address, group.ShardID)

			nhc := dconfig.NodeHostConfig{
				NodeHostDir:    fmt.Sprintf("tmp/data/node%d", nodeID),
				WALDir:         fmt.Sprintf("tmp/wal/node%d", nodeID),
				RTTMillisecond: 200,
				RaftAddress:    address,
			}

			nh, err := dragonboat.NewNodeHost(nhc)
			if err != nil {
				log.Fatalf("failed to create NodeHost: %v", err)
			}

			rc := dconfig.Config{
				NodeID:             nodeID,
				ClusterID:          group.ShardID,
				ElectionRTT:        10,
				HeartbeatRTT:       2,
				CheckQuorum:        true,
				SnapshotEntries:    100,
				CompactionOverhead: 10,
			}

			// Start Raft node
			if err := nh.StartCluster(group.Members, false, sm.NewKVStore, rc); err != nil {
				log.Fatalf("failed to start node %d in cluster %d: %v", nodeID, group.ShardID, err)
			}

			nodeHostList = append(nodeHostList, nh)
			log.Printf("Started Raft node %d in group %d", nodeID, group.ShardID)
		}
		log.Printf("Started Raft group %d with Members: %v", group.ShardID, group.Members)
	}

	defer func() {
		for _, nh := range nodeHostList {
			nh.Stop()
		}
	}()

	// Initialize Metadata Manager
	metadata, err := metadata.NewMetadata(&config)
	if err != nil {
		log.Fatalf("failed to initialize Metadata Manager: %v", err)
	}

	// Initialize Shard Controller and gRPC server
	controller := controller.NewController(metadata)

	controllerGrpcServer := grpc.NewServer()
	protoController.RegisterControllerServer(controllerGrpcServer, controller)
	reflection.Register(controllerGrpcServer)

	controllerAddress := "localhost:8082"
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

	// Initialize Request Router and HTTP server
	router := router.NewRouter(controller, metadata)

	go func() {
		http.HandleFunc("/", router.HandleRequest)
		log.Printf("HTTP server listening on localhost:8080")
		if err := http.ListenAndServe("localhost:8080", nil); err != nil {
			log.Fatalf("failed to start HTTP server: %v", err)
		}
	}()

	// Initialize gRPC server
	routerGrpcServer := grpc.NewServer()
	proto.RegisterShardRouterServer(routerGrpcServer, router)
	reflection.Register(routerGrpcServer)

	routerGrpcAddress := "localhost:8081"
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
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Log that AdaptoDB is running
	log.Println("AdaptoDB is running and awaiting requests...")

	// Wait for termination signal
	<-sigChan
	log.Println("Shutdown signal received. Shutting down AdaptoDB...")

	// Shutdown sequence
	controller.Stop()
	balancer.Stop()
	controllerGrpcServer.GracefulStop()
	routerGrpcServer.GracefulStop()

	log.Println("AdaptoDB has shut down gracefully.")
}
