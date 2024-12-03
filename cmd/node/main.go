// node/main.go
package main

import (
	"adaptodb/pkg/schema"
	"adaptodb/pkg/sm"
	"adaptodb/pkg/utils"
	"context"
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

	pb "adaptodb/pkg/proto/proto"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type NodeConfig struct {
	nodeID      uint64
	address     string
	groupID     uint64
	dataDir     string
	walDir      string
	members     string
	keyrange    string
	ctrlAddress string
	initialized bool
}

func main() {
	logger := setupLogger()
	nodeConfig := parseNodeConfigFlags(logger)

	setupLogFile(nodeConfig.nodeID, logger)

	handleDarwinSignal()

	nodeHost := initializeNodeHost(nodeConfig, logger)
	defer nodeHost.Stop()

	startCluster(nodeHost, nodeConfig, logger)

	if !nodeConfig.initialized {
		initializeKeyRanges(nodeHost, nodeConfig, logger)
	}

	startGRPCServers(nodeHost, nodeConfig, logger)
	startHTTPServer(nodeHost, nodeConfig, logger)

	waitForShutdown(nodeHost, logger)
}

func setupLogger() struct {
	Logf   func(format string, v ...interface{})
	Fatalf func(format string, v ...interface{})
} {
	logger := utils.NamedLogger("Node Main")
	return logger
}

// Parses node config flags that contain node information
func parseNodeConfigFlags(logger struct {
	Logf   func(format string, v ...interface{})
	Fatalf func(format string, v ...interface{})
},
) *NodeConfig {
	nodeID := flag.Uint64("id", 0, "NodeID to start")
	address := flag.String("address", "", "Node address (e.g. localhost:63001)")
	groupID := flag.Uint64("group-id", 0, "Raft group ID")
	dataDir := flag.String("data-dir", "", "Data directory path")
	walDir := flag.String("wal-dir", "", "WAL directory path")
	members := flag.String("members", "", "Comma-separated list of member addresses (format: id1=addr1,id2=addr2)")
	keyrange := flag.String("keyrange", "", "Key range managed by this node (format: start-end)")
	ctrlAddress := flag.String("ctrl-address", "", "Controller address (e.g. localhost:50001)")
	flag.Parse()

	if *nodeID == 0 || *address == "" || *groupID == 0 {
		flag.Usage()
		logger.Fatalf("Missing required flags.")
	}

	initialized := isNodeInitialized(*nodeID)

	return &NodeConfig{
		nodeID:      *nodeID,
		address:     *address,
		groupID:     *groupID,
		dataDir:     *dataDir,
		walDir:      *walDir,
		members:     *members,
		keyrange:    *keyrange,
		ctrlAddress: *ctrlAddress,
		initialized: initialized,
	}
}

func isNodeInitialized(nodeID uint64) bool {
	logFilePath := fmt.Sprintf("tmp/log/%d.log", nodeID)
	_, err := os.Stat(logFilePath)
	return err == nil
}

func setupLogFile(nodeID uint64, logger struct {
	Logf   func(format string, v ...interface{})
	Fatalf func(format string, v ...interface{})
},
) {
	logDir := "tmp/log"
	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		if err := os.MkdirAll(logDir, 0755); err != nil {
			logger.Fatalf("Failed to create log directory: %v", err)
		}
	}

	logFile, err := os.OpenFile(fmt.Sprintf("%s/%d.log", logDir, nodeID), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		logger.Fatalf("Failed to open log file: %v", err)
	}
	log.SetOutput(logFile)
	log.SetFlags(log.Ltime | log.Lshortfile)
}

// handleDarwinSignal addresses a macOS-specific issue: https://github.com/golang/go/issues/17393
func handleDarwinSignal() {
	if runtime.GOOS == "darwin" {
		signal.Ignore(syscall.Signal(0xd))
	}
}

func initializeNodeHost(nodeConfig *NodeConfig, logger struct {
	Logf   func(format string, v ...interface{})
	Fatalf func(format string, v ...interface{})
},
) *dragonboat.NodeHost {
	nodeHostConfig := config.NodeHostConfig{
		NodeHostDir:    nodeConfig.dataDir,
		WALDir:         nodeConfig.walDir,
		RTTMillisecond: 200,
		RaftAddress:    nodeConfig.address,
	}

	nodeHost, err := dragonboat.NewNodeHost(nodeHostConfig)
	if err != nil {
		logger.Fatalf("Failed to create NodeHost: %v", err)
	}

	return nodeHost
}

func startCluster(nodeHost *dragonboat.NodeHost, nodeConfig *NodeConfig, logger struct {
	Logf   func(format string, v ...interface{})
	Fatalf func(format string, v ...interface{})
},
) {
	membersMap, err := schema.ParseMembers(nodeConfig.members)
	if err != nil {
		logger.Fatalf("Failed to parse members: %v", err)
	}

	// Explicitly refer to dragonboat/config.Config
	rc := config.Config{
		NodeID:             nodeConfig.nodeID,
		ClusterID:          nodeConfig.groupID,
		ElectionRTT:        10,
		HeartbeatRTT:       2,
		CheckQuorum:        true,
		SnapshotEntries:    100,
		CompactionOverhead: 10,
	}

	if err := nodeHost.StartCluster(membersMap, false, sm.NewKVStore, rc); err != nil {
		logger.Fatalf("Failed to start node: %v", err)
	}

	logger.Logf("Node started: ID=%d, Address=%s, GroupID=%d", nodeConfig.nodeID, nodeConfig.address, nodeConfig.groupID)
}

func initializeKeyRanges(nodeHost *dragonboat.NodeHost, config *NodeConfig, logger struct {
	Logf   func(format string, v ...interface{})
	Fatalf func(format string, v ...interface{})
},
) {
	logger.Logf("Waiting for raft to stabilize...")
	time.Sleep(schema.RAFT_LAUNCH_TIMEOUT)

	leader, _, err := nodeHost.GetLeaderID(config.groupID)
	if err == nil && leader == config.nodeID {
		keyRanges := schema.ParseKeyRanges(config.keyrange)
		ctx, cancel := context.WithTimeout(context.Background(), schema.READ_WRITE_TIMEOUT)
		defer cancel()

		session := nodeHost.GetNoOPSession(config.groupID)
		cmd := fmt.Sprintf("apply_schedule:init,%s", schema.KeyRangeToString(keyRanges))
		if _, err := nodeHost.SyncPropose(ctx, session, []byte(cmd)); err != nil {
			logger.Fatalf("Failed to propose initial key ranges: %v", err)
		}

		logger.Logf("Proposed initial key ranges: %s", keyRanges)
	}
}

func startGRPCServers(nh *dragonboat.NodeHost, nodeConfig *NodeConfig, logger struct {
	Logf   func(format string, v ...interface{})
	Fatalf func(format string, v ...interface{})
},
) {
	membersMap, err := schema.ParseMembers(nodeConfig.members)
	if err != nil {
		logger.Fatalf("Failed to parse members: %v", err)
	}
	statsServer := NewNodeStatsServer(nh, nodeConfig.groupID)
	startGRPCServer(fmt.Sprintf("localhost:%d", 53000+nodeConfig.nodeID), pb.NodeStatsServer(statsServer), pb.RegisterNodeStatsServer, logger)

	router := NewRouter(nh, nodeConfig.nodeID, nodeConfig.groupID, membersMap, nodeConfig.ctrlAddress, statsServer)
	startGRPCServer(fmt.Sprintf("localhost:%d", 51000+nodeConfig.nodeID), pb.NodeRouterServer(router), pb.RegisterNodeRouterServer, logger)
}

func startGRPCServer[T any](address string, server T, registerFunc func(grpc.ServiceRegistrar, T), logger struct {
	Logf   func(format string, v ...interface{})
	Fatalf func(format string, v ...interface{})
},
) {
	grpcServer := grpc.NewServer()
	registerFunc(grpcServer, server)
	reflection.Register(grpcServer)

	go func() {
		lis, err := net.Listen("tcp", address)
		if err != nil {
			logger.Fatalf("Failed to listen on %s: %v", address, err)
		}
		logger.Logf("gRPC server listening on %s", address)

		if err := grpcServer.Serve(lis); err != nil {
			logger.Fatalf("Failed to serve: %v", err)
		}
	}()
}

func startHTTPServer(nh *dragonboat.NodeHost, nodeConfig *NodeConfig, logger struct {
	Logf   func(format string, v ...interface{})
	Fatalf func(format string, v ...interface{})
},
) {
	membersMap, err := schema.ParseMembers(nodeConfig.members)
	if err != nil {
		logger.Fatalf("Failed to parse members: %v", err)
	}
	router := NewRouter(nh, nodeConfig.nodeID, nodeConfig.groupID, membersMap, nodeConfig.ctrlAddress, nil)

	httpServer := &http.Server{
		Addr: fmt.Sprintf("localhost:%d", 52000+nodeConfig.nodeID),
	}

	http.HandleFunc("/transfer", router.HandleTransfer)
	http.HandleFunc("/read", router.HandleRead)
	http.HandleFunc("/write", router.HandleWrite)

	go func() {
		logger.Logf("Starting HTTP server on %s", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("HTTP server error: %v", err)
		}
	}()
}

func waitForShutdown(nh *dragonboat.NodeHost, logger struct {
	Logf   func(format string, v ...interface{})
	Fatalf func(format string, v ...interface{})
},
) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM)
	<-sig

	logger.Logf("Shutting down node...")
	nh.Stop()
	logger.Logf("Node stopped")
}
