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
	dlogger "github.com/lni/dragonboat/v3/logger"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	logger := utils.NamedLogger("Node Main")

	dlogger.GetLogger("dragonboat").SetLevel(dlogger.WARNING)
	dlogger.GetLogger("rsm").SetLevel(dlogger.WARNING)
	dlogger.GetLogger("raft").SetLevel(dlogger.WARNING)
	dlogger.GetLogger("config").SetLevel(dlogger.WARNING)
	dlogger.GetLogger("logdb").SetLevel(dlogger.WARNING)
	dlogger.GetLogger("transport").SetLevel(dlogger.WARNING)

	nodeID := flag.Uint64("id", 0, "NodeID to start")
	address := flag.String("address", "", "Node address (e.g. localhost)")
	name := flag.String("name", "", "Node host name in container (e.g. node-3)")
	groupID := flag.Uint64("group-id", 0, "Raft group ID")
	dataDir := flag.String("data-dir", "", "Data directory path")
	walDir := flag.String("wal-dir", "", "WAL directory path")
	members := flag.String("members", "", "Comma-separated list of member addresses (format: id1=addr1,id2=addr2)")
	keyrange := flag.String("keyrange", "", "Key range managed by this node (format: start-end)")
	ctrlAddress := flag.String("ctrl-address", "", "Controller address (e.g. 127.0.0.1:50001)")
	flag.Parse()

	fmt.Println(*nodeID, *address, *groupID, *dataDir, *walDir, *members, *keyrange, *ctrlAddress)

	// Validate required flags
	if *nodeID == 0 || *address == "" || *groupID == 0 {
		flag.Usage()
		os.Exit(1)
	}

	// Create log directory if it does not exist
	logDir := "tmp/log"
	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		if err := os.MkdirAll(logDir, 0755); err != nil {
			log.Fatalf("failed to create log directory: %v", err)
		}
	}
	initialized := false
	if _, err := os.Stat(fmt.Sprintf("tmp/log/%d.log", *nodeID)); err == nil {
		initialized = true
	}
	logFile, err := os.OpenFile(fmt.Sprintf("tmp/log/%d.log", *nodeID), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("failed to open log file: %v", err)
	}
	log.SetOutput(logFile)
	log.SetFlags(log.Ltime | log.Lshortfile)

	if runtime.GOOS == "darwin" {
		signal.Ignore(syscall.Signal(0xd))
	}

	nhc := config.NodeHostConfig{
		NodeHostDir:    *dataDir,
		WALDir:         *walDir,
		RTTMillisecond: 200,
		RaftAddress:    schema.GetDragronboatAddr(*name),
	}

	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		log.Fatalf("failed to create NodeHost: %v", err)
	}

	rc := config.Config{
		NodeID:             *nodeID,
		ClusterID:          *groupID,
		ElectionRTT:        10,
		HeartbeatRTT:       2,
		CheckQuorum:        true,
		SnapshotEntries:    100,
		CompactionOverhead: 10,
	}

	// Parse members string into map
	membersMap, err := schema.ParseMembers(*members)
	if err != nil {
		log.Fatalf("failed to parse members: %v", err)
	}

	if err := nh.StartCluster(membersMap, false, sm.NewKVStore, rc); err != nil {
		log.Fatalf("failed to start node: %v", err)
	}

	logger.Logf("Started node with the following configuration:")

	logger.Logf("NodeID: %d", *nodeID)
	logger.Logf("Address: %s", *address)
	logger.Logf("GroupID: %d", *groupID)
	logger.Logf("DataDir: %s", *dataDir)
	logger.Logf("WALDir: %s", *walDir)
	logger.Logf("Members: %s", *members)
	logger.Logf("KeyRange: %s", *keyrange)

	// If not initialized, wait for all nodes and propose initial key ranges
	if !initialized {
		log.Println("Waiting for raft stabilize...")
		time.Sleep(schema.RAFT_LAUNCH_TIMEOUT)
		leader, _, err := nh.GetLeaderID(*groupID)
		if err == nil && leader == *nodeID {
			keyRanges := schema.ParseKeyRanges(*keyrange)
			ctx, cancel := context.WithTimeout(context.Background(), schema.READ_WRITE_TIMEOUT)
			defer cancel()
			session := nh.GetNoOPSession(*groupID)
			cmd := fmt.Sprintf("apply_schedule:init,%s", schema.KeyRangeToString(keyRanges))
			_, err := nh.SyncPropose(ctx, session, []byte(cmd))
			if err != nil {
				log.Fatalf("Failed to propose initial key ranges. PLEASE DELETE TMP DIR. %v", err)
			} else {
				log.Println("Proposed initial key ranges: ", keyRanges)
			}
		}
	}

	// Initialize NodeStatsServer
	statsServer := NewNodeStatsServer(nh, *groupID)

	// Start gRPC server for stats
	statsGrpcServer := grpc.NewServer()
	pb.RegisterNodeStatsServer(statsGrpcServer, statsServer)
	reflection.Register(statsGrpcServer)

	statsGrpcAddress := fmt.Sprintf("0.0.0.0:%d", schema.NodeStatsPort)
	statesLis, err := net.Listen("tcp", statsGrpcAddress)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", statsGrpcAddress, err)
	}
	logger.Logf("Listening on %s", statsGrpcAddress)

	go func() {
		if err := statsGrpcServer.Serve(statesLis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// start grpc server for client communication
	router := NewRouter(nh, *nodeID, *groupID, membersMap, *ctrlAddress, statsServer)
	nodeGrpcServer := grpc.NewServer()
	pb.RegisterNodeRouterServer(nodeGrpcServer, router)
	reflection.Register(nodeGrpcServer)

	nodeGrpcAddress := fmt.Sprintf("0.0.0.0:%d", schema.NodeGrpcPort)
	nodeLis, err := net.Listen("tcp", nodeGrpcAddress)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", nodeGrpcAddress, err)
	}

	go func() {
		logger.Logf("Starting GRPC server listening on %s", nodeGrpcAddress)
		if err := nodeGrpcServer.Serve(nodeLis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	nodeHttpAddress := fmt.Sprintf("0.0.0.0:%d", schema.NodeHttpPort)

	// Create HTTP server for WebSocket connections
	httpServer := &http.Server{
		Addr: nodeHttpAddress,
	}

	// Setup WebSocket handler
	http.HandleFunc("/transfer", router.HandleTransfer)
	http.HandleFunc("/read", router.HandleRead)
	http.HandleFunc("/write", router.HandleWrite)

	// Start HTTP server in goroutine
	go func() {
		logger.Logf("Starting HTTP server on %s", nodeHttpAddress)
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			logger.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Handle shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM)
	<-sig

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Stop all servers
	nodeGrpcServer.GracefulStop()
	if err := httpServer.Shutdown(ctx); err != nil {
		logger.Logf("HTTP server shutdown error: %v", err)
	}
	nh.Stop()
	nodeGrpcServer.Stop()
	statsGrpcServer.Stop()
	logger.Logf("Node stopped")
}
