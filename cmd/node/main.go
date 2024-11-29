// node/main.go
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	pb "adaptodb/pkg/proto/proto"
	"adaptodb/pkg/schema"
	"adaptodb/pkg/sm"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)



func parseMembers(m string) (map[uint64]string, error) {
	membersMap := make(map[uint64]string)
	members := strings.Split(m, ",")
	for _, member := range members {
		parts := strings.Split(member, "=")
		if len(parts) != 2 {
			return nil, errors.New("invalid member string")
		}
		nodeID, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse node ID: %w", err)
		}
		membersMap[nodeID] = parts[1]
	}
	return membersMap, nil
}

// kr format: start1-end1,start2-end2,...
func parseKeyRange(kr string) []schema.KeyRange {
	var ranges []schema.KeyRange
	pairs := strings.Split(kr, ",")
	for _, pair := range pairs {
		parts := strings.Split(pair, "-")
		log.Printf("parts: %v %d", parts, len(parts))
		if len(parts) != 2 {
			continue
		}
		ranges = append(ranges, schema.KeyRange{
			Start: parts[0],
			End:   parts[1],
		})
	}
	return ranges
}

func main() {
	nodeID := flag.Uint64("id", 0, "NodeID to start")
	address := flag.String("address", "", "Node address (e.g. localhost:63001)")
	groupID := flag.Uint64("group-id", 0, "Raft group ID")
	dataDir := flag.String("data-dir", "", "Data directory path")
	walDir := flag.String("wal-dir", "", "WAL directory path")
	members := flag.String("members", "", "Comma-separated list of member addresses (format: id1=addr1,id2=addr2)")
	keyrange := flag.String("keyrange", "", "Key range managed by this node (format: start-end)")
	flag.Parse()

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
	logFile, err := os.OpenFile(fmt.Sprintf("tmp/log/%d.log", *nodeID), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("failed to open log file: %v", err)
	}
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))

	if runtime.GOOS == "darwin" {
		signal.Ignore(syscall.Signal(0xd))
	}

	nhc := config.NodeHostConfig{
		NodeHostDir:    *dataDir,
		WALDir:         *walDir,
		RTTMillisecond: 200,
		RaftAddress:    *address,
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
	membersMap, error := parseMembers(*members)
	if error != nil {
		log.Fatalf("failed to parse members: %v", error)
	}

	if err := nh.StartCluster(membersMap, false, sm.NewKVStore, rc); err != nil {
		log.Fatalf("failed to start node: %v", err)
	}

	log.Println("Started node with the following configuration:")
	log.Printf("NodeID: %d", *nodeID)
	log.Printf("Address: %s", *address)
	log.Printf("GroupID: %d", *groupID)
	log.Printf("DataDir: %s", *dataDir)
	log.Printf("WALDir: %s", *walDir)
	log.Printf("Members: %s", *members)
	log.Printf("KeyRange: %s", *keyrange)
	
	// Initialize NodeStatsServer
	statsServer := NewNodeStatsServer(nh, *groupID)

	// Start gRPC server for stats
	statsGrpcServer := grpc.NewServer()
	pb.RegisterNodeStatsServer(statsGrpcServer, statsServer)
	reflection.Register(statsGrpcServer)

	statsGrpcAddress := fmt.Sprintf("localhost:%d", 53000+*nodeID)
	lis, err := net.Listen("tcp", statsGrpcAddress)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", statsGrpcAddress, err)
	}
	log.Printf("Listening on %s", statsGrpcAddress)

	go func() {
		if err := statsGrpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// start grpc server for client communication
	keyRanges := parseKeyRange(*keyrange)
	log.Println("KeyRanges:", keyRanges)
	router := NewRouter(nh, *groupID, keyRanges, statsServer)
	nodeGrpcServer := grpc.NewServer()
	pb.RegisterNodeRouterServer(nodeGrpcServer, router)
	reflection.Register(nodeGrpcServer)

	nodeGrpcAddress := fmt.Sprintf("localhost:%d", 51000+*nodeID)
	lis, err = net.Listen("tcp", nodeGrpcAddress)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", nodeGrpcAddress, err)
	}
	log.Printf("Listening on %s", nodeGrpcAddress)

	go func() {
		if err := nodeGrpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// Handle shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM)
	<-sig
	nh.Stop()
	nodeGrpcServer.Stop()
	statsGrpcServer.Stop()
	log.Println("Node stopped")
}
