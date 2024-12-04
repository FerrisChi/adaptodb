// node/main.go
package main

import (
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

	"github.com/pkg/profile"

	pb "adaptodb/pkg/proto/proto"
	"adaptodb/pkg/schema"
	"adaptodb/pkg/sm"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	nodeID := flag.Uint64("id", 0, "NodeID to start")
	address := flag.String("address", "", "Node address (e.g. localhost:63001)")
	groupID := flag.Uint64("group-id", 0, "Raft group ID")
	dataDir := flag.String("data-dir", "", "Data directory path")
	walDir := flag.String("wal-dir", "", "WAL directory path")
	members := flag.String("members", "", "Comma-separated list of member addresses (format: id1=addr1,id2=addr2)")
	keyrange := flag.String("keyrange", "", "Key range managed by this node (format: start-end)")
	ctrlAddress := flag.String("ctrl-address", "", "Controller address (e.g. localhost:50001)")
	flag.Parse()

	// Profiling
	// Create profiling directory and files
	profDir := fmt.Sprintf("tmp/prof/%d", *nodeID)
	if err := os.MkdirAll(profDir, 0755); err != nil {
		log.Fatal(err)
	}

	defer profile.Start(profile.ProfilePath(profDir)).Stop()

	// p := profile.Start(profile.MutexProfile, profile.ProfilePath(profDir), profile.NoShutdownHook)
	// defer p.Stop()

	// cpuFile, err := os.OpenFile(
	// 	fmt.Sprintf("%s/cpu.prof", profDir),
	// 	os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
	// 	0644,
	// )
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// if err := pprof.StartCPUProfile(cpuFile); err != nil {
	// 	log.Fatal(err)
	// }

	// // Memory Profile
	// memFile, err := os.OpenFile(
	// 	fmt.Sprintf("%s/mem.prof", profDir),
	// 	os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
	// 	0644,
	// )
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// mutexFile, err := os.OpenFile(
	// 	fmt.Sprintf("%s/mutex.prof", profDir),
	// 	os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
	// 	0644,
	// )
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// // Enable mutex profiling
	// runtime.SetMutexProfileFraction(1) // 1 means profile all lock events


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
	membersMap, err := schema.ParseMembers(*members)
	if err != nil {
		log.Fatalf("failed to parse members: %v", err)
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
	router := NewRouter(nh, *nodeID, *groupID, membersMap, *ctrlAddress, statsServer)
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

	nodeHttpAddress := fmt.Sprintf("localhost:%d", 52000+*nodeID)

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
		log.Printf("Starting HTTP server on %s", nodeHttpAddress)
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
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
		log.Printf("HTTP server shutdown error: %v", err)
	}
	nh.Stop()
	nodeGrpcServer.Stop()
	statsGrpcServer.Stop()
	log.Println("Node stopped")

	// runtime.GC() // get up-to-date statistics
	// if err := pprof.WriteHeapProfile(memFile); err != nil {
	// 	log.Fatal(err)
	// }
	// memFile.Close()

	// pprof.StopCPUProfile()
	// cpuFile.Close()

	// // In shutdown sequence before exit:
	// if mp := pprof.Lookup("mutex"); mp != nil {
	// 	mp.WriteTo(mutexFile, 0)
	// }
	// mutexFile.Close()
}
