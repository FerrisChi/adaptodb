package main

import (
	"context"
	"sync"
	"time"

	pb "adaptodb/pkg/proto/proto"
	"adaptodb/pkg/sm"

	"github.com/lni/dragonboat/v3"
	"google.golang.org/protobuf/types/known/timestamppb"
)


type NodeStatsServer struct {
	pb.UnimplementedNodeStatsServer
	mu sync.Mutex
	entries int64
	successfulRequests int64
	failedRequests int64
	nh *dragonboat.NodeHost	
	clusterID uint64
	lastResetTime time.Time
}

func NewNodeStatsServer(nh *dragonboat.NodeHost, clusterID uint64) *NodeStatsServer {
	return &NodeStatsServer{
		nh: nh,
		clusterID: clusterID,
		lastResetTime: time.Now(),
	}
}

// This method is called by the Balancer via gRPC to report the stats
// of the current node. `successfulRequests` and `failedRequests` will
// be reset after this call so each call to GetStats will report the
// number of successful and failed requests since the last call - the
// Balancer has the responsibility and the freedom to query this data
// at regular intervals.
func (s *NodeStatsServer) GetStats(ctx context.Context, req *pb.GetStatsRequest) (*pb.GetStatsResponse, error) {
	// It might return a stale entry count, but it is good enough
	// for metrics reporting purposes. We query the entry count
	// using `nh` because we don't have access to the state machine
	// instance itself.
	res, err := s.nh.StaleRead(s.clusterID, "a")
	var numEntries int64
	if err == nil || err.Error() == "key not found" {
		numEntries = res.(sm.LookupResult).NumEntries
	} else {
		numEntries = -1  // should not happen!
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	ret := &pb.GetStatsResponse{
		Entries: numEntries,
		SuccessfulRequests: s.successfulRequests,
		FailedRequests: s.failedRequests,
		LastResetTime: timestamppb.New(s.lastResetTime),
	}
	s.successfulRequests = 0
	s.failedRequests = 0
	s.lastResetTime = time.Now()
	return ret, nil
}

// func (s *NodeStatsServer) IncrementEntries() {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()
// 	s.entries++
// }

// func (s *NodeStatsServer) DecrementEntries() {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()
// 	s.entries--
// }

// func (s *NodeStatsServer) SetEntries(entries int64) {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()
// 	s.entries = entries
// }

func (s *NodeStatsServer) GetEntries() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entries
}

func (s *NodeStatsServer) IncrementSuccessfulRequests() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.successfulRequests++
}

func (s *NodeStatsServer) IncrementFailedRequests() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failedRequests++
}

func (s *NodeStatsServer) GetSuccessfulRequests() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.successfulRequests
}

func (s *NodeStatsServer) GetFailedRequests() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failedRequests
}
