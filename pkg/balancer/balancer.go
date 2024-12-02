package balancer

import (
	pb "adaptodb/pkg/proto/proto"
	"context"
	"log"
	"time"

	"adaptodb/pkg/schema"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// The Balancer will periodically query *all* nodes for their current
// NodeMetrics, analyze the loads, and send new schedules to the controller.
// Every time a node is queried and *successfully* responded, the node
// will reset its metrics, hence `LastResetTime`. Under normal circumstances,
// the `LastResetTime` of all nodes should be similar so that the stats
// are comparable. If not, it might indicate that some nodes failed to respond
// to the previous requests, so the Balancer might want to take that into
// account as well.
//
// Similarly, if any metric for a node is negative, it might indicate an error
// on the node. The Balancer could then assume the node is down and try to
// recover it. For now, we don't collect CPU and memory metrics because all
// nodes are running locally.
type NodeMetrics struct {
	ShardID              uint64
	NodeID               uint64
	NumEntries           int64
	NumSuccessfulRequets int64
	NumFailedRequests    int64
	LastResetTime        time.Time
	// CPU     float64
	// Memory  float64
	// QPS     int64
}

type Analyzer interface {
	AnalyzeLoads() ([]schema.Schedule, bool)
}

type Balancer struct {
	client   pb.ControllerClient
	stopCh   chan struct{}
	analyzer Analyzer
}

func NewBalancer(address string, analyzer Analyzer) (*Balancer, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	client := pb.NewControllerClient(conn)
	return &Balancer{
		client:   client,
		stopCh:   make(chan struct{}),
		analyzer: analyzer,
	}, nil
}

func (b *Balancer) StartMonitoring() {
	ticker := time.NewTicker(30000 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			if newSchedule, reschedule := b.analyzer.AnalyzeLoads(); reschedule {
				b.sendShardUpdate(newSchedule)
			}
		}
	}
}

func (c *Balancer) sendShardUpdate(newSchedule []schema.Schedule) {
	protoSchedule := make([]*pb.Schedule, 0, len(newSchedule))
	for _, shard := range newSchedule {
		schedule := &pb.Schedule{
			ShardId: shard.ShardID,
		}
		for _, keyRange := range shard.KeyRanges {
			schedule.KeyRanges = append(schedule.KeyRanges, &pb.KeyRange{
				Start: keyRange.Start,
				End:   keyRange.End,
			})
		}
		protoSchedule = append(protoSchedule, schedule)
	}

	req := &pb.UpdateScheduleRequest{Schedule: protoSchedule}

	go func() {
		resp, err := c.client.UpdateSchedule(context.Background(), req)
		if err != nil {
			log.Printf("Error sending shard update: %v", err)
			return
		}
		log.Printf("Shard update response: %s", resp.Message)
	}()
}

func (b *Balancer) Stop() {
	close(b.stopCh)
}
