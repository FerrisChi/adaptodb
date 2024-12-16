package balancer

import (
	pb "adaptodb/pkg/proto/proto"
	"adaptodb/pkg/schema"
	"adaptodb/pkg/utils"
	"context"
	"fmt"
	"time"

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
	AnalyzeLoads(algo ImbalanceAlgorithm, algoParam float64) ([]schema.Schedule, bool)
}

type Balancer struct {
	ctrlLocalAddress string
	stopCh           chan struct{}
	analyzer         Analyzer
}

func NewBalancer(address string, analyzer Analyzer) (*Balancer, error) {
	return &Balancer{
		ctrlLocalAddress: address,
		stopCh:           make(chan struct{}),
		analyzer:         analyzer,
	}, nil
}

func (b *Balancer) StartMonitoring(algo ImbalanceAlgorithm, algoParam float64) {
	logger := utils.NamedLogger("Balancer")

	logger.Logf("Starting to Balance with load balancing algorithm: %v and algorithm parmeter: %d", algo, algoParam)

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			if newSchedule, reschedule := b.analyzer.AnalyzeLoads(algo, algoParam); reschedule {
				b.sendShardUpdate(newSchedule)
			}
		}
	}
}

func (c *Balancer) sendShardUpdate(newSchedule []schema.Schedule) error {
	logger := utils.NamedLogger("Balancer")
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

	conn, err := grpc.NewClient(c.ctrlLocalAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	client := pb.NewControllerClient(conn)

	resp, err := client.UpdateSchedule(context.Background(), req)
	logger.Logf("Sending update schedule request: %v\n", req)
	if err != nil {
		return fmt.Errorf("error sending shard update: %v", err)
	}
	logger.Logf("Shard update response: %s", resp.Message)
	return nil
}

func (b *Balancer) Stop() {
	close(b.stopCh)
}
