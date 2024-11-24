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

type ShardMetrics struct {
	ShardID uint64
	CPU     float64
	Memory  float64
	QPS     int64
}

type Analyzer interface {
	AnalyzeLoads() ([]schema.ShardMetadata, bool)
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
	ticker := time.NewTicker(30 * time.Second)
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

func (c *Balancer) sendShardUpdate(newSchedule []schema.ShardMetadata) {
	protoSchedule := make([]*pb.ShardMetadata, 0, len(newSchedule))
	for _, shard := range newSchedule {
		protoSchedule = append(protoSchedule, &pb.ShardMetadata{
			ShardId: shard.ShardID,
			KeyRange: &pb.KeyRange{
				Start: shard.KeyRange.Start,
				End:   shard.KeyRange.End,
			},
		})
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
