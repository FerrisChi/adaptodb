package balancer

import (
	"adaptodb/pkg/metadata"
	pb "adaptodb/pkg/proto/proto"
	"adaptodb/pkg/schema"
	"adaptodb/pkg/utils"
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type DefaultAnalyzer struct {
	strategy string
	metadata *metadata.Metadata
}

func NewDefaultAnalyzer(strategy string, metadata *metadata.Metadata) Analyzer {
	return &DefaultAnalyzer{
		strategy: strategy,
		metadata: metadata,
	}
}

func (a *DefaultAnalyzer) AnalyzeLoads() ([]schema.Schedule, bool) {
	// 1. collect metrics
	loads, err := a.collectMetrics()
	if err != nil {
		log.Printf("Failed to collect metrics: %v", err)
		return []schema.Schedule{}, false
	}

	// 2. detect imbalances
	imbalancedShards, err := a.detectImbalanceShards(loads)
	if err != nil {
		log.Printf("Failed to detect imbalanced shards: %v", err)
		return []schema.Schedule{}, false
	}
	if len(imbalancedShards) == 0 {
		return []schema.Schedule{}, false
	}

	// 3. create new schedules
	newSchedules, err := a.createShardSchedules(imbalancedShards, loads, a.strategy)
	
	// 4. mark failed nodes
	failureInfo := schema.Schedule{}
	for _, load := range loads {
		if load.NumEntries < 0 {
			// mark the node as failed
			failureInfo.FailedNodes = append(failureInfo.FailedNodes, load.NodeID)
		}
	}
	if len(failureInfo.FailedNodes) > 0 {
		newSchedules = append(newSchedules, failureInfo)
		log.Println("Detected failed nodes: ", failureInfo.FailedNodes)
	}

	if err != nil {
		log.Printf("Failed to create new schedules: %v", err)
		return []schema.Schedule{}, false
	}

	return newSchedules, len(newSchedules) > 0
}

func (a *DefaultAnalyzer) detectImbalanceShards(loads []*NodeMetrics) ([]uint64, error) {
	var imbalancedShards []uint64
	avgEntries := 0.0
	for _, load := range loads {
		avgEntries += float64(load.NumEntries)
	}
	avgEntries /= float64(len(loads))

	// If a shard is responsible for more than twice the average number of entries, it is considered imbalanced
	for idx, load := range loads {
		if float64(load.NumEntries) > avgEntries*2 {
			imbalancedShards = append(imbalancedShards, uint64(idx))
		}
	}
	return imbalancedShards, nil
}

func queryNodeStats(nodeAddress string) (*NodeMetrics, error) {
	conn, err := grpc.NewClient(nodeAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server: %v", err)
		return nil, err
	}
	defer conn.Close()

	client := pb.NewNodeStatsClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.GetStats(ctx, &pb.GetStatsRequest{})
	if err != nil {
		log.Fatalf("Failed to get stats: %v", err)
		return nil, err
	}

	return &NodeMetrics{
		NumEntries:           resp.Entries,
		NumSuccessfulRequets: resp.SuccessfulRequests,
		NumFailedRequests:    resp.FailedRequests,
		LastResetTime:        resp.LastResetTime.AsTime(),
	}, nil
}

func (a *DefaultAnalyzer) collectMetrics() ([]*NodeMetrics, error) {
	logger := utils.NamedLogger("DefaultAnalyzer")
	// Use gRPC to collect metrics from each shard
	var ret []*NodeMetrics
	for _, shard := range a.metadata.Config.RaftGroups {
		// Try to query all members of the shard
		for _, member := range shard.Members {
			statsAddr := fmt.Sprintf("%s:%d", strings.Split(member.GrpcAddress, ":")[0], 53000+member.ID)
			logger.Logf("Querying node stats for %s", statsAddr)
			res, err := queryNodeStats(statsAddr)
			if err != nil {
				logger.Logf("Failed to query node stats: %v", err)
				ret = append(ret, &NodeMetrics{
					ShardID:              shard.ShardID,
					NodeID:               member.ID,
					NumEntries:           -1,
					NumSuccessfulRequets: -1,
					NumFailedRequests:    -1,
				})
			} else {
				logger.Logf("Node %d in Shard %d has %d entries:", member.ID, shard.ShardID, res.NumEntries)
				logger.Logf("Successful Requests: ", res.NumSuccessfulRequets)
				logger.Logf("Failed Requests: ", res.NumFailedRequests)
				logger.Logf("Last Reset Time: ", res.LastResetTime)
				res.ShardID = shard.ShardID
				res.NodeID = member.ID
				ret = append(ret, res)
			}
		}
	}

	return ret, nil
}

// Create new shard key range schedules for the imbalanced shards
func (a *DefaultAnalyzer) createShardSchedules(imbalancedShards []uint64, loads []*NodeMetrics, strategy string) ([]schema.Schedule, error) {
	newSchedules := make([]schema.Schedule, 0)
	// Example: Dummy logic to create new schedules
	for _, idx := range imbalancedShards {
		// create a new key range based on the strategy
		shardID := loads[idx].ShardID
		orig_ranges := a.metadata.GetShardKeyRanges(shardID)
		if orig_ranges == nil {
			continue
		}
		newSchedules = append(newSchedules, schema.Schedule{
			ShardID:   shardID,
			KeyRanges: orig_ranges,
		})
	}
	return newSchedules, nil
}
