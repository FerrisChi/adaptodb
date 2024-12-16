package balancer

import (
	"adaptodb/pkg/metadata"
	pb "adaptodb/pkg/proto/proto"
	"adaptodb/pkg/schema"
	"adaptodb/pkg/utils"
	"context"
	"errors"
	"fmt"
	"log"
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

func (a *DefaultAnalyzer) AnalyzeLoads(algo ImbalanceAlgorithm, algoParam float64) ([]schema.Schedule, bool) {
	logger := utils.NamedLogger("DefaultAnalyzer")
	// 1. collect metrics
	loads, err := a.collectMetrics()
	if err != nil {
		logger.Logf("Failed to collect metrics: %v", err)
		return []schema.Schedule{}, false
	}

	// Logging metrics
	// for i, metric := range loads {
	// 	if metric == nil {
	// 		logger.Logf("Metric %d: nil", i)
	// 		continue
	// 	}
	// 	logger.Logf("Metric %d: %+v", i, *metric)
	// }

	// 2. detect imbalances
	// imbalancedShards := DetectRelativeImbalance(loads, 10)
	imbalancedShards := ChooseImbalanceDetections(loads, algo, algoParam)
	logger.Logf("Algorithm: %s, ImbalancedShards: %v", algo, imbalancedShards)
	if len(imbalancedShards) == 0 {
		return []schema.Schedule{}, false
	}

	// 3. create new schedules
	newSchedules, err := a.createShardSchedules(loads, imbalancedShards)
	if err != nil {
		log.Printf("Failed to create new schedules: %v", err)
		return []schema.Schedule{}, false
	}

	return newSchedules, len(newSchedules) > 0
	// return []schema.Schedule{}, false
}

func queryNodeStats(nodeAddress string) (*NodeMetrics, error) {
	conn, err := grpc.NewClient(nodeAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to connect to gRPC server: %v", err)
		return nil, err
	}
	defer conn.Close()

	client := pb.NewNodeStatsClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.GetStats(ctx, &pb.GetStatsRequest{})
	if err != nil {
		log.Printf("Failed to get stats: %v", err)
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
	raftGroup := a.metadata.GetConfig()
	for _, shard := range raftGroup {
		// Try to query all members of the shard
		for _, member := range shard.Members {
			statsAddr := fmt.Sprintf("%s:%d", member.Address, schema.NodeStatsPort+member.ID)
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
				// logger.Logf("Successful Requests: %d, Failed Requests, %d, Last Reset Time: %d", res.NumSuccessfulRequets, res.NumFailedRequests, res.LastResetTime)
				res.ShardID = shard.ShardID
				res.NodeID = member.ID
				ret = append(ret, res)
			}
		}
	}

	return ret, nil
}

// Create new shard key range schedules for the imbalanced shards
func (a *DefaultAnalyzer) createShardSchedules(loads []*NodeMetrics, imbalancedShards []uint64) ([]schema.Schedule, error) {
	logger := utils.NamedLogger("DefaultAnalyzer")
	allShardKeyRanges := make(map[uint64][]schema.KeyRange)

	for idx := range loads {
		// Get the shard ID
		shardID := loads[idx].ShardID

		// Retrieve original key ranges for the shard
		shardKeyRanges := a.metadata.GetShardKeyRanges(shardID)
		if shardKeyRanges == nil {
			continue
		}

		// Populate the map with key ranges
		allShardKeyRanges[shardID] = shardKeyRanges
	}
	logger.Logf("Current Key Ranges: %v", allShardKeyRanges)

	recommendedSchedules := BalanceStringKeyRangesByMidpoint(loads, imbalancedShards, allShardKeyRanges)

	if recommendedSchedules == nil {
		return nil, errors.New("No recommended schedules")
	}

	logger.Logf("Recommending schedules: %v", recommendedSchedules)

	return recommendedSchedules, nil
}
