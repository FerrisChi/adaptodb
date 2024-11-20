package balancer

import (
	"adaptodb/pkg/metadata"
	"adaptodb/pkg/schema"
	"log"
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

func (a *DefaultAnalyzer) AnalyzeLoads() ([]schema.ShardMetadata, bool) {

	// 1. collect metrics
	loads, err := a.collectMetrics()
	if err != nil {
		log.Printf("Failed to collect metrics: %v", err)
		return []schema.ShardMetadata{}, false
	}

	// 2. detect imbalances
	imbalancedShards, err := a.detectImbalanceShards(loads)
	if err != nil {
		log.Printf("Failed to detect imbalanced shards: %v", err)
		return []schema.ShardMetadata{}, false
	}
	if len(imbalancedShards) == 0 {
		return []schema.ShardMetadata{}, false
	}

	// 3. create new schedules
	newSchedules, err := a.createShardSchedules(imbalancedShards, loads, a.strategy)
	if err != nil {
		log.Printf("Failed to create new schedules: %v", err)
		return []schema.ShardMetadata{}, false
	}

	return newSchedules, len(newSchedules) > 0
}

func (a *DefaultAnalyzer) detectImbalanceShards(loads []*ShardMetrics) ([]uint64, error) {
	var imbalancedShards []uint64
	for _, load := range loads {
		if load.CPU > 80.0 || load.Memory > 1<<30 { // 1 GiB
			imbalancedShards = append(imbalancedShards, load.ShardID)
		}
	}
	return imbalancedShards, nil
}

func (a *DefaultAnalyzer) collectMetrics() ([]*ShardMetrics, error) {
	// Example: Dummy logic to collect shard metrics
	return []*ShardMetrics{}, nil
}

// Create new shard key range schedules for the imbalanced shards
func (a *DefaultAnalyzer) createShardSchedules(imbalancedShards []uint64, loads []*ShardMetrics, strategy string) ([]schema.ShardMetadata, error) {
	newSchedules := make([]schema.ShardMetadata, 0)
	// Example: Dummy logic to create new schedules
	for _, idx := range imbalancedShards {
		// create a new key range based on the strategy
		shardID := loads[idx].ShardID
		orin_range, err := a.metadata.GetShardKeyRange(shardID)
		if err != nil {
			log.Printf("Failed to get key range for shard %d: %v", shardID, err)
			continue
		}
		newSchedules = append(newSchedules, schema.ShardMetadata{
			ShardID:  shardID,
			KeyRange: orin_range,
		})
	}
	return newSchedules, nil
}
