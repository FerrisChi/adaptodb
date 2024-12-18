package balancer

import (
	"log"
	"math"
)

// This method detects shards whose load is significantly higher relative (precisely higher by "threshold" factor)
// to the least-loaded shard.
func DetectRelativeImbalance(loads []*NodeMetrics, threshold float64) []uint64 {
	var imbalancedShards []uint64

	if checkForSameNumberOfEntries(loads) {
		// Return empty list if same number of entries across all entries
		return imbalancedShards
	}

	// Find the least-loaded shard
	var leastLoaded int64 = math.MaxInt64
	for _, load := range loads {
		if load.NumEntries >= 0 && load.NumEntries < leastLoaded {
			leastLoaded = load.NumEntries
		}
	}

	// Identify shards exceeding the threshold
	for _, load := range loads {
		loadRatio := float64(load.NumEntries) / float64(leastLoaded)
		if leastLoaded == 0 {
			loadRatio = float64(load.NumEntries)
		}
		log.Printf("%f > threshold: %f", loadRatio, threshold)
		if load.NumEntries >= 0 && loadRatio > threshold {
			imbalancedShards = append(imbalancedShards, load.ShardID)
		}
	}

	return imbalancedShards
}

// This method uses percentiles to detect shards with loads in the top X percentile.
func DetectPercentileImbalance(loads []*NodeMetrics, percentage float64) []uint64 {
	var imbalancedShards []uint64
	shardEntryCounts := make(map[uint64]int64)

	if checkForSameNumberOfEntries(loads) == true {
		// Return empty list if same number of entries across all entries
		return imbalancedShards
	}

	// Collect the number of entries per shard
	for _, load := range loads {
		// Only consider one node per shard to calculate the total number of entries
		if _, exists := shardEntryCounts[load.ShardID]; !exists {
			shardEntryCounts[load.ShardID] = load.NumEntries
		}
	}

	// Calculate the total number of entries across all shards
	var totalEntries int64
	for _, count := range shardEntryCounts {
		totalEntries += count
	}

	// Calculate the threshold as a percentage of the total entries
	threshold := int64(float64(totalEntries) * (percentage / 100.0))

	// Identify shards exceeding the threshold
	for shardID, count := range shardEntryCounts {
		log.Printf("Shard %d: %d entries > threshold: %d (percentage: %.2f%%)", shardID, count, threshold, percentage)
		if count >= threshold {
			imbalancedShards = append(imbalancedShards, shardID)
		}
	}

	return imbalancedShards
}

// This method uses statistical thresholds like the mean and standard deviation to detect outliers.
func DetectStatisticalImbalance(loads []*NodeMetrics, factor float64) []uint64 {
	var imbalancedShards []uint64
	var entryCounts []float64

	if checkForSameNumberOfEntries(loads) == true {
		// Return empty list if same number of entries across all entries
		return imbalancedShards
	}

	// Collect entry counts
	for _, load := range loads {
		if load.NumEntries >= 0 {
			entryCounts = append(entryCounts, float64(load.NumEntries))
		}
	}

	// Calculate mean and standard deviation
	mean, stdDev := calculateMeanAndStdDev(entryCounts)

	log.Printf("mean: %f, stdDev: %f", mean, stdDev)

	// Identify shards exceeding the threshold
	for _, load := range loads {
		if load.NumEntries >= 0 {
			diff := float64(load.NumEntries) - mean
			log.Printf("Load: %d, Diff: %f > factor (%f) * stdDev (%f) = %f", load.NumEntries, diff, factor, stdDev, factor*stdDev)
			if diff > factor*stdDev {
				imbalancedShards = append(imbalancedShards, load.ShardID)
			}
		}
	}

	return imbalancedShards
}

// Helper function to calculate mean and standard deviation
func calculateMeanAndStdDev(data []float64) (mean, stdDev float64) {
	// Calc mean
	sum := 0.0
	for _, value := range data {
		sum += value
	}
	mean = sum / float64(len(data))

	// Calc stdDev
	sumSqDiff := 0.0
	for _, value := range data {
		sumSqDiff += (value - mean) * (value - mean)
	}
	stdDev = math.Sqrt(sumSqDiff / float64(len(data)))

	return mean, stdDev
}

// If same number of entries across the loads is true, then return the values
// Otherwise return false if entries are of different amount types
func checkForSameNumberOfEntries(loads []*NodeMetrics) bool {
	if len(loads) == 0 {
		return true // No entries to compare, so they are trivially the same
	}

	var firstEntryCount int64
	isFirst := true

	// Iterate through the loads
	for _, load := range loads {
		if load.NumEntries >= 0 {
			if isFirst {
				// Set the first entry count
				firstEntryCount = load.NumEntries
				isFirst = false
			} else if load.NumEntries != firstEntryCount {
				// If any entry doesn't match the first, return false
				return false
			}
		}
	}

	return true // All entry counts are the same
}

func RetrieveMostAndLeastLoadedNode(loads []*NodeMetrics) (*NodeMetrics, *NodeMetrics) {
	var leastLoadedNode, mostLoadedNode *NodeMetrics
	leastLoaded := int64(math.MaxInt64) // Maximum int64
	mostLoaded := int64(math.MinInt64)  // Maximum int64

	// Identify the least-loaded and most-loaded shards
	for _, load := range loads {
		if load.NumEntries >= 0 {
			if load.NumEntries < leastLoaded {
				leastLoaded = load.NumEntries
				leastLoadedNode = load
			}
			if load.NumEntries > mostLoaded {
				mostLoaded = load.NumEntries
				mostLoadedNode = load
			}
		}
	}

	return leastLoadedNode, mostLoadedNode
}
