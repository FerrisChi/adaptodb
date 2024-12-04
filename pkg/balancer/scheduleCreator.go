package balancer

import (
	"adaptodb/pkg/schema"
	"fmt"
	"log"
	"math"
	"sort"
)

func BalanceStringKeyRangesByMidpoint(
	loads []*NodeMetrics,
	imbalancedShards []uint64,
	keyRanges map[uint64][]schema.KeyRange,
) []schema.Schedule {
	var schedules []schema.Schedule
	var keyRangesToAppend []schema.KeyRange

	// Find the most loaded shard
	var mostLoadedShard *NodeMetrics
	mostLoadedEntries := int64(math.MinInt64)
	for _, load := range loads {
		if contains(imbalancedShards, load.ShardID) && load.NumEntries > mostLoadedEntries {
			mostLoadedEntries = load.NumEntries
			mostLoadedShard = load
		}
	}

	// If no valid most loaded shard is found, return
	if mostLoadedShard == nil {
		return nil
	}

	// Sort the imbalanced shards by their load (ascending)
	shardIds := extractShardIDs(loads)
	sortedShards := sortByLoad(loads, shardIds)
	leastLoadedShard := sortedShards[0] // The least loaded shard

	if mostLoadedShard.ShardID == leastLoadedShard.ShardID {
		log.Printf("Most loaded shard %d and least loaded shard %d have the same number of entries", mostLoadedShard.ShardID, leastLoadedShard.ShardID)
		return nil
	}

	// Split all key ranges for the mostLoadedShard and redistribute to leastLoadedShard
	mostLoadedRanges := keyRanges[mostLoadedShard.ShardID]

	for i := range mostLoadedRanges {
		// Find a lexographical midpoint
		mid := findLexographicalMidpoint(mostLoadedRanges[i].Start, mostLoadedRanges[i].End)

		// Assign upper half to another shard
		if len(sortedShards) > 1 {
			// keyRanges[leastLoadedShard.ShardID] = append(keyRanges[leastLoadedShard.ShardID], schema.KeyRange{
			// 	Start: mostLoadedRanges[i].Start,
			// 	End:   mid,
			// })

			keyRangesToAppend = append(keyRangesToAppend, schema.KeyRange{
				Start: mostLoadedRanges[i].Start,
				End:   mid,
			})

			// Adjust the remaining range for the most loaded shard
			mostLoadedRanges[i].Start = mid
		}
	}

	schedules = append(schedules, schema.Schedule{
		ShardID:   leastLoadedShard.ShardID,
		KeyRanges: keyRangesToAppend,
	})

	return schedules
}

// computeMidpointKey computes the midpoint key of a given key range by appending
// the middle character of the specified character range to the start key.
//
// Parameters:
//   - charStart (rune): The starting character of the character range.
//   - charEnd   (rune): The ending character of the character range.
//   - startKey  (string): The start key of the key range.
//   - endKey    (string): The end key of the key range.
//
// Returns:
//   - (string): The midpoint key.
//   - (error): An error if the character range is invalid.
//
// Example:
//
//	midKey, err := computeMidpointKey('a', 'z', "a", "a")
//	// midKey will be "am"
func computeMidpointKey(charStart, charEnd rune, startKey, endKey string) (string, error) {
	// Ensure the character range is valid
	if charStart > charEnd {
		return "", fmt.Errorf("invalid character range: [%c, %c]", charStart, charEnd)
	}

	// Compute the middle character of the character range
	midCharCode := (int(charStart) + int(charEnd)) / 2
	midChar := rune(midCharCode)

	// Ensure midChar is within the character range
	if midChar < charStart || midChar > charEnd {
		return "", fmt.Errorf("middle character %c is out of the character range [%c, %c]", midChar, charStart, charEnd)
	}

	midCharStr := string(midChar)

	// Append the middle character to the start key to create the midpoint key
	midKey := startKey + midCharStr

	return midKey, nil
}

// Helper: Find the lexographical midpoint b/w 2 strings
func findLexographicalMidpoint(start, end string) string {
	if start == end {
		// Handle edge case where start == end
		return start + "n"
	}

	// Find the common prefix
	commonPrefix := ""
	i := 0
	for i < len(start) && i < len(end) && start[i] == end[i] {
		commonPrefix += string(start[i])
		i++
	}

	// If there are differing characters
	if i < len(start) && i < len(end) {
		// Find a midpoint character strictly between start[i] and end[i]
		midChar := string((start[i] + end[i]) / 2)
		return commonPrefix + midChar
	}

	// Handle edge case where one string is a prefix of the other
	if i < len(start) {
		return commonPrefix + start[i:i+1] + "n"
	}
	if i < len(end) {
		return commonPrefix + end[i:i+1] + "n"
	}

	// If no characters remain, append "n" to the common prefix
	return commonPrefix + "n"
}

// Helper: Sort shards by load (ascending)
func sortByLoad(loads []*NodeMetrics, shardIDs []uint64) []*NodeMetrics {
	var sorted []*NodeMetrics
	for _, id := range shardIDs {
		for _, load := range loads {
			if load.ShardID == id {
				sorted = append(sorted, load)
			}
		}
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].NumEntries < sorted[j].NumEntries
	})
	return sorted
}

// Helper: Check if a slice contains a value
func contains(slice []uint64, value uint64) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}

func extractShardIDs(loads []*NodeMetrics) []uint64 {
	var shardIDs []uint64
	for _, load := range loads {
		shardIDs = append(shardIDs, load.ShardID)
	}
	return shardIDs
}
