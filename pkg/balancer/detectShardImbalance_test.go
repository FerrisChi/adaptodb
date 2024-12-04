package balancer

import "testing"

// Test RetrieveMostAndLeastLoadedNode
func TestRetrieveMostAndLeastLoadedNode(t *testing.T) {
	tests := []struct {
		name          string
		loads         []*NodeMetrics
		expectedLeast *NodeMetrics
		expectedMost  *NodeMetrics
	}{
		{
			name: "Normal case with mixed loads",
			loads: []*NodeMetrics{
				{ShardID: 1, NumEntries: 100},
				{ShardID: 2, NumEntries: 50},
				{ShardID: 3, NumEntries: 200},
				{ShardID: 4, NumEntries: 30},
			},
			expectedLeast: &NodeMetrics{ShardID: 4, NumEntries: 30},
			expectedMost:  &NodeMetrics{ShardID: 3, NumEntries: 200},
		},
		{
			name: "All nodes with equal loads",
			loads: []*NodeMetrics{
				{ShardID: 1, NumEntries: 100},
				{ShardID: 2, NumEntries: 100},
				{ShardID: 3, NumEntries: 100},
			},
			expectedLeast: &NodeMetrics{ShardID: 1, NumEntries: 100}, // Any node can be chosen
			expectedMost:  &NodeMetrics{ShardID: 1, NumEntries: 100}, // Any node can be chosen
		},
		{
			name:          "Empty input",
			loads:         []*NodeMetrics{},
			expectedLeast: nil,
			expectedMost:  nil,
		},
		{
			name: "Negative load values (should ignore negatives)",
			loads: []*NodeMetrics{
				{ShardID: 1, NumEntries: -10},
				{ShardID: 2, NumEntries: 0},
				{ShardID: 3, NumEntries: 200},
			},
			expectedLeast: &NodeMetrics{ShardID: 2, NumEntries: 0},
			expectedMost:  &NodeMetrics{ShardID: 3, NumEntries: 200},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			leastLoaded, mostLoaded := RetrieveMostAndLeastLoadedNode(test.loads)

			// Check least loaded node
			if !compareNodes(leastLoaded, test.expectedLeast) {
				t.Errorf("Test %s failed for least loaded node. Expected %+v, got %+v", test.name, test.expectedLeast, leastLoaded)
			}

			// Check most loaded node
			if !compareNodes(mostLoaded, test.expectedMost) {
				t.Errorf("Test %s failed for most loaded node. Expected %+v, got %+v", test.name, test.expectedMost, mostLoaded)
			}
		})
	}
}

// Test DetectRelativeImbalance
// func TestDetectRelativeImbalance(t *testing.T) {
// 	loads := []*NodeMetrics{
// 		{ShardID: 1, NumEntries: 100},
// 		{ShardID: 2, NumEntries: 50},
// 		{ShardID: 3, NumEntries: 200},
// 		{ShardID: 4, NumEntries: 30},
// 	}
//
// 	threshold := 2.0
// 	expected := []uint64{3} // Shard 3 has > 2x the entries of Shard 4 (least loaded)
// 	result := DetectRelativeImbalance(loads, threshold)
//
// 	if !equalSlices(result, expected) {
// 		t.Errorf("DetectRelativeImbalance failed. Expected %v, got %v", expected, result)
// 	}
// }

// Test DetectPercentileImbalance
// func TestDetectPercentileImbalance(t *testing.T) {
// 	loads := []*NodeMetrics{
// 		{ShardID: 1, NumEntries: 100},
// 		{ShardID: 2, NumEntries: 50},
// 		{ShardID: 3, NumEntries: 200},
// 		{ShardID: 4, NumEntries: 30},
// 	}
//
// 	percentile := 75.0
// 	expected := []uint64{1, 3} // Shards 1 and 3 are in the top 25%
// 	result := DetectPercentileImbalance(loads, percentile)
//
// 	if !equalSlices(result, expected) {
// 		t.Errorf("DetectPercentileImbalance failed. Expected %v, got %v", expected, result)
// 	}
// }
//
// // Test DetectStatisticalImbalance
// func TestDetectStatisticalImbalance(t *testing.T) {
// 	loads := []*NodeMetrics{
// 		{ShardID: 1, NumEntries: 100},
// 		{ShardID: 2, NumEntries: 50},
// 		{ShardID: 3, NumEntries: 200},
// 		{ShardID: 4, NumEntries: 30},
// 	}
//
// 	factor := 2.0
// 	expected := []uint64{3} // Shard 3 is > 2 standard deviations above hte mean
// 	result := DetectStatisticalImbalance(loads, factor)
//
// 	if !equalSlices(result, expected) {
// 		t.Errorf("DetectStatisticalImbalance failed. Expected %v, got %v", expected, result)
// 	}
// }

// Helper function to compare two NodeMetrics objects
func compareNodes(a, b *NodeMetrics) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.ShardID == b.ShardID && a.NumEntries == b.NumEntries
}

// Helper function to compare 2 slices of uint64
func equalSlices(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
