package balancer

import (
	"adaptodb/pkg/schema"
	"testing"
)

// Test BalanceStringKeyRangesByMidpoint
func TestBalanceStringKeyRangesByMidpoint(t *testing.T) {
	tests := []struct {
		name             string
		loads            []*NodeMetrics
		imbalancedShards []uint64
		keyRanges        map[uint64][]schema.KeyRange
		expected         map[uint64][]schema.KeyRange
	}{
		{
			name: "Simple redistribution",
			loads: []*NodeMetrics{
				{ShardID: 1, NumEntries: 200},
				{ShardID: 2, NumEntries: 50},
			},
			imbalancedShards: []uint64{1, 2},
			keyRanges: map[uint64][]schema.KeyRange{
				1: {{Start: "a", End: "m"}},
				2: {{Start: "m", End: "z"}},
			},
			expected: map[uint64][]schema.KeyRange{
				1: {{Start: "g", End: "m"}},
				2: {{Start: "m", End: "z"}, {Start: "a", End: "g"}},
			},
		},
		{
			name: "No redistribution needed",
			loads: []*NodeMetrics{
				{ShardID: 1, NumEntries: 50},
				{ShardID: 2, NumEntries: 50},
			},
			imbalancedShards: []uint64{},
			keyRanges: map[uint64][]schema.KeyRange{
				1: {{Start: "a", End: "m"}},
				2: {{Start: "m", End: "z"}},
			},
			expected: map[uint64][]schema.KeyRange{
				1: {{Start: "a", End: "m"}},
				2: {{Start: "m", End: "z"}},
			},
		},
		{
			name: "Edge case with single shard",
			loads: []*NodeMetrics{
				{ShardID: 1, NumEntries: 200},
			},
			imbalancedShards: []uint64{1},
			keyRanges: map[uint64][]schema.KeyRange{
				1: {{Start: "a", End: "z"}},
			},
			expected: map[uint64][]schema.KeyRange{
				1: {{Start: "a", End: "z"}},
			},
		},
		{
			name: "Redistribution after redistribution",
			loads: []*NodeMetrics{
				{ShardID: 1, NumEntries: 200},
				{ShardID: 2, NumEntries: 50},
			},
			imbalancedShards: []uint64{1, 2},
			keyRanges: map[uint64][]schema.KeyRange{
				1: {{Start: "g", End: "m"}},
				2: {{Start: "m", End: "z"}, {Start: "a", End: "g"}},
			},
			expected: map[uint64][]schema.KeyRange{
				1: {{Start: "j", End: "m"}},
				2: {{Start: "m", End: "z"}, {Start: "a", End: "g"}, {Start: "g", End: "j"}},
			},
		},
		{
			name: "Redistribution after redistribution after redistribution",
			loads: []*NodeMetrics{
				{ShardID: 1, NumEntries: 200},
				{ShardID: 2, NumEntries: 50},
			},
			imbalancedShards: []uint64{1, 2},
			keyRanges: map[uint64][]schema.KeyRange{
				1: {{Start: "j", End: "m"}},
				2: {{Start: "m", End: "z"}, {Start: "a", End: "g"}, {Start: "g", End: "j"}},
			},
			expected: map[uint64][]schema.KeyRange{
				1: {{Start: "k", End: "m"}},
				2: {{Start: "m", End: "z"}, {Start: "a", End: "g"}, {Start: "g", End: "j"}, {Start: "j", End: "k"}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := BalanceStringKeyRangesByMidpoint(test.loads, test.imbalancedShards, test.keyRanges)

			// Validate schedules
			for _, schedule := range result {
				if !compareKeyRanges(schedule.KeyRanges, test.expected[schedule.ShardID]) {
					t.Errorf("Failed %s: Expected key ranges for shard %d: %v, got: %v",
						test.name, schedule.ShardID, test.expected[schedule.ShardID], schedule.KeyRanges)
				}
			}
		})
	}
}

// Test findLexographicalMidpoint
func TestFindLexographicalMidpoint(t *testing.T) {
	tests := []struct {
		start    string
		end      string
		expected string
	}{
		{start: "a", end: "z", expected: "m"},
		{start: "a", end: "a", expected: "an"},
		// {start: "abc", end: "abd", expected: "abcn"},
		// {start: "prefix", end: "prefixz", expected: "prefixn"},
	}

	for _, test := range tests {
		t.Run(test.start+"_"+test.end, func(t *testing.T) {
			mid := findLexographicalMidpoint(test.start, test.end)
			if mid != test.expected {
				t.Errorf("Expected midpoint between %s and %s to be %s, got %s",
					test.start, test.end, test.expected, mid)
			}
		})
	}
}

// Helper: Compare key ranges
func compareKeyRanges(a, b []schema.KeyRange) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Start != b[i].Start || a[i].End != b[i].End {
			return false
		}
	}
	return true
}
