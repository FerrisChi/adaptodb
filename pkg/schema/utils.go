package schema

import "sort"

// Helper function to consolidate overlapping key ranges
func ConsolidateKeyRanges(ranges []KeyRange) []KeyRange {
	if len(ranges) <= 1 {
		return ranges
	}

	// Sort ranges by start key
	sort.Slice(ranges, func(i, j int) bool {
		return ranges[i].Start < ranges[j].Start
	})

	result := make([]KeyRange, 0)
	current := ranges[0]

	for i := 1; i < len(ranges); i++ {
		if current.End >= ranges[i].Start {
			// Ranges overlap, merge them
			if ranges[i].End > current.End {
				current.End = ranges[i].End
			}
		} else {
			// No overlap, add current to result and start new current
			result = append(result, current)
			current = ranges[i]
		}
	}
	result = append(result, current)

	return result
}

// add ranges2 to ranges1
func AddKeyRanges(ranges1, ranges2 []KeyRange) []KeyRange {
	ranges := append(ranges1, ranges2...)
	return ConsolidateKeyRanges(ranges)
}

// remove ranges2 from ranges1
func RemoveKeyRanges(ranges1, ranges2 []KeyRange) []KeyRange {
	result := make([]KeyRange, 0)
	for _, r1 := range ranges1 {
		modified := []KeyRange{r1}
		for _, r2 := range ranges2 {
			temp := []KeyRange{}
			for _, m := range modified {
				if m.Start >= r2.End || m.End <= r2.Start {
					temp = append(temp, m)
				} else {
					if m.Start < r2.Start {
						temp = append(temp, KeyRange{Start: m.Start, End: r2.Start})
					}
					if m.End > r2.End {
						temp = append(temp, KeyRange{Start: r2.End, End: m.End})
					}
				}
			}
			modified = temp
		}
		result = append(result, modified...)
	}
	return ConsolidateKeyRanges(result)
}
