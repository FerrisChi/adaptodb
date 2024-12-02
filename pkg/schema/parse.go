package schema

import (
	pb "adaptodb/pkg/proto/proto"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// kr format: start1-end1,start2-end2,...
func ParseKeyRanges(kr interface{}) []KeyRange {
	var ranges []KeyRange
	var inputStr string

	// Handle different types of input
	switch v := kr.(type) {
	case string:
		inputStr = v
	case []byte:
		inputStr = string(v)
	case *pb.KeyRangeList:
		for _, pbKr := range v.KeyRanges {
			ranges = append(ranges, KeyRange{
				Start: pbKr.GetStart(),
				End:   pbKr.GetEnd(),
			})
		}
		return ranges
	default:
		return nil
	}

	// Split the input string by comma
	pairs := strings.Split(inputStr, ",")
	for _, pair := range pairs {
		parts := strings.Split(pair, "-")
		if len(parts) != 2 {
			continue // Skip invalid pairs
		}
		ranges = append(ranges, KeyRange{
			Start: parts[0],
			End:   parts[1],
		})
	}
	return ranges
}

// m format: id1=addr1,id2=addr2,...
func ParseMembers(m string) (map[uint64]string, error) {
	membersMap := make(map[uint64]string)
	members := strings.Split(m, ",")
	for _, member := range members {
		parts := strings.Split(member, "=")
		if len(parts) != 2 {
			return nil, errors.New("invalid member string")
		}
		nodeID, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse node ID: %w", err)
		}
		membersMap[nodeID] = parts[1]
	}
	return membersMap, nil
}

func KeyRangesToPbKeyRangeList(krs []KeyRange) *pb.KeyRangeList {
	var pbKrs pb.KeyRangeList
	for _, kr := range krs {
		pbKrs.KeyRanges = append(pbKrs.KeyRanges, &pb.KeyRange{
			Start: kr.Start,
			End:   kr.End,
		})
	}
	return &pbKrs
}
