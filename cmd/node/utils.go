package main

import (
	pb "adaptodb/pkg/proto/proto"
	"fmt"
)

func formatPBKeyRanges(krs []*pb.KeyRange) string {
	var res string
	for _, kr := range krs {
		res += fmt.Sprintf("%s-%s", kr.GetStart(), kr.GetEnd())
		if kr != krs[len(krs)-1] {
			res += ","
		}
	}
	return res
}
