package main

import (
	pb "adaptodb/pkg/proto/proto"
	"adaptodb/pkg/schema"
	"context"
	"fmt"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

func (r Router) cancelMigrationFromNode() {
	conn, err := grpc.NewClient(r.ctrlAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server: %v", err)
	}
	defer conn.Close()
	client := pb.NewControllerClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), schema.READ_WRITE_TIMEOUT)
	defer cancel()
	req := &pb.CancelMigrationRequest{TaskId: r.migTaskId}
	_, err = client.CancelMigrationFromNode(ctx, req)
	if err != nil {
		log.Fatalf("Failed to cancel migration: %v", err)
	}

	// Reset migration state
	r.migTaskId = 0
	r.migKeyRanges = nil
	r.migFlag = 0
	r.migCache = nil
}
