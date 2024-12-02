package utils

import (
	"context"
	"log"

	"google.golang.org/grpc"
)

func LoggingInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	log.Printf("Received gRPC request - Method: %s, Request: %v", info.FullMethod, req)
	resp, err := handler(ctx, req)
	if err != nil {
		log.Printf("gRPC Error - Method: %s, Error: %v", info.FullMethod, err)
	}
	return resp, err
}
