package utils

import (
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RetryGRPCConnection attempts to establish a gRPC connection with retries.
// Parameters:
// - address: The target address for the gRPC server (e.g., "127.0.0.1:51001").
// - maxRetries: Maximum number of retries before giving up.
// - retryInterval: Time to wait between retries (e.g., 2 * time.Second).
// Returns:
// - *grpc.ClientConn: The established connection.
// - error: Any error encountered after exceeding retries.
func RetryGRPCConnection(address string, maxRetries int, retryInterval time.Duration) (*grpc.ClientConn, error) {
	var conn *grpc.ClientConn
	var err error

	for i := 0; i < maxRetries; i++ {
		log.Printf("Attempting to connect to gRPC server at %s (attempt %d/%d)", address, i+1, maxRetries)
		conn, err = grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			log.Printf("Successfully connected to gRPC server at %s", address)
			return conn, nil
		}
		log.Printf("Failed to connect to %s: %v", address, err)

		// Wait before retrying
		time.Sleep(retryInterval)
	}

	// If all retries fail, return the last encountered error
	log.Printf("Exceeded maximum retries (%d) for gRPC connection to %s", maxRetries, address)
	return nil, err
}
