package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "adaptodb/pkg/proto/proto"
	"adaptodb/pkg/schema"
)

func test() error {
	// Remove the tmp directory if it exists
	if err := os.RemoveAll("tmp"); err != nil {
		log.Fatalf("Failed to remove tmp directory: %v", err)
	}
	cmd := exec.Command("./kill-ports.sh")
	if err := cmd.Run(); err != nil {
		log.Fatalf("Failed to kill AdaptoDB: %v", err)
	}

	// Start the AdaptoDB server
	cmd = exec.Command("./bin/release/adaptodb")
	if err := cmd.Start(); err != nil {
		log.Fatalf("Failed to start AdaptoDB: %v", err)
	}
	defer func() {
		cmd.Process.Signal(syscall.SIGINT)
		cmd.Wait()
	}()

	fmt.Print("Waiting for AdaptoDB init\n")

	time.Sleep(schema.ADAPTODB_LAUNCH_TIMEOUT) // Wait for the server to start

	// Connect to the gRPC server
	conn, err := grpc.NewClient("localhost:51001", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server: %v", err)
	}
	defer conn.Close()
	client := pb.NewNodeRouterClient(conn)

	fmt.Print("Test started\n")

	// Step 2: Read a key 'hello' from shard 1, expect an error
	_, err = client.Read(context.Background(), &pb.ReadRequest{ClusterID: 1, Key: "hello"})
	if err == nil {
		log.Fatalf("Expected error when reading key 'hello' from shard 1")
	}
	fmt.Print("Step 2 passed\n")

	// Step 3: Write key value "hello", "hello-dragonboat" to shard 1
	_, err = client.Write(context.Background(), &pb.WriteRequest{ClusterID: 1, Key: "hello", Value: "hello-dragonboat"})
	if err != nil {
		log.Fatalf("Failed to write key 'hello' to shard 1: %v", err)
	}
	_, err = client.Write(context.Background(), &pb.WriteRequest{ClusterID: 1, Key: "hi", Value: "hi-dragonboat"})
	if err != nil {
		log.Fatalf("Failed to write key 'hello' to shard 1: %v", err)
	}
	fmt.Print("Step 3 passed\n")

	// Step 4: Read a key 'hello' from shard 1, expect the value 'hello-dragonboat'
	resp, err := client.Read(context.Background(), &pb.ReadRequest{ClusterID: 1, Key: "hello"})
	if err != nil || resp.Value != "hello-dragonboat" {
		log.Fatalf("Failed to read key 'hello' from shard 1: %v", err)
	}
	resp, err = client.Read(context.Background(), &pb.ReadRequest{ClusterID: 1, Key: "hi"})
	if err != nil || resp.Value != "hi-dragonboat" {
		log.Fatalf("Failed to read key 'hi' from shard 1: %v", err)
	}
	fmt.Print("Step 4 passed\n")

	// Step 5: Send an UpdateSchedule() gRPC to controller
	ctrlConn, err := grpc.NewClient("localhost:60082", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to controller gRPC server: %v", err)
	}
	defer ctrlConn.Close()
	ctrlClient := pb.NewControllerClient(ctrlConn)

	_, err = ctrlClient.UpdateSchedule(context.Background(), &pb.UpdateScheduleRequest{
		Schedule: []*pb.Schedule{
			{
				ShardId: 2,
				KeyRanges: []*pb.KeyRange{
					{Start: "h", End: "m"},
				},
			},
		},
	})
	if err != nil {
		log.Fatalf("Failed to update schedule: %v", err)
	}

	time.Sleep(5 * time.Second) // Wait for migration to complete
	fmt.Print("Step 5 passed\n")

	// Step 6: Read a key 'hello' from shard 1, expect an error
	_, err = client.Read(context.Background(), &pb.ReadRequest{ClusterID: 1, Key: "hello"})
	if err == nil {
		log.Fatalf("Expected error when reading key 'hello' from shard 1 after migration")
	}
	_, err = client.Read(context.Background(), &pb.ReadRequest{ClusterID: 1, Key: "hi"})
	if err == nil {
		log.Fatalf("Expected error when reading key 'hi' from shard 1 after migration")
	}
	fmt.Print("Step 6 passed\n")

	// Step 7: Read a key 'hello' from shard 2, expect the value 'hello-dragonboat'
	conn2, err := grpc.NewClient("localhost:51004", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server for shard 2: %v", err)
	}
	defer conn2.Close()
	client2 := pb.NewNodeRouterClient(conn2)

	resp, err = client2.Read(context.Background(), &pb.ReadRequest{ClusterID: 2, Key: "hello"})
	if err != nil || resp.Value != "hello-dragonboat" {
		log.Fatalf("Failed to read key 'hello' from shard 2: %v", err)
	}
	resp, err = client2.Read(context.Background(), &pb.ReadRequest{ClusterID: 2, Key: "hi"})
	if err != nil || resp.Value != "hi-dragonboat" {
		log.Fatalf("Failed to read key 'hi' from shard 2: %v", err)
	}
	fmt.Print("Step 7 passed\n")

	// Step 8: Kill the ./bin/release/adaptodb by passing SIGINT
	cmd.Process.Signal(syscall.SIGINT)
	cmd.Wait()
	cmd = exec.Command("./kill-ports.sh")
	if err := cmd.Run(); err != nil {
		log.Fatalf("Failed to kill AdaptoDB: %v", err)
	}

	// Step 9: Restart the ./bin/release/adaptodb
	cmd = exec.Command("./bin/release/adaptodb")
	if err := cmd.Start(); err != nil {
		log.Fatalf("Failed to restart AdaptoDB: %v", err)
	}
	defer func() {
		cmd.Process.Signal(syscall.SIGINT)
		cmd.Wait()
	}()

	fmt.Print("Waiting for AdaptoDB restart\n")
	time.Sleep(schema.ADAPTODB_LAUNCH_TIMEOUT) // Wait for the server to start

	// Step 10: Read a key 'hello' from shard 2, expect the value 'hello-dragonboat'
	resp, err = client2.Read(context.Background(), &pb.ReadRequest{ClusterID: 2, Key: "hello"})
	if err != nil || resp.Value != "hello-dragonboat" {
		log.Fatalf("Failed to read key 'hello' from shard 2 after restart: %v", err)
	}
	resp, err = client2.Read(context.Background(), &pb.ReadRequest{ClusterID: 2, Key: "hi"})
	if err != nil || resp.Value != "hi-dragonboat" {
		log.Fatalf("Failed to read key 'hi' from shard 2 after restart: %v", err)
	}
	fmt.Print("Step 10 passed\n")

	fmt.Println("Data migration test completed successfully")
	return nil
}

func main() {
	if err := test(); err != nil {
		log.Fatalf("Test failed: %v", err)
	}
}