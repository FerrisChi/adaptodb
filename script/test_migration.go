package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "adaptodb/pkg/proto/proto"
	"adaptodb/pkg/schema"
)

func test() error {

	data := make([]string, 0, 26)
	for i := 'a'; i <= 'z'; i++ {
		for len := 1; len < 100; len++ {
			// append string with length of len and same letter i
			data = append(data, strings.Repeat(string(i), len))
		}
	}

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
	conn2, err := grpc.NewClient("localhost:51004", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server: %v", err)
	}
	defer conn2.Close()
	client2 := pb.NewNodeRouterClient(conn2)

	fmt.Print("Test started\n")

	// Step 2: Read a key 'hello' from shard 1, expect an error
	for _, key := range data {
		var clusterId uint64
		var c pb.NodeRouterClient
		if key < "n" {
			clusterId = 1
			c = client
		} else {
			clusterId = 2
			c = client2
		}
		_, err = c.Read(context.Background(), &pb.ReadRequest{ClusterID: clusterId, Key: key})
		if err == nil {
			log.Fatalf("Expected error when reading key %s from shard %d", key, clusterId)
		}
	}
	fmt.Print("Step 2 passed\n")

	// Step 3: Write key value "hello", "hello-dragonboat" to shard 1
	for _, key := range data {
		var clusterId uint64
		var c pb.NodeRouterClient
		if key < "n" {
			clusterId = 1
			c = client
		} else {
			clusterId = 2
			c = client2
		}
		_, err = c.Write(context.Background(), &pb.WriteRequest{ClusterID: clusterId, Key: key, Value: key})
		if err != nil {
			log.Fatalf("Failed to write key %s to shard %d: %v", key, clusterId, err)
		}
	}
	fmt.Print("Step 3 passed\n")

	// Step 4: Read a key 'hello' from shard 1, expect the value 'hello-dragonboat'
	for _, key := range data {
		var clusterId uint64
		var c pb.NodeRouterClient
		if key < "n" {
			clusterId = 1
			c = client
		} else {
			clusterId = 2
			c = client2
		}
		resp, err := c.Read(context.Background(), &pb.ReadRequest{ClusterID: clusterId, Key: key})
		if err != nil || resp.GetData() != key {
			log.Fatalf("Failed to read key %s from shard %d: %v", key, clusterId, err)
		}
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

	// Step 6: Read after migration, expect error
	for _, key := range data {
		if key >= "h" && key < "m" {
			_, err = client.Read(context.Background(), &pb.ReadRequest{ClusterID: 1, Key: key})
			if err == nil {
				log.Fatalf("Expected error when reading key 'hello' from shard 1 after migration")
			}
		}
	}
	fmt.Print("Step 6 passed\n")

	// Step 7: Read after migration
	for _, key := range data {
		var clusterId uint64
		var c pb.NodeRouterClient
		if key < "h" || (key >= "m" && key < "n") {
			clusterId = 1
			c = client
		} else {
			clusterId = 2
			c = client2
		}
		resp, err := c.Read(context.Background(), &pb.ReadRequest{ClusterID: clusterId, Key: key})
		if err != nil || resp.GetData() != key {
			log.Fatalf("Failed to read key 'hello' from shard %d: %v", clusterId, err)
		}
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

	conn, err = grpc.NewClient("localhost:51001", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server: %v", err)
	}
	defer conn.Close()
	client = pb.NewNodeRouterClient(conn)
	conn2, err = grpc.NewClient("localhost:51004", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server for shard 2: %v", err)
	}
	defer conn2.Close()
	client2 = pb.NewNodeRouterClient(conn2)

	// Step 10: Read a key 'hello' from shard 2, expect the value 'hello-dragonboat'
	for _, key := range data {
		var clusterId uint64
		var c pb.NodeRouterClient
		if key < "h" || (key >= "m" && key < "n") {
			clusterId = 1
			c = client
		} else {
			clusterId = 2
			c = client2
		}
		resp, err := c.Read(context.Background(), &pb.ReadRequest{ClusterID: clusterId, Key: key})
		if err != nil || resp.GetData() != key {
			log.Fatalf("Failed to read key 'hello' from shard %d: %v", clusterId, err)
		}
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
