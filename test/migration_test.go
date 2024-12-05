package main

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "adaptodb/pkg/proto/proto"
	"adaptodb/pkg/schema"
)

func generateTestData() []string {
	data := make([]string, 0, 26)
	for i := 'a'; i <= 'z'; i++ {
		for len := 1; len < 5; len++ {
			data = append(data, strings.Repeat(string(i), len))
		}
	}
	return data
}

func setupAdaptoDB(t *testing.T) (*exec.Cmd, pb.NodeRouterClient, pb.NodeRouterClient) {
	if err := os.RemoveAll("tmp"); err != nil {
		t.Fatalf("Failed to remove tmp directory: %v", err)
	}

	cmd := exec.Command("./kill-ports.sh")
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to kill AdaptoDB: %v", err)
	}

	cmd = exec.Command("./bin/release/adaptodb")
	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start AdaptoDB: %v", err)
	}

	t.Log("Waiting for AdaptoDB init")
	time.Sleep(schema.ADAPTODB_LAUNCH_TIMEOUT)

	conn, err := grpc.NewClient("127.0.0.1:51001", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to gRPC server: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	conn2, err := grpc.NewClient("127.0.0.1:51004", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to gRPC server: %v", err)
	}
	t.Cleanup(func() { conn2.Close() })

	return cmd, pb.NewNodeRouterClient(conn), pb.NewNodeRouterClient(conn2)
}

func TestDataMigration(t *testing.T) {

	data := generateTestData()
	cmd, client, client2 := setupAdaptoDB(t)
	defer func() {
		cmd.Process.Signal(syscall.SIGINT)
		cmd.Wait()
	}()

	t.Run("Initial Read Should Fail", func(t *testing.T) {
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
			_, err := c.Read(context.Background(), &pb.ReadRequest{ClusterID: clusterId, Key: key})
			if err == nil {
				t.Errorf("Expected error when reading key %s from shard %d", key, clusterId)
			}
		}
	})

	t.Run("Write Data", func(t *testing.T) {
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
			_, err := c.Write(context.Background(), &pb.WriteRequest{ClusterID: clusterId, Key: key, Value: key})
			if err != nil {
				t.Fatalf("Failed to write key %s to shard %d: %v", key, clusterId, err)
			}
		}
	})

	t.Run("Read After Write", func(t *testing.T) {
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
				t.Fatalf("Failed to read key %s from shard %d: %v", key, clusterId, err)
			}
		}
	})

	t.Run("Update Schedule", func(t *testing.T) {
		ctrlConn, err := grpc.NewClient("127.0.0.1:60082", grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("Failed to connect to controller gRPC server: %v", err)
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
			t.Fatalf("Failed to update schedule: %v", err)
		}

		time.Sleep(7 * time.Second)
	})

	t.Run("Read From Old Shard Should Fail", func(t *testing.T) {
		for _, key := range data {
			if key >= "h" && key < "m" {
				_, err := client.Read(context.Background(), &pb.ReadRequest{ClusterID: 1, Key: key})
				if err == nil {
					t.Errorf("Expected error when reading key %s from shard 1 after migration", key)
				}
			}
		}
	})

	t.Run("Read From New Shard", func(t *testing.T) {
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
				t.Fatalf("Failed to read key %s from shard %d: %v", key, clusterId, err)
			}
		}
	})

	t.Run("Restart Server", func(t *testing.T) {
		cmd.Process.Signal(syscall.SIGINT)
		cmd.Wait()

		kill := exec.Command("./kill-ports.sh")
		if err := kill.Run(); err != nil {
			t.Fatalf("Failed to kill AdaptoDB: %v", err)
		}

		newCmd := exec.Command("./bin/release/adaptodb")
		if err := newCmd.Start(); err != nil {
			t.Fatalf("Failed to restart AdaptoDB: %v", err)
		}
		defer func() {
			newCmd.Process.Signal(syscall.SIGINT)
			newCmd.Wait()
		}()

		t.Log("Waiting for AdaptoDB restart")
		time.Sleep(schema.ADAPTODB_LAUNCH_TIMEOUT)

		conn, err := grpc.NewClient("127.0.0.1:51001", grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("Failed to connect to gRPC server: %v", err)
		}
		defer conn.Close()

		conn2, err := grpc.NewClient("127.0.0.1:51004", grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("Failed to connect to gRPC server: %v", err)
		}
		defer conn2.Close()

		client := pb.NewNodeRouterClient(conn)
		client2 := pb.NewNodeRouterClient(conn2)

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
				t.Fatalf("Failed to read key %s from shard %d after restart: %v", key, clusterId, err)
			}
		}
	})
}
