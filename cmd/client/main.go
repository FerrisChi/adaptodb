package main

import (
	pb "adaptodb/pkg/proto/proto"
	"adaptodb/pkg/schema"
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Shard struct {
	keyRanges   []schema.KeyRange
	memberIds   []uint64
	memberAddrs []string
}

var shards map[uint64]Shard

func getShardForKey(key string) uint64 {
	// scan the shard mapping to find the shard for the key
	for shardId, shard := range getShardMapping() {
		for _, keyRange := range shard.keyRanges {
			if key >= keyRange.Start && key < keyRange.End {
				return shardId
			}
		}
	}

	// make a request to the metadata service to get the shard for the key
	// return the shard ID
	request := fmt.Sprintf("http://127.0.0.1:60080?key=%s", key)
	resp, err := http.Get(request)
	if err != nil {
		log.Fatalf("Failed to get shard for key %s: %v", key, err)
	}
	// return the shard ID
	defer resp.Body.Close()
	var result map[string]uint64
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.Fatalf("Failed to decode response: %v", err)
	}
	return result["shard"]
}

func getShardMapping() map[uint64]Shard {
	// make a grpc request to the metadata service to get the shard mapping
	// return the mapping: shard ID -> list of key ranges

	conn, err := grpc.NewClient("127.0.0.1:60081", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server: %v", err)
	}
	defer conn.Close()

	client := pb.NewShardRouterClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), schema.READ_WRITE_TIMEOUT)
	defer cancel()

	resp, err := client.GetConfig(ctx, &pb.GetConfigRequest{})
	if err != nil {
		log.Fatalf("Failed to get shard mapping: %v", err)
	}
	// convert the response to []schema.KeyRange
	mapping := make(map[uint64][]schema.KeyRange)
	for shardId, keyRangeList := range resp.GetShardMap() {
		for _, keyRange := range keyRangeList.KeyRanges {
			mapping[shardId] = append(mapping[shardId], schema.KeyRange{
				Start: keyRange.Start,
				End:   keyRange.End,
			})
		}
	}
	shards := make(map[uint64]Shard)
	for shardId, members := range resp.GetMembers() {
		shard := Shard{
			keyRanges: mapping[shardId],
		}
		for _, member := range members.GetMembers() {
			shard.memberIds = append(shard.memberIds, member.GetId())
			shard.memberAddrs = append(shard.memberAddrs, member.GetAddr())
		}
		shards[shardId] = shard
	}

	return shards
}

// sync read
func read(key string) (string, error) {
	shardId := getShardForKey(key)
	idx := rand.Intn(len(shards[shardId].memberIds))
	// connect to a node with grpc and get the value
	conn, err := grpc.NewClient(shards[shardId].memberAddrs[idx], grpc.WithTransportCredentials(insecure.NewCredentials()))
	for err != nil {
		idx = (idx + 1) % len(shards[shardId].memberIds)
		conn, err = grpc.NewClient(shards[shardId].memberAddrs[idx], grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), schema.READ_WRITE_TIMEOUT)
	defer cancel()
	client := pb.NewNodeRouterClient(conn)
	resp, err := client.Read(ctx, &pb.ReadRequest{ClusterID: shardId, Key: key})
	if err != nil {
		return "", err
	}

	return string(resp.GetData()), nil
}

// sync write
func write(key, value string) (uint64, error) {
	shardId := getShardForKey(key)
	// connect to a dragonboat node in the shard
	idx := rand.Intn(len(shards[shardId].memberIds))
	conn, err := grpc.NewClient(shards[shardId].memberAddrs[idx], grpc.WithTransportCredentials(insecure.NewCredentials()))
	for err != nil {
		idx = (idx + 1) % len(shards[shardId].memberIds)
		conn, err = grpc.NewClient(shards[shardId].memberAddrs[idx], grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	client := pb.NewNodeRouterClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), schema.READ_WRITE_TIMEOUT)
	defer cancel()
	resp, err := client.Write(ctx, &pb.WriteRequest{ClusterID: shardId, Key: key, Value: value})
	if err != nil {
		log.Printf("Failed to connect to gRPC server: %v", err)
	}
	return resp.GetStatus(), nil
}

func process(parts []string) error {
	var op, key, value string
	if len(parts) == 2 && parts[0] == "f" { // find the shard id for key
		key = parts[1]
		shardId := getShardForKey(key)
		fmt.Printf("Key %s is in shard %d\n", key, shardId)
	} else if parts[0] == "r" || parts[0] == "w" { // Expecting format "w key value"
		op = parts[0]
		key = parts[1]
		if op == "w" {
			if len(parts) != 3 {
				return fmt.Errorf("FORMAT_ERROR")
			}
			value = parts[2]
		}
		// connect to a dragonboat node in the shard
		if op == "r" {
			if len(parts) != 2 {
				return fmt.Errorf("FORMAT_ERROR")
			}
			value, err := read(key)
			if err != nil {
				return fmt.Errorf("READ_FAILED %v", err)
			}
			fmt.Println("Read result: ", value)
		} else {
			status, err := write(key, value)
			if err != nil {
				return fmt.Errorf("WRITE_FAILED %v", err)
			} else {
				fmt.Println("Write successful (status code: ", status, ").")
			}
		}
	} else {
		return fmt.Errorf("FORMAT_ERROR")
	}
	return nil
}

func main() {
	// get the shard mapping
	shards = getShardMapping()
	fmt.Println(shards)

	reader := bufio.NewReader(os.Stdin) // Create a buffered reader
	fmt.Print("Enter command (e.g., 'r key' or 'w key value'):\n")
	for {
		fmt.Print("> ")
		input, err := reader.ReadString('\n') // Read the entire line
		if err != nil {
			fmt.Println("Error reading input. Please try again.")
			continue
		}

		input = strings.TrimSpace(input) // Trim newline and extra spaces
		parts := strings.Fields(input)   // Split the input by whitespace

		err = process(parts)
		if err != nil {
			if strings.HasPrefix(err.Error(), "FORMAT_ERROR") {
				fmt.Println("Invalid command format. Please use 'r key' or 'w key value'")
			} else if strings.HasPrefix(err.Error(), "READ_FAILED") {
				fmt.Println("Failed to read key:", strings.Split(err.Error(), "READ_FAILED")[1])
			} else {
				fmt.Println("Error:", err)
			}
		}
	}
}
