package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
)

func getShardForKey(key string) uint64 {
	// make a request to the metadata service to get the shard for the key
	// return the shard ID
	request := fmt.Sprintf("http://localhost:8080?key=%s", key)
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

func main() {
	reader := bufio.NewReader(os.Stdin) // Create a buffered reader
	fmt.Print("Enter command (e.g., 'r key' or 'w key value'):\n")
	for {
		input, err := reader.ReadString('\n') // Read the entire line
		if err != nil {
			fmt.Println("Error reading input. Please try again.")
			continue
		}

		input = strings.TrimSpace(input) // Trim newline and extra spaces
		parts := strings.Fields(input)   // Split the input by whitespace

		var op, key, value string
		switch len(parts) {
		case 2: // Expecting format "r key"
			op = parts[0]
			key = parts[1]
			fmt.Printf("Parsed operation: %s, key: %s\n", op, key)
		case 3: // Expecting format "w key value"
			op = parts[0]
			key = parts[1]
			value = parts[2]
			fmt.Printf("Parsed operation: %s, key: %s, value: %s\n", op, key, value)
		default:
			fmt.Println("Invalid input format. Use 'r key' or 'w key value'.")
		}

		shardID := getShardForKey(key)
		fmt.Printf("Key %s is in shard %d\n", key, shardID)
		// connect to the shard and perform the operation
	}
}
