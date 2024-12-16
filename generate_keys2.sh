#!/bin/bash

# Number of random keys to generate per iteration
NUM_KEYS=20

# Random word generator
generate_random_word() {
  # Set LC_ALL to C to avoid illegal byte sequence errors
  local length=$((RANDOM % 6 + 5)) # Length between 5 and 10
  LC_ALL=C tr -dc 'a-z' < /dev/urandom | head -c $length
}

# Fetch shard configuration
get_shard_config() {
  grpcurl -plaintext -d '{}' localhost:60081 proto.ShardRouter/GetConfig
}

# Determine the shard for the given key, handling multiple key ranges
find_shard() {
  local key=$1
  local config=$2
  local shard_id=""

  # Parse the shard map and iterate through all key ranges
  echo "$config" | jq -c '.shardMap | to_entries | .[]' | while IFS= read -r entry; do
    shard=$(echo "$entry" | jq -r '.key')
    key_ranges=$(echo "$entry" | jq -c '.value.keyRanges[]')

    while IFS= read -r range; do
      start=$(echo "$range" | jq -r '.start')
      end=$(echo "$range" | jq -r '.end')
      
      # Check if the key falls within this range
      if [[ "$key" > "$start" && "$key" < "$end" ]]; then
        echo "$shard"
        return
      fi
    done <<< "$key_ranges"
  done
  
  echo "" # Return empty if no shard is found
}

# Inject key-value pairs into the appropriate shard member
inject_key() {
  local key=$1
  local value=$2
  local shard_id=$3
  local config=$4
  local addr=""
  local port=""

  # Find one member of the corresponding shard
  addr=$(echo "$config" | jq -r ".members[\"$shard_id\"].members[0].addr")

  # Set port based on shard_id
  case "$shard_id" in
    "1")
      port=51001
      ;;
    "2")
      port=51004
      ;;
    "3")
      port=51007
      ;;
    "4")
      port=51010
      ;;
    "5")
      port=51013
      ;;
    *)
      echo "Error: Invalid shard ID $shard_id"
      return
      ;;
  esac

  if [[ -n $addr ]]; then
    echo "Injecting key: $key, value: $value into shard $shard_id (member: $addr, port: $port)"
    grpcurl -plaintext -d "{\"clusterID\": $shard_id, \"key\": \"$key\", \"value\": \"$value\"}" "$addr:$port" proto.NodeRouter/Write
  else
    echo "Error: No member found for shard $shard_id"
  fi
}

# Main process
run_iterations() {
  # Repeat the key generation and injection 3 times
  for ((iteration=1; iteration<=3; iteration++)); do
    echo "Starting iteration $iteration..."

    # Generate and inject random keys
    for ((i=1; i<=NUM_KEYS; i++)); do
      key=$(generate_random_word)
      
      # Ensure key is non-empty
      if [[ -z "$key" ]]; then
        echo "Skipping empty key."
        continue
      fi

      value="value_$key"

      # Fetch the latest shard configuration dynamically
      echo "Fetching latest shard configuration..."
      SHARD_CONFIG=$(get_shard_config)

      if [[ -z "$SHARD_CONFIG" ]]; then
        echo "Error: Could not fetch shard configuration."
        continue
      fi

      # Determine the appropriate shard
      shard_id=$(find_shard "$key" "$SHARD_CONFIG")

      if [[ -n $shard_id ]]; then
        inject_key "$key" "$value" "$shard_id" "$SHARD_CONFIG"
      else
        echo "Error: No shard found for key $key"
      fi

      # Add a small delay to avoid overloading
      sleep 0.5
    done

    # Sleep for 60 seconds after each iteration
    if [[ $iteration -lt 3 ]]; then
      echo "Sleeping for 60 seconds before the next iteration..."
      sleep 60
    fi
  done

  echo "Key generation and injection completed!"
}

# Run the process
run_iterations
