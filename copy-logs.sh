#!/bin/bash

# Fetch all container IDs
container_ids=(
7f9f8bfcddfa
c04f64742a90
1a0ef2a15f47
ffaba64235a2
d170e35ef64b
aca7238d8f93
fdee89855d53
)

# Destination directory on the host
destination_dir="container-logs"

# Ensure the destination directory exists
mkdir -p "$destination_dir"

# Iterate over each container ID and copy the /tmp/log file to the host
for container_id in "${container_ids[@]}"; do
    echo "Copying /tmp/log from container $container_id to $destination_dir/$container_id.log"
    docker cp "$container_id:/app/tmp/log" "$destination_dir/$container_id"
    if [ $? -eq 0 ]; then
        echo "Successfully copied /tmp/log from container $container_id"
    else
        echo "Failed to copy /tmp/log from container $container_id"
    fi
done
