# adaptodb
An Adaptive shard-balancing key-value database

## Build

0. Setup `host.docker.internal` for container's access to host if you're using OS other than MacOS.
1. If you are migrating from dragonboat v4 to v3, please delete the `tmp/` data generated from previous runs first: `rm -r tmp/`
2. Download go mods: `go mod tidy`
3. Make the project: `make`

    > If you run into an error like:
    > ```
    > protoc-gen-go-grpc: program not found or is not executable
    > Please specify a program using absolute path or make sure the program is available in > your PATH system variable
    > --go-grpc_out: protoc-gen-go-grpc: Plugin failed with status code 1.
    > ```
    > Make sure `protobuf` and the [gRPC](https://grpc.io/docs/languages/go/quickstart/) plugins (`protoc-gen-go` and `protoc-gen-go-grpc`) are installed by running
    > ```shell
    > $ go install google.golang.org/protobuf/cmd/protoc-gen-go
    > $ go install google.golang.org/grpc/cmd/protoc-gen-go-grpc
    > $ export PATH="$PATH:$(go env GOPATH)/bin"
    > ```

4. Run the executable: `./bin/{release|debug}/adaptodb`

### Other build options
1. `make clean` delete executables and protos
2. `make docker-build` build docker image for node
3. `make docker-clean` delete all docker containers and docker network for node
4. `make build` build adaptodb
5. `make proto` generate protobuf `.pb` code

## Ports

See ports in `pkg/schema/constant.go`

* AdaptoDB
  * Controller <-> Balancer (within Controller)
    * gRPC: **60082**
    * Handles new schedule advice from balancer
  * Controller Router <-> Client
    * HTTP: **60080**
    * gRPC: **60081**
    * Handles metadata queries

* Node
  * Node (Router) <-> Client [gRPC]
    * **51000** (in container network)
    * 51000 + node ID (in localhost)
    * Handles read/write requests from client and manipulates state machine
  * Node (Router) <-> Node [HTTP]
    * **52000** (in container network)
    * 52000 + node ID (in localhost)
    * Handles data transfer for load balancing
  * Node (StatsServer) <-> Balancer [gRPC]
    * **53000** (in container network)
    * 53000 + node ID (in localhost)
    * Transfers stats
  * Dragonboat Internal (Raft) [gRPC]
    * **54000** (in container network)
    * Handles Dragonboat internal communication
  * Debug
    * **55000** (in container network)
    * 55000 + node ID (in localhost)
    * Debug only.

## Access AdaptoDB
### Use client
`./bin/{release|debug}/client`

### APIs
#### Get the shard metadata:

`grpcurl -plaintext -d '{}' localhost:60081 proto.ShardRouter/GetConfig`

#### Read/Write:

1. Read: 
`grpcurl -plaintext -d '{"clusterID": 1, "key": "hello"}' localhost:51001 proto.NodeRouter/Read`

2. Write:

    **!!!!!!!!! key starts with [a-z], value only contains [a-zA-Z0-9] !!!!!!!!!**

    `grpcurl -plaintext -d '{"clusterID": 1, "key": "hello", "value": "hello-dragonboat"}' localhost:51001 proto.NodeRouter/Write`

#### Migrate

`grpcurl -plaintext -d '{"schedule": [{"shardId": 2, "keyRanges": [{"start": "h", "end": "m"}]}]}' localhost:60082 proto.Controller/UpdateSchedule`

#### Get the shard id for key (test only)

1. http

   `curl "http://localhost:60080/?key=key123"`

2. grpc
   
   Note: install grpcurl via `brew install grpcurl` if running for the first time

    `grpcurl -plaintext -d '{"key":"test-key"}' localhost:60081 proto.ShardRouter/GetShard`


## Code
```
cmd/
    adaptodb/ # Main entry point for the database server
        launcher.go # Used by controller to lauch Dragonboat nodes in local/remote
        main.gp # Start Central Controller (Main entrance)
    node/ # Node server implementation
        main.go # Start NodeHost in Dragonboat and manipulate local statemachine
        router.go # router to handle read/write request from clients
    client/ # Client CLI implementation
        main.go # User Interface
        types.go
pkg/
    balancer # Load balancing and shard distribution logic
    controller # Central controller for managing nodes and shards
        controller.go # Analyse advice from balancer and make decision
        operator.go # process migrate
    metadata # Metadata management for shards and nodes
        metadata.go 
    router # Handle metadata request 
        router.go
    schema - Data schema definitions
        types.go
    sm - State machine implementation using key-value store
        sm.go
    Protocol Buffers:
        proto # Protocol buffer definitions for:
            Controller service # For new schedule advice
            Node router service # For read/write request
            Shard metadata # For metadata query
config.yaml
makefile
```

## Debugging

- To debug a node spawned by the Controller, you need to attach the debugger to a running process. Run the `Attch to Process` task in VSCode's debugger and search for `node` to attach the debugger to the given node.
  
- When a program exited unexpectedly (e.g., as a result of `log.Fatalf()`), run `kill-ports.sh` to cleanup the remaining processes that are still running.

- If a process was terminated abnormally, it might not release the lock to its data. In which case, you might need to `rm -r tmp/` to remove all stale data and restart the nodes.

## Test

### Run go test

1. `cd test`
2. `go test`
   * Add `-v` to see verbose output

### Run test script

1. `go run script/stress.go`

### Run Locust simulation (executing adaptodb before running)
1. `cd script`
2. `pip3 install locust`
3. Start the simulation: `locust`