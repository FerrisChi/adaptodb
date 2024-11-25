# adaptodb
An Adaptive shard-balancing key-value database

## Build

0. If you are migrating from dragonboat v4 to v3, please delete the `tmp/` data generated from previous runs first: `rm -r tmp/`
1. Download go mods: `go mod tidy`
2. Make the repo: `make build`

> If you run into an error like:
> ```
> protoc-gen-go-grpc: program not found or is not executable
> Please specify a program using absolute path or make sure the program is available in > your PATH system variable
> --go-grpc_out: protoc-gen-go-grpc: Plugin failed with status code 1.
> ```
> Make sure the [gRPC](https://grpc.io/docs/languages/go/quickstart/) plugins (`protoc-gen-go` and `protoc-gen-go-grpc`) are installed by running
> ```shell
> $ go install google.golang.org/protobuf/cmd/protoc-gen-go
> $ go install google.golang.org/grpc/cmd/protoc-gen-go-grpc
> $ export PATH="$PATH:$(go env GOPATH)/bin"
> ```

3. Run the executable: `./adaptodb`

## Ports
* Controller
  * grpc: 8082
  * Handle new schedule advice from balancer.
* Controller Router
  * http: 8080
  * gprc: 8081
  * Handle metadata query.
* Dragonboat node router
  * grpc: 51000 + node id
  * Handle read/write request from client and manipulate statemachine.
* Dragonboat internal
  * ip:port set in `config.yaml`
  * Hanlde dragonboat internal communication.

## Access AdaptoDB
### Use client
`./client`

### APIs
### Get the shard metadata:

`grpcurl -plaintext -d '{}' localhost:8081 proto.ShardRouter/GetConfig`

### Read/Write:

1. Read: 
`grpcurl -plaintext -d '{"clusterID": 1, "key": "hello"}' localhost:51001 proto.NodeRouter/Read`

2. Write:

**!!!!!!!!! key starts with [a-z] !!!!!!!!!**

**!!!!!!!!! value only contains [a-zA-Z0-9] !!!!!!!!!**

`grpcurl -plaintext -d '{"clusterID": 1, "key": "hello", "value": "hello-dragonboat"}' localhost:51001 proto.NodeRouter/Write`

### Get the shard id for key (test only)

#### http
`curl "http://localhost:8080/?key=key123"`

#### grpc
Note: install grpcurl via `brew install grpcurl` if running for the first time
`grpcurl -plaintext -d '{"key":"test-key"}' localhost:8081 proto.ShardRouter/GetShard`


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

