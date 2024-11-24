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

## API
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
