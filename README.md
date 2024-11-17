# adaptodb
An Adaptive shard-balancing key-value database

## Build
1. Download go mods: `go mod tidy`
2. Make the repo: `make build`

## Get the shard id of a key:
### http
`curl "http://localhost:8080/?key=key123"`

### grpc
`grpcurl -plaintext -d '{"key":"test-key"}' localhost:8081 router.ShardRouter/GetShard`