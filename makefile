GO_DEBUG_CMD=go build -gcflags=all="-N -l" # add -gcflags=all="-N -l" for debugging
GO_RELEASE_CMD=go build

all: build

build: debug release

debug: proto
	$(GO_DEBUG_CMD) -o ./bin/debug/adaptodb ./cmd/adaptodb
	$(GO_DEBUG_CMD) -o ./bin/debug/client ./cmd/client
	$(GO_DEBUG_CMD) -o ./bin/debug/node ./cmd/node

release: proto
	$(GO_RELEASE_CMD) -o ./bin/release/adaptodb ./cmd/adaptodb
	$(GO_RELEASE_CMD) -o ./bin/release/client ./cmd/client
	$(GO_RELEASE_CMD) -o ./bin/release/node ./cmd/node

proto:
	@echo "Generating protobuf code..."
	mkdir -p pkg/proto/proto; \
	protoc --proto_path=pkg/proto \
		--go_out=pkg/proto/proto --go_opt=paths=source_relative \
		--go-grpc_out=pkg/proto/proto --go-grpc_opt=paths=source_relative \
		pkg/proto/*.proto;

clean:
	rm -f adaptodb
	rm -f client
	rm -f node
	rm -f pkg/*/proto/*.pb.go

.PHONY: build debug release clean proto