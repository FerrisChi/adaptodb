# Build Commands
GO_DEBUG_CMD = go build -gcflags=all="-N -l" # Debugging flags
GO_RELEASE_CMD = go build                    # Release build

# Directories
DEBUG_BIN_DIR = ./bin/debug
RELEASE_BIN_DIR = ./bin/release
PROTO_OUT_DIR = ./pkg/proto/proto
PROTO_FILES = $(wildcard pkg/proto/*.proto)

# Targets
all: build

# Build all binaries for both debug and release
build: debug release

# Build debug binaries
debug: proto
	@echo "Building debug binaries..."
	mkdir -p $(DEBUG_BIN_DIR)
	$(GO_DEBUG_CMD) -o $(DEBUG_BIN_DIR)/adaptodb ./cmd/adaptodb
	$(GO_DEBUG_CMD) -o $(DEBUG_BIN_DIR)/client ./cmd/client
	$(GO_DEBUG_CMD) -o $(DEBUG_BIN_DIR)/node ./cmd/node

# Build release binaries
release: proto
	@echo "Building release binaries..."
	mkdir -p $(RELEASE_BIN_DIR)
	$(GO_RELEASE_CMD) -o $(RELEASE_BIN_DIR)/adaptodb ./cmd/adaptodb
	$(GO_RELEASE_CMD) -o $(RELEASE_BIN_DIR)/client ./cmd/client
	$(GO_RELEASE_CMD) -o $(RELEASE_BIN_DIR)/node ./cmd/node

# Generate Protobuf code
proto: $(PROTO_FILES)
	@echo "Generating protobuf code..."
	mkdir -p $(PROTO_OUT_DIR)
	protoc --proto_path=pkg/proto \
		--go_out=$(PROTO_OUT_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_OUT_DIR) --go-grpc_opt=paths=source_relative \
		$^

# Clean build artifacts
clean:
	@echo "Cleaning up build artifacts..."
	rm -rf $(DEBUG_BIN_DIR) $(RELEASE_BIN_DIR)
	rm -f $(PROTO_OUT_DIR)/*.pb.go

# Docker Build Targets
docker-build-main:
	@echo "Building Docker image for adaptodb-main..."
	docker build -f Dockerfile-main -t adaptodb-main .

docker-build-node:
	@echo "Building Docker image for adaptodb-node..."
	docker build -f Dockerfile-node -t adaptodb-node .

docker-build: docker-build-main docker-build-node

# Docker Compose Targets
docker-up: docker-build
	@echo "Starting services with Docker Compose..."
	docker-compose up --build

docker-down:
	@echo "Stopping services with Docker Compose..."
	docker-compose down

.PHONY: all build debug release clean proto docker-build docker-build-main docker-build-node docker-up docker-down
