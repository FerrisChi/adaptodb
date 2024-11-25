GOCMD=go build

build: proto
	$(GOCMD) -o adaptodb ./cmd/adaptodb
	$(GOCMD) -o client ./cmd/client
	$(GOCMD) -o node ./cmd/node

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

.PHONY: build clean proto