GOCMD=go build
PROTO_DIRS=pkg/router pkg/controller

build: proto
	$(GOCMD) -o adaptodb ./cmd/adaptodb

proto:
	@echo "Generating protobuf code..."
	@for dir in $(PROTO_DIRS); do \
		mkdir -p $$dir/proto; \
		protoc --proto_path=$$dir \
			--go_out=$$dir/proto --go_opt=paths=source_relative \
			--go-grpc_out=$$dir/proto --go-grpc_opt=paths=source_relative \
			$$dir/*.proto; \
	done

clean:
	rm -f adaptodb
	rm -f pkg/*/proto/*.pb.go

.PHONY: build clean proto