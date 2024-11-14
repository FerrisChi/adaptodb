GOCMD=go build -v
PROTO_DIR=pkg/router
GO_OUT_DIR=pkg/router/proto

build:
	$(GOCMD) -o adaptodb ./cmd/adaptodb

proto:
	@echo "Generating protobuf code..."
	@mkdir -p $(GO_OUT_DIR)
	@protoc --proto_path=$(PROTO_DIR) \
		--go_out=$(GO_OUT_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(GO_OUT_DIR) --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/*.proto

clean:
	rm -f adaptodb
	rm -f $(GO_OUT_DIR)/*.pb.go

.PHONY: build clean proto