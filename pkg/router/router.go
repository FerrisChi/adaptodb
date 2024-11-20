package router

import (
	"adaptodb/pkg/controller"
	"adaptodb/pkg/metadata"
	pb "adaptodb/pkg/router/proto"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Router struct {
	pb.UnimplementedShardRouterServer
	controller *controller.Controller
	metadata   *metadata.Metadata
}

func NewRouter(sc *controller.Controller, metadata *metadata.Metadata) *Router {
	return &Router{
		controller: sc,
		metadata:   metadata,
	}
}

// http.HandleFunc("/", router.HandleRequest)
func (r *Router) HandleRequest(w http.ResponseWriter, q *http.Request) {
	log.Printf("Received request from %s", q.RemoteAddr)
	key := q.URL.Query().Get("key")

	// find the corresponding shard for the key
	shardID, error := r.metadata.GetShardForKey(key)
	if error != nil {
		http.Error(w, fmt.Sprintf("Key %s not assigned", key), http.StatusNotFound)
		return
	}
	// return the shard id
	log.Printf("Key %s is in shard %d", key, shardID)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]uint64{"shard": shardID})
}

// gRPC handler
func (r *Router) GetShard(ctx context.Context, req *pb.GetShardRequest) (*pb.GetShardResponse, error) {
	log.Printf("Received request from GRPC")
	key := req.Key

	// find the corresponding shard for the key
	shardID, error := r.metadata.GetShardForKey(key)
	if error != nil {
		return nil, status.Errorf(codes.NotFound, "Key %s not found", key)
	}
	// return the shard id
	log.Printf("Key %s is in shard %d", key, shardID)
	return &pb.GetShardResponse{ShardId: shardID}, nil
}
