package router

import (
	"adaptodb/pkg/metadata"
	pb "adaptodb/pkg/proto/proto"
	"adaptodb/pkg/schema"
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
	metadata *metadata.Metadata
}

func NewRouter(metadata *metadata.Metadata) *Router {
	return &Router{
		metadata: metadata,
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

// http.HandleFunc("/config", router.HandleConfigRequest)
func (r *Router) HandleConfigRequest(w http.ResponseWriter, q *http.Request) {
	log.Printf("Received request from %s", q.RemoteAddr)

	// Get and convert member information
	result := make(map[uint64][]schema.KeyRange)
	memberMap := make(map[uint64][]schema.NodeInfo)
	raftGroup := r.metadata.GetConfig()
	for _, v := range raftGroup {
		shardId := v.ShardID
		memberMap[shardId] = v.Members
		result[shardId] = v.KeyRanges
	}

	// Prepare combined response
	response := schema.ConfigResponse{
		ShardMap: result,
		Members:  memberMap,
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// gRPC handler
func (r *Router) GetShard(ctx context.Context, req *pb.GetShardRequest) (*pb.GetShardResponse, error) {
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

func (r *Router) GetConfig(ctx context.Context, req *pb.GetConfigRequest) (*pb.GetConfigResponse, error) {
	mapping, error := r.metadata.GetAllShardKeyRanges()
	if error != nil {
		return nil, status.Errorf(codes.Internal, "Failed to get shard mapping")
	}
	// convert the mapping to the proto format
	result := make(map[uint64]*pb.KeyRangeList)
	for k, v := range mapping {
		ranges := make([]*pb.KeyRange, 0, len(v))
		for _, kr := range v {
			ranges = append(ranges, &pb.KeyRange{Start: kr.Start, End: kr.End})
		}
		result[k] = &pb.KeyRangeList{KeyRanges: ranges}
	}

	memberMap := make(map[uint64]*pb.Members)
	for _, v := range r.metadata.GetConfig() {
		shardId := v.ShardID
		members := make([]*pb.Member, 0, len(v.Members))
		for _, nodeInfo := range v.Members {
			members = append(members, &pb.Member{Id: nodeInfo.ID, Addr: nodeInfo.GrpcAddress, User: nodeInfo.User, SshKeyPath: nodeInfo.SSHKeyPath})
		}
		memberMap[shardId] = &pb.Members{Members: members}
	}

	return &pb.GetConfigResponse{ShardMap: result, Members: memberMap}, nil
}
