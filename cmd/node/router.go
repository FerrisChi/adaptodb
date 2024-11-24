package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	pb "adaptodb/pkg/proto/proto"
	"adaptodb/pkg/schema"

	"github.com/lni/dragonboat/v3"
)

type Router struct {
	node      *dragonboat.NodeHost
	clusterID uint64
	keyRanges []schema.KeyRange
	pb.UnimplementedNodeRouterServer
}

func NewRouter(nh *dragonboat.NodeHost, clusterID uint64, keyranges []schema.KeyRange) *Router {
	return &Router{
		node:      nh,
		clusterID: clusterID,
		keyRanges: keyranges,
	}
}

func (r *Router) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	if req.GetClusterID() != r.clusterID {
		return nil, errors.New("cluster ID mismatch")
	}
	flag := false
	for _, kr := range r.keyRanges {
		if req.Key >= kr.Start && req.Key < kr.End {
			flag = true
			break
		}
	}
	if !flag {
		return nil, errors.New("key not managed by this node")
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	data, error := r.node.SyncRead(ctx, r.clusterID, req.Key) // Using shardID 1, adjust as needed
	if error != nil {
		return nil, error
	}
	return &pb.ReadResponse{Value: data.(string)}, nil
}

func (r *Router) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	if req.GetClusterID() != r.clusterID {
		return nil, errors.New("cluster ID mismatch")
	}

	flag := false
	for _, kr := range r.keyRanges {
		if req.Key >= kr.Start && req.Key < kr.End {
			flag = true
			break
		}
	}
	if !flag {
		return nil, errors.New("key not managed by this node")
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	session := r.node.GetNoOPSession(r.clusterID)
	data := []byte(fmt.Sprintf("write:%s,%s", req.GetKey(), req.GetValue()))
	result, error := r.node.SyncPropose(ctx, session, data)
	if error != nil {
		return nil, error
	}
	return &pb.WriteResponse{Status: result.Value}, nil
}
