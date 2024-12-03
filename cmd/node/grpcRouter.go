package main

import (
	"context"
	"errors"
	"fmt"

	pb "adaptodb/pkg/proto/proto"
	"adaptodb/pkg/schema"
	"adaptodb/pkg/sm"

	"github.com/lni/dragonboat/v3"
)

type Router struct {
	migTaskId    uint64 // migration task ID, 0 if not migrating
	migKeyRanges []schema.KeyRange
	migFlag      uint64 // migration flag, 0 if not migrating, 1 if source, 2 if destination
	migCache     []string
	nh           *dragonboat.NodeHost
	nodeId       uint64
	clusterId    uint64
	ctrlAddress  string
	peers        map[uint64]string
	statsServer  *NodeStatsServer
	pb.UnimplementedNodeRouterServer
}

func NewRouter(nh *dragonboat.NodeHost, nodeId, clusterId uint64, peers map[uint64]string, ctrlAddress string, statsServer *NodeStatsServer) *Router {
	return &Router{
		nh:          nh,
		nodeId:      nodeId,
		clusterId:   clusterId,
		peers:       peers,
		ctrlAddress: ctrlAddress,
		statsServer: statsServer,
	}
}

func (r *Router) Read(ctx context.Context, req *pb.ReadRequest) (*pb.RWResponse, error) {
	if req.GetClusterID() != r.clusterId {
		r.statsServer.IncrementFailedRequests()
		return nil, errors.New("cluster ID mismatch")
	}

	ctx, cancel := context.WithTimeout(ctx, schema.READ_WRITE_TIMEOUT)
	defer cancel()
	cmd := fmt.Sprintf("read:%s", req.GetKey())
	data, err := r.nh.SyncRead(ctx, r.clusterId, cmd)
	if err != nil {
		r.statsServer.IncrementFailedRequests()
		return nil, errors.Join(errors.New("dragonboat error and panic"), err)
	}
	lookupResult, ok := data.(sm.LookupResult)
	if !ok {
		r.statsServer.IncrementFailedRequests()
		return nil, errors.New("failed to assert data as sm.LookupResult")
	}
	if lookupResult.Value == 0 {
		r.statsServer.IncrementFailedRequests()
		return nil, errors.New("failed to read: " + lookupResult.Data)
	}
	r.statsServer.IncrementSuccessfulRequests()
	return &pb.RWResponse{Status: lookupResult.Value, Data: lookupResult.Data}, nil
}

func (r *Router) Write(ctx context.Context, req *pb.WriteRequest) (*pb.RWResponse, error) {
	if req.GetClusterID() != r.clusterId {
		r.statsServer.IncrementFailedRequests()
		return nil, errors.New("cluster ID mismatch")
	}

	ctx, cancel := context.WithTimeout(ctx, schema.READ_WRITE_TIMEOUT)
	defer cancel()
	session := r.nh.GetNoOPSession(r.clusterId)
	data := []byte(fmt.Sprintf("write:%s,%s", req.GetKey(), req.GetValue()))
	result, err := r.nh.SyncPropose(ctx, session, data)
	if err != nil {
		r.statsServer.IncrementFailedRequests()
		return nil, errors.Join(errors.New("dragonboat error and panic"), err)
	}
	if result.Value == 0 {
		r.statsServer.IncrementFailedRequests()
		return nil, errors.New("failed to propose write: " + string(result.Data))
	}
	r.statsServer.IncrementSuccessfulRequests()
	return &pb.RWResponse{Status: result.Value, Data: string(result.Data)}, nil
}

func (r *Router) Remove(ctx context.Context, req *pb.RemoveRequest) (*pb.RWResponse, error) {
	if req.GetClusterID() != r.clusterId {
		r.statsServer.IncrementFailedRequests()
		return nil, errors.New("cluster ID mismatch")
	}

	ctx, cancel := context.WithTimeout(ctx, schema.READ_WRITE_TIMEOUT)
	defer cancel()
	session := r.nh.GetNoOPSession(r.clusterId)
	data := []byte(fmt.Sprintf("remove:%s", req.GetKey()))
	result, err := r.nh.SyncPropose(ctx, session, data)
	if err != nil {
		r.statsServer.IncrementFailedRequests()
		return nil, errors.Join(errors.New("dragonboat error and panic"), err)
	}
	if result.Value == 0 {
		r.statsServer.IncrementFailedRequests()
		return nil, errors.New("failed to propose remove: " + string(result.Data))
	}
	r.statsServer.IncrementSuccessfulRequests()
	return &pb.RWResponse{Status: result.Value, Data: string(result.Data)}, nil
}

func (r *Router) StartMigration(ctx context.Context, req *pb.MigrateRequest) (*pb.MigrateResponse, error) {
	if req.GetFromClusterId() == r.clusterId {
		if _, ok := ctx.Deadline(); !ok {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, schema.READ_WRITE_TIMEOUT)
			defer cancel()
		}

		leaderId, avai, err := r.nh.GetLeaderID(r.clusterId)
		if err != nil || !avai {
			return nil, errors.New("leader not available")
		}
		session := r.nh.GetNoOPSession(r.clusterId)
		data := []byte(fmt.Sprintf("migrate_src:%d,%d,%s,%s,%s", req.GetTaskId(), leaderId, r.ctrlAddress, req.GetAddress(), formatPBKeyRanges(req.GetKeyRanges())))
		result, err := r.nh.SyncPropose(ctx, session, data)
		if err != nil {
			return nil, errors.Join(errors.New("dragonboat error and panic"), err)
		}
		if result.Value == 0 {
			return nil, errors.New("failed to propose migration task source: " + string(result.Data))
		}
		r.migTaskId = req.GetTaskId()
		r.migFlag = 1
		r.migKeyRanges = schema.ParseKeyRanges(formatPBKeyRanges(req.GetKeyRanges()))
		return &pb.MigrateResponse{Status: result.Value, Data: result.Data}, nil
	} else if req.GetToClusterId() == r.clusterId {
		if _, ok := ctx.Deadline(); !ok {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, schema.READ_WRITE_TIMEOUT)
			defer cancel()
		}
		session := r.nh.GetNoOPSession(r.clusterId)
		data := []byte(fmt.Sprintf("migrate_dst:%d,%s,%s,%s", req.GetTaskId(), r.ctrlAddress, req.GetAddress(), formatPBKeyRanges(req.GetKeyRanges())))
		result, err := r.nh.SyncPropose(ctx, session, data)
		if err != nil {
			return nil, errors.Join(errors.New("dragonboat error and panic"), err)
		}
		if result.Value == 0 {
			return nil, errors.New("failed to propose migration task destination: " + string(result.Data))
		}
		r.migTaskId = req.GetTaskId()
		r.migFlag = 2
		r.migKeyRanges = schema.ParseKeyRanges(formatPBKeyRanges(req.GetKeyRanges()))
		return &pb.MigrateResponse{Status: result.Value, Data: result.Data}, nil
	} else {
		return nil, errors.New("cluster ID mismatch")
	}
}

func (r *Router) ApplyMigration(ctx context.Context, req *pb.ApplyMigrationRequest) (*pb.ApplyMigrationResponse, error) {
	if req.GetTaskId() != r.migTaskId {
		return nil, errors.New("task ID mismatch")
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, schema.READ_WRITE_TIMEOUT)
		defer cancel()
	}

	session := r.nh.GetNoOPSession(r.clusterId)
	var flag string
	if r.migFlag == 1 {
		flag = "remove"
	} else if r.migFlag == 2 {
		flag = "add"
	}
	data := []byte(fmt.Sprintf("apply_schedule:%s,%s", flag, schema.KeyRangeToString(r.migKeyRanges)))
	result, err := r.nh.SyncPropose(ctx, session, data)
	if err != nil {
		return nil, errors.Join(errors.New("dragonboat error and panic"), err)
	}
	if result.Value == 0 {
		return nil, errors.New("failed to propose migration schedule: " + string(result.Data))
	}
	r.migTaskId = 0
	r.migKeyRanges = nil
	r.migFlag = 0
	return &pb.ApplyMigrationResponse{Status: result.Value}, nil
}

func (r *Router) GetKeyRanges(ctx context.Context, req *pb.GetKeyRangesRequest) (*pb.GetKeyRangesResponse, error) {
	if req.GetClusterID() != r.clusterId {
		r.statsServer.IncrementFailedRequests()
		return nil, errors.New("cluster ID mismatch")
	}
	ctx, cancel := context.WithTimeout(ctx, schema.READ_WRITE_TIMEOUT)
	defer cancel()
	resp, err := r.nh.SyncRead(ctx, r.clusterId, "schedule")
	if err != nil {
		r.statsServer.IncrementFailedRequests()
		return nil, errors.Join(errors.New("dragonboat error and panic"), err)
	}
	result, ok := resp.(sm.LookupResult)
	if !ok {
		r.statsServer.IncrementFailedRequests()
		return nil, errors.New("failed to assert data as sm.LookupResult")
	}
	if result.Value == 0 {
		r.statsServer.IncrementFailedRequests()
		return nil, errors.New("failed to get key ranges: " + result.Data)
	}
	krs := schema.ParseKeyRanges(result.Data)
	krsPb := schema.KeyRangesToPbKeyRangeList(krs)
	r.statsServer.IncrementSuccessfulRequests()
	return &pb.GetKeyRangesResponse{KeyRanges: krsPb}, nil
}
