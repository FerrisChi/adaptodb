package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	pb "adaptodb/pkg/proto/proto"
	"adaptodb/pkg/schema"
	"adaptodb/pkg/sm"

	"github.com/gorilla/websocket"
	"github.com/lni/dragonboat/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Router struct {
	buffer       map[string]string
	migTaskId    uint64 // migration task ID, 0 if not migrating
	migKeyRanges []schema.KeyRange
	migFlag      uint64 // migration flag, 0 if not migrating, 1 if source, 2 if destination
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

func (r *Router) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	if req.GetClusterID() != r.clusterId {
		return nil, errors.New("cluster ID mismatch")
	}

	ctx, cancel := context.WithTimeout(ctx, schema.READ_WRITE_TIMEOUT)
	defer cancel()
	cmd := fmt.Sprintf("read:%s", req.GetKey())
	data, error := r.nh.SyncRead(ctx, r.clusterId, cmd)
	if error != nil {
		r.statsServer.IncrementFailedRequests()
		return nil, error
	}
	r.statsServer.IncrementSuccessfulRequests()
	lookupResult, ok := data.(sm.LookupResult)
	if !ok {
		return nil, errors.New("failed to assert data as sm.LookupResult")
	}
	return &pb.ReadResponse{Value: lookupResult.Value}, nil
}

func (r *Router) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	if req.GetClusterID() != r.clusterId {
		return nil, errors.New("cluster ID mismatch")
	}

	ctx, cancel := context.WithTimeout(ctx, schema.READ_WRITE_TIMEOUT)
	defer cancel()
	session := r.nh.GetNoOPSession(r.clusterId)
	data := []byte(fmt.Sprintf("write:%s,%s", req.GetKey(), req.GetValue()))
	result, error := r.nh.SyncPropose(ctx, session, data)
	if error != nil {
		r.statsServer.IncrementFailedRequests()
		return nil, error
	}
	r.statsServer.IncrementSuccessfulRequests()
	return &pb.WriteResponse{Status: result.Value}, nil
}

func (r *Router) Remove(ctx context.Context, req *pb.RemoveRequest) (*pb.RemoveResponse, error) {
	if req.GetClusterID() != r.clusterId {
		return nil, errors.New("cluster ID mismatch")
	}

	ctx, cancel := context.WithTimeout(ctx, schema.READ_WRITE_TIMEOUT)
	defer cancel()
	session := r.nh.GetNoOPSession(r.clusterId)
	data := []byte(fmt.Sprintf("remove:%s", req.GetKey()))
	result, error := r.nh.SyncPropose(ctx, session, data)
	if error != nil {
		return nil, error
	}
	return &pb.RemoveResponse{Status: result.Value}, nil
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
			log.Println("Failed to propose migration task source:", err)
			return nil, err
		}
		r.migTaskId = req.GetTaskId()
		r.migFlag = 1
		r.migKeyRanges = schema.ParseKeyRanges(formatPBKeyRanges(req.GetKeyRanges()))
		return &pb.MigrateResponse{Status: result.Value}, nil
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
			log.Println("Failed to propose migration task destination:", err)
			return nil, err
		}
		r.migTaskId = req.GetTaskId()
		r.migFlag = 2
		r.migKeyRanges = schema.ParseKeyRanges(formatPBKeyRanges(req.GetKeyRanges()))
		return &pb.MigrateResponse{Status: result.Value}, nil
	} else {
		return nil, errors.New("from cluster ID mismatch")
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
		log.Println("Failed to propose apply migration task:", err)
		return nil, err
	}
	r.migTaskId = 0
	r.migKeyRanges = nil
	r.migFlag = 0
	return &pb.ApplyMigrationResponse{Status: result.Value}, nil
}

func (r *Router) GetKeyRanges(ctx context.Context, req *pb.GetKeyRangesRequest) (*pb.GetKeyRangesResponse, error) {
	if req.GetClusterID() != r.clusterId {
		return nil, errors.New("cluster ID mismatch")
	}
	ctx, cancel := context.WithTimeout(ctx, schema.READ_WRITE_TIMEOUT)
	defer cancel()
	resp, err := r.nh.SyncRead(ctx, r.clusterId, "schedule")
	if err != nil {
		return nil, err
	}
	krs, ok := resp.([]schema.KeyRange)
	if !ok {
		return nil, errors.New("failed to assert data as sm.LookupResult")
	}
	krsPb := schema.KeyRangesToPbKeyRangeList(krs)
	return &pb.GetKeyRangesResponse{KeyRanges: krsPb}, nil
}

// HandleTransfer handles data transfer during migration
func (r *Router) HandleTransfer(w http.ResponseWriter, req *http.Request) {
	taskId, err := strconv.ParseUint(req.URL.Query().Get("taskId"), 10, 64)
	if err != nil || r.migTaskId != taskId {
		http.Error(w, "Invalid taskId", http.StatusBadRequest)
		return
	}

	// Setup WebSocket server
	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024 * 64,
		WriteBufferSize: 1024 * 64,
		CheckOrigin: func(req *http.Request) bool {
			return true // Consider adding proper origin checks
		},
	}

	ws, err := upgrader.Upgrade(w, req, nil)
	if err != nil {
		log.Printf("Failed to upgrade websocket: %v", err)
		http.Error(w, "Failed to upgrade websocket", http.StatusInternalServerError)
		return
	}
	defer ws.Close()

	log.Printf("Data transfer initiated from %s for task %d", req.RemoteAddr, taskId)
	const batchSize = 100

	for {
		messageType, p, err := ws.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("Websocket error: %v", err)
			}
			break
		}

		if messageType != websocket.BinaryMessage {
			continue
		}

		// Ensure message is long enough to contain count
		if len(p) < 4 {
			log.Printf("Received malformed message: too short")
			continue
		}

		count := binary.LittleEndian.Uint32(p[:4])
		buf := p[4:]

		var batch strings.Builder
		batchCount := 0

		for i := uint32(0); i < count; i++ {
			if len(buf) < 2 {
				log.Printf("Malformed message: insufficient data for key length")
				break
			}

			kLen := binary.LittleEndian.Uint16(buf)
			buf = buf[2:]

			if len(buf) < int(kLen) {
				log.Printf("Malformed message: insufficient data for key")
				break
			}

			key := string(buf[:kLen])
			buf = buf[kLen:]

			if len(buf) < 2 {
				log.Printf("Malformed message: insufficient data for value length")
				break
			}

			vLen := binary.LittleEndian.Uint16(buf)
			buf = buf[2:]

			if len(buf) < int(vLen) {
				log.Printf("Malformed message: insufficient data for value")
				break
			}

			value := string(buf[:vLen])
			buf = buf[vLen:]

			if batchCount > 0 {
				batch.WriteString("|")
			}
			batch.WriteString(key)
			batch.WriteString(",")
			batch.WriteString(value)
			batchCount++

			if batchCount >= batchSize || i == count-1 {
				cmd := fmt.Sprintf("batch_write:%d,%s", r.migTaskId, batch.String())
				ctx, cancel := context.WithTimeout(context.Background(), schema.READ_WRITE_TIMEOUT)
				session := r.nh.GetNoOPSession(r.clusterId)
				_, err := r.nh.SyncPropose(ctx, session, []byte(cmd))
				cancel()

				if err != nil {
					log.Printf("Failed to propose batch: %v", err)
					continue
				}

				batch.Reset()
				batchCount = 0
			}
		}
	}

	log.Printf("Data transfer completed from %s for task %d", req.RemoteAddr, taskId)

	// Notify controller
	if ctrlConn, err := grpc.NewClient(r.ctrlAddress, grpc.WithTransportCredentials(insecure.NewCredentials())); err == nil {
		defer ctrlConn.Close()
		client := pb.NewControllerClient(ctrlConn)
		ctx, cancel := context.WithTimeout(context.Background(), schema.READ_WRITE_TIMEOUT)
		defer cancel()

		if resp, err := client.FinishMigration(ctx, &pb.FinishMigrationRequest{
			TaskId:  r.migTaskId,
			ShardId: r.clusterId,
		}); err != nil {
			log.Printf("Failed to notify controller: %v", err)
		} else {
			log.Printf("Migration task %d completed: %v", taskId, resp)
		}
	}
}
