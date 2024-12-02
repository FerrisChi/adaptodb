package main

import (
	pb "adaptodb/pkg/proto/proto"
	"adaptodb/pkg/schema"
	"adaptodb/pkg/sm"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// HandleRead handles read requests
func (r *Router) HandleRead(w http.ResponseWriter, req *http.Request) {
	shardId := req.URL.Query().Get("shardId")
	shardIdUint, err := strconv.ParseUint(shardId, 10, 64)
	if shardId == "" || err != nil || r.clusterId != shardIdUint {
		r.statsServer.IncrementFailedRequests()
		http.Error(w, "Invalid shard ID", http.StatusBadRequest)
		return
	}
	key := req.URL.Query().Get("key")
	if key == "" {
		r.statsServer.IncrementFailedRequests()
		http.Error(w, "Invalid key", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), schema.READ_WRITE_TIMEOUT)
	defer cancel()
	cmd := fmt.Sprintf("read:%s", key)
	data, err := r.nh.SyncRead(ctx, r.clusterId, cmd)
	if err != nil {
		r.statsServer.IncrementFailedRequests()
		http.Error(w, "dragonboat error and panic "+err.Error(), http.StatusInternalServerError)
		return
	}
	lookupResult, ok := data.(sm.LookupResult)
	if !ok {
		r.statsServer.IncrementFailedRequests()
		http.Error(w, "Failed to assert data as sm.LookupResult", http.StatusInternalServerError)
		return
	}
	if lookupResult.Value == 0 {
		r.statsServer.IncrementFailedRequests()
		http.Error(w, "Failed to read key"+lookupResult.Data, http.StatusNotFound)
		return
	}
	r.statsServer.IncrementSuccessfulRequests()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": strconv.FormatUint(lookupResult.Value, 10), "data": lookupResult.Data})
}

// HandleWrite handles write requests
func (r *Router) HandleWrite(w http.ResponseWriter, req *http.Request) {
	shardId := req.URL.Query().Get("shardId")
	shardIdUint, err := strconv.ParseUint(shardId, 10, 64)
	if shardId == "" || err != nil || r.clusterId != shardIdUint {
		r.statsServer.IncrementFailedRequests()
		http.Error(w, "Invalid shard ID", http.StatusBadRequest)
		return
	}
	key := req.URL.Query().Get("key")
	if key == "" {
		r.statsServer.IncrementFailedRequests()
		http.Error(w, "Invalid key", http.StatusBadRequest)
		return
	}
	value := req.URL.Query().Get("value")
	if value == "" {
		r.statsServer.IncrementFailedRequests()
		http.Error(w, "Invalid value", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), schema.READ_WRITE_TIMEOUT)
	defer cancel()
	session := r.nh.GetNoOPSession(r.clusterId)
	cmd := fmt.Sprintf("write:%s,%s", key, value)
	result, err := r.nh.SyncPropose(ctx, session, []byte(cmd))
	if err != nil {
		r.statsServer.IncrementFailedRequests()
		http.Error(w, "dragonboat error and panic "+err.Error(), http.StatusInternalServerError)
		return
	}
	if result.Value == 0 {
		r.statsServer.IncrementFailedRequests()
		http.Error(w, "Failed to propose write: "+string(result.Data), http.StatusInternalServerError)
		return
	}
	r.statsServer.IncrementSuccessfulRequests()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": string(result.Value), "data": string(result.Data)})
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
				// cancel migration
				r.cancelMigrationFromNode()
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
				result, err := r.nh.SyncPropose(ctx, session, []byte(cmd))
				cancel()
				if err != nil {
					ws.WriteControl(websocket.CloseMessage,
						websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "dragonboat error and panic "+err.Error()),
						time.Now().Add(time.Second))
					log.Println("dragonaobt error and panic. Migration failed")
					r.cancelMigrationFromNode()
					return
				}
				if result.Value == 0 {
					log.Println("Failed to propose batch write. saving into cache")
					r.migCache = append(r.migCache, batch.String())
				}

				batch.Reset()
				batchCount = 0
			}
		}
	}

	// Retry batch write remaining cache
	for _, batch := range r.migCache {
		cmd := fmt.Sprintf("batch_write:%d,%s", r.migTaskId, batch)
		ctx, cancel := context.WithTimeout(context.Background(), schema.READ_WRITE_TIMEOUT)
		session := r.nh.GetNoOPSession(r.clusterId)
		result, err := r.nh.SyncPropose(ctx, session, []byte(cmd))
		cancel()
		if err != nil {
			r.cancelMigrationFromNode()
			return
		}
		if result.Value == 0 {
			log.Println("Failed to retry propose batch write. Migration failed")
			r.cancelMigrationFromNode()
			return
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
