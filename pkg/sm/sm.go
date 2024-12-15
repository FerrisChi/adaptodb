package sm

import (
	"adaptodb/pkg/schema"
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"

	"sync"

	"github.com/lni/dragonboat/v3/statemachine"
)

// serializedKV is used for gob encoding
type serializedKV struct {
	Key   interface{}
	Value interface{}
}

type serializedSnapshot struct {
	KVPairs    []serializedKV
	Krs        []schema.KeyRange
	NumEntries int64
}

type KVStore struct {
	clusterId       uint64
	Data            sync.Map `yaml:"data" json:"data"` // The key-value data store
	krs             []schema.KeyRange
	migrationTaskId uint64
	migrate_krs     []schema.KeyRange
	NumEntries      int64 `yaml:"num_entries" json:"num_entries"`
	nodeId          uint64
}

type LookupResult struct {
	Value      uint64
	Data       string
	NumEntries int64
}

// NewKVStore initializes and returns a new KVStore state machine instance.
func NewKVStore(shardID uint64, replicaID uint64) statemachine.IStateMachine {
	return &KVStore{
		Data:            sync.Map{},
		clusterId:       shardID,
		nodeId:          replicaID,
		NumEntries:      0,
		krs:             []schema.KeyRange{},
		migrationTaskId: 0,
		migrate_krs:     nil,
	}
}

// Lookup performs a read-only query. This is used to serve read requests.
func (s *KVStore) Lookup(query interface{}) (interface{}, error) {
	str, ok := query.(string)
	if !ok {
		return LookupResult{Value: 0, Data: "invalid query type", NumEntries: s.NumEntries}, nil
	}
	log.Println("Lookup query:", str)
	tmp := strings.Split(str, ":")
	switch tmp[0] {
	case "read":
		key := tmp[1]
		for _, kr := range s.krs {
			if key >= kr.Start && key < kr.End {
				value, exists := s.Data.Load(key)
				if !exists {
					return LookupResult{Value: 0, Data: "key " + key + " not found", NumEntries: 1}, nil
				}
				return LookupResult{Value: 1, Data: value.(string), NumEntries: s.NumEntries}, nil
			}
		}
		for _, kr := range s.migrate_krs {
			if key >= kr.Start && key < kr.End {
				return LookupResult{Value: 0, Data: "key " + key + " in migration", NumEntries: s.NumEntries}, nil
			}
		}
		return LookupResult{Value: 0, Data: "key " + key + " not managed by this node", NumEntries: s.NumEntries}, nil
	case "schedule":
		return LookupResult{Value: 1, Data: schema.KeyRangeToString(s.krs), NumEntries: s.NumEntries}, nil
	default:
		return LookupResult{Value: 0, Data: "invalid query", NumEntries: s.NumEntries}, nil
	}
}

// Update applies a mutation to the state machine. This handles write requests.
// Returns status 1 if an old entry is modified, 2 if a new entry is added.
// opType:params
func (s *KVStore) Update(data []byte) (statemachine.Result, error) {
	tmp := bytes.SplitN(data, []byte(":"), 2)
	if len(tmp) != 2 {
		// invalid operation format
		return statemachine.Result{Value: 0, Data: []byte("invalid operation format")}, nil
	}
	opType, params := string(tmp[0]), tmp[1]
	log.Println("opType", opType, "params", string(params))
	switch opType {
	case "write":
		// Format: write:key,value
		kv := bytes.SplitN(params, []byte(","), 2)
		if len(kv) != 2 {
			// invalid write format
			return statemachine.Result{Value: 0, Data: []byte("invalid write format")}, nil
		}
		key, value := string(kv[0]), string(kv[1])
		for _, kr := range s.krs {
			if key >= kr.Start && key < kr.End {
				_, loaded := s.Data.Swap(key, value)
				if !loaded {
					s.NumEntries++
				}
				return statemachine.Result{Value: 1, Data: []byte(fmt.Sprintf("Key %s Value %s written successfully.", key, value))}, nil
			}
		}
		return statemachine.Result{Value: 0, Data: []byte("key " + key + " not managed by this node. Key ranges: " + fmt.Sprint(s.krs))}, nil
	case "remove":
		// Format: remove:key
		key := string(params)
		if _, exists := s.Data.Load(key); !exists {
			return statemachine.Result{Value: 0, Data: []byte(fmt.Sprintf("Key %s does not exist.", key))}, nil
		}
		s.Data.Delete(key)
		s.NumEntries--
		return statemachine.Result{Value: 0, Data: []byte(fmt.Sprintf("Key %s removed successfully.", key))}, nil
	case "migrate_src":
		// Format: migrate_src:taskId,leaderId,ctrlAddress,toAddress,keyRange
		kv := bytes.SplitN(params, []byte(","), 5)
		if len(kv) != 5 {
			return statemachine.Result{Value: 0, Data: []byte("invalid migrate format")}, nil
		}
		taskId, err := strconv.ParseUint(string(kv[0]), 10, 64)
		if err != nil {
			return statemachine.Result{Value: 0, Data: []byte("invalid task ID")}, nil
		}
		leaderId, err := strconv.ParseUint(string(kv[1]), 10, 64)
		if err != nil {
			return statemachine.Result{Value: 0, Data: []byte("invalid leader ID")}, nil
		}
		if s.migrationTaskId != 0 {
			return statemachine.Result{Value: 0, Data: []byte("another migration task in progress")}, nil
		}
		toAddress := string(kv[3])
		krs := schema.ParseKeyRanges(kv[4])
		s.migrationTaskId = taskId
		s.migrate_krs = krs
		// remove key ranges from local group
		s.krs = schema.RemoveKeyRanges(s.krs, krs)

		if s.nodeId != leaderId {
			return statemachine.Result{Value: 1, Data: []byte(fmt.Sprintf("Migration task %d started successfully on source node %d", taskId, leaderId))}, nil
		}
		// use a new go routine to transfer data to node in target address
		go func(data *sync.Map) {
			err := startTransfer(taskId, toAddress, krs, data)
			if err != nil {
				log.Println("Error transferring data:", err)
			}
		}(&s.Data)
		return statemachine.Result{Value: 1, Data: []byte(fmt.Sprintf("Migration task %d started successfully on source node %d.", taskId, leaderId))}, nil
	case "migrate_dst":
		// Format: migrate_dst:taskId,strlAddress,fromAddress,keyRange
		kv := bytes.SplitN(params, []byte(","), 4)
		if len(kv) != 4 {
			return statemachine.Result{Value: 0, Data: []byte("invalid migrate format")}, nil
		}
		taskId, err := strconv.ParseUint(string(kv[0]), 10, 64)
		if err != nil {
			return statemachine.Result{Value: 0, Data: []byte("invalid task ID")}, nil
		}
		if s.migrationTaskId != 0 {
			return statemachine.Result{Value: 0, Data: []byte("another migration task in progress")}, nil
		}
		krs := schema.ParseKeyRanges(kv[3])
		s.migrationTaskId = taskId
		s.migrate_krs = krs
		return statemachine.Result{Value: 1, Data: []byte(fmt.Sprintf("Migration task %d started successfully on destination shard %d.", taskId, s.clusterId))}, nil
	case "batch_write":
		// Format: batch_write:taskId,key1,value1|key2,value2|...
		params := bytes.SplitN(params, []byte(","), 2)
		taskId, err := strconv.ParseUint(string(params[0]), 10, 64)
		if err != nil {
			return statemachine.Result{Value: 0, Data: []byte("invalid task ID")}, nil
		}
		if s.migrationTaskId != taskId {
			return statemachine.Result{Value: 0, Data: []byte("task ID mismatch")}, nil
		}
		pairs := bytes.Split(params[1], []byte("|"))
		succ := 0
		for _, pair := range pairs {
			kv := bytes.SplitN(pair, []byte(","), 2)
			if len(kv) != 2 {
				continue
			}
			key, value := string(kv[0]), string(kv[1])
			s.Data.Store(key, value)
			s.NumEntries++
			succ++
		}
		return statemachine.Result{Value: uint64(succ), Data: []byte(fmt.Sprintf("%d data written successfully.", succ))}, nil
	case "apply_schedule":
		// Format: apply_schedule:flag,krs
		params := bytes.SplitN(params, []byte(","), 2)
		flag := string(params[0])
		krs := schema.ParseKeyRanges(params[1])
		cnt := s.updateSchedule(flag, krs)
		s.migrationTaskId = 0
		s.migrate_krs = nil
		log.Printf("Applied Key Ranges %s %v. Now Schedule: %v", flag, krs, s.krs)
		return statemachine.Result{Value: 1, Data: []byte("Schedule" + string(params[1]) + " applied." + strconv.FormatUint(cnt, 10) + " entries modified.")}, nil
	default:
		return statemachine.Result{Value: 0, Data: []byte("unknown operation type")}, nil
	}
}

// SaveSnapshot saves the current state for recovery in case of failure.
func (s *KVStore) SaveSnapshot(writer io.Writer, _ statemachine.ISnapshotFileCollection, _ <-chan struct{}) error {
	var kvPairs []serializedKV

	s.Data.Range(func(key, value interface{}) bool {
		kvPairs = append(kvPairs, serializedKV{
			Key:   key,
			Value: value,
		})
		return true
	})

	snapshot := serializedSnapshot{
		KVPairs:    kvPairs,
		Krs:        s.krs,
		NumEntries: s.NumEntries,
	}

	encoder := gob.NewEncoder(writer)
	return encoder.Encode(snapshot)
}

// RecoverFromSnapshot recovers the state machine from a snapshot.
func (s *KVStore) RecoverFromSnapshot(reader io.Reader, _ []statemachine.SnapshotFile, _ <-chan struct{}) error {
	decoder := gob.NewDecoder(reader)

	var snapshot serializedSnapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return err
	}

	for _, kv := range snapshot.KVPairs {
		s.Data.Store(kv.Key, kv.Value)
	}

	s.krs = snapshot.Krs

	return nil
}

// Close releases any resources held by the state machine.
func (s *KVStore) Close() error {
	// Clean up resources here if necessary
	return nil
}
