package sm

import (
	"adaptodb/pkg/schema"
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"

	"sync"

	"github.com/lni/dragonboat/v3/statemachine"
)

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
	Value      string
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
		return nil, errors.New("invalid query type")
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
					return LookupResult{Value: "", NumEntries: 1}, errors.New("key not found")
				}
				return LookupResult{Value: value.(string), NumEntries: s.NumEntries}, nil
			}
		}
		for _, kr := range s.migrate_krs {
			if key >= kr.Start && key < kr.End {
				return LookupResult{Value: "", NumEntries: s.NumEntries}, errors.New("key in migration")
			}
		}
		return LookupResult{Value: "", NumEntries: s.NumEntries}, errors.New("key not managed by this node")
	case "schedule":
		return s.krs, nil
	default:
		return LookupResult{Value: "", NumEntries: s.NumEntries}, errors.New("invalid query")
	}
}

// Update applies a mutation to the state machine. This handles write requests.
// Returns status 1 if an old entry is modified, 2 if a new entry is added.
// opType:params
func (s *KVStore) Update(data []byte) (statemachine.Result, error) {
	tmp := bytes.SplitN(data, []byte(":"), 2)
	if len(tmp) != 2 {
		// invalid operation format
		return statemachine.Result{}, errors.New("invalid operation format")
	}
	opType, params := string(tmp[0]), tmp[1]
	log.Println("opType", opType, "params", string(params))
	switch opType {
	case "write":
		kv := bytes.SplitN(params, []byte(","), 2)
		if len(kv) != 2 {
			// invalid write format
			return statemachine.Result{}, errors.New("invalid write format")
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
		return statemachine.Result{}, errors.New("key not managed by this node")
	case "remove":
		key := string(params)
		s.Data.Delete(key)
		return statemachine.Result{Value: 1, Data: []byte(fmt.Sprintf("Key %s removed successfully.", key))}, nil
	case "migrate_src":
		// Format: migrate_src:taskId,leaderId,ctrlAddress,toAddress,keyRange
		kv := bytes.SplitN(params, []byte(","), 5)
		if len(kv) != 5 {
			return statemachine.Result{}, errors.New("invalid migrate format")
		}
		taskId, err := strconv.ParseUint(string(kv[0]), 10, 64)
		if err != nil {
			return statemachine.Result{}, errors.New("invalid task ID")
		}
		leaderId, err := strconv.ParseUint(string(kv[1]), 10, 64)
		if err != nil {
			return statemachine.Result{}, errors.New("invalid leader ID")
		}
		if s.migrationTaskId != 0 {
			return statemachine.Result{}, errors.New("another migration task in progress")
		}
		toAddress := string(kv[3])
		krs := schema.ParseKeyRanges(kv[4])
		s.migrationTaskId = taskId
		s.migrate_krs = krs
		// remove key ranges from local group
		s.krs = schema.RemoveKeyRanges(s.krs, krs)

		if s.nodeId != leaderId {
			return statemachine.Result{}, nil
		}
		// use a new go routine to transfer data to node in target address
		go func(data *sync.Map) {
			err := startTransfer(taskId, toAddress, krs, data)
			if err != nil {
				log.Println("Error transferring data:", err)
			}
		}(&s.Data)
		return statemachine.Result{Value: 1, Data: []byte(fmt.Sprintf("Migration task %d started successfully on source.", taskId))}, nil
	case "migrate_dst":
		// Format: migrate_dst:taskId,strlAddress,fromAddress,keyRange
		kv := bytes.SplitN(params, []byte(","), 4)
		if len(kv) != 4 {
			return statemachine.Result{}, errors.New("invalid migrate format")
		}
		taskId, err := strconv.ParseUint(string(kv[0]), 10, 64)
		if err != nil {
			return statemachine.Result{}, errors.New("invalid task ID")
		}
		if s.migrationTaskId != 0 {
			return statemachine.Result{}, errors.New("another migration task in progress")
		}
		krs := schema.ParseKeyRanges(kv[3])
		s.migrationTaskId = taskId
		s.migrate_krs = krs
		return statemachine.Result{Value: 1, Data: []byte(fmt.Sprintf("Migration task %d started successfully on destination.", taskId))}, nil
	case "batch_write":
		// Format: batch_write:taskId,key1,value1|key2,value2|...
		params := bytes.SplitN(params, []byte(","), 2)
		taskId, err := strconv.ParseUint(string(params[0]), 10, 64)
		if err != nil {
			return statemachine.Result{}, errors.New("invalid task ID")
		}
		if s.migrationTaskId != taskId {
			return statemachine.Result{}, errors.New("task ID mismatch")
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
			succ++
		}
		return statemachine.Result{Value: uint64(succ), Data: []byte(fmt.Sprintf("%d data written successfully.", succ))}, nil
	case "apply_schedule":
		// Format: apply_schedule:flag,krs
		params := bytes.Split(params, []byte(","))
		flag := string(params[0])
		krs := schema.ParseKeyRanges(params[1])
		cnt := s.updateSchedule(flag, krs)
		s.migrationTaskId = 0
		s.migrate_krs = nil
		log.Println("Applied Key Ranges", s.krs)
		return statemachine.Result{Value: 1, Data: []byte("Schedule" + string(params[1]) + " applied." + strconv.FormatUint(cnt, 10) + " entries modified.")}, nil
	default:
		return statemachine.Result{}, errors.New("unknown operation type")
	}
}

// SaveSnapshot saves the current state for recovery in case of failure.
func (s *KVStore) SaveSnapshot(writer io.Writer, _ statemachine.ISnapshotFileCollection, _ <-chan struct{}) error {
	encoder := gob.NewEncoder(writer)
	return encoder.Encode(&s.Data)
}

// RecoverFromSnapshot restores the state machine's data from a snapshot.
func (s *KVStore) RecoverFromSnapshot(reader io.Reader, _ []statemachine.SnapshotFile, _ <-chan struct{}) error {
	decoder := gob.NewDecoder(reader)
	return decoder.Decode(&s.Data)
}

// Close releases any resources held by the state machine.
func (s *KVStore) Close() error {
	// Clean up resources here if necessary
	return nil
}
