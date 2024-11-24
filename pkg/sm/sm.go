package sm

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"

	"github.com/lni/dragonboat/v3/statemachine"
)

type KVStore struct {
	Data map[string]string `yaml:"data" json:"data"` // The key-value data store
}

// NewKVStore initializes and returns a new KVStore state machine instance.
func NewKVStore(shardID uint64, replicaID uint64) statemachine.IStateMachine {
	return &KVStore{
		Data: make(map[string]string),
	}
}

// Lookup performs a read-only query. This is used to serve read requests.
func (s *KVStore) Lookup(query interface{}) (interface{}, error) {
	key, ok := query.(string)
	if !ok {
		return nil, errors.New("invalid query type")
	}
	value, exists := s.Data[key]
	if !exists {
		return nil, errors.New("key not found")
	}
	return value, nil
}

// Update applies a mutation to the state machine. This handles write requests.
func (s *KVStore) Update(data []byte) (statemachine.Result, error) {
	ops := bytes.SplitN(data, []byte(";"), -1)
	succ := 0
	for _, op := range ops {
		tmp := bytes.SplitN(op, []byte(":"), -1)
		if len(tmp) != 2 {
			// invalid operation format
			continue
		}
		opType, params := string(tmp[0]), tmp[1]
		switch opType {
		case "write":
			kv := bytes.SplitN(params, []byte(","), 2)
			if len(kv) != 2 {
				// invalid write format
				continue
			}
			key, value := string(kv[0]), string(kv[1])
			s.Data[key] = value
			succ++
		default:
			// unknown operation
			continue
		}
	}

	return statemachine.Result{Value: uint64(succ)}, nil
}

// SaveSnapshot saves the current state for recovery in case of failure.
func (s *KVStore) SaveSnapshot(writer io.Writer, _ statemachine.ISnapshotFileCollection, _ <-chan struct{}) error {
	encoder := gob.NewEncoder(writer)
	return encoder.Encode(s.Data)
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
