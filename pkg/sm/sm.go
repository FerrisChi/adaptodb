package sm

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"

	"github.com/lni/dragonboat/v3/statemachine"
)

type KVStore struct {
	data map[string]string // The key-value data store
}

// NewKVStore initializes and returns a new KVStore state machine instance.
func NewKVStore(shardID uint64, replicaID uint64) statemachine.IStateMachine {
	return &KVStore{
		data: make(map[string]string),
	}
}

// Lookup performs a read-only query. This is used to serve read requests.
func (s *KVStore) Lookup(query interface{}) (interface{}, error) {
	key, ok := query.(string)
	if !ok {
		return nil, errors.New("invalid query type")
	}
	value, exists := s.data[key]
	if !exists {
		return nil, errors.New("key not found")
	}
	return value, nil
}

// Update applies a mutation to the state machine. This handles write requests.
func (s *KVStore) Update(data []byte) (statemachine.Result, error) {
	kv := bytes.SplitN(data, []byte("="), 2)
	if len(kv) != 2 {
		return statemachine.Result{}, errors.New("invalid data format")
	}
	key, value := string(kv[0]), string(kv[1])
	s.data[key] = value
	return statemachine.Result{Value: uint64(len(data))}, nil
}

// SaveSnapshot saves the current state for recovery in case of failure.
func (s *KVStore) SaveSnapshot(writer io.Writer, _ statemachine.ISnapshotFileCollection, _ <-chan struct{}) error {
	encoder := gob.NewEncoder(writer)
	return encoder.Encode(s.data)
}

// RecoverFromSnapshot restores the state machine's data from a snapshot.
func (s *KVStore) RecoverFromSnapshot(reader io.Reader, _ []statemachine.SnapshotFile, _ <-chan struct{}) error {
	decoder := gob.NewDecoder(reader)
	return decoder.Decode(&s.data)
}

// Close releases any resources held by the state machine.
func (s *KVStore) Close() error {
	// Clean up resources here if necessary
	return nil
}
