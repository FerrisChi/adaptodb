package metadata

type Metadata struct {
	KeysToShardID map[string]uint64
}

func NewMetadata() (*Metadata, error) {
	return &Metadata{}, nil
}

func (m *Metadata) AddKey(key string, shardID uint64) {
	if m.KeysToShardID == nil {
		m.KeysToShardID = make(map[string]uint64)
	}
	m.KeysToShardID[key] = shardID
}

func (m *Metadata) GetShardID(key string) (uint64, bool) {
	dummy_shardID := uint64(1)
	return dummy_shardID, true

	shardID, exists := m.KeysToShardID[key]
	return shardID, exists
}
