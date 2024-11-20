package metadata

import (
	"adaptodb/pkg/schema"
	"fmt"
	"log"
)

type Metadata struct {
	KeysToShardID map[string]uint64 `yaml:"keys_to_shard_id" json:"keys_to_shard_id"`
	shardMapping  []schema.ShardMetadata // A list of shard mappings
	config        *schema.Config
}

func NewMetadata(config *schema.Config) (*Metadata, error) {
	m := &Metadata{
		KeysToShardID: make(map[string]uint64),
		config:        config,
	}

	// Initialize shard mapping from config
	batch := 26 / len(config.RaftGroups)
	for idx, group := range config.RaftGroups {
		startchar := 'a' + (idx * batch)
		endchar := 'a' + ((idx + 1) * batch)
		if idx == len(config.RaftGroups)-1 {
			endchar = '{'
		}
		log.Printf("Shard %d: %c-%c\n", group.ShardID, startchar, endchar)
		m.shardMapping = append(m.shardMapping, schema.ShardMetadata{
			ShardID: group.ShardID,
			KeyRange: schema.KeyRange{
				// Initialize with full range - will be adjusted during reconciliation
				Start: string(startchar),
				End:   string(endchar),
			},
		})
	}
	return m, nil
}

// TODO: use etcd
// func (m *Metadata) InitializeFromEtcd(etcdClient *etcd.Client) error {
// 	// Get existing shard config from etcd
//     existingConfig, err := m.getEtcdShardConfig(etcdClient)
//     if err != nil {
//         return fmt.Errorf("failed to get etcd config: %w", err)
//     }

//     // Reconcile configurations
//     if err := m.reconcileConfigurations(existingConfig); err != nil {
//         return fmt.Errorf("failed to reconcile configurations: %w", err)
//     }

//     // Store reconciled config back to etcd
//     if err := m.saveToEtcd(etcdClient); err != nil {
//         return fmt.Errorf("failed to save reconciled config: %w", err)
//     }

// 	return nil
// }

func (m *Metadata) AddKey(key string, shardID uint64) {
	if m.KeysToShardID == nil {
		m.KeysToShardID = make(map[string]uint64)
	}
	m.KeysToShardID[key] = shardID
}

// GetShardForKey returns the shard ID for a given key
func (ms *Metadata) GetShardForKey(key string) (uint64, error) {
	for _, shard := range ms.shardMapping {
		log.Println(shard.KeyRange.Start, shard.KeyRange.End, key, key >= shard.KeyRange.Start, key < shard.KeyRange.End)
		if key >= shard.KeyRange.Start && key < shard.KeyRange.End {
			return shard.ShardID, nil
		}
	}
	log.Printf("Key %s not found in any shard", key)
	return 0, fmt.Errorf("key not found in any shard")
}

func (ms *Metadata) UpdateShardKeyRange(shardID uint64, keyRange schema.KeyRange) error {
	for i, shard := range ms.shardMapping {
		if shard.ShardID == shardID {
			ms.shardMapping[i].KeyRange = keyRange
			return nil
		}
	}
	// Add new shard
	ms.shardMapping = append(ms.shardMapping, schema.ShardMetadata{
		ShardID:  shardID,
		KeyRange: keyRange,
	})
	return nil
}

func (ms *Metadata) GetShardKeyRange(shardID uint64) (schema.KeyRange, error) {
	for _, shard := range ms.shardMapping {
		if shard.ShardID == shardID {
			return shard.KeyRange, nil
		}
	}
	return schema.KeyRange{}, fmt.Errorf("shard not found")
}

// GetAllShardKeyRanges returns all key ranges for each shard
func (ms *Metadata) GetAllShardKeyRanges() (map[uint64][]schema.KeyRange, error) {
	ranges := make(map[uint64][]schema.KeyRange)
	for _, shard := range ms.shardMapping {
		ranges[shard.ShardID] = append(ranges[shard.ShardID], shard.KeyRange)
	}
	return ranges, nil
}
