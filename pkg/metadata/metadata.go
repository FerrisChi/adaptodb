package metadata

import (
	"adaptodb/pkg/schema"
	"context"
	"fmt"
	"log"

	pb "adaptodb/pkg/proto/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Metadata struct {
	KeysToShardID map[string]uint64 `yaml:"keys_to_shard_id" json:"keys_to_shard_id"`
	Config        *schema.Config
}

func NewMetadata(config *schema.Config) (*Metadata, error) {
	m := &Metadata{
		KeysToShardID: make(map[string]uint64),
		Config:        config,
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
		m.Config.RaftGroups[idx].KeyRanges = append(m.Config.RaftGroups[idx].KeyRanges, schema.KeyRange{
			// Initialize with full range - will be adjusted during reconciliation
			Start: string(startchar),
			End:   string(endchar),
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
	for _, shard := range ms.Config.RaftGroups {
		for _, keyRange := range shard.KeyRanges {
			if key >= keyRange.Start && key < keyRange.End {
				return shard.ShardID, nil
			}
		}
	}
	log.Printf("Key %s not found in any shard", key)
	return 0, fmt.Errorf("key not found in any shard")
}

// func (ms *Metadata) UpdateShardKeyRange(shardID uint64, keyRange schema.KeyRange) error {
// 	for i, shard := range ms.config.RaftGroups {
// 		if shard.ShardID == shardID {
// 			ms.config.RaftGroups[i].KeyRange = keyRange
// 			return nil
// 		}
// 	}
// 	// Add new shard
// 	ms.shardMapping = append(ms.shardMapping, schema.ShardMetadata{
// 		ShardID:  shardID,
// 		KeyRange: keyRange,
// 	})
// 	return nil
// }

func (ms *Metadata) GetShardKeyRanges(shardID uint64) []schema.KeyRange {
	for _, shard := range ms.Config.RaftGroups {
		if shard.ShardID == shardID {
			return shard.KeyRanges
		}
	}
	return nil
}

// GetAllShardKeyRanges returns all key ranges for each shard
func (ms *Metadata) GetAllShardKeyRanges() (map[uint64][]schema.KeyRange, error) {
	ranges := make(map[uint64][]schema.KeyRange)
	for _, shard := range ms.Config.RaftGroups {
		ranges[shard.ShardID] = shard.KeyRanges
	}
	return ranges, nil
}

func (ms *Metadata) GetAllShardMembers() []schema.RaftGroup {
	return ms.Config.RaftGroups
}

// GetNodeInfo returns the node info for a given node ID, {} if not found
func (ms *Metadata) GetNodeInfo(nodeID uint64) schema.NodeInfo {
	for _, group := range ms.Config.RaftGroups {
		for _, node := range group.Members {
			if node.ID == nodeID {
				return node
			}
		}
	}
	return schema.NodeInfo{}
}

// GetNodeInfoBatch returns the node info for a given list of node IDs
func (ms *Metadata) GetNodeInfoBatch(nodeIDs []uint64) []schema.NodeInfo {
	nodeInfo := make([]schema.NodeInfo, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		nodeInfo = append(nodeInfo, ms.GetNodeInfo(nodeID))
	}
	return nodeInfo
}

// GetShard returns the Raft cluster configuration
func (ms *Metadata) GetShard(shardID uint64) schema.RaftGroup {
	for _, group := range ms.Config.RaftGroups {
		if group.ShardID == shardID {
			return group
		}
	}
	return schema.RaftGroup{}
}

// GetShardBatch returns the Raft cluster configuration for a list of shard IDs
func (ms *Metadata) GetShardBatch(shardIDs []uint64) []schema.RaftGroup {
	groups := make([]schema.RaftGroup, 0, len(shardIDs))
	for _, shardID := range shardIDs {
		groups = append(groups, ms.GetShard(shardID))
	}
	return groups
}

func (ms *Metadata) GetNumNodes() int {
	numNodes := 0
	for _, group := range ms.Config.RaftGroups {
		numNodes += len(group.Members)
	}
	return numNodes
}

func (ms *Metadata) UpdateMigratedKeyRanges(fromShardId, toShardId uint64, keyRanges []schema.KeyRange) {
	for i, shard := range ms.Config.RaftGroups {
		if shard.ShardID == fromShardId {
			ms.Config.RaftGroups[i].KeyRanges = schema.RemoveKeyRanges(ms.Config.RaftGroups[i].KeyRanges, keyRanges)
		}
		if shard.ShardID == toShardId {
			ms.Config.RaftGroups[i].KeyRanges = schema.AddKeyRanges(ms.Config.RaftGroups[i].KeyRanges, keyRanges)
		}
	}
}

// UpdateKeyRangeFromNode updates the key ranges for each shard from the first node in the shard
func (ms *Metadata) UpdateKeyRangeFromNode() {
	for i := range ms.Config.RaftGroups {
		shard := &ms.Config.RaftGroups[i]
		for _, node := range shard.Members {
			conn, err := grpc.NewClient(node.GrpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("failed to connect to node %d: %v", node.ID, err)
				continue
			}
			defer conn.Close()
			client := pb.NewNodeRouterClient(conn)
			resp, err := client.GetKeyRanges(context.Background(), &pb.GetKeyRangesRequest{ClusterID: shard.ShardID})
			if err != nil {
				log.Printf("failed to get shard mapping from node %d: %v", node.ID, err)
				continue
			}
			shard.KeyRanges = schema.ParseKeyRanges(resp.KeyRanges)
			log.Printf("Updated shard %d key ranges from node %d: %v", shard.ShardID, node.ID, shard.KeyRanges)
			break
		}
	}
}
