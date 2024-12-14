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
	config *schema.Config
}

func NewMetadata(config *schema.Config) (*Metadata, error) {
	m := &Metadata{
		config: config,
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
		m.config.RaftGroups[idx].KeyRanges = append(m.config.RaftGroups[idx].KeyRanges, schema.KeyRange{
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

// GetShardForKey returns the shard ID for a given key
func (ms *Metadata) GetShardForKey(key string) (uint64, error) {
	for _, shard := range ms.config.RaftGroups {
		for _, keyRange := range shard.KeyRanges {
			if key >= keyRange.Start && key < keyRange.End {
				return shard.ShardID, nil
			}
		}
	}
	log.Printf("Key %s not found in any shard", key)
	return 0, fmt.Errorf("key not found in any shard")
}

// GetShardKeyRanges returns the key ranges for a given shard
func (ms *Metadata) GetShardKeyRanges(shardID uint64) []schema.KeyRange {
	for _, shard := range ms.config.RaftGroups {
		if shard.ShardID == shardID {
			ranges := make([]schema.KeyRange, len(shard.KeyRanges))
			copy(ranges, shard.KeyRanges)
			return ranges
		}
	}
	return nil
}

// GetAllShardKeyRanges returns all key ranges for each shard
func (ms *Metadata) GetAllShardKeyRanges() (map[uint64][]schema.KeyRange, error) {
	ranges := make(map[uint64][]schema.KeyRange)
	for _, shard := range ms.config.RaftGroups {
		rangeCopy := make([]schema.KeyRange, len(shard.KeyRanges))
		copy(rangeCopy, shard.KeyRanges)
		ranges[shard.ShardID] = rangeCopy
	}
	return ranges, nil
}

// GetConfig returns all Raft group configuration
func (ms *Metadata) GetConfig() []schema.RaftGroup {
	groups := make([]schema.RaftGroup, len(ms.config.RaftGroups))
	copy(groups, ms.config.RaftGroups)
	return groups
}

// GetNodeInfo returns the node info for a given node ID, {} if not found
func (ms *Metadata) GetNodeInfo(nodeID uint64) schema.NodeInfo {
	for _, group := range ms.config.RaftGroups {
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

// GetShard returns one raft group configuration
func (ms *Metadata) GetShard(shardID uint64) schema.RaftGroup {
	for _, group := range ms.config.RaftGroups {
		if group.ShardID == shardID {
			return group
		}
	}
	return schema.RaftGroup{}
}

// GetShardBatch returns raft group configuration for a list of shard IDs
func (ms *Metadata) GetShardBatch(shardIDs []uint64) []schema.RaftGroup {
	groups := make([]schema.RaftGroup, 0, len(shardIDs))
	for _, shardID := range shardIDs {
		groups = append(groups, ms.GetShard(shardID))
	}
	return groups
}

func (ms *Metadata) GetNumNodes() int {
	numNodes := 0
	for _, group := range ms.config.RaftGroups {
		numNodes += len(group.Members)
	}
	return numNodes
}

func (ms *Metadata) UpdateMigratedKeyRanges(fromShardId, toShardId uint64, keyRanges []schema.KeyRange) {
	for i, shard := range ms.config.RaftGroups {
		if shard.ShardID == fromShardId {
			ms.config.RaftGroups[i].KeyRanges = schema.RemoveKeyRanges(ms.config.RaftGroups[i].KeyRanges, keyRanges)
		}
		if shard.ShardID == toShardId {
			ms.config.RaftGroups[i].KeyRanges = schema.AddKeyRanges(ms.config.RaftGroups[i].KeyRanges, keyRanges)
		}
	}
}

// UpdateKeyRangeFromNode updates the key ranges for each shard from the first node in the shard
func (ms *Metadata) UpdateKeyRangeFromNode() {
	for i := range ms.config.RaftGroups {
		shard := &ms.config.RaftGroups[i]
		for _, node := range shard.Members {
			target := fmt.Sprintf("%s:%d", node.Address, schema.NodeGrpcPort+node.ID)
			conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

// UpdateNodeInfo update the node info for a given node ID
func (ms *Metadata) UpdateNodeInfo(nodeID uint64, nodeInfo schema.NodeInfo) {
	for i, group := range ms.config.RaftGroups {
		for j, node := range group.Members {
			if node.ID == nodeID {
				ms.config.RaftGroups[i].Members[j] = nodeInfo
				return
			}
		}
	}
}
