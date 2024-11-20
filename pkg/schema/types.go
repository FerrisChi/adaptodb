// pkg/schema/types.go
package schema

type Config struct {
	RaftGroups []RaftGroup `yaml:"raftGroups"`
}

type RaftGroup struct {
	ShardID uint64            `yaml:"shardID"`
	Members map[uint64]string `yaml:"members"`
}

type KeyRange struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

type ShardMetadata struct {
	ShardID  uint64   `json:"shardID"`
	KeyRange KeyRange `json:"keyRange"`
}

// BalanceAdvice represents the recommendation from the balancer
type BalanceAdvice struct {
	OverloadedShards  []string
	UnderloadedShards []string
	RecommendedMoves  map[string]string // source -> target shard
}
