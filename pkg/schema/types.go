// pkg/schema/types.go
package schema

type Config struct {
	RaftGroups []RaftGroup `yaml:"raftGroups"`
}

type NodeInfo struct {
	Address    string `yaml:"address"`              // Required
	User       string `yaml:"user,omitempty"`       // Optional, for remote nodes
	Host       string `yaml:"host,omitempty"`       // Optional
	SSHKeyPath string `yaml:"sshKeyPath,omitempty"` // Optional, for remote nodes
}

type RaftGroup struct {
	ShardID uint64              `yaml:"shardID"`
	Members map[uint64]NodeInfo `yaml:"members"`
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

type WriteRequest struct {
	Key   string
	Value string
}

func KeyRangeToString(keyRanges []KeyRange) string {
	str := ""
	for _, kr := range keyRanges {
		if str != "" {
			str += ","
		}
		str += kr.Start + "-" + kr.End
	}
	return str
}
