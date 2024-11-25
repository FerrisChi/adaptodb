// pkg/schema/types.go
package schema

type Config struct {
	RaftGroups []RaftGroup `yaml:"raftGroups"`
}

type RaftGroup struct {
	ShardID   uint64     `yaml:"shardID"`
	Members   []NodeInfo `yaml:"members"`
	KeyRanges []KeyRange `yaml:"keyRanges,omitempty"`
}

type NodeInfo struct {
	ID         uint64 `yaml:"id"`                   // Required
	Address    string `yaml:"address"`              // Required
	User       string `yaml:"user,omitempty"`       // Optional, for remote nodes
	Host       string `yaml:"host,omitempty"`       // Optional
	SSHKeyPath string `yaml:"sshKeyPath,omitempty"` // Optional, for remote nodes
}

type KeyRange struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

// BalanceAdvice represents the recommendation from the balancer
type BalanceAdvice struct {
	OverloadedShards  []string
	UnderloadedShards []string
	RecommendedMoves  map[string]string // source -> target shard
}

type Schedule struct {
	ShardID   uint64
	KeyRanges []KeyRange
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
