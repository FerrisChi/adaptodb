package balancer

import metadata "adaptodb/pkg/metadata"

type Balancer struct {
	metadata *metadata.Metadata `yaml:"metadata" json:"metadata"`
}

func NewBalancer(mm *metadata.Metadata) *Balancer {
	return &Balancer{metadata: mm}
}

func (bw *Balancer) StartMonitoring() {
	// Implement monitoring logic here
}

func (bw *Balancer) Stop() {
	// Implement shutdown logic here
}
