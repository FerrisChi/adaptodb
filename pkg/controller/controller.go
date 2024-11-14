package controller

import (
	"adaptodb/pkg/balancer"
	"adaptodb/pkg/metadata"
)

type Controller struct {
	balancer *balancer.Balancer
	metadata *metadata.Metadata
}

func NewController(balancer *balancer.Balancer, metadata *metadata.Metadata) *Controller {
	return &Controller{balancer: balancer, metadata: metadata}
}

func (sc *Controller) Start() {
	// Implement shard management logic here
}

func (sc *Controller) Stop() {
	// Implement shutdown logic here
}
