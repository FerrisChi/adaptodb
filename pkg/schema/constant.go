package schema

import "time"

// timeout
const (
	RAFT_LAUNCH_TIMEOUT     = 10 * time.Second
	NODE_LAUNCH_TIMEOUT     = 15 * time.Second
	READ_WRITE_TIMEOUT      = 5 * time.Second
	ADAPTODB_LAUNCH_TIMEOUT = 20 * time.Second
	MIGRATION_TIMEOUT       = 20 * time.Second
)

// port
const (
	NodeGrpcPort       = 51000
	NodeHttpPort       = 52000
	NodeStatsPort      = 53000
	NodeDragonboatPort = 54000
	NodeDebugPort      = 55000
)
