package schema

import "time"

// timeout
const RAFT_LAUNCH_TIMEOUT = 10 * time.Second
const NODE_LAUNCH_TIMEOUT = 15 * time.Second
const READ_WRITE_TIMEOUT = 5 * time.Second
const ADAPTODB_LAUNCH_TIMEOUT = 20 * time.Second
const MIGRATION_TIMEOUT = 20 * time.Second
