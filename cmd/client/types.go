package client

// Operation types for database requests
const (
	OpRead  = "read" // Uppercase for exported constants
	OpWrite = "write"
)

// RequestParams holds the parsed parameters from HTTP requests
type RequestParams struct { // Uppercase for exported struct
	Operation string
	Key       string
	Value     string
}
