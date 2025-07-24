package transport

import "time"

// CompressionType defines the compression algorithm used
type CompressionType string

// Standard compression options
const (
	CompressionNone   CompressionType = "none"
	CompressionGzip   CompressionType = "gzip"
	CompressionSnappy CompressionType = "snappy"
)

// RetryPolicy defines how retries are handled
type RetryPolicy struct {
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	BackoffFactor  float64
	Jitter         float64
}

// TransportOptions contains common configuration across all transport types
type TransportOptions struct {
	Timeout        time.Duration
	RetryPolicy    RetryPolicy
	Compression    CompressionType
	MaxMessageSize int
	TLSEnabled     bool
	CertFile       string
	KeyFile        string
	CAFile         string
}

// TransportStatus contains information about the current transport state
type TransportStatus struct {
	// Connected     bool
	// LastConnected time.Time
	LastError     error
	BytesSent     uint64
	BytesReceived uint64
	RTT           time.Duration
}

type ClientConfig struct {
	Endpoint         string
	TransportOptions TransportOptions
}
