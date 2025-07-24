// Copyright 2025 Jeremy Tregunna
// Copyright 2025 Sreram K (sreramk360@gmail.com)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/chzyer/readline"

	"github.com/KevoDB/kevo/pkg/engine"
	"github.com/KevoDB/kevo/pkg/grpc/transport"
	"github.com/KevoDB/kevo/pkg/transaction"

	// Import transaction package to register the transaction creator
	_ "github.com/KevoDB/kevo/pkg/transaction"
)

// Command completer for readline
var completer = readline.NewPrefixCompleter(
	readline.PcItem(".help"),
	readline.PcItem(".open"),
	readline.PcItem(".close"),
	readline.PcItem(".exit"),
	readline.PcItem(".stats"),
	readline.PcItem(".flush"),
	readline.PcItem("BEGIN",
		readline.PcItem("TRANSACTION"),
		readline.PcItem("READONLY"),
	),
	readline.PcItem("COMMIT"),
	readline.PcItem("ROLLBACK"),
	readline.PcItem("PUT"),
	readline.PcItem("GET"),
	readline.PcItem("DELETE"),
	readline.PcItem("SCAN",
		readline.PcItem("RANGE"),
		readline.PcItem("SUFFIX"),
	),
)

const helpText = `
Kevo (kevo) - A lightweight, minimalist, storage engine.

Usage:
  kevo [options] [database_path]  - Start with an optional database path

Options:
  -server                 - Run in server mode, exposing a gRPC API
  -daemon                 - Run in daemon mode (detached from terminal)
  -address string         - Address to listen on in server mode (default "localhost:50051")

Commands (interactive mode only):
  .help                   - Show this help message
  .open PATH              - Open a database at PATH
  .close                  - Close the current database
  .exit                   - Exit the program
  .stats                  - Show database statistics
  .flush                  - Force flush memtables to disk

  BEGIN [TRANSACTION]     - Begin a transaction (default: read-write)
  BEGIN READONLY          - Begin a read-only transaction
  COMMIT                  - Commit the current transaction
  ROLLBACK                - Rollback the current transaction

  PUT key value           - Store a key-value pair
  GET key                 - Retrieve a value by key
  DELETE key              - Delete a key-value pair

  SCAN                    - Scan all key-value pairs
  SCAN prefix             - Scan key-value pairs with given prefix
  SCAN SUFFIX suffix      - Scan key-value pairs with given suffix
  SCAN RANGE start end    - Scan key-value pairs in range [start, end)
                          - Note: start and end are treated as string keys, not numeric indices
`

// Config holds the application configuration
type Config struct {
	ServerMode  bool
	DaemonMode  bool
	ListenAddr  string
	DBPath      string
	TLSEnabled  bool
	TLSCertFile string
	TLSKeyFile  string
	TLSCAFile   string

	// Replication settings
	ReplicationEnabled bool
	ReplicationMode    string // "primary", "replica", or "standalone"
	ReplicationAddr    string // Address for replication service
	PrimaryAddr        string // Address of primary (for replicas)

	// Client configuration
	ClientOptions *transport.ClientConfig
}

// go run ./cmd/kevo -server db/
// go run ./cmd/kevo -client
func main() {

	// Open database if path provided
	var eng *engine.EngineFacade
	var err error

	// Parse command line arguments and get configuration
	config, err := parseFlags()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error in validating options: %s\n", err)
		os.Exit(1)
	}

	// Check if we should run in server mode
	if config.ServerMode {

		if config.DBPath == "" {
			fmt.Fprintf(os.Stderr, "Error DBPath must be provided in server mode")
			os.Exit(1)
		}
		fmt.Printf("Opening database at %s\n", config.DBPath)
		// Use the new facade-based engine implementation
		eng, err = engine.NewEngineFacade(config.DBPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening database: %s\n", err)
			os.Exit(1)
		}
		defer eng.Close()

		if eng == nil {
			fmt.Fprintf(os.Stderr, "Error: Server mode requires a database path\n")
			os.Exit(1)
		}

		runServer(eng, *config)
		return
	}

	// Run in interactive mode
	runInteractive(config.ClientOptions)
}

var ErrAmbiguousOperatingMode = errors.New("Ambiguous operating mode. Application started with both server mode and client mode enabled")
var ErrNoModeSelected = errors.New("No modes selected")

// parseFlags parses command line flags and returns a Config
func parseFlags() (*Config, error) {

	fmt.Println("Args: ", os.Args)
	// Define custom usage message
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Kevo - A lightweight key-value storage engine\n\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: kevo [options] [database_path]\n\n")
		fmt.Fprintf(flag.CommandLine.Output(), "By default, kevo runs in interactive mode with a command-line interface.\n")
		fmt.Fprintf(flag.CommandLine.Output(), "If -server flag is provided, kevo runs as a server exposing a gRPC API.\n\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(), "\nInteractive mode commands (when not using -server):\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  PUT key value           - Store a key-value pair\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  GET key                 - Retrieve a value by key\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  DELETE key              - Delete a key-value pair\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  SCAN                    - Scan all key-value pairs\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  BEGIN TRANSACTION       - Begin a read-write transaction\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  BEGIN READONLY          - Begin a read-only transaction\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  COMMIT                  - Commit the current transaction\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  ROLLBACK                - Rollback the current transaction\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  .help                   - Show detailed help\n")
		fmt.Fprintf(flag.CommandLine.Output(), "  .exit                   - Exit the program\n\n")
		fmt.Fprintf(flag.CommandLine.Output(), "For more details, start kevo and type .help\n")
	}

	serverMode := flag.Bool("server", false, "Run in server mode, exposing a gRPC API")
	daemonMode := flag.Bool("daemon", false, "Run in daemon mode (detached from terminal)")
	listenAddr := flag.String("address", "localhost:50051", "Address to listen on in server mode")

	// Client configuration
	clientMode := flag.Bool("client", false, "Enable client mode. This connects with a remote server process with CLI")
	endpoint := flag.String("endpoint", "localhost:50051", "Host of a running Kevo server")
	connectTimeout := flag.Int64("connect-timeout", int64(time.Second*5), "Timeout for establishing connection to the Kevo server")

	// Retry options (unimplemented)
	maxRetries := flag.Int("max-retries", 3, "Unimplemented: this option exists for future use")
	initialBackoff := flag.Int64("initial-backoff", int64(time.Millisecond*100), "Unimplemented: this option exists for future use")
	maxBackoff := flag.Int64("max-backoff", int64(time.Second*2), "Unimplemented: this option exists for future use")
	backoffFactor := flag.Float64("backoff-factor", float64(1.5), "Unimplemented: this option exists for future use")
	retryJitter := flag.Float64("retry-jitter", float64(0.2), "Unimplemented: this option exists for future use")

	// Performance options
	compression := flag.String("compression-type", string(transport.CompressionNone), "Compression type to use")
	maxMsgSize := flag.Int("max-message-size", int(16*1024*1024), "caps the size of messages.")

	// TLS options
	tlsEnabled := flag.Bool("tls", false, "Enable TLS for secure connections")
	tlsCertFile := flag.String("cert", "", "TLS certificate file path")
	tlsKeyFile := flag.String("key", "", "TLS private key file path")
	tlsCAFile := flag.String("ca", "", "TLS CA certificate file for client verification")

	// Replication options
	replicationEnabled := flag.Bool("replication", false, "Enable replication")
	replicationMode := flag.String("replication-mode", "standalone", "Replication mode: primary, replica, or standalone")
	replicationAddr := flag.String("replication-address", "localhost:50052", "Address for replication service")
	primaryAddr := flag.String("primary", "localhost:50052", "Address of primary node (for replicas)")

	// Parse flags
	flag.Parse()

	if *serverMode && *clientMode {
		return nil, ErrAmbiguousOperatingMode
	}

	if !*serverMode && !*clientMode {
		return nil, ErrNoModeSelected
	}

	var clientOptions *transport.ClientConfig

	if *clientMode {
		clientOptions = &transport.ClientConfig{
			Endpoint: *endpoint,
			TransportOptions: transport.TransportOptions{
				Timeout: time.Duration(*connectTimeout),
				RetryPolicy: transport.RetryPolicy{
					MaxRetries:     *maxRetries,
					InitialBackoff: time.Duration(*initialBackoff),
					MaxBackoff:     time.Duration(*maxBackoff),
					BackoffFactor:  *backoffFactor,
					Jitter:         *retryJitter,
				},
				Compression:    transport.CompressionType(*compression),
				MaxMessageSize: *maxMsgSize,
				TLSEnabled:     *tlsEnabled,
				CertFile:       *tlsCertFile,
				KeyFile:        *tlsKeyFile,
				CAFile:         *tlsCAFile,
			},
		}

	}

	// Get database path from remaining arguments
	var dbPath string
	if flag.NArg() > 0 {
		dbPath = flag.Arg(0)
	}

	// Debug output for flag values
	fmt.Printf("DEBUG: Parsed flags: replication=%v, mode=%s, addr=%s, primary=%s\n",
		*replicationEnabled, *replicationMode, *replicationAddr, *primaryAddr)

	config := &Config{
		ServerMode:  *serverMode,
		DaemonMode:  *daemonMode,
		ListenAddr:  *listenAddr,
		DBPath:      dbPath,
		TLSEnabled:  *tlsEnabled,
		TLSCertFile: *tlsCertFile,
		TLSKeyFile:  *tlsKeyFile,
		TLSCAFile:   *tlsCAFile,

		// Replication settings
		ReplicationEnabled: *replicationEnabled,
		ReplicationMode:    *replicationMode,
		ReplicationAddr:    *replicationAddr,
		PrimaryAddr:        *primaryAddr,
	}

	config.ClientOptions = clientOptions

	fmt.Printf("DEBUG: Config created: ReplicationEnabled=%v, ReplicationMode=%s\n",
		config.ReplicationEnabled, config.ReplicationMode)

	return config, nil
}

// runServer initializes and runs the Kevo server
func runServer(eng *engine.EngineFacade, config Config) {
	// Set up daemon mode if requested
	if config.DaemonMode {
		setupDaemonMode()
	}

	// Create and start the server
	fmt.Printf("DEBUG: Before server creation: ReplicationEnabled=%v, ReplicationMode=%s\n",
		config.ReplicationEnabled, config.ReplicationMode)

	server := NewServer(eng, config)

	// Start the server (non-blocking)
	if err := server.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Error starting server: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Kevo server started on %s\n", config.ListenAddr)

	// Set up signal handling for graceful shutdown
	setupGracefulShutdown(server, eng)

	// Start serving (blocking)
	if err := server.Serve(); err != nil {
		fmt.Fprintf(os.Stderr, "Error serving: %v\n", err)
		os.Exit(1)
	}
}

// setupDaemonMode configures process to run as a daemon
func setupDaemonMode() {
	// Redirect standard file descriptors to /dev/null
	null, err := os.OpenFile("/dev/null", os.O_RDWR, 0)
	if err != nil {
		log.Fatalf("Failed to open /dev/null: %v", err)
	}

	// Redirect standard file descriptors to /dev/null
	err = syscall.Dup2(int(null.Fd()), int(os.Stdin.Fd()))
	if err != nil {
		log.Fatalf("Failed to redirect stdin: %v", err)
	}

	err = syscall.Dup2(int(null.Fd()), int(os.Stdout.Fd()))
	if err != nil {
		log.Fatalf("Failed to redirect stdout: %v", err)
	}

	err = syscall.Dup2(int(null.Fd()), int(os.Stderr.Fd()))
	if err != nil {
		log.Fatalf("Failed to redirect stderr: %v", err)
	}

	// Create a new process group
	_, err = syscall.Setsid()
	if err != nil {
		log.Fatalf("Failed to create new session: %v", err)
	}

	fmt.Println("Daemon mode enabled, detaching from terminal...")
}

// setupGracefulShutdown configures graceful shutdown on signals
func setupGracefulShutdown(server *Server, eng *engine.Engine) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		fmt.Printf("\nReceived signal %v, shutting down...\n", sig)

		// Graceful shutdown logic
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Shut down the server
		if err := server.Shutdown(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Error shutting down server: %v\n", err)
		}

		// The engine will be closed by the defer in main()

		fmt.Println("Shutdown complete")
		os.Exit(0)
	}()
}

type TxState struct {
	TxActive bool
	// TxId     string
	TxMode transaction.TransactionMode
}

func (t *TxState) SetTx(txMode transaction.TransactionMode) {
	t.TxActive = true
	// t.TxId = txId
	t.TxMode = txMode
}

func (t *TxState) Reset() {
	t.TxActive = false
	// t.TxId = ""
	t.TxMode = transaction.UnknownTxMode
}

// runInteractive starts the interactive CLI mode
func runInteractive(ClientOptions *transport.ClientConfig) {
	fmt.Println("Kevo (kevo) version 1.0.2")
	fmt.Println("Enter .help for usage hints.")

	// var tx interfaces.Transaction
	var txState TxState
	var err error

	cl, _ := transport.NewGRPCClient(ClientOptions.Endpoint,
		ClientOptions.TransportOptions)

	err = cl.Connect(context.Background())

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error establishing connection: %s\n", err)
		os.Exit(1)
	}

	// Setup readline with history support
	historyFile := filepath.Join(os.TempDir(), ".kevo_history")
	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "kevo> ",
		HistoryFile:     historyFile,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
		AutoComplete:    completer,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing readline: %s\n", err)
		os.Exit(1)
	}
	defer rl.Close()

	for {
		txInfo := cl.GetCachedTxInfo()
		txState.TxActive = txInfo.IsActive
		txState.TxMode = txInfo.Mode
		// Update prompt based on current state
		var prompt string
		if txState.TxActive {
			switch txState.TxMode {
			case transaction.ReadOnly:
				prompt = fmt.Sprintf("kevo:%s[read only]> ", ClientOptions.Endpoint)
			case transaction.ReadWriteSerialized:
				prompt = fmt.Sprintf("kevo:%s[serialized]> ", ClientOptions.Endpoint)
			case transaction.WriteReadCommitted:
				prompt = fmt.Sprintf("kevo:%s[read committed]> ", ClientOptions.Endpoint)
			default:
				prompt = fmt.Sprintf("kevo:%s[unknown]> ", ClientOptions.Endpoint)
			}
		} else {
			prompt = fmt.Sprintf("kevo:%s> ", ClientOptions.Endpoint)
		}
		rl.SetPrompt(prompt)

		// Read command
		line, readErr := rl.Readline()
		if readErr != nil {
			if readErr == readline.ErrInterrupt {
				if len(line) == 0 {
					break
				} else {
					continue
				}
			} else if readErr == io.EOF {
				fmt.Println("Goodbye!")
				break
			}
			fmt.Fprintf(os.Stderr, "Error reading input: %s\n", readErr)
			continue
		}

		// Line is already trimmed by readline
		if line == "" {
			continue
		}

		// Process command
		parts := strings.Fields(line)
		cmd := strings.ToUpper(parts[0])

		// Special dot commands
		if strings.HasPrefix(cmd, ".") {
			cmd = strings.ToLower(cmd)
			switch cmd {
			case ".help":
				fmt.Print(helpText)

			case ".exit":
				if txState.TxActive {
					cl.RollbackTransaction(context.Background())
				}
				fmt.Println("Goodbye!")
				return

			default:
				fmt.Printf("Unknown command: %s\n", cmd)
			}
			continue
		}

		// Regular commands
		switch cmd {
		case "BEGIN":

			// Check if we already have a transaction
			if txState.TxActive {
				fmt.Println("Error: Transaction already in progress")
				continue
			}
			var txMode transaction.TransactionMode = transaction.ReadWriteSerialized
			if len(parts) >= 2 {
				if strings.ToUpper(parts[1]) == "READONLY" {
					txMode = transaction.ReadOnly
				} else if strings.ToUpper(parts[1]) == "SERIALIZED" {
					txMode = transaction.ReadWriteSerialized
				} else if len(parts) >= 3 &&
					strings.ToUpper(parts[1]) == "READ" &&
					strings.ToUpper(parts[2]) == "COMMITTED" {
					txMode = transaction.WriteReadCommitted
				} else {
					fmt.Fprintf(os.Stderr, "Error: unknown transaction mode")
					continue
				}
			}
			err = cl.BeginTransaction(context.Background(), txMode)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error beginning transaction: %s", err)
				continue
			}

			txState.SetTx(txMode)

			switch txMode {
			case transaction.ReadOnly:
				fmt.Println("Started read-only transaction")
			case transaction.ReadWriteSerialized:
				fmt.Println("Started serialized read-write transaction")
			case transaction.WriteReadCommitted:
				fmt.Println("Started read-write transaction with read-committed isolation")
			}

		case "COMMIT":
			if !txState.TxActive {
				fmt.Fprintf(os.Stderr, "Error: No transaction in progress\n")
				continue
			}

			txState.Reset()

			// Commit transaction
			startTime := time.Now()
			err := cl.CommitTransaction(context.Background())

			if err != nil {
				fmt.Fprintf(os.Stderr, "Error committing transaction: %s\n", err)
				continue
			}

			fmt.Printf("Transaction committed (%.2f ms)\n", float64(time.Since(startTime).Microseconds())/1000.0)

		case "ROLLBACK":
			if !txState.TxActive {
				fmt.Fprintf(os.Stderr, "Error: No transaction in progress\n")
				continue
			}
			txState.Reset()

			err := cl.RollbackTransaction(context.Background())

			// Rollback transaction
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error rolling back transaction: %s\n", err)
			}

			fmt.Println("Transaction rolled back")

		case "PUT":
			if len(parts) < 3 {
				fmt.Fprintf(os.Stderr, "Error: PUT requires key and value arguments\n")
				continue
			}

			err = cl.Put(context.Background(),
				[]byte(parts[1]),
				[]byte(strings.Join(parts[2:], " ")))

			if err != nil {
				fmt.Fprintf(os.Stderr, "Error PUT failed: %s\n", err)
				continue
			}

			fmt.Println("PUT")

		case "GET":
			if len(parts) < 2 {
				fmt.Fprintf(os.Stderr, "Error: GET requires a key argument\n")
				continue
			}

			var val []byte
			var found bool

			val, found, err = cl.Get(
				context.Background(),
				[]byte(parts[1]),
			)

			if err != nil {
				fmt.Fprintf(os.Stderr, "Error in GET: %s\n", err)
				continue
			}

			if !found {
				fmt.Fprintf(os.Stderr, "Error key `%s` not found\n", parts[1])
				continue
			}

			fmt.Printf("%s\n", string(val))

		case "DELETE":
			if len(parts) < 2 {
				fmt.Println("Error: DELETE requires a key argument")
				continue
			}

			err := cl.Delete(context.Background(), []byte(parts[1]))

			if err != nil {
				fmt.Fprintf(os.Stderr, "Error deletion failed: %s\n", err)
				continue
			}

		case "SCAN":

			var scanResponse *transport.ScanResponse
			var prefix, suffix, startKey, endKey []byte
			var limit int32

			if len(parts) == 2 {

				// SCAN <prefix>
				prefix = []byte(parts[1])
			} else if len(parts) == 3 {
				// SCAN SUFFIX <suffix>
				if strings.ToUpper(parts[1]) == "SUFFIX" {
					suffix = []byte(parts[2])
				}
			} else if len(parts) == 4 {
				// SCAN PREFIX <limit> <prefix>
				// SCAN SUFFIX <limit> <suffix>
				// SCAN RANGE <start-key> <end-key>

				if strings.ToUpper(parts[1]) == "PREFIX" {
					l := int(0)
					l, err = strconv.Atoi(parts[2])
					if err != nil {
						fmt.Fprintf(os.Stderr, "Error invalid syntax for SCAN PREFIX. Expected limit to be a valid integer, but got %s\n", parts[2])
						continue
					}
					if l > math.MaxInt32 {
						l = math.MaxInt32
					}

					limit = int32(l)

					prefix = []byte(parts[3])
				} else if strings.ToUpper(parts[1]) == "SUFFIX" {
					l := int(0)
					l, err = strconv.Atoi(parts[2])
					if err != nil {
						fmt.Fprintf(os.Stderr, "Error invalid syntax for SCAN SUFFIX. Expected limit to be a valid integer, but got %s\n", parts[2])
						continue
					}
					if l > math.MaxInt32 {
						l = math.MaxInt32
					}

					limit = int32(l)
					suffix = []byte(parts[3])
				} else if strings.ToUpper(parts[1]) == "RANGE" {
					startKey = []byte(parts[2])
					endKey = []byte(parts[3])
				}
			} else if len(parts) == 5 {
				if strings.ToUpper(parts[1]) == "RANGE" {
					// SCAN RANGE <limit> <start-key> <end-key>
					l := int(0)
					l, err = strconv.Atoi(parts[2])
					if err != nil {
						fmt.Fprintf(os.Stderr, "Error invalid syntax for SCAN SUFFIX. Expected limit to be a valid integer, but got %s\n", parts[2])
						continue
					}
					if l > math.MaxInt32 {
						l = math.MaxInt32
					}

					limit = int32(l)
					startKey = []byte(parts[3])
					endKey = []byte(parts[4])
				}
			} else {
				fmt.Println("Error Invalid SCAN syntax. See .help for usage")
				continue
			}

			scanResponse, err = cl.Scan(context.Background(),
				prefix, suffix, startKey, endKey, limit)

			if err != nil {
				fmt.Fprintf(os.Stderr, "Error in SCAN: %s\n", err)
				continue
			}

			count := int32(0)

			for {
				msg, err := scanResponse.Recv()
				if err == io.EOF {
					break
				}

				if err != nil {
					fmt.Fprintf(os.Stderr, "Error receiving from stream: %s\n")
				}

				fmt.Printf("%s: %s\n", msg.Key, msg.Value)
				count++
			}

			fmt.Printf("%d entries found\n", count)

		default:
			fmt.Printf("Unknown command: %s\n", cmd)
		}
	}
}

// makeKeySuccessor creates the successor key for a prefix scan
// by adding a 0xFF byte to the end of the prefix
func makeKeySuccessor(prefix []byte) []byte {
	successor := make([]byte, len(prefix)+1)
	copy(successor, prefix)
	successor[len(prefix)] = 0xFF
	return successor
}

// hasSuffix checks if a byte slice ends with a specific suffix
func hasSuffix(data, suffix []byte) bool {
	if len(data) < len(suffix) {
		return false
	}
	for i := 0; i < len(suffix); i++ {
		if data[len(data)-len(suffix)+i] != suffix[i] {
			return false
		}
	}
	return true
}

// toTitle replaces strings.Title which is deprecated
// It converts the first character of each word to title case
func toTitle(s string) string {
	prev := ' '
	return strings.Map(
		func(r rune) rune {
			if unicode.IsSpace(prev) || unicode.IsPunct(prev) {
				prev = r
				return unicode.ToTitle(r)
			}
			prev = r
			return r
		},
		s)
}
