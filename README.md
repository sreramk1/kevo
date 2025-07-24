# Extension Introduced:

*The original Kevo project strictly follows the single-writer model and does not support transaction isolation. In this extension, the key-value store supports multiple concurrent transactional writes (synchronized in batches when being written to the disk) and introduces significant architectural changes, without making any noticeable changes to the lower level API (which includes replication, compaction and other features). Supported transaction-isolation levels: (1) `READ COMMITTED`, (2) `READ ONLY`, (3) `SERIALIZED`.*

*The current state of this extension should only be treated as a proof-of-concept, since there are lot of bugs to be fixed and edge cases to be tested.*

## Summary of the changes made:
**NOTE: Many critical bugs were fixed in the original Kevo repository after this fork was created, but those fixes have not been incorporated into this repository.** 

1. Implements the following transactions: SERIALIZED, READ COMMITTED and READ ONLY.
2. Unlike the original implementation of Kevo, this one allows multiple transactions to run simultaneously. **Changes are synchronized with locks, and this also detects and resolves deadlocks.**
3. The CLI does not directly interact with a locally created database. Instead, it tries to first connect with the server instance, and the CLI commands are translated to gRPC requests which are then sent to the server.
4. Runs in two modes: 
    1. Server mode: `go run ./cmd/kevo -server db/`
    2. Client mode: `go run ./cmd/kevo -client`

    The client attempts to connect with the default endpoint, `localhost:50051` or any other specified endpoint specified with the flag `--endpoint`. Use the flag `--address` for changing the default listen address (which is also `localhost:50051`) in server mode.
5. Within a transaction, all keys associated with the values that are modified are locked until the transaction commits or rollbacks.
6. `SERIALIZED` transaction strictly serializes all modifications across transactions, guaranteeing a consistent view at the expense of having only one transaction execute at a time.
7. Non-transactional commands are also implicitly wrapped within a transaction to allow the additionally introduced transactional rules to be enforced.


## Example use (demonstrating deadlock detection and resolution): 

**Note: This repository does not include bug fixes introduced to the main Kevo repository after this fork was created. As a consequence, a bug that prevents correct reloading of data written with bulk-writes (executed by the storage layer) still exists in this repository. Therefore, re-loading from the WAL during startup causes written data to be ignored since this modification primarily relies on bulk-writes.**

1. Open three terminals (`T1`, `T2`, `T3`) from within the project's root directory.

2. Start the server in `T1` by executing `go run ./cmd/kevo -server db/`

3. Connect to the server from `T2` and `T3` by executing `go run ./cmd/kevo -client` from them.

4. Now, execute the following from `T2` and `T3`:

    1. From `T2` run: `BEGIN READ COMMITTED`.
    2. From `T3` run: `BEGIN READ COMMITTED`.
    3. From `T2` run: `PUT x y`. Note: `x` is the key and `y` is the value.
    4. From `T3` run: `PUT y x`.
    5. From `T2` run: `PUT y z1`. Running this would cause `T2` to block, since the key  `y` was locked by `T3`.
    6. From `T3` run: `PUT x z2`. This also blocks, since `T2` has already locked `x`.
    7. Now, since the above leads to a deadlock, one or more of the blocking transactions will be terminated.


## Tasks to Improve Stability:
*I may not resume working on this project for sometime.*
1. Incorporate the bug fixes introduced in the original project.
2. Introduce robust integration tests with benchmarking.
3. Introduce `GET-FOR-UPDATE`. This must work similar to `SELECT...FOR UPDATE`, holding locks on all fields that are read as if it was a write operation.
4. Introduce `REPEATABLE READ` isolation, which would rely on a unique serial-number assigned to every operation ever introduce, which must be strictly increasing. This will be needed in addition to the record-level locks introduced in this repository.
5. Optimize `SERIALIZED` isolation. Currently, this blocks all concurrent transactions and strictly allows only one transaction to execute at any given time, making this strictly pessimistic. In the future, we could modify this to be partially optimistic by not blocking transactions that apply to disjoint set of records in the database. So, instead of strict serialization, certain transactions could exit upon "serialization-failures".

# Kevo

[![Go Report Card](https://goreportcard.com/badge/github.com/KevoDB/kevo)](https://goreportcard.com/report/github.com/KevoDB/kevo)
[![GoDoc](https://godoc.org/github.com/KevoDB/kevo?status.svg)](https://godoc.org/github.com/KevoDB/kevo)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A lightweight, minimalist Log-Structured Merge (LSM) tree storage engine written in Go.

## Overview

Kevo is a clean, composable storage engine that follows LSM tree principles, focusing on simplicity while providing the building blocks needed for higher-level database implementations. It's designed to be both educational and practically useful for embedded storage needs.

## Features

- **Clean, idiomatic Go implementation** of the LSM tree architecture
- **Facade-based architecture** for separation of concerns and modularity
- **Single-writer architecture** for simplicity and reduced concurrency complexity
- **Complete storage primitives**: WAL, MemTable, SSTable, Compaction
- **Configurable durability** guarantees (sync vs. batched fsync)
- **Interface-driven design** with clear component boundaries
- **Comprehensive statistics collection** for monitoring and debugging
- **ACID-compliant transactions** with SQLite-inspired reader-writer concurrency
- **Primary-replica replication** with automatic client request routing

## Use Cases

- **Educational Tool**: Learn and teach storage engine internals
- **Embedded Storage**: Applications needing local, durable storage
- **Prototype Foundation**: Base layer for experimenting with novel database designs
- **Go Ecosystem Component**: Reusable storage layer for Go applications

## Getting Started

### Installation

```bash
go get github.com/KevoDB/kevo
```

### Client SDKs

Kevo provides client SDKs for different languages to connect to a Kevo server:

- **Go**: [github.com/KevoDB/kevo/pkg/client](https://github.com/KevoDB/kevo/pkg/client)
- **Python**: [github.com/KevoDB/python-sdk](https://github.com/KevoDB/python-sdk)

### Basic Usage

```go
package main

import (
    "fmt"
    "log"

    "github.com/KevoDB/kevo/pkg/engine"
)

func main() {
    // Create or open a storage engine at the specified path
    // The EngineFacade implements the Engine interface
    eng, err := engine.NewEngineFacade("/path/to/data")
    if err != nil {
        log.Fatalf("Failed to open engine: %v", err)
    }
    defer eng.Close()

    // Store a key-value pair
    if err := eng.Put([]byte("hello"), []byte("world")); err != nil {
        log.Fatalf("Failed to put: %v", err)
    }

    // Retrieve a value by key
    value, err := eng.Get([]byte("hello"))
    if err != nil {
        log.Fatalf("Failed to get: %v", err)
    }
    fmt.Printf("Value: %s\n", value)

    // Using transactions
    tx, err := eng.BeginTransaction(false) // false = read-write transaction
    if err != nil {
        log.Fatalf("Failed to start transaction: %v", err)
    }

    // Perform operations within the transaction
    if err := tx.Put([]byte("foo"), []byte("bar")); err != nil {
        tx.Rollback()
        log.Fatalf("Failed to put in transaction: %v", err)
    }

    // Commit the transaction
    if err := tx.Commit(); err != nil {
        log.Fatalf("Failed to commit: %v", err)
    }

    // Scan all key-value pairs
    iter, err := eng.GetIterator()
    if err != nil {
        log.Fatalf("Failed to get iterator: %v", err)
    }

    for iter.SeekToFirst(); iter.Valid(); iter.Next() {
        fmt.Printf("%s: %s\n", iter.Key(), iter.Value())
    }

    // Get statistics from the engine
    stats := eng.GetStats()
    fmt.Printf("Operations - Puts: %v, Gets: %v\n",
        stats["put_ops"], stats["get_ops"])
}
```

### Interactive CLI Tool

Included is an interactive CLI tool (`kevo`) for exploring and manipulating databases:

```bash
go run ./cmd/kevo/main.go [database_path]
```

Will create a directory at the path you create (e.g., `/tmp/foo.db` will be a directory called `foo.db` in `/tmp` where the database will live).

Example session:

```
kevo> PUT user:1 {"name":"John","email":"john@example.com"}
Value stored

kevo> GET user:1
{"name":"John","email":"john@example.com"}

kevo> BEGIN TRANSACTION
Started read-write transaction

kevo> PUT user:2 {"name":"Jane","email":"jane@example.com"}
Value stored in transaction (will be visible after commit)

kevo> COMMIT
Transaction committed (0.53 ms)

kevo> SCAN user:
user:1: {"name":"John","email":"john@example.com"}
user:2: {"name":"Jane","email":"jane@example.com"}
2 entries found

kevo> SCAN SUFFIX @example.com
user:1: {"name":"John","email":"john@example.com"}
user:2: {"name":"Jane","email":"jane@example.com"}
2 entries found
```

Type `.help` in the CLI for more commands.

### Run Server

```bash
# Run as standalone node (default)
go run ./cmd/kevo/main.go -server [database_path]

# Run as primary node
go run ./cmd/kevo/main.go -server [database_path] -replication.enabled=true -replication.mode=primary -replication.listen=:50053

# Run as replica node
go run ./cmd/kevo/main.go -server [database_path] -replication.enabled=true -replication.mode=replica -replication.primary=localhost:50053
```

## Configuration

Kevo offers extensive configuration options to optimize for different workloads:

```go
// Create custom config for write-intensive workload
config := config.NewDefaultConfig(dbPath)
config.MemTableSize = 64 * 1024 * 1024  // 64MB MemTable
config.WALSyncMode = config.SyncBatch   // Batch sync for better throughput
config.SSTableBlockSize = 32 * 1024     // 32KB blocks

// Save the config to disk
if err := config.SaveManifest(dbPath); err != nil {
    log.Fatalf("Failed to save configuration: %v", err)
}

// Create engine using the saved config
eng, err := engine.NewEngineFacade(dbPath)
if err != nil {
    log.Fatalf("Failed to create engine: %v", err)
}
```

See [CONFIG_GUIDE.md](./docs/CONFIG_GUIDE.md) for detailed configuration guidance.

## Architecture

Kevo implements a facade-based design over the LSM tree architecture, consisting of:

### Core Components

- **EngineFacade**: Central coordinator that delegates to specialized managers
- **StorageManager**: Handles data storage operations across multiple layers
- **TransactionManager**: Manages transaction lifecycle and isolation
- **CompactionManager**: Coordinates background optimization processes
- **ReplicationManager**: Handles primary-replica replication and node role management
- **Statistics Collector**: Provides comprehensive metrics for monitoring

### Storage Layer

- **Write-Ahead Log (WAL)**: Ensures durability of writes before they're in memory
- **MemTable**: In-memory data structure (skiplist) for fast writes
- **SSTables**: Immutable, sorted files for persistent storage
- **Compaction**: Background process to merge and optimize SSTables

### Replication Layer

- **Primary Node**: Single writer that streams WAL entries to replicas
- **Replica Node**: Read-only node that receives and applies WAL entries from the primary
- **Client Routing**: Smart client SDK that automatically routes reads to replicas and writes to the primary

### Interface-Driven Design

The system is designed around clear interfaces that define contracts between components:

```
     ┌───────────────────┐
     │    Client Code    │
     └─────────┬─────────┘
               │
               ▼
     ┌───────────────────┐
     │  Engine Interface │
     └─────────┬─────────┘
               │
               ▼
     ┌───────────────────┐
     │   EngineFacade    │
     └─────────┬─────────┘
               │
     ┌─────────┼─────────┐
     ▼         ▼         ▼
┌─────────┐ ┌───────┐ ┌──────────┐
│ Storage │ │  Tx   │ │Compaction│
│ Manager │ │Manager│ │ Manager  │
└─────────┘ └───────┘ └──────────┘
```

For more details on each component, see the documentation in the [docs](./docs) directory.

## Benchmarking

The storage-bench tool provides comprehensive performance testing:

```bash
go run ./cmd/storage-bench/... -type=all
```

See [storage-bench README](./cmd/storage-bench/README.md) for detailed options.

## Non-Goals

- **Feature Parity with Other Engines**: Not competing with RocksDB, LevelDB, etc.
- **Multi-Node Distribution**: Focusing on single-node operation
- **Complex Query Planning**: Higher-level query features are left to layers built on top

## Building and Testing

```bash
# Build the project
go build ./...

# Run tests
go test ./...

# Run benchmarks
go test ./pkg/path/to/package -bench .

# Run with race detector
go test -race ./...
```

## Project Status

This project is under active development. While the core functionality is stable, the API may change as we continue to improve the engine.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

See our [contribution guidelines](CONTRIBUTING.md) for more information.

## License

Copyright 2025 Jeremy Tregunna

Copyright 2025 Sreram K (sreramk360@gmail.com)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[https://www.apache.org/licenses/LICENSE-2.0](https://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
