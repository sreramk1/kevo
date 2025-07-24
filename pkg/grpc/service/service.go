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

package service

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/KevoDB/kevo/pkg/common/iterator"
	"github.com/KevoDB/kevo/pkg/engine"
	"github.com/KevoDB/kevo/pkg/grpc/interceptors"
	"github.com/KevoDB/kevo/pkg/replication"
	"github.com/KevoDB/kevo/pkg/sessreg"
	"github.com/KevoDB/kevo/pkg/transaction"
	pb "github.com/KevoDB/kevo/proto/kevo"
)

var _ pb.KevoServiceServer = (*KevoServiceServer)(nil)

// KevoServiceServer implements the gRPC KevoService interface
type KevoServiceServer struct {
	pb.UnimplementedKevoServiceServer
	engine  *engine.EngineFacade
	sessreg *sessreg.SessionRegistry
	// txRegistry         *txregold.TransactionRegistry
	// activeTx           sync.Map // map[string]interfaces.Transaction
	txMu sync.Mutex
	// compactionSem      chan struct{}           // Semaphore for limiting concurrent compactions
	maxKeySize         int                     // Maximum allowed key size
	maxValueSize       int                     // Maximum allowed value size
	maxBatchSize       int                     // Maximum number of operations in a batch
	maxTransactions    int                     // Maximum number of concurrent transactions
	transactionTTL     int64                   // Maximum time in seconds a transaction can be idle
	activeTransCount   int32                   // Count of active transactions
	replicationManager ReplicationInfoProvider // Interface to the replication manager
}

// RenewSession implements proto.KevoServiceServer.
func (s *KevoServiceServer) RenewSession(
	ctx context.Context,
	req *pb.RenewSessionRequest,
) (*pb.RenewSessionResponse, error) {

	sessionId, _, err := interceptors.SessionIdValue(ctx)
	if err != nil {
		return nil, err
	}
	s.sessreg.RenewSession(sessionId)
	return &pb.RenewSessionResponse{}, nil
}

// ReplicationInfoProvider defines an interface for accessing replication topology information
type ReplicationInfoProvider interface {
	// GetNodeInfo returns information about the replication topology
	// Returns: nodeRole, primaryAddr, replicas, lastSequence, readOnly
	GetNodeInfo() (string, string, []ReplicaInfo, uint64, bool)
}

// ReplicaInfo contains information about a replica node
// This should mirror the structure in pkg/replication/info_provider.go
type ReplicaInfo = replication.ReplicationNodeInfo

// NewKevoServiceServer creates a new KevoServiceServer
func NewKevoServiceServer(engine *engine.EngineFacade, sReg *sessreg.SessionRegistry, replicationManager ReplicationInfoProvider) *KevoServiceServer {
	return &KevoServiceServer{
		engine:  engine,
		sessreg: sReg,
		// txRegistry:         txRegistry,
		// activeTx:           sync.Map{},
		replicationManager: replicationManager,
		// compactionSem:      make(chan struct{}, 1), // Allow only one compaction at a time
		maxKeySize:      4096,             // 4KB
		maxValueSize:    10 * 1024 * 1024, // 10MB
		maxBatchSize:    1000,
		maxTransactions: 1000,
		transactionTTL:  300, // 5 minutes
	}
}

// incAndRetrieveTx increments ActiveUseCount by calling
// and returns a method that decrements the use count. The
// returned method, is differed in the calling method.
// This also returns the transaction from the SessionState.
func (s *KevoServiceServer) incAndRetrieveTx(
	ctx context.Context,
) (session *sessreg.SessionState,
	tx *transaction.Transaction, decActiveUseCount func(), err error) {

	sessionId, _, err := interceptors.SessionIdValue(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	session, err = s.sessreg.IncActiveUseCount(sessionId)

	if err != nil {
		return nil, nil, nil, err
	}

	decActiveUseCount = func() {
		s.sessreg.DecActiveUseCount(sessionId)
	}

	tx, err = session.GetTx()

	if err != nil {
		decActiveUseCount()
		return nil, nil, nil, err
	}

	return session, tx, decActiveUseCount, nil
}

// Get retrieves a value for a given key
func (s *KevoServiceServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	// increment ActiveUseCount and retrieve transaction if available
	_, tx, decActiveUseCount, err := s.incAndRetrieveTx(ctx)
	if err != nil {
		return nil, err
	}
	defer decActiveUseCount()
	//--------------------------------------------------------------------

	if len(req.Key) == 0 || len(req.Key) > s.maxKeySize {
		return nil, fmt.Errorf("invalid key size\n")
	}

	isLocalTransaction := false
	if tx == nil {
		tx, err = s.engine.BeginTransaction(transaction.ReadOnly)
		if err != nil {
			return nil, fmt.Errorf("failed to start transaction: %w\n", err)
		}

		// Ensure we either commit or rollback
		defer tx.Rollback()
		isLocalTransaction = true
	}

	value, found, err := tx.Get(req.Key)

	if err != nil {
		return nil, fmt.Errorf("failed to execute Get request: %w\n", err)
	}

	if isLocalTransaction {
		// Commit the transaction
		if err = tx.Commit(); err != nil {
			return nil, fmt.Errorf("failed to commit transaction: %w\n", err)
		}
	}

	return &pb.GetResponse{
		Value: value,
		Found: found,
	}, nil
}

// Put stores a key-value pair
func (s *KevoServiceServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {

	// increment ActiveUseCount and retrieve transaction if available
	_, tx, decActiveUseCount, err := s.incAndRetrieveTx(ctx)
	if err != nil {
		return nil, err
	}
	defer decActiveUseCount()
	//--------------------------------------------------------------------

	if len(req.Key) == 0 || len(req.Key) > s.maxKeySize {
		return nil, fmt.Errorf("invalid key size\n")
	}

	if len(req.Value) > s.maxValueSize {
		return nil, fmt.Errorf("value too large\n")
	}

	isLocalTransaction := false
	if tx == nil {
		tx, err = s.engine.BeginTransaction(transaction.WriteReadCommitted)
		if err != nil {
			return nil, fmt.Errorf("failed to start transaction: %w\n", err)
		}

		// Ensure we either commit or rollback
		defer tx.Rollback()
		isLocalTransaction = true
	}

	lockAcqSuccess, err := tx.Put(req.Key, req.Value)

	if err != nil {
		return nil, err
	}

	if !lockAcqSuccess {
		return nil, fmt.Errorf("failed to acquire lock on key: %s\n", string(req.Key))
	}

	if isLocalTransaction {
		// Commit the transaction
		if err = tx.Commit(); err != nil {
			return nil, err
		}
	}

	return &pb.PutResponse{}, nil
}

// Delete removes a key-value pair
func (s *KevoServiceServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {

	// increment ActiveUseCount and retrieve transaction if available
	_, tx, decActiveUseCount, err := s.incAndRetrieveTx(ctx)
	if err != nil {
		return nil, err
	}
	defer decActiveUseCount()
	//--------------------------------------------------------------------

	if len(req.Key) == 0 || len(req.Key) > s.maxKeySize {
		return nil, fmt.Errorf("invalid key size\n")
	}

	isLocalTransaction := false
	if tx == nil {
		tx, err = s.engine.BeginTransaction(transaction.WriteReadCommitted)
		if err != nil {
			return nil, fmt.Errorf("failed to start transaction: %w\n", err)
		}

		// Ensure we either commit or rollback
		defer tx.Rollback()
		isLocalTransaction = true
	}

	lockAcqSuccess, err := tx.Delete(req.Key)

	if err != nil {
		return nil, err
	}

	if !lockAcqSuccess {
		return nil, fmt.Errorf("failed to acquire lock on key: %s\n", string(req.Key))
	}

	if isLocalTransaction {
		// Commit the transaction
		if err = tx.Commit(); err != nil {
			return nil, err
		}
	}

	return &pb.DeleteResponse{}, nil
}

// BatchWrite performs multiple operations in a batch
func (s *KevoServiceServer) BatchWrite(ctx context.Context, req *pb.BatchWriteRequest) (*pb.BatchWriteResponse, error) {

	// increment ActiveUseCount and retrieve transaction if available
	_, tx, decActiveUseCount, err := s.incAndRetrieveTx(ctx)
	if err != nil {
		return nil, err
	}
	defer decActiveUseCount()
	//--------------------------------------------------------------------

	if len(req.Operations) == 0 {
		return &pb.BatchWriteResponse{Success: true}, nil
	}

	if len(req.Operations) > s.maxBatchSize {
		return nil, fmt.Errorf("batch size exceeds maximum allowed (%d)\n", s.maxBatchSize)
	}

	isLocalTransaction := false
	if tx == nil {
		// Start a transaction for atomic batch operations
		tx, err = s.engine.BeginTransaction(transaction.WriteReadCommitted)
		if err != nil {
			return &pb.BatchWriteResponse{Success: false}, fmt.Errorf("failed to start transaction: %w\n", err)
		}

		// Ensure we either commit or rollback
		defer tx.Rollback()
		isLocalTransaction = true
	}

	// Process each operation
	for _, op := range req.Operations {
		if len(op.Key) == 0 || len(op.Key) > s.maxKeySize {
			err = fmt.Errorf("invalid key size in batch operation\n")
			return &pb.BatchWriteResponse{Success: false}, err
		}

		switch op.Type {
		case pb.Operation_PUT:
			if len(op.Value) > s.maxValueSize {
				err = fmt.Errorf("value too large in batch operation\n")
				return &pb.BatchWriteResponse{Success: false}, err
			}
			lockAcqSuccess, err := tx.Put(op.Key, op.Value)
			if err != nil {
				return &pb.BatchWriteResponse{Success: false}, err
			}
			if !lockAcqSuccess {
				return nil, fmt.Errorf("failed to acquire lock on key: %s\n", string(op.Key))
			}
		case pb.Operation_DELETE:
			lockAcqSuccess, err := tx.Delete(op.Key)
			if err != nil {
				return nil, err
			}

			if !lockAcqSuccess {
				return nil, fmt.Errorf("failed to acquire lock on key: %s\n", string(op.Key))
			}
		default:
			err = fmt.Errorf("unknown operation type\n")
			return &pb.BatchWriteResponse{Success: false}, err
		}
	}

	if isLocalTransaction {
		// Commit the transaction
		if err = tx.Commit(); err != nil {
			return &pb.BatchWriteResponse{Success: false}, err
		}
	}

	return &pb.BatchWriteResponse{Success: true}, nil
}

// Scan iterates over a range of keys
func (s *KevoServiceServer) Scan(req *pb.ScanRequest, stream pb.KevoService_ScanServer) error {
	// increment ActiveUseCount and retrieve transaction if available
	_, tx, decActiveUseCount, err := s.incAndRetrieveTx(stream.Context())
	if err != nil {
		return err
	}
	defer decActiveUseCount()
	//--------------------------------------------------------------------

	var limit int32 = 0
	if req.Limit > 0 {
		limit = req.Limit
	}

	// Use a longer timeout for scan operations
	// We create a timeout context but don't need to use it explicitly as the gRPC context
	// will handle timeouts at the transport level

	isLocalTransaction := false
	if tx == nil {
		tx, err = s.engine.BeginTransaction(transaction.ReadOnly)
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w\n", err)
		}
		defer tx.Rollback() // Always rollback read-only TX when done
		isLocalTransaction = true
	}

	// Create appropriate iterator based on request parameters
	var iter iterator.Iterator
	if len(req.Prefix) > 0 && len(req.Suffix) > 0 {
		// Create a combined prefix-suffix iterator
		baseIter := tx.NewIterator()
		prefixIter := newPrefixIterator(baseIter, req.Prefix)
		iter = newSuffixIterator(prefixIter, req.Suffix)
	} else if len(req.Prefix) > 0 {
		// Create a prefix iterator
		prefixIter := tx.NewIterator()
		iter = newPrefixIterator(prefixIter, req.Prefix)
	} else if len(req.Suffix) > 0 {
		// Create a suffix iterator
		suffixIter := tx.NewIterator()
		iter = newSuffixIterator(suffixIter, req.Suffix)
	} else if len(req.StartKey) > 0 || len(req.EndKey) > 0 {
		// Create a range iterator
		iter = tx.NewRangeIterator(req.StartKey, req.EndKey)
	} else {
		// Create a full scan iterator
		iter = tx.NewIterator()
	}

	count := int32(0)
	// Position iterator at the first entry
	iter.SeekToFirst()

	// Iterate through all valid entries
	for iter.Valid() {
		if limit > 0 && count >= limit {
			break
		}

		// Skip tombstones (deletion markers)
		if !iter.IsTombstone() {
			if err := stream.Send(&pb.ScanResponse{
				Key:   iter.Key(),
				Value: iter.Value(),
			}); err != nil {
				return err
			}
			count++
		}

		// Move to the next entry
		iter.Next()
	}

	if isLocalTransaction {
		// Commit the transaction
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

// prefixIterator wraps another iterator and filters for a prefix
type prefixIterator struct {
	iter   iterator.Iterator
	prefix []byte
	err    error
}

func newPrefixIterator(iter iterator.Iterator, prefix []byte) *prefixIterator {
	return &prefixIterator{
		iter:   iter,
		prefix: prefix,
	}
}

func (pi *prefixIterator) Next() bool {
	for pi.iter.Next() {
		// Check if current key has the prefix
		key := pi.iter.Key()
		if len(key) >= len(pi.prefix) &&
			equalByteSlice(key[:len(pi.prefix)], pi.prefix) {
			return true
		}
	}
	return false
}

func (pi *prefixIterator) Key() []byte {
	return pi.iter.Key()
}

func (pi *prefixIterator) Value() []byte {
	return pi.iter.Value()
}

func (pi *prefixIterator) Valid() bool {
	if !pi.iter.Valid() {
		return false
	}

	// Check if the current key has the correct prefix
	key := pi.iter.Key()
	if len(key) < len(pi.prefix) {
		return false
	}

	return equalByteSlice(key[:len(pi.prefix)], pi.prefix)
}

func (pi *prefixIterator) IsTombstone() bool {
	return pi.iter.IsTombstone()
}

func (pi *prefixIterator) SeekToFirst() {
	pi.iter.SeekToFirst()

	// After seeking to first, we need to advance to the first key
	// that actually matches our prefix
	if pi.iter.Valid() {
		key := pi.iter.Key()
		if len(key) >= len(pi.prefix) {
			if equalByteSlice(key[:len(pi.prefix)], pi.prefix) {
				// Found a match, no need to advance
				return
			}
		}

		// Current key doesn't match, find the first one that does
		pi.Next()
	}
}

func (pi *prefixIterator) SeekToLast() {
	pi.iter.SeekToLast()
}

func (pi *prefixIterator) Seek(target []byte) bool {
	return pi.iter.Seek(target)
}

// equalByteSlice compares two byte slices for equality
func equalByteSlice(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// suffixIterator wraps another iterator and filters for a suffix
type suffixIterator struct {
	iter   iterator.Iterator
	suffix []byte
	err    error
}

func newSuffixIterator(iter iterator.Iterator, suffix []byte) *suffixIterator {
	return &suffixIterator{
		iter:   iter,
		suffix: suffix,
	}
}

func (si *suffixIterator) Next() bool {
	// Keep advancing the underlying iterator until we find a key with the correct suffix
	// or reach the end
	for si.iter.Next() {
		// Check if current key has the suffix
		key := si.iter.Key()
		if len(key) >= len(si.suffix) {
			// Compare the suffix portion
			suffixStart := len(key) - len(si.suffix)
			if equalByteSlice(key[suffixStart:], si.suffix) {
				return true
			}
		}
	}
	return false
}

func (si *suffixIterator) Key() []byte {
	return si.iter.Key()
}

func (si *suffixIterator) Value() []byte {
	return si.iter.Value()
}

func (si *suffixIterator) Valid() bool {
	if !si.iter.Valid() {
		return false
	}

	// Check if the current key has the correct suffix
	key := si.iter.Key()
	if len(key) < len(si.suffix) {
		return false
	}

	suffixStart := len(key) - len(si.suffix)
	return equalByteSlice(key[suffixStart:], si.suffix)
}

func (si *suffixIterator) IsTombstone() bool {
	return si.iter.IsTombstone()
}

func (si *suffixIterator) SeekToFirst() {
	si.iter.SeekToFirst()

	// After seeking to first, we need to advance to the first key
	// that actually matches our suffix
	if si.iter.Valid() {
		key := si.iter.Key()
		if len(key) >= len(si.suffix) {
			suffixStart := len(key) - len(si.suffix)
			if equalByteSlice(key[suffixStart:], si.suffix) {
				// Found a match, no need to advance
				return
			}
		}

		// Current key doesn't match, find the first one that does
		si.Next()
	}
}

func (si *suffixIterator) SeekToLast() {
	si.iter.SeekToLast()
}

func (si *suffixIterator) Seek(target []byte) bool {
	return si.iter.Seek(target)
}

var ErrInvalidTransactionMode = errors.New("invalid transaction mode")

// BeginTransaction starts a new transaction
func (s *KevoServiceServer) BeginTransaction(ctx context.Context, req *pb.BeginTransactionRequest) (*pb.BeginTransactionResponse, error) {
	// increment ActiveUseCount and retrieve transaction if available
	session, tx, decActiveUseCount, err := s.incAndRetrieveTx(ctx)
	if err != nil {
		return nil, err
	}
	defer decActiveUseCount()
	//--------------------------------------------------------------------

	// Force clean up of old transactions before creating new ones

	var txMode transaction.TransactionMode

	txMode = transaction.TransactionMode(req.TxMode.Number())

	if !txMode.IsValid() {
		return nil, ErrInvalidTransactionMode
	}

	if tx != nil {
		return nil, fmt.Errorf("Failed to begin Transaction: an active transaction already exists\n")
	}

	tx, err = s.engine.BeginTransaction(txMode)
	if err != nil {
		return nil, fmt.Errorf("Failed to begin transaction %w\n", err)
	}

	err = session.SetTx(tx)
	if err != nil {
		return nil, err
	}

	return &pb.BeginTransactionResponse{}, nil
}

// CommitTransaction commits an ongoing transaction
func (s *KevoServiceServer) CommitTransaction(ctx context.Context, req *pb.CommitTransactionRequest) (*pb.CommitTransactionResponse, error) {
	// increment ActiveUseCount and retrieve transaction if available
	session, tx, decActiveUseCount, err := s.incAndRetrieveTx(ctx)
	if err != nil {
		return nil, err
	}
	defer decActiveUseCount()
	//--------------------------------------------------------------------

	// tx, exists := s.txRegistry.Get(req.TransactionId)
	// txAny, exists := s.activeTx.Load(req.TransactionId)
	if tx == nil {
		fmt.Printf("Commit failed - There is no active transaction\n")
		return nil, fmt.Errorf("Commit failed - There is no active transaction\n")
	}

	// Remove the transaction after commit
	defer func() {
		session.UnsetTx()

	}()

	if err := tx.Commit(); err != nil {
		fmt.Printf("Failed to commit transaction\n", err)
		return nil, err
	}

	fmt.Printf("Successfully committed transaction\n")
	return &pb.CommitTransactionResponse{}, nil
}

// RollbackTransaction aborts an ongoing transaction
func (s *KevoServiceServer) RollbackTransaction(ctx context.Context, req *pb.RollbackTransactionRequest) (*pb.RollbackTransactionResponse, error) {

	// increment ActiveUseCount and retrieve transaction if available
	session, tx, decActiveUseCount, err := s.incAndRetrieveTx(ctx)
	if err != nil {
		return nil, err
	}
	defer decActiveUseCount()
	//--------------------------------------------------------------------

	if tx == nil {
		fmt.Printf("Commit failed - There is no active transaction\n")
		return nil, fmt.Errorf("Commit failed - There is no active transaction\n")
	}

	// Remove the transaction after rollback
	defer func() {
		session.UnsetTx()
	}()

	if err := tx.Rollback(); err != nil {
		fmt.Printf("Failed to roll back transaction \n %w", err)
		return nil, err
	}

	fmt.Printf("Successfully rolled back transaction\n")
	return &pb.RollbackTransactionResponse{}, nil
}

// GetStats retrieves database statistics
func (s *KevoServiceServer) GetStats(ctx context.Context, req *pb.GetStatsRequest) (*pb.GetStatsResponse, error) {
	// Collect basic stats that we know are available
	keyCount := int64(0)
	sstableCount := int32(0)
	memtableCount := int32(1) // At least 1 active memtable

	// Create a read-only transaction to count keys
	tx, err := s.engine.BeginTransaction(transaction.ReadOnly)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction for stats: %w", err)
	}
	defer tx.Rollback()

	// Use an iterator to count keys
	iter := tx.NewIterator()

	// Count keys and estimate size
	var totalSize int64
	for iter.Next() {
		keyCount++
		totalSize += int64(len(iter.Key()) + len(iter.Value()))
	}

	response := &pb.GetStatsResponse{
		KeyCount:           keyCount,
		StorageSize:        totalSize,
		MemtableCount:      memtableCount,
		SstableCount:       sstableCount,
		WriteAmplification: 1.0, // Placeholder
		ReadAmplification:  1.0, // Placeholder
		OperationCounts:    make(map[string]uint64),
		LatencyStats:       make(map[string]*pb.LatencyStats),
		ErrorCounts:        make(map[string]uint64),
		RecoveryStats:      &pb.RecoveryStats{},
	}

	// Populate detailed stats if the engine implements stats collection

	allStats := s.engine.GetStats()

	// Populate operation counts
	for key, value := range allStats {
		if isOperationCounter(key) {
			if count, ok := value.(uint64); ok {
				response.OperationCounts[key] = count
			}
		}
	}

	// Populate latency statistics
	for key, value := range allStats {
		if isLatencyStat(key) {
			if latency, ok := value.(map[string]interface{}); ok {
				stats := &pb.LatencyStats{}

				if count, ok := latency["count"].(uint64); ok {
					stats.Count = count
				}
				if avg, ok := latency["avg_ns"].(uint64); ok {
					stats.AvgNs = avg
				}
				if min, ok := latency["min_ns"].(uint64); ok {
					stats.MinNs = min
				}
				if max, ok := latency["max_ns"].(uint64); ok {
					stats.MaxNs = max
				}

				response.LatencyStats[key] = stats
			}
		}
	}

	// Populate error counts
	if errors, ok := allStats["errors"].(map[string]uint64); ok {
		for errType, count := range errors {
			response.ErrorCounts[errType] = count
		}
	}

	// Populate performance metrics
	if val, ok := allStats["total_bytes_read"].(uint64); ok {
		response.TotalBytesRead = int64(val)
	}
	if val, ok := allStats["total_bytes_written"].(uint64); ok {
		response.TotalBytesWritten = int64(val)
	}
	if val, ok := allStats["flush_count"].(uint64); ok {
		response.FlushCount = int64(val)
	}
	if val, ok := allStats["compaction_count"].(uint64); ok {
		response.CompactionCount = int64(val)
	}

	// Populate recovery stats
	if recovery, ok := allStats["recovery"].(map[string]interface{}); ok {
		if val, ok := recovery["wal_files_recovered"].(uint64); ok {
			response.RecoveryStats.WalFilesRecovered = val
		}
		if val, ok := recovery["wal_entries_recovered"].(uint64); ok {
			response.RecoveryStats.WalEntriesRecovered = val
		}
		if val, ok := recovery["wal_corrupted_entries"].(uint64); ok {
			response.RecoveryStats.WalCorruptedEntries = val
		}
		if val, ok := recovery["wal_recovery_duration_ms"].(int64); ok {
			response.RecoveryStats.WalRecoveryDurationMs = val
		}
	}

	return response, nil
}

// isOperationCounter checks if a stat key represents an operation counter
func isOperationCounter(key string) bool {
	return len(key) > 4 && key[len(key)-4:] == "_ops"
}

// isLatencyStat checks if a stat key represents latency statistics
func isLatencyStat(key string) bool {
	return len(key) > 8 && key[len(key)-8:] == "_latency"
}

// Compact triggers database compaction
func (s *KevoServiceServer) Compact(ctx context.Context, req *pb.CompactRequest) (*pb.CompactResponse, error) {

	err := s.engine.TriggerCompaction()
	if err != nil {
		return nil, err
	}

	err = s.engine.ReloadSSTables()
	if err != nil {
		return nil, err
	}

	return &pb.CompactResponse{Success: true}, nil
}

// GetNodeInfo returns information about this node and the replication topology
func (s *KevoServiceServer) GetNodeInfo(ctx context.Context, req *pb.GetNodeInfoRequest) (*pb.GetNodeInfoResponse, error) {
	// Create default response for standalone mode
	response := &pb.GetNodeInfoResponse{
		NodeRole:       pb.GetNodeInfoResponse_STANDALONE, // Default to standalone
		ReadOnly:       false,
		PrimaryAddress: "",
		Replicas:       nil,
		LastSequence:   0,
	}

	// Return default values if replication manager is nil
	if s.replicationManager == nil {
		return response, nil
	}

	// Get node role and replication info from the manager
	nodeRole, primaryAddr, replicas, lastSeq, readOnly := s.replicationManager.GetNodeInfo()

	// Set node role
	switch nodeRole {
	case "primary":
		response.NodeRole = pb.GetNodeInfoResponse_PRIMARY
	case "replica":
		response.NodeRole = pb.GetNodeInfoResponse_REPLICA
	default:
		response.NodeRole = pb.GetNodeInfoResponse_STANDALONE
	}

	// Set primary address if available
	response.PrimaryAddress = primaryAddr

	// Set replicas information if any
	if replicas != nil {
		for _, replica := range replicas {
			replicaInfo := &pb.ReplicaInfo{
				Address:      replica.Address,
				LastSequence: replica.LastSequence,
				Available:    replica.Available,
				Region:       replica.Region,
				Meta:         replica.Meta,
			}
			response.Replicas = append(response.Replicas, replicaInfo)
		}
	}

	// Set sequence and read-only status
	response.LastSequence = lastSeq
	response.ReadOnly = readOnly

	return response, nil
}
