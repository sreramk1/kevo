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
package transaction

import (
	"errors"
	"sync"

	"github.com/KevoDB/kevo/pkg/common"
	"github.com/KevoDB/kevo/pkg/common/iterator"
	"github.com/KevoDB/kevo/pkg/common/iterator/bounded"
	"github.com/KevoDB/kevo/pkg/common/iterator/composite"
	"github.com/KevoDB/kevo/pkg/engine/storage"
	"github.com/KevoDB/kevo/pkg/locks"
	"github.com/KevoDB/kevo/pkg/wal"
)

// Transaction implements the Transaction interface
type Transaction struct {
	id *common.ReadOnly[string]

	// Transaction mode (ReadOnly, ReadWriteSerialized, WriteReadCommitted)
	mode *common.ReadOnly[TransactionMode]

	// Buffer for transaction operations
	buffer *Buffer

	// Tracks if the transaction is still active
	active common.Concurrent[bool]

	// Lock for transaction-level synchronization
	mu sync.Mutex

	// sharedTxState is shared with all the other transactions
	// created from the same TransactionManager instance.
	sharedTxState *SharedTxState

	// Stats collector
	stats StatsCollector
}

type SharedTxState struct {

	// Storage backend for transaction operations
	storage *storage.StorageManager

	locks *locks.Locks

	// Transaction isolation lock
	txLock sync.RWMutex
}

type TransactionInfo struct {
	TxID                     string
	Mode                     TransactionMode
	NumberOfQueuedOperations int64
	// SizeOfBufferInBytes      int64
	IsActive bool
}

func (tx *Transaction) GetInfo() *TransactionInfo {
	return &TransactionInfo{
		TxID:                     tx.id.Get(),
		Mode:                     tx.mode.Get(),
		NumberOfQueuedOperations: int64(tx.buffer.NumberOfQueuedOperations()),
		// SizeOfBufferInBytes:      m.buffer.SizeInBytes(),
		IsActive: tx.active.Get(),
	}
}

// AcquireSerializeLock locks the entire database.
// This is a database level lock used for strict serialized writes
// and contends with every other lock.
func (m *SharedTxState) AcquireSerializeLock() {
	m.txLock.Lock()
}

// ReleaseSerializeLock releases serialize lock
func (m *SharedTxState) ReleaseSerializeLock() {
	m.txLock.Unlock()
}

// AcquireNonSerializeLock is acquired for all kinds of transactions
// other than strict database-level serialization.
// This lock does not contends with itself, therefore, can be acquired
// multiple times. The only contends with the Serialize Lock.
func (m *SharedTxState) AcquireNonSerializeLock() {
	m.txLock.RLock()
}

// ReleaseNonSerializeLock releases the non-serialize lock. Although this
// lock does not contend with itself, it blocks SerializeLock until it is
// released.
func (m *SharedTxState) ReleaseNonSerializeLock() {
	m.txLock.RUnlock()
}

// StatsCollector defines the interface for collecting transaction statistics
type StatsCollector interface {
	IncrementTxCompleted()
	IncrementTxAborted()
}

func (tx *Transaction) ID() string {
	return tx.id.Get()
}

func (tx *Transaction) GetForUpdate(key []byte) (val []byte, lockAcqSuccess bool, found bool, err error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	lockAcqSuccess, err = tx.acquireWriteTxLock(key)

	if err != nil {
		return nil, false, false, err
	}

	// indicates lock acquisition failed
	if !lockAcqSuccess {
		return nil, false, false, nil
	}

	val, found, err = tx.Get(key)

	return val, true, found, err
}

// Get retrieves a value for the given key
// Returns true for found if the value exists.
// This does not return an error if the key was not
// found
func (tx *Transaction) Get(key []byte) (val []byte, found bool, err error) {
	// Use transaction lock for consistent view
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Check if transaction is still active
	if !tx.active.Get() {
		return nil, false, ErrTransactionClosed
	}

	// First check the transaction buffer for any pending changes
	if val, found := tx.buffer.Get(key); found {
		if val == nil {
			// This is a deletion marker
			return nil, false, nil //ErrKeyNotFound
		}
		return val, true, nil
	}

	// Not in the buffer, get from the underlying storage
	return tx.sharedTxState.storage.Get(key)
}

var ErrInvalidTxMode = errors.New("invalid transaction isolation")

// Put adds or updates a key-value pair
func (tx *Transaction) Put(key, value []byte) (lockAcqSuccess bool, err error) {

	// Use transaction lock for consistent view
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Check if transaction is still active
	if !tx.active.Get() {
		return false, ErrTransactionClosed
	}

	lockAcqSuccess, err = tx.acquireWriteTxLock(key)

	if err != nil {
		return false, err
	}

	// indicates lock acquisition failed
	if !lockAcqSuccess {
		return false, nil
	}

	// Buffer the change - it will be applied on commit
	tx.buffer.Put(key, value)
	return true, nil
}

// Delete removes a key
func (tx *Transaction) Delete(key []byte) (lockAcqSuccess bool, err error) {

	// Use transaction lock for consistent view
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Check if transaction is still active
	if !tx.active.Get() {
		return false, ErrTransactionClosed
	}

	lockAcqSuccess, err = tx.acquireWriteTxLock(key)

	if err != nil {
		return false, err
	}

	if !lockAcqSuccess {
		return false, nil
	}

	// Buffer the deletion - it will be applied on commit
	tx.buffer.Delete(key)
	return true, nil
}

// NewIterator returns an iterator over the entire keyspace
func (tx *Transaction) NewIterator() iterator.Iterator {
	// Use transaction lock for consistent view
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Check if transaction is still active
	if !tx.active.Get() {
		// Return an empty iterator
		return &emptyIterator{}
	}

	// Get the storage iterator
	storageIter, err := tx.sharedTxState.storage.GetIterator()
	if err != nil {
		// If we can't get a storage iterator, return a buffer-only iterator
		return tx.buffer.NewIterator()
	}

	// If there are no changes in the buffer, just use the storage's iterator
	if tx.buffer.NumberOfQueuedOperations() == 0 {
		return storageIter
	}

	// Merge buffer and storage iterators
	bufferIter := tx.buffer.NewIterator()

	// Use composite hierarchical iterator
	return composite.NewHierarchicalIterator([]iterator.Iterator{bufferIter, storageIter})
}

// NewRangeIterator returns an iterator limited to a specific key range
func (tx *Transaction) NewRangeIterator(startKey, endKey []byte) iterator.Iterator {
	// Use transaction lock for consistent view
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Check if transaction is still active
	if !tx.active.Get() {
		// Return an empty iterator
		return &emptyIterator{}
	}

	// Get the storage iterator for the range
	storageIter, err := tx.sharedTxState.storage.GetRangeIterator(startKey, endKey)
	if err != nil {
		// If we can't get a storage iterator, use a bounded buffer iterator
		bufferIter := tx.buffer.NewIterator()
		return bounded.NewBoundedIterator(bufferIter, startKey, endKey)
	}

	// If there are no changes in the buffer, just use the storage's range iterator
	if tx.buffer.NumberOfQueuedOperations() == 0 {
		return storageIter
	}

	// Create a bounded buffer iterator
	bufferIter := tx.buffer.NewIterator()
	boundedBufferIter := bounded.NewBoundedIterator(bufferIter, startKey, endKey)

	// Merge the bounded buffer iterator with the storage range iterator
	return composite.NewHierarchicalIterator([]iterator.Iterator{boundedBufferIter, storageIter})
}

// Commit makes all changes permanent. If this results in an error while
// writing the changes, this behaves like rollback,
func (tx *Transaction) Commit() error {

	// Use transaction lock for consistent view
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Only proceed if the transaction is still active
	if !tx.active.Get() {
		return ErrTransactionClosed
	}

	tx.active.Set(false)

	if !tx.mode.Get().IsValid() {
		return ErrInvalidTxMode
	}

	var err error

	if tx.mode.Get() == ReadOnly {
		tx.sharedTxState.ReleaseNonSerializeLock()

		if tx.stats != nil {
			tx.stats.IncrementTxCompleted()
		}

		return nil
	}

	// Release locks only after updating the storage

	if tx.mode.Get() == WriteReadCommitted {
		defer tx.sharedTxState.ReleaseNonSerializeLock()
		defer tx.sharedTxState.locks.ReleaseLocksAndUnregisterTx(tx.ID())
	}

	if tx.mode.Get() == ReadWriteSerialized {
		defer tx.sharedTxState.ReleaseSerializeLock()
	}

	if tx.buffer.NumberOfQueuedOperations() > 0 {
		// Get operations from the buffer
		ops := tx.buffer.Operations()

		// Create a batch for all operations
		walBatch := make([]*wal.Entry, 0, len(ops))

		// Build WAL entries for each operation
		for _, op := range ops {
			if op.IsDelete {
				// Create delete entry
				walBatch = append(walBatch, &wal.Entry{
					Type: wal.OpTypeDelete,
					Key:  op.Key,
				})
			} else {
				// Create put entry
				walBatch = append(walBatch, &wal.Entry{
					Type:  wal.OpTypePut,
					Key:   op.Key,
					Value: op.Value,
				})
			}
		}

		// Apply the batch atomically
		err = tx.sharedTxState.storage.ApplyBatch(walBatch)
		if err != nil {
			if tx.stats != nil {
				tx.stats.IncrementTxAborted()
			}
			return err
		}
	}

	if tx.stats != nil {
		tx.stats.IncrementTxCompleted()
	}

	return err
}

// Rollback discards all transaction changes. This will not block when called
// concurrently, since it will release all the key-level locks immediately.
func (tx *Transaction) Rollback() error {
	// Rollback should work even if it was concurrently issued by a different
	// goroutine. This can only be possible if the transaction is unregistered
	// from the locks.
	tx.sharedTxState.locks.ReleaseLocksAndUnregisterTx(tx.ID())

	// Use transaction lock for consistent view
	tx.mu.Lock()
	defer tx.mu.Unlock()

	return tx.rollbackLocked()
}

func (tx *Transaction) rollbackSafeForRegisterTxLocked() error {
	// Only proceed if the transaction is still active
	if !tx.active.Get() {
		return ErrTransactionClosed
	}

	tx.active.Set(false)

	// Clear the buffer
	tx.buffer.Clear()

	switch tx.mode.Get() {
	case ReadOnly, WriteReadCommitted:
		tx.sharedTxState.ReleaseNonSerializeLock()
	case ReadWriteSerialized:
		tx.sharedTxState.ReleaseSerializeLock()
	default:
		return ErrUnknownTransactionMode
	}

	// Track transaction abort
	if tx.stats != nil {
		tx.stats.IncrementTxAborted()
	}

	return nil
}

func (tx *Transaction) rollbackLocked() error {
	// Only proceed if the transaction is still active
	if !tx.active.Get() {
		return ErrTransactionClosed
	}

	tx.active.Set(false)

	// Clear the buffer
	tx.buffer.Clear()

	// Release locks based on transaction mode
	if tx.mode.Get() == WriteReadCommitted {
		tx.sharedTxState.locks.ReleaseLocksAndUnregisterTx(tx.ID())
	}

	switch tx.mode.Get() {
	case ReadOnly, WriteReadCommitted:
		tx.sharedTxState.ReleaseNonSerializeLock()
	case ReadWriteSerialized:
		tx.sharedTxState.ReleaseSerializeLock()
	default:
		return ErrUnknownTransactionMode
	}

	// Track transaction abort
	if tx.stats != nil {
		tx.stats.IncrementTxAborted()
	}

	return nil
}

func (tx *Transaction) acquireWriteTxLock(key []byte) (acquiredLock bool, err error) {
	if !tx.mode.Get().IsValid() {
		return false, ErrInvalidTxMode
	}

	// Check if transaction is read-only
	if tx.mode.Get() == ReadOnly {
		return false, ErrReadOnlyTransaction
	}

	if tx.mode.Get() == ReadWriteSerialized {
		// Since serialized transaction blocks all operations by
		// default, we just return true. Since technically, it has
		// already acquired a more stronger lock.
		return true, nil
	}

	if tx.mode.Get() == WriteReadCommitted {
		err := tx.sharedTxState.locks.AcquireLock(string(key), tx.ID())
		if err != nil {
			tx.rollbackLocked()
			return false, err
		}
		return true, nil
	}

	return false, nil
}

// emptyIterator is a simple iterator implementation that returns no results
type emptyIterator struct{}

func (it *emptyIterator) SeekToFirst()      {}
func (it *emptyIterator) SeekToLast()       {}
func (it *emptyIterator) Seek([]byte) bool  { return false }
func (it *emptyIterator) Next() bool        { return false }
func (it *emptyIterator) Key() []byte       { return nil }
func (it *emptyIterator) Value() []byte     { return nil }
func (it *emptyIterator) Valid() bool       { return false }
func (it *emptyIterator) IsTombstone() bool { return false }
