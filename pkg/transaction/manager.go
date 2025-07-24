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
	"sync/atomic"

	"github.com/KevoDB/kevo/pkg/common"
	"github.com/KevoDB/kevo/pkg/engine/storage"
	"github.com/KevoDB/kevo/pkg/locks"
	"github.com/KevoDB/kevo/pkg/stats"
)

// TransactionManager implements the TransactionManager interface
type TransactionManager struct {

	// Statistics collector
	stats stats.Collector

	// sharedTxState is shared across all
	// transactions
	sharedTxState *SharedTxState

	// Transaction counters
	txStarted   atomic.Uint64
	txCompleted atomic.Uint64
	txAborted   atomic.Uint64
}

// NewManager creates a new transaction manager
func NewManager(storage *storage.StorageManager, stats stats.Collector) *TransactionManager {
	return &TransactionManager{
		// storage: storage,
		stats: stats,
		sharedTxState: &SharedTxState{
			locks:   locks.NewLocksWithDefaults(),
			txLock:  sync.RWMutex{},
			storage: storage,
		},
	}
}

var ErrUnknownTransactionMode = errors.New("unknown transaction mode")

// BeginTransaction starts a new transaction
func (m *TransactionManager) BeginTransaction(mode TransactionMode) (*Transaction, error) {
	// Track transaction start
	if m.stats != nil {
		m.stats.TrackOperation(stats.OpTxBegin)
	}
	m.txStarted.Add(1)

	id, err := common.GenerateRandom256Bit()
	if err != nil {
		return nil, err
	}

	// Create a new transaction
	tx := &Transaction{
		id:            common.NewReadOnly(id),
		mode:          common.NewReadOnly(mode),
		buffer:        NewBuffer(),
		sharedTxState: m.sharedTxState, // &m.txLock,
		stats:         m,
	}

	// Set transaction as active
	tx.active.Set(true)

	// Acquire appropriate lock
	switch mode {
	case ReadWriteSerialized:
		m.sharedTxState.AcquireSerializeLock()
	case WriteReadCommitted, ReadOnly:
		m.sharedTxState.AcquireNonSerializeLock()
		if mode == WriteReadCommitted {
			tx.sharedTxState.locks.RegisterTx(tx.ID(), func() {
				tx.rollbackSafeForRegisterTxLocked()
			})
		}
	default:
		return nil, ErrUnknownTransactionMode
	}

	return tx, nil
}

// IncrementTxCompleted increments the completed transaction counter
func (m *TransactionManager) IncrementTxCompleted() {
	m.txCompleted.Add(1)

	// Track the commit operation
	if m.stats != nil {
		m.stats.TrackOperation(stats.OpTxCommit)
	}
}

// IncrementTxAborted increments the aborted transaction counter
func (m *TransactionManager) IncrementTxAborted() {
	m.txAborted.Add(1)

	// Track the rollback operation
	if m.stats != nil {
		m.stats.TrackOperation(stats.OpTxRollback)
	}
}

// GetTransactionStats returns transaction statistics
func (m *TransactionManager) GetTransactionStats() map[string]interface{} {
	stats := make(map[string]any)

	stats["tx_started"] = m.txStarted.Load()
	stats["tx_completed"] = m.txCompleted.Load()
	stats["tx_aborted"] = m.txAborted.Load()

	// Calculate active transactions
	active := m.txStarted.Load() - m.txCompleted.Load() - m.txAborted.Load()
	stats["tx_active"] = active

	return stats
}
