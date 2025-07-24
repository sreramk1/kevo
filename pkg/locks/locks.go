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
package locks

import (
	"errors"
	"math"
	"sync"
	"time"

	"github.com/KevoDB/kevo/pkg/common/reqprocessor"
	"github.com/KevoDB/kevo/pkg/common/retry"
)

// TODO (2025-05-14 PM 10:10):
// 1. Identify the path though which the error message resulting from a detected
//    deadlock reaches the top level interface, and check how it is handled. Ensure
//    that the deadlock signal (which is created by returning the
//    ErrTerminatingToResolveDeadlock) appropriately causes the transaction to be
//    invalidated and removed from the registry. [DONE]
// 2. Ensure the registry maps the client with a randomly generated ID. This is done
//    by introducing `client sessions`. The server returns a cryptographically
//    secure random ID to identify clients uniquely. This must be used as auth code.[DONE]

type Locks struct {
	// lockableTxRegistry stores all the IDs of transactions that uses this
	// record-level lock feature.
	lockableTxRegistry map[string]struct{}

	// onReleaseLock is an event optionally registered when creating a new
	// lock associated with a transaction. This executes when the locks
	// associated with a transaction is released.
	// This maps IDs of transactions (that uses this record-level lock
	// feature) with their corresponding callbacks that need to be called
	// when that transaction is being released.
	onReleaseLock map[string]func()

	// locks map keys with the transaction ID that leased them.
	locks map[string]string

	// locksTxIdToKeys maps the transaction-ids with the keys they lease.
	locksTxIdToKeys map[string]map[string]struct{}

	// awaitingLocks maps keys with the IDs of transactions
	// waiting to acquire leases to them.
	//
	// This information is used for detecting deadlocks.
	awaitingLocks map[string]map[string]struct{}

	// awaitingLocksTxIdToKey maps TxIds with keys they are
	// awaiting to lease. Note that, at any given time, a
	// transaction can wait trying to acquire exactly one lock,
	// since the lock-acquisition process blocks with busy wait
	// (with capped backoff and jitter) until a lock acquisition
	// is successful.
	awaitingLocksTxIdToKey map[string]string

	//awaitingLocksCanceler stores all the cancel function
	// awaitingLocksCanceler map[string]chan error

	// terminateForDeadlock stores the transaction-ids that must be
	// terminated to resolve deadlock.
	//
	// The termination will be done by a separate process.
	terminateForDeadlock map[string]struct{}

	retryConfig *retry.RetryConfig

	// deadlockJobProcessor queues the txIDs that must be checked for deadlocks.
	deadlockJobProcessor *reqprocessor.RequestProcessor[string]

	mu sync.Mutex
}

func NewLocks(deadlockCheckInterval time.Duration, retry *retry.RetryConfig) *Locks {
	l := &Locks{
		lockableTxRegistry:     map[string]struct{}{},
		onReleaseLock:          map[string]func(){},
		locks:                  map[string]string{},
		locksTxIdToKeys:        map[string]map[string]struct{}{},
		awaitingLocks:          map[string]map[string]struct{}{},
		awaitingLocksTxIdToKey: map[string]string{},
		terminateForDeadlock:   map[string]struct{}{},
		retryConfig:            retry,
		mu:                     sync.Mutex{},
	}

	l.deadlockJobProcessor = reqprocessor.NewRequestProcessor(
		func(curTxID string) { l.detectDeadlock(curTxID) },
		deadlockCheckInterval,
		func(req string) string { return req })

	l.deadlockJobProcessor.Start()

	return l
}

func NewLocksWithDefaults() *Locks {
	return NewLocks(
		100*time.Millisecond,
		&retry.RetryConfig{
			MaxRetries:     -1,
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     250 * time.Millisecond,
			// CapTotalWaitTime must be the maximum transaction idle duration.
			CapTotalWaitTime: time.Hour,
		})
}

// ErrFailedToAcquireLock returned to the caller of AcquireLock does not
// exit the transaction. It allows the transaction to continue, unlike other
// errors which immediately cause the transaction to terminate.
var ErrFailedToAcquireLock = errors.New("failed to acquire lock")
var ErrTerminatingToResolveDeadlock = errors.New("terminating to resolve deadlock")
var ErrTerminatedWait = errors.New("terminating wait")
var ErrTransactionNotRegisteredAsKVLock = errors.New("transaction not registered for key-level lock or was unregistered")

// AcquireLock tries acquiring a lock and blocks by busy waiting until it
// succeeds. If it fails because of deadlock, and if this specific transaction
// is chosen to be aborted, this blocks and returns ErrTerminatingToResolveDeadlock
// When this returns ErrFailedToAcquireLock, which it would when the busy wait
// timeouts, this won't terminate the active transaction.
func (l *Locks) AcquireLock(key string, txID string) (err error) {
	attemptCount := int64(0)

	err = retry.RetryWithConfig(l.retryConfig,
		func() error {

			l.mu.Lock()
			defer l.mu.Unlock()

			if !l.checkLockableLocked(txID) {
				return ErrTransactionNotRegisteredAsKVLock
			}

			if attemptCount > 1 {
				// suspected deadlock
				l.enqueueDeadlockDetectionJob(txID)
			}

			if _, ok := l.terminateForDeadlock[txID]; ok {
				return ErrTerminatingToResolveDeadlock
			}

			owningTxID, ok := l.locks[key]

			if !ok || txID == owningTxID {
				// the lock was either acquired successfully, or it was
				// already acquired before.
				l.addToLockLocked(key, txID)
				l.removeAwaitingLocked(key, txID)
				return nil
			}

			// A different transaction holds the lock, add the key to
			// awaitingLocks, or renew it.
			l.addAwaitingLocked(key, txID)
			attemptCount++
			return ErrFailedToAcquireLock
		},
		// retryable
		func(err error) bool {
			return IsFailedToAcquireLockErr(err)
		})

	if err != nil {
		l.ReleaseLocksAndUnregisterTx(txID)
		return err
	}

	return nil
}

func (l *Locks) RegisterTx(txId string, callbackOnRelease func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lockableTxRegistry[txId] = struct{}{}
	l.onReleaseLock[txId] = callbackOnRelease
}

func (l *Locks) checkLockableLocked(txId string) bool {
	_, ok := l.lockableTxRegistry[txId]
	return ok
}

// ReleaseLocksAndUnregisterTx unlocks all the acquired key-level locks and
// unregisters the transaction. All waiting locks will also be released with
// an error stating that it was released because the transaction was unregistered.
func (l *Locks) ReleaseLocksAndUnregisterTx(txId string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.checkLockableLocked(txId) {
		keys := l.locksTxIdToKeys[txId]
		for key := range keys {
			l.removeFromLockLocked(key, txId)
		}
		key := l.awaitingLocksTxIdToKey[txId]
		l.removeAwaitingLocked(key, txId)
		delete(l.terminateForDeadlock, txId)
		delete(l.lockableTxRegistry, txId)
		callback := l.onReleaseLock[txId]
		if callback != nil {
			callback()
		}
	}
}

func (l *Locks) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.deadlockJobProcessor.Close()
}

func (l *Locks) enqueueDeadlockDetectionJob(txId string) {
	l.deadlockJobProcessor.AddJob(txId)
}

func (l *Locks) detectDeadlock(startTxId string) (txMarkedToAbort string, deadlockDetected bool) {

	l.mu.Lock()
	defer l.mu.Unlock()

	getNextDependingNode := func(currentNode string) (nextNode string, ok bool) {
		awaitingKey, ok := l.awaitingLocksTxIdToKey[currentNode]
		if !ok {
			return "", false
		}
		nextNode = l.locks[awaitingKey]
		return nextNode, true
	}

	nodesPartOfCycle, deadlockDetected := DetectDeadlockCycle(startTxId,
		getNextDependingNode)

	if deadlockDetected {
		// The transaction with the least number of locks must be aborted.
		// This is necessary to allow transactions that depend on a lot of
		// resources to continue.
		tx := nodesPartOfCycle[0]
		numOfAcquiredLocks := math.MaxInt64
		for _, curTx := range nodesPartOfCycle {
			keys := l.locksTxIdToKeys[curTx]
			if len(keys) < numOfAcquiredLocks {
				numOfAcquiredLocks = len(keys)
				tx = curTx
			}
		}

		l.terminateForDeadlock[tx] = struct{}{} //ErrTerminatingToResolveDeadlock

		return tx, true
	}

	return "", false
}

func (l *Locks) addToLockLocked(key, txId string) {
	keys, ok := l.locksTxIdToKeys[txId]
	if !ok {
		keys = make(map[string]struct{})
		l.locksTxIdToKeys[txId] = keys
	}
	l.locks[key] = txId
	keys[key] = struct{}{}
}

func (l *Locks) removeFromLockLocked(key, txId string) {
	keys, ok := l.locksTxIdToKeys[txId]
	if ok {
		delete(keys, key)
		if len(keys) == 0 {
			delete(l.locksTxIdToKeys, txId)
		}
	}
	delete(l.locks, key)
}

func (l *Locks) addAwaitingLocked(key, txId string) {
	txExpiryMap, ok := l.awaitingLocks[key]
	if !ok {
		txExpiryMap = make(map[string]struct{})
		l.awaitingLocks[key] = txExpiryMap
	}
	txExpiryMap[txId] = struct{}{}

	l.awaitingLocksTxIdToKey[txId] = key
}

func (l *Locks) removeAwaitingLocked(key, txId string) {
	txExpiryMap, ok := l.awaitingLocks[key]
	if ok {
		delete(txExpiryMap, txId)
		if len(txExpiryMap) == 0 {
			delete(l.awaitingLocks, key)
		}
	}
	delete(l.awaitingLocksTxIdToKey, txId)
}
