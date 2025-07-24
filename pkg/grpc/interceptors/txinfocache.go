package interceptors

import (
	"strconv"
	"sync"

	"github.com/KevoDB/kevo/pkg/transaction"
	"google.golang.org/grpc/metadata"
)

type TransactionInfoCache struct {
	info *transaction.TransactionInfo
	mu   sync.RWMutex
}

func NewTransactionInfoCache() *TransactionInfoCache {
	return &TransactionInfoCache{
		info: &transaction.TransactionInfo{},
		mu:   sync.RWMutex{},
	}
}

func (c *TransactionInfoCache) UnsetTxInfoCache() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.info = &transaction.TransactionInfo{}
}

func (c *TransactionInfoCache) SetTxInfoCache(header metadata.MD) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	txid := header.Get(INFO_TRANSACTION_ID)
	mode := header.Get(INFO_MODE)
	numOfQueuedOperations := header.Get(INFO_NUM_OF_OPERATIONS)
	isActive := header.Get(INFO_IS_ACTIVE)
	if len(txid) == 0 ||
		len(mode) == 0 ||
		len(numOfQueuedOperations) == 0 ||
		len(isActive) == 0 {
		return false
	}

	modeInt, err := strconv.Atoi(mode[0])
	if err != nil {
		return false
	}

	numOfQueuedOperationsInt, err := strconv.Atoi(numOfQueuedOperations[0])
	if err != nil {
		return false
	}

	isActiveBool, err := strconv.ParseBool(isActive[0])
	if err != nil {
		return false
	}

	newInfo := &transaction.TransactionInfo{
		TxID:                     txid[0],
		Mode:                     transaction.TransactionMode(modeInt),
		NumberOfQueuedOperations: int64(numOfQueuedOperationsInt),
		IsActive:                 isActiveBool,
	}

	c.info = newInfo

	return true
}

func (c *TransactionInfoCache) GetTxInfoCache() transaction.TransactionInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return *c.info
}
