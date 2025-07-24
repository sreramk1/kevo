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
package sessreg

import (
	"errors"
	"sync"

	"github.com/KevoDB/kevo/pkg/transaction"
)

type SessionState struct {
	tx     *transaction.Transaction
	closed bool
	mu     sync.RWMutex
}

func NewSessionState() *SessionState {
	return &SessionState{
		tx:     nil,
		closed: false,
	}
}

func (s *SessionState) SessionStateInfo() (info *transaction.TransactionInfo, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}

	if s.tx != nil {
		return s.tx.GetInfo(), nil
	}

	return nil, nil
}

var ErrClosed = errors.New("Session was closed")

func (s *SessionState) GetTx() (*transaction.Transaction, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	return s.tx, nil
}

func (s *SessionState) UnsetTx() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}

	s.tx = nil

	return nil
}

func (s *SessionState) SetTx(tx *transaction.Transaction) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrClosed
	}

	s.tx = tx

	return nil
}

func (s *SessionState) IsClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closed
}

func (s *SessionState) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.tx != nil {
		s.tx.Rollback()
	}

	s.closed = true
}
