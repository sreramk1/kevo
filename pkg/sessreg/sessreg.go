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
	"time"

	"github.com/KevoDB/kevo/pkg/common"
	"github.com/KevoDB/kevo/pkg/transaction"
)

type Session struct {
	ExpiryTs       int64
	State          *SessionState
	ActiveUseCount int64
}

type SessionRegistry struct {
	sessions           map[string]*Session
	ttl                time.Duration
	autoExpiryCanceler chan struct{}
	autoExpiryDuration time.Duration
	closed             bool
	mu                 sync.Mutex
}

func NewSessionRegistryWithDefaults() *SessionRegistry {
	return NewSessionRegistry(time.Second*10, time.Second*30)
}

func NewSessionRegistry(ttl time.Duration, autoExpiryDuration time.Duration) *SessionRegistry {
	return &SessionRegistry{
		sessions:           make(map[string]*Session),
		ttl:                ttl,
		autoExpiryCanceler: nil,
		autoExpiryDuration: autoExpiryDuration,
		closed:             false,
		mu:                 sync.Mutex{},
	}
}

func (r *SessionRegistry) ensureNotClosedLocked() {
	if r.closed {
		panic("use of closed SessionRegistry is forbidden")
	}
}

func (r *SessionRegistry) CreateSession(secret string) (sessionId string, sessionValue string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureNotClosedLocked()

	sessionId, err = common.GenerateRandom256Bit()
	if err != nil {
		return "", "", err
	}

	sessionValue = common.CreateSessionValue(secret, sessionId)
	ts := time.Now().UnixNano()

	r.sessions[sessionId] = &Session{
		ExpiryTs:       ts + int64(r.ttl),
		State:          NewSessionState(),
		ActiveUseCount: 0,
	}

	return sessionId, sessionValue, nil
}

func (r *SessionRegistry) RenewSession(sessionId string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureNotClosedLocked()
	return r.renewSessionLocked(sessionId)
}

func (r *SessionRegistry) Remove(sessionId string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureNotClosedLocked()
	r.removeLocked(sessionId)
}

var ErrSessionValidationFailed = errors.New("Session validation failed")

func (r *SessionRegistry) VerifySession(sessionId, sessionValue, secret string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureNotClosedLocked()

	computedSessionValue := common.CreateSessionValue(secret, sessionId)
	if !common.CompareSessionValues(sessionValue, computedSessionValue) {
		return ErrSessionValidationFailed
	}

	_, ok := r.sessions[sessionId]

	if !ok {
		return ErrInvalidSession
	}

	err := r.renewSessionLocked(sessionId)
	if err != nil {
		return err
	}

	return nil
}

var ErrExpiryJobRunning = errors.New("Expiry job is already running")

func (r *SessionRegistry) StartAutoExpiry() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureNotClosedLocked()

	if r.autoExpiryCanceler != nil {
		return ErrExpiryJobRunning
	}

	r.autoExpiryCanceler = make(chan struct{})

	autoExpiryDuration := r.autoExpiryDuration

	go func() {
		ticker := time.NewTicker(autoExpiryDuration)
		defer ticker.Stop()
		for {
			select {
			case <-r.autoExpiryCanceler:
				return
			case <-ticker.C:

				currentTs := time.Now().UnixNano()

				r.mu.Lock()
				r.ensureNotClosedLocked()
				for sId, session := range r.sessions {
					if session.ActiveUseCount > 0 {
						continue
					}
					if session.ExpiryTs <= currentTs {
						r.removeLocked(sId)
					}
				}
				r.mu.Unlock()
			}
		}
	}()
	return nil
}

var ErrAutoExpiryJobWasRunning = errors.New("Auto expiry job was not runnings")

func (r *SessionRegistry) StopAutoExpiry() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureNotClosedLocked()
	r.stopAutoExpiryLocked()

}

func (r *SessionRegistry) stopAutoExpiryLocked() {

	if r.autoExpiryCanceler != nil {
		close(r.autoExpiryCanceler)
		r.autoExpiryCanceler = nil
	}
}

func (r *SessionRegistry) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureNotClosedLocked()

	r.stopAutoExpiryLocked()

	for _, session := range r.sessions {
		if !session.State.IsClosed() {
			session.State.Close()
		}
	}

	r.sessions = nil
	r.closed = true

}

func (r *SessionRegistry) ActiveUseCount(sessionId string) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureNotClosedLocked()

	err := r.renewSessionLocked(sessionId)
	if err != nil {
		return 0, err
	}

	sess := r.sessions[sessionId]
	if sess != nil {
		return sess.ActiveUseCount, nil
	}

	return 0, ErrInvalidSession

}

func (r *SessionRegistry) GetSessionInfo(sessionId string) (*transaction.TransactionInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureNotClosedLocked()

	// Check if SessionState was closed. If it was closed, remove the session.

	session, ok := r.sessions[sessionId]
	if !ok {
		return nil, ErrInvalidSession
	}

	if session.State.IsClosed() {
		r.removeLocked(sessionId)
		return nil, ErrSessionWasClosed
	}
	//------------------------------------------------------------------------
	return session.State.SessionStateInfo()
}

var ErrInvalidSession = errors.New("invalid session")
var ErrSessionWasClosed = errors.New("session was already closed")

// IncActiveUseCount increments the active use count, which records the number of RPC
// calls are concurrently using the specific session state. This prevents the
// SessionState from being expired for as long as the server uses it on behalf
// of the client.
func (r *SessionRegistry) IncActiveUseCount(sessionId string) (*SessionState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureNotClosedLocked()

	// Check if SessionState was closed. If it was closed, remove the session.

	session, ok := r.sessions[sessionId]
	if !ok {
		return nil, ErrInvalidSession
	}

	if session.State.IsClosed() {
		r.removeLocked(sessionId)
		return nil, ErrSessionWasClosed
	}

	//------------------------------------------------------------------------

	// Renew the session (while making sure it was not expired) if the
	// session-state was't being actively used. However, if it was being
	// actively used, then skip trying to renew the session, since the
	// SessionState cannot expire when at least one RPC call is actively
	// using it (i.e., when its active use-count is greater than zero).

	if session.ActiveUseCount == 0 {
		err := r.renewSessionLocked(sessionId)
		if err != nil {
			return nil, err
		}
	}

	session.ActiveUseCount++

	//------------------------------------------------------------------------

	return session.State, nil
}

var ErrSessionNotInUse = errors.New("Session is not in use")

// DecActiveUseCount decrements the active use count. When it reaches zero,
// then the expiration for the session calculated at that point will
// tale effect.
func (r *SessionRegistry) DecActiveUseCount(sessionId string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureNotClosedLocked()

	// Check if SessionState was closed. If it was closed, remove the session.

	session, ok := r.sessions[sessionId]
	if !ok {
		return ErrInvalidSession
	}

	if session.State.IsClosed() {
		r.removeLocked(sessionId)
		return nil
	}

	//------------------------------------------------------------------------

	// Decrement active-use count by one, if it exists.

	if session.ActiveUseCount == 0 {
		return ErrSessionNotInUse
	}

	if session.ActiveUseCount > 0 {
		session.ActiveUseCount--
	}

	//------------------------------------------------------------------------
	// Force-reset session expiry time.
	// This is done because, the execution time when session.ActiveUseCount
	// is greater than zero should not be counted for expiration. So, as soon as
	// session.ActiveUseCount reaches zero, the new expiry time that gets set becomes
	// effective and if the session remains unused or if it is not renewed leading to
	// its expiration, it gets removed.
	if session.ActiveUseCount == 0 {
		ts := time.Now().UnixNano()
		expTs := ts + int64(r.ttl)
		session.ExpiryTs = expTs
	}
	//------------------------------------------------------------------------

	return nil
}

func (r *SessionRegistry) removeLocked(sessionId string) {
	r.ensureNotClosedLocked()

	session := r.sessions[sessionId]
	if session != nil {
		session.State.Close()
		delete(r.sessions, sessionId)
	}
}

var ErrSessionExpired = errors.New("session already expired")

func (r *SessionRegistry) renewSessionLocked(sessionId string) error {
	r.ensureNotClosedLocked()

	session, ok := r.sessions[sessionId]
	if !ok {
		return ErrInvalidSession
	}

	// If a SessionState is in active use, it cannot be expired (unless it is forced
	// by calling Close()). And when the SessionState is removed from being active,
	// it extends the expiry duration automatically, regardless of whether it may have
	// been expired. Also, an error is not returned when the state is in active use
	// because, the expiry time of the session in active use automatically gets renewed
	// as soon as the number of RPC calls that use the SessionState at that drops to
	// zero.
	if session.ActiveUseCount == 0 {
		ts := time.Now().UnixNano()
		if ts >= session.ExpiryTs {
			r.removeLocked(sessionId)
			return ErrSessionExpired
		}

		expTs := ts + int64(r.ttl)
		session.ExpiryTs = expTs
	}

	return nil
}
