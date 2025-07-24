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
package common

import (
	"crypto/rand"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

type ReadOnly[T any] struct {
	value T
}

func NewReadOnly[T any](value T) *ReadOnly[T] {
	return &ReadOnly[T]{
		value: value,
	}
}

func (r *ReadOnly[T]) Get() T {
	return r.value
}

type Concurrent[T any] struct {
	value T
	mu    sync.RWMutex
}

func (cf *Concurrent[T]) Set(value T) {
	cf.mu.Lock()
	defer cf.mu.Unlock()
	cf.value = value
}

func (cf *Concurrent[T]) Get() T {
	cf.mu.RLock()
	defer cf.mu.RUnlock()
	return cf.value
}

func SleepBreakable[T any](canceler chan T, onSuccess T, duration time.Duration) T {

	select {
	case <-time.After(duration):
		return onSuccess
	case resp := <-canceler:
		return resp
	}

}

// GenerateRandom256Bit returns a cryptographically secure random 256-bit (32-byte) ID
// encoded as a hexadecimal string. It returns an error if the random number generation fails.
func GenerateRandom256Bit() (string, error) {
	// 256 bits = 32 bytes
	b := make([]byte, 32)
	// Read cryptographically secure random bytes
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate random ID: %w", err)
	}
	// Encode to hex string
	return hex.EncodeToString(b), nil
}

func CreateSessionValue(secret, sessionId string) (sessionValue string) {
	return computeSHA512_256([]byte(secret + sessionId))
}

func CompareSessionValues(sessionValue1, sessionValue2 string) bool {
	return subtle.ConstantTimeCompare([]byte(sessionValue1), []byte(sessionValue2)) == 1
}

func computeSHA512_256(data []byte) string {
	hasher := sha512.New512_256()
	hasher.Write(data)
	hash := hasher.Sum(nil)
	return hex.EncodeToString(hash)
}
