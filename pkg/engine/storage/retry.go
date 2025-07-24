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
package storage

import (
	"github.com/KevoDB/kevo/pkg/common/retry"
	"github.com/KevoDB/kevo/pkg/wal"
)

// RetryOnWALRotating retries the operation if it fails with ErrWALRotating
func RetryOnWALRotating(operation func() error) error {
	r := retry.DefaultRetryConfig()
	return retry.RetryWithConfig(r, operation, isWALRotating)
}

// RetryWithSequence retries the operation if it fails with ErrWALRotating
// and returns the sequence number
func RetryWithSequence(operation func() (uint64, error)) (uint64, error) {
	r := retry.DefaultRetryConfig()
	var seq uint64

	err := retry.RetryWithConfig(r, func() error {
		var opErr error
		seq, opErr = operation()
		return opErr
	}, isWALRotating)

	return seq, err
}

// isWALRotating checks if the error is due to WAL rotation or closure
func isWALRotating(err error) bool {
	// Both ErrWALRotating and ErrWALClosed can occur during WAL rotation
	// Since WAL rotation is a normal operation, we should retry in both cases
	return err == wal.ErrWALRotating || err == wal.ErrWALClosed
}
