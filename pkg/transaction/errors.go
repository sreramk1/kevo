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

import "errors"

// Common errors for transaction operations
var (
	// ErrReadOnlyTransaction is returned when a write operation is attempted on a read-only transaction
	ErrReadOnlyTransaction = errors.New("cannot write to a read-only transaction")

	// ErrTransactionClosed is returned when an operation is attempted on a closed transaction
	ErrTransactionClosed = errors.New("transaction already committed or rolled back")

	// ErrKeyNotFound is returned when a key doesn't exist
	// ErrKeyNotFound = errors.New("key not found")

	// ErrInvalidEngine is returned when an incompatible engine type is provided
	// ErrInvalidEngine = errors.New("invalid engine type")
)

func IsReadOnlyTransactionErr(err error) bool {
	return errors.Is(err, ErrReadOnlyTransaction)
}

func IsTransactionClosedErr(err error) bool {
	return errors.Is(err, ErrTransactionClosed)
}
