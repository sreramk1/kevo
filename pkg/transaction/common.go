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

// TransactionMode defines the transaction access mode (ReadOnly, ReadWriteSerialized, WriteReadCommitted)
type TransactionMode int

const (
	UnknownTxMode TransactionMode = iota
	// ReadOnly transactions only read from the database
	ReadOnly

	// ReadWriteSerialized transactions can both read and write to the database
	ReadWriteSerialized

	// WriteReadCommitted allows multiple clients to write to the database
	// simultaneously without blocking, as long as they operate on disjoint keys.
	// When multiple transactions attempt to modify the same key, it blocks the
	// operation until timeout.
	WriteReadCommitted
)

func (tm TransactionMode) IsValid() bool {
	return tm == ReadOnly || tm == ReadWriteSerialized || tm == WriteReadCommitted
}
