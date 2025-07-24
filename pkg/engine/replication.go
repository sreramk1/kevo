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
package engine

import (
	"github.com/KevoDB/kevo/pkg/common/log"
	"github.com/KevoDB/kevo/pkg/wal"
)

// GetWAL exposes the WAL for replication purposes
func (e *EngineFacade) GetWAL() *wal.WAL {
	// This is an enhancement to the EngineFacade to support replication
	// It's used by the replication manager to access the WAL
	if e.storage == nil {
		return nil
	}

	// Get WAL from storage manager
	// For now, we'll use type assertion since the interface doesn't
	// have a GetWAL method
	type walProvider interface {
		GetWAL() *wal.WAL
	}

	return e.storage.GetWAL()

	// if provider, ok := e.storage.(walProvider); ok {
	// 	return provider.GetWAL()
	// }

	// return nil
}

// SetReadOnly sets the engine to read-only mode for replicas
func (e *EngineFacade) SetReadOnly(readOnly bool) {
	// This is an enhancement to the EngineFacade to support replication
	// Setting this will force the engine to reject write operations
	// Used by replicas to ensure they don't accept direct writes
	e.readOnly.Store(readOnly)
	log.Info("Engine read-only mode set to: %v", readOnly)
}

// IsReadOnly returns whether the engine is in read-only mode
func (e *EngineFacade) IsReadOnly() bool {
	return e.readOnly.Load()
}
