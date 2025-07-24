// Copyright 2025 Jeremy Tregunna
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
package compaction

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/KevoDB/kevo/pkg/common/iterator"
	"github.com/KevoDB/kevo/pkg/common/iterator/composite"
	"github.com/KevoDB/kevo/pkg/config"
	"github.com/KevoDB/kevo/pkg/sstable"
)

// DefaultCompactionExecutor handles the actual compaction process
type DefaultCompactionExecutor struct {
	// Configuration
	cfg *config.Config

	// SSTable directory
	sstableDir string

	// Tombstone manager for tracking deletions
	tombstoneManager TombstoneManager
}

// NewCompactionExecutor creates a new compaction executor
func NewCompactionExecutor(cfg *config.Config, sstableDir string, tombstoneManager TombstoneManager) *DefaultCompactionExecutor {
	return &DefaultCompactionExecutor{
		cfg:              cfg,
		sstableDir:       sstableDir,
		tombstoneManager: tombstoneManager,
	}
}

// CompactFiles performs the actual compaction of the input files
func (e *DefaultCompactionExecutor) CompactFiles(task *CompactionTask) ([]string, error) {
	// Create a merged iterator over all input files
	var iterators []iterator.Iterator

	// Add iterators from both levels
	for level := 0; level <= task.TargetLevel; level++ {
		for _, file := range task.InputFiles[level] {
			// We need an iterator that preserves delete markers
			if file.Reader != nil {
				iterators = append(iterators, file.Reader.NewIterator())
			}
		}
	}

	// Create hierarchical merged iterator
	mergedIter := composite.NewHierarchicalIterator(iterators)

	// Track keys to skip duplicate entries (for tombstones)
	var lastKey []byte
	var outputFiles []string
	var currentWriter *sstable.Writer
	var currentOutputPath string
	var outputFileSequence uint64 = 1
	var entriesInCurrentFile int

	// Function to create a new output file
	// This function does the following:
	// 1. If currentWriter is not nil, it indicates there was a previous
	//    write operation. This is appended into outputFiles.
	// 2. Increments the outputFileSequence for each filename
	//    it generates.
	// 3. Generates the new filename based on modified outputFileSequence
	//    (modified during the last time the function was called).
	// 4. Opens and assigns the new writer to currentWriter.
	createNewOutputFile := func() error {
		if currentWriter != nil {
			if err := currentWriter.Finish(); err != nil {
				return fmt.Errorf("failed to finish SSTable: %w", err)
			}
			outputFiles = append(outputFiles, currentOutputPath)
		}

		// Create a new output file
		timestamp := time.Now().UnixNano()
		currentOutputPath = fmt.Sprintf(task.OutputPathTemplate,
			task.TargetLevel, outputFileSequence, timestamp)
		outputFileSequence++

		var err error
		currentWriter, err = sstable.NewWriter(currentOutputPath)
		if err != nil {
			return fmt.Errorf("failed to create SSTable writer: %w", err)
		}

		entriesInCurrentFile = 0
		return nil
	}

	// Create a tombstone filter if we have a tombstone manager
	var tombstoneFilter *BasicTombstoneFilter
	if e.tombstoneManager != nil {
		tombstoneFilter = NewBasicTombstoneFilter(
			task.TargetLevel,
			e.cfg.MaxLevelWithTombstones,
			e.tombstoneManager,
		)
	}

	// Create the first output file
	if err := createNewOutputFile(); err != nil {
		return nil, err
	}

	// Iterate through all keys in sorted order
	mergedIter.SeekToFirst()
	for mergedIter.Valid() {
		key := mergedIter.Key()
		value := mergedIter.Value()

		// Skip duplicates (we've already included the newest version)
		if lastKey != nil && bytes.Equal(key, lastKey) {
			mergedIter.Next()
			continue
		}

		// Determine if we should keep this entry
		// If we have a tombstone filter, use it, otherwise use the default logic
		var shouldKeep bool
		isTombstone := mergedIter.IsTombstone()

		if tombstoneFilter != nil && isTombstone {
			// Use the tombstone filter for tombstones
			shouldKeep = tombstoneFilter.ShouldKeep(key, nil)
		} else {
			// Default logic - always keep non-tombstones, and keep tombstones in lower levels
			shouldKeep = !isTombstone || task.TargetLevel <= e.cfg.MaxLevelWithTombstones
		}

		if shouldKeep {
			var err error

			// Use the explicit AddTombstone method if this is a tombstone
			if isTombstone {
				err = currentWriter.AddTombstone(key)
			} else {
				err = currentWriter.Add(key, value)
			}

			if err != nil {
				return nil, fmt.Errorf("failed to add entry to SSTable: %w", err)
			}
			entriesInCurrentFile++
		}

		// If the current file is big enough, start a new one
		if int64(entriesInCurrentFile) >= e.cfg.SSTableMaxSize {
			if err := createNewOutputFile(); err != nil {
				return nil, err
			}
		}

		// Remember this key to skip duplicates
		lastKey = append(lastKey[:0], key...)
		mergedIter.Next()
	}

	// Finish the last output file
	if currentWriter != nil && entriesInCurrentFile > 0 {
		if err := currentWriter.Finish(); err != nil {
			return nil, fmt.Errorf("failed to finish SSTable: %w", err)
		}
		outputFiles = append(outputFiles, currentOutputPath)
	} else if currentWriter != nil {
		// No entries were written, abort the file
		currentWriter.Abort()
	}

	return outputFiles, nil
}

// DeleteCompactedFiles removes the input files that were successfully compacted
func (e *DefaultCompactionExecutor) DeleteCompactedFiles(filePaths []string) error {
	for _, path := range filePaths {
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("failed to delete compacted file %s: %w", path, err)
		}
	}
	return nil
}
