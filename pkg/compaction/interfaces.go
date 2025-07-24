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
package compaction

// CompactionStrategy defines the interface for selecting files for compaction
type CompactionStrategy interface {
	// SelectCompaction selects files for compaction and returns a CompactionTask
	SelectCompaction() (*CompactionTask, error)

	// CompactRange selects files within a key range for compaction
	CompactRange(minKey, maxKey []byte) error

	// LoadSSTables reloads SSTable information from disk
	LoadSSTables() error

	// Close closes any resources held by the strategy
	Close() error
}

// CompactionExecutor defines the interface for executing compaction tasks
type CompactionExecutor interface {
	// CompactFiles performs the actual compaction of the input files
	CompactFiles(task *CompactionTask) ([]string, error)

	// DeleteCompactedFiles removes the input files that were successfully compacted
	DeleteCompactedFiles(filePaths []string) error
}

// FileTracker defines the interface for tracking file states during compaction
type FileTracker interface {
	// MarkFileObsolete marks a file as obsolete (can be deleted)
	MarkFileObsolete(path string)

	// MarkFilePending marks a file as being used in a compaction
	MarkFilePending(path string)

	// UnmarkFilePending removes the pending mark from a file
	UnmarkFilePending(path string)

	// IsFileObsolete checks if a file is marked as obsolete
	IsFileObsolete(path string) bool

	// IsFilePending checks if a file is marked as pending compaction
	IsFilePending(path string) bool

	// CleanupObsoleteFiles removes files that are no longer needed
	CleanupObsoleteFiles() error
}

// TombstoneManager defines the interface for tracking and managing tombstones
type TombstoneManager interface {
	// AddTombstone records a key deletion
	AddTombstone(key []byte)

	// ForcePreserveTombstone marks a tombstone to be preserved indefinitely
	ForcePreserveTombstone(key []byte)

	// ShouldKeepTombstone checks if a tombstone should be preserved during compaction
	ShouldKeepTombstone(key []byte) bool

	// CollectGarbage removes expired tombstone records
	CollectGarbage()
}
