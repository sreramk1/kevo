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
package reqprocessor

import "sync"

// Queue is a thread-safe FIFO queue for any item type with lazy compaction.
// T represents the type of items stored in the queue.
type Queue[T any] struct {
	lock        sync.Mutex
	items       []T
	head        int
	hashFn      func(T) string
	uniqueCheck map[string]struct{}
}

// NewQueue initializes and returns a new Queue for type T.
func NewQueue[T any](hash func(T) string) *Queue[T] {
	return &Queue[T]{
		lock:        sync.Mutex{},
		items:       make([]T, 0),
		head:        0,
		hashFn:      hash,
		uniqueCheck: make(map[string]struct{}),
	}
}

// Enqueue adds an item of type T to the queue.
func (q *Queue[T]) Enqueue(item T) {
	q.lock.Lock()
	if q.hashFn != nil {
		h := q.hashFn(item)
		q.uniqueCheck[h] = struct{}{}
	}
	q.items = append(q.items, item)
	q.lock.Unlock()
}

// Dequeue removes and returns the next item from the queue without blocking.
// The boolean returned is false if the queue is empty.
// Lazy compaction occurs when head surpasses half of the slice to prevent growth.
func (q *Queue[T]) Dequeue() (T, bool) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.head >= len(q.items) {
		var zero T
		return zero, false
	}

	item := q.items[q.head]
	q.head++

	// Lazy compaction: only reconstruct slice when head > half length
	if q.head > len(q.items)/2 {
		newLen := len(q.items) - q.head
		newItems := make([]T, newLen)
		copy(newItems, q.items[q.head:])
		q.items = newItems
		q.head = 0
	}

	if q.hashFn != nil {
		h := q.hashFn(item)
		delete(q.uniqueCheck, h)
	}

	return item, true
}
