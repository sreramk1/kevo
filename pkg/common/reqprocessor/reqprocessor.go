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

import (
	"time"
)

type ProcessHandler[T any] func(T)

// RequestProcessor launches a goroutine that periodically processes all queued items
// at the given interval until the done channel is closed.
// - q: the Queue to pull items from
// - handler: function to process each item of type T
// - interval: how often to check and process the queue
// - done: channel to signal shutdown
type RequestProcessor[T any] struct {
	q        *Queue[T]
	handler  ProcessHandler[T]
	interval time.Duration
	done     chan struct{}
	execute  chan struct{}
}

func NewRequestProcessor[T any](
	handler ProcessHandler[T],
	interval time.Duration,
	hashJobReq func(req T) string,
) *RequestProcessor[T] {
	return &RequestProcessor[T]{
		q:        NewQueue(hashJobReq),
		handler:  handler,
		interval: interval,
		done:     make(chan struct{}),
		execute:  make(chan struct{}),
	}
}

func (r *RequestProcessor[T]) AddJob(item T) {
	r.q.Enqueue(item)
}

func (r *RequestProcessor[T]) Close() {
	close(r.done)
}

func (r *RequestProcessor[T]) Start() {
	go func() {
		ticker := time.NewTicker(r.interval)
		defer ticker.Stop()
		for {
			select {
			case <-r.done:
				return
			case <-ticker.C:
				// Process all available items on each tick
				r.doExecute()
			case <-r.execute:
				r.doExecute()
			}
		}
	}()
}

func (r *RequestProcessor[T]) ExecuteNow() {
	r.execute <- struct{}{}
}

func (r *RequestProcessor[T]) doExecute() {
	for {
		item, ok := r.q.Dequeue()
		if !ok {
			break
		}
		r.handler(item)
	}
}

// func StartRequestProcessor[T any](
// 	q *Queue[T],
// 	handler ProcessHandler[T],
// 	interval time.Duration,
// 	done <-chan struct{},
// ) {
// 	go func() {
// 		ticker := time.NewTicker(interval)
// 		defer ticker.Stop()
// 		for {
// 			select {
// 			case <-done:
// 				return
// 			case <-ticker.C:
// 				// Process all available items on each tick
// 				for {
// 					item, ok := q.TryDequeue()
// 					if !ok {
// 						break
// 					}
// 					handler(item)
// 				}
// 			}
// 		}
// 	}()
// }
