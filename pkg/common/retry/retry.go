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
package retry

import (
	"errors"
	"math/rand"
	"time"
)

// RetryConfig defines parameters for retry operations
type RetryConfig struct {
	MaxRetries       int           // Maximum number of retries
	InitialBackoff   time.Duration // Initial backoff duration
	MaxBackoff       time.Duration // Maximum backoff duration
	CapTotalWaitTime time.Duration // Total time that can be spent waiting
}

// DefaultRetryConfig returns default retry configuration
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxRetries:       3,
		InitialBackoff:   5 * time.Millisecond,
		MaxBackoff:       50 * time.Millisecond,
		CapTotalWaitTime: 50 * time.Millisecond,
	}
}

var ErrRetryFailedBecauseOfInvalidConfig = errors.New("failed to retry. At least one terminating limit must be provided: either MaxRetries or CapTotalWaitTime should be provided, or both.")

// var ErrRetryTerminatedExternally = errors.New("retry was terminated externally")

// RetryWithConfig retries an operation with the given configuration
//
// Total time waited after `n` retries: InitialBackoff*10*((1.1)^{n+1}-1)
// But the actual waited time can vary based on MaxRetries and
// CapTotalWaitTime, which are limits that tend to cut the retry operation
// as soon as their limit is reached. MaxBackoff makes sure the individual
// backoff duration (the time waited sleeping) does not exceed the specified
// cap. This is useful when the retry logic includes operations that
// must respond to external state changes, which might be harder, if
// the retry duration was too high (although it may be well below
// CapTotalWaitTime).
func RetryWithConfig(config *RetryConfig, operation func() error, isRetryable func(error) bool) error {

	if config.CapTotalWaitTime <= 0 && config.MaxRetries <= 0 {
		return ErrRetryFailedBecauseOfInvalidConfig
	}

	backoff := config.InitialBackoff
	cumulativeWaitedTime := time.Duration(0)

	// This loop becomes "infinite" when r.MaxRetries is negative
	// or zero, implying that it goes on until CapTotalWaitTime is
	// reached.
	for i := 0; (i < config.MaxRetries) || (config.MaxRetries <= 0); i++ {
		// Attempt the operation
		err := operation()
		if err == nil {
			return nil
		}

		// Check if we should retry
		if !isRetryable(err) {
			return err
		}

		// Add some jitter to the backoff
		jitter := time.Duration(rand.Int63n(int64(backoff / 10)))
		backoff = min(backoff+jitter, config.MaxBackoff)

		if cumulativeWaitedTime+backoff > config.CapTotalWaitTime {
			backoff = config.CapTotalWaitTime - cumulativeWaitedTime
		}

		if backoff <= 0 {
			break
		}

		// // Wait before retrying
		// err = common.SleepBreakable(canceler, nil, backoff)
		// if err != nil {
		// 	return err
		// }

		// if status == common.CANCELED_SleepBreakableStatus {
		// 	return ErrRetryTerminatedExternally
		// }

		time.Sleep(backoff)
		cumulativeWaitedTime += backoff

	}

	return nil
}
