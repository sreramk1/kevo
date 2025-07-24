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
package interceptors

import (
	"context"
	"errors"

	"google.golang.org/grpc/metadata"
)

const (
	SESSION_ID             = "session-id"
	SESSION_VALUE          = "session-value"
	INFO_TRANSACTION_ID    = "info-transaction-id"
	INFO_MODE              = "info-mode"
	INFO_NUM_OF_OPERATIONS = "info-num-of-operations"
	INFO_IS_ACTIVE         = "info-is-active"
)

var ErrSessionUnavailable = errors.New("Session unavailable")
var ErrMetadataUnavailable = errors.New("Metadata unavailable")
var ErrMultipleSessionsProvided = errors.New("Multiple session-ids and/or session-values were provided")

func SessionIdValue(ctx context.Context) (sessionId, sessionValue string, err error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", "", ErrMetadataUnavailable
	}
	sessionIdL := md.Get(SESSION_ID)
	sessionValL := md.Get(SESSION_VALUE)

	if len(sessionIdL) > 1 || len(sessionValL) > 1 {
		return "", "", ErrMultipleSessionsProvided
	}

	if len(sessionIdL) == 0 || len(sessionValL) == 0 {
		return "", "", ErrSessionUnavailable
	}

	return sessionIdL[0], sessionValL[0], nil
}
