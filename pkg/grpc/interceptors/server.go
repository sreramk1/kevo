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
	"strconv"

	"github.com/KevoDB/kevo/pkg/sessreg"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var ErrNoIncomingMetadataInRPCContext = errors.New("no incoming metadata in rpc context")
var ErrInvalidSession = errors.New("Invalid session")

type wrappedStreamServer struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *wrappedStreamServer) Context() context.Context {
	return s.ctx
}

func CreateServerStreamInterceptor(secret string, sReg *sessreg.SessionRegistry) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		md, ok := metadata.FromIncomingContext(ss.Context())
		if !ok {
			return ErrNoIncomingMetadataInRPCContext
		}

		sessionId := md.Get(SESSION_ID)
		sessionValue := md.Get(SESSION_VALUE)

		if len(sessionId) > 1 || len(sessionValue) > 1 {
			return ErrMultipleSessionsProvided
		}

		var ctx context.Context

		if len(sessionId) == 0 || len(sessionValue) == 0 {
			newSessionId, newSessionValue, err := sReg.CreateSession(secret)
			// newSessionId, err := common.GenerateRandom256Bit()
			if err != nil {
				return err
			}

			// newSessionValue := common.CreateSessionValue(secret, newSessionId)
			md.Append(SESSION_ID, newSessionId)
			md.Append(SESSION_VALUE, newSessionValue)
			sessionId = []string{newSessionId}
			sessionValue = []string{newSessionValue}
		} else {
			err := sReg.VerifySession(sessionId[0], sessionValue[0], secret)
			if err != nil {
				return err
			}

		}
		ctx = metadata.NewIncomingContext(ss.Context(), md)

		// Call the handler to complete the normal execution of the RPC.
		err := handler(srv, &wrappedStreamServer{ss, ctx})

		// Create and set responseHeader metadata from interceptor to client.
		responseHeader := metadata.Pairs(
			SESSION_ID, sessionId[0],
			SESSION_VALUE, sessionValue[0],
		)

		info, errSessInfo := sReg.GetSessionInfo(sessionId[0])

		if errSessInfo == nil && info != nil {

			var isActive string

			if info.IsActive {
				isActive = "true"
			} else {
				isActive = "false"
			}

			responseHeader.Append(INFO_TRANSACTION_ID, info.TxID)
			responseHeader.Append(INFO_MODE, strconv.Itoa(int(info.Mode)))
			responseHeader.Append(INFO_IS_ACTIVE, isActive)
			responseHeader.Append(INFO_NUM_OF_OPERATIONS, strconv.Itoa(int(info.NumberOfQueuedOperations)))
		}

		ss.SetHeader(responseHeader)

		return err
	}
}

func CreateServerUnaryInterceptor(secret string, sreg *sessreg.SessionRegistry) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, ErrNoIncomingMetadataInRPCContext
		}

		sessionId := md.Get(SESSION_ID)
		sessionValue := md.Get(SESSION_VALUE)

		if len(sessionId) > 1 || len(sessionValue) > 1 {
			return nil, ErrMultipleSessionsProvided
		}

		if len(sessionId) == 0 || len(sessionValue) == 0 {

			newSessionId, newSessionValue, err := sreg.CreateSession(secret)
			// newSessionId, err := common.GenerateRandom256Bit()
			if err != nil {
				return nil, err
			}

			// newSessionValue := common.CreateSessionValue(secret, newSessionId)
			md.Append(SESSION_ID, newSessionId)
			md.Append(SESSION_VALUE, newSessionValue)
			sessionId = []string{newSessionId}
			sessionValue = []string{newSessionValue}
		} else {
			// verify if the session is valid and was actually created by the server
			err := sreg.VerifySession(sessionId[0], sessionValue[0], secret)
			if err != nil {
				return nil, err
			}

		}

		ctx = metadata.NewIncomingContext(ctx, md)

		// Call the handler to complete the normal execution of the RPC.
		resp, err := handler(ctx, req)

		responseHeader := metadata.Pairs(
			SESSION_ID, sessionId[0],
			SESSION_VALUE, sessionValue[0],
		)

		info, errSessInfo := sreg.GetSessionInfo(sessionId[0])

		if errSessInfo == nil && info != nil {

			var isActive string

			if info.IsActive {
				isActive = "true"
			} else {
				isActive = "false"
			}

			responseHeader.Append(INFO_TRANSACTION_ID, info.TxID)
			responseHeader.Append(INFO_MODE, strconv.Itoa(int(info.Mode)))
			responseHeader.Append(INFO_IS_ACTIVE, isActive)
			responseHeader.Append(INFO_NUM_OF_OPERATIONS, strconv.Itoa(int(info.NumberOfQueuedOperations)))
		}

		grpc.SetHeader(ctx, responseHeader)

		return resp, err
	}
}
