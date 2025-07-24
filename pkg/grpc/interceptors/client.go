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
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type ClientInterceptorsCreator struct {
	*TransactionInfoCache
	sessionId, sessionValue string

	mu sync.RWMutex
}

func NewClientInterceptorCreators() *ClientInterceptorsCreator {
	return &ClientInterceptorsCreator{
		TransactionInfoCache: NewTransactionInfoCache(),
		sessionId:            "",
		sessionValue:         "",
		mu:                   sync.RWMutex{},
	}
}

func (c *ClientInterceptorsCreator) CreateClientStreamInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		c.mu.RLock()
		sessionId := c.sessionId
		sessionValue := c.sessionValue
		c.mu.RUnlock()

		if sessionId != "" && sessionValue != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, SESSION_ID, sessionId, SESSION_VALUE, sessionValue)
		}

		var header metadata.MD
		opts = append(opts, grpc.Header(&header))

		s, err := streamer(ctx, desc, cc, method, opts...)

		if !c.SetTxInfoCache(header) {
			c.UnsetTxInfoCache()
		}

		if err != nil {
			// reset global session on error
			c.mu.Lock()
			c.sessionId = ""
			c.sessionValue = ""
			c.mu.Unlock()
			c.UnsetTxInfoCache()
			return nil, err
		}

		newSessionIdL := header.Get(SESSION_ID)
		newSessionValueL := header.Get(SESSION_VALUE)

		if len(newSessionIdL) > 0 && len(newSessionValueL) > 0 {
			newSessionId := newSessionIdL[0]
			newSessionValue := newSessionValueL[0]

			if newSessionId != sessionId && newSessionValue != sessionValue {
				c.mu.Lock()
				c.sessionId = newSessionId
				c.sessionValue = newSessionValue
				c.mu.Unlock()
			}
		}

		return newWrappedStreamClient(s), nil

	}
}

func (c *ClientInterceptorsCreator) CreateClientUnaryInterceptorFactory() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		c.mu.RLock()
		sessionId := c.sessionId
		sessionValue := c.sessionValue
		c.mu.RUnlock()

		if sessionId != "" && sessionValue != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, SESSION_ID, sessionId, SESSION_VALUE, sessionValue)
		}

		var header metadata.MD
		opts = append(opts, grpc.Header(&header))

		err := invoker(ctx, method, req, reply, cc, opts...)

		if !c.SetTxInfoCache(header) {
			c.UnsetTxInfoCache()
		}

		if err != nil {
			// reset global session on error
			c.mu.Lock()
			c.sessionId = ""
			c.sessionValue = ""
			c.mu.Unlock()
			c.UnsetTxInfoCache()
			return err
		}

		newSessionIdL := header.Get(SESSION_ID)
		newSessionValueL := header.Get(SESSION_VALUE)

		if len(newSessionIdL) > 0 && len(newSessionValueL) > 0 {
			newSessionId := newSessionIdL[0]
			newSessionValue := newSessionValueL[0]

			if newSessionId != sessionId && newSessionValue != sessionValue {
				c.mu.Lock()
				c.sessionId = newSessionId
				c.sessionValue = newSessionValue
				c.mu.Unlock()
			}
		}

		return nil
	}
}

// wrappedStream  wraps around the embedded grpc.ClientStream, and intercepts the RecvMsg and
// SendMsg method call.
type wrappedStreamClient struct {
	grpc.ClientStream
}

func (w *wrappedStreamClient) RecvMsg(m any) error {
	return w.ClientStream.RecvMsg(m)
}

func (w *wrappedStreamClient) SendMsg(m any) error {
	return w.ClientStream.SendMsg(m)
}

func newWrappedStreamClient(s grpc.ClientStream) grpc.ClientStream {
	return &wrappedStreamClient{s}
}
