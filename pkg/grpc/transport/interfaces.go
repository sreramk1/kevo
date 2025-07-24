// Copyright 2025 Sreram K (sreramk360@gmail.com)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transport

import (
	"context"

	"github.com/KevoDB/kevo/pkg/transaction"
	pb "github.com/KevoDB/kevo/proto/kevo"
	"google.golang.org/grpc/metadata"
)

// type IScanResponseCommon interface {
// 	CloseSend() error
// 	Context() context.Context
// 	Header() (metadata.MD, error)
// 	Trailer() metadata.MD
// }

// type ITxScanResponse interface {
// 	IScanResponseCommon
// 	Recv() (*pb.TxScanResponse, error)
// }

type Response struct {
	Key   []byte
	Value []byte
}

// type IScanResponse interface {
// 	IScanResponseCommon
// 	Recv() (*pb.ScanResponse, error)
// }

// var _ IScanResponse = (*scanResponseWrapper)(nil)

type ScanResponse struct {
	resp IScanResponse
}

// CloseSend implements IScanResponse.
func (s *ScanResponse) CloseSend() error {
	return s.resp.CloseSend()
}

// Context implements IScanResponse.
func (s *ScanResponse) Context() context.Context {
	return s.resp.Context()
}

// Header implements IScanResponse.
func (s *ScanResponse) Header() (metadata.MD, error) {
	return s.resp.Header()
}

// Recv implements IScanResponse.
func (s *ScanResponse) Recv() (*Response, error) {
	var result Response

	r, err := s.resp.Recv()
	if err != nil {
		return nil, err
	}
	result.Key = r.Key
	result.Value = r.Value

	return &result, nil
}

// Trailer implements IScanResponse.
func (s *ScanResponse) Trailer() metadata.MD {
	return s.resp.Trailer()
}

type IScanResponse interface {
	CloseSend() error
	Context() context.Context
	Header() (metadata.MD, error)
	Recv() (*pb.ScanResponse, error)
	Trailer() metadata.MD
}

type IServiceClient interface {
	Get(ctx context.Context, key []byte) (value []byte, found bool, err error)
	Put(ctx context.Context, key []byte, value []byte) (err error)
	Delete(ctx context.Context, Key []byte) (err error)
	BatchWrite(ctx context.Context, operations []pb.Operation) (success bool, err error)
	Scan(ctx context.Context, prefix, suffix, startKey, endKey []byte, limit int32) (*ScanResponse, error)
	// Transaction Operations
	BeginTransaction(ctx context.Context, txmode transaction.TransactionMode) (err error)
	CommitTransaction(ctx context.Context) (err error)
	RollbackTransaction(ctx context.Context) (err error)
	// // Transaction Operations within an active transaction
	// TxGet(ctx context.Context, transactionId string, key []byte) (value []byte, found bool, err error)
	// TxPut(ctx context.Context, transactionId string, key, value []byte) (success bool, err error)
	// TxDelete(ctx context.Context, TransactionId string, Key []byte) (success bool, err error)
	// TxScan(ctx context.Context, transactionId string, prefix, suffix, startKey,
	// 	endKey []byte, limit int32) (*ScanResponse, error)
	// Administrative Operations
	GetStats(ctx context.Context) (*pb.GetStatsResponse, error)
	Compact(ctx context.Context, force bool) (success bool, err error)
	GetNodeInfo(ctx context.Context) (*pb.GetNodeInfoResponse, error)
}
