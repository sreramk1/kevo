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

package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"time"

	"github.com/KevoDB/kevo/pkg/grpc/interceptors"
	"github.com/KevoDB/kevo/pkg/transaction"
	pb "github.com/KevoDB/kevo/proto/kevo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// var _ transport.Client = (*GRPCClient)(nil)
var _ IServiceClient = (*GRPCClient)(nil)

// GRPCClient implements the transport.Client interface for gRPC
type GRPCClient struct {
	endpoint           string
	options            TransportOptions
	conn               *grpc.ClientConn
	client             pb.KevoServiceClient
	metrics            MetricsCollector
	sessionRenewalDone chan struct{}
	interceptorState   *interceptors.ClientInterceptorsCreator
}

// NewGRPCClient creates a new gRPC client
func NewGRPCClient(endpoint string, options TransportOptions) (*GRPCClient, error) {
	return &GRPCClient{
		endpoint: endpoint,
		options:  options,
		metrics:  NewMetricsCollector(),
	}, nil
}

// Connect establishes a connection to the server
func (c *GRPCClient) Connect(ctx context.Context) error {
	dialOptions := []grpc.DialOption{
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                15 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	}

	// Configure TLS if enabled
	if c.options.TLSEnabled {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}

		// Load client certificate if provided
		if c.options.CertFile != "" && c.options.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(c.options.CertFile, c.options.KeyFile)
			if err != nil {
				c.metrics.RecordConnection(false)
				return fmt.Errorf("failed to load client certificate: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		// Add credentials to dial options
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		// Use insecure credentials if TLS is not enabled
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	interceptorState := interceptors.NewClientInterceptorCreators()

	dialOptions = append(dialOptions,
		grpc.WithUnaryInterceptor(interceptorState.CreateClientUnaryInterceptorFactory()),
		grpc.WithStreamInterceptor(interceptorState.CreateClientStreamInterceptor()))

	conn, err := grpc.NewClient(c.endpoint, dialOptions...)
	if err != nil {
		c.metrics.RecordConnection(false)
		return err
	}

	conn.Connect()

	state := conn.GetState()

	// Set timeout for connection
	dialCtx, cancel := context.WithTimeout(ctx, c.options.Timeout)
	defer cancel()

	for state != connectivity.Ready {
		if !conn.WaitForStateChange(dialCtx, state) {
			return dialCtx.Err()
		}
		state = conn.GetState()
	}

	c.conn = conn
	c.client = pb.NewKevoServiceClient(conn)
	c.metrics.RecordConnection(true)

	if c.sessionRenewalDone != nil {
		close(c.sessionRenewalDone)
	}

	c.sessionRenewalDone = make(chan struct{})

	c.interceptorState = interceptorState
	// Renew session:

	go func() {

		ticker := time.NewTicker(time.Second * 5)
		defer ticker.Stop()

		// done := make(chan struct{})

		for {
			select {
			case <-c.sessionRenewalDone:
				return
			case <-ticker.C:
				_, err := c.client.RenewSession(context.Background(), &pb.RenewSessionRequest{})

				if err != nil {
					fmt.Fprintf(os.Stderr, "failed to renew session %s\n", err)
				}
			}
		}
	}()

	return nil
}
func listToPointerList[T any](l []T) []*T {
	result := []*T{}
	for _, e := range l {
		result = append(result, &e)
	}
	return result
}

func (c *GRPCClient) GetCachedTxInfo() transaction.TransactionInfo {
	return c.interceptorState.GetTxInfoCache()
}

// BatchWrite implements IServiceClient.
func (c *GRPCClient) BatchWrite(ctx context.Context, operations []pb.Operation) (success bool, err error) {

	grpcResp, err := c.client.BatchWrite(ctx,
		&pb.BatchWriteRequest{
			Operations: listToPointerList(operations),
		})

	if err != nil {
		return false, err
	}

	return grpcResp.Success, nil
}

// BeginTransaction implements IServiceClient.
func (c *GRPCClient) BeginTransaction(ctx context.Context, txmode transaction.TransactionMode) (err error) {
	grpcReq := &pb.BeginTransactionRequest{
		TxMode: pb.TransactionMode(txmode),
	}

	_, err = c.client.BeginTransaction(ctx, grpcReq)
	if err != nil {
		return err
	}

	return nil

}

// CommitTransaction implements IServiceClient.
func (c *GRPCClient) CommitTransaction(ctx context.Context) (err error) {
	grpcReq := &pb.CommitTransactionRequest{}
	_, err = c.client.CommitTransaction(ctx, grpcReq)

	if err != nil {
		return err
	}

	return nil
}

// Compact implements IServiceClient.
func (c *GRPCClient) Compact(ctx context.Context, force bool) (success bool, err error) {
	grpcReq := &pb.CompactRequest{
		Force: force,
	}

	grpcResp, err := c.client.Compact(ctx, grpcReq)

	if err != nil {
		return false, err
	}

	return grpcResp.Success, nil
}

// Delete implements IServiceClient.
func (c *GRPCClient) Delete(ctx context.Context, Key []byte) (err error) {
	grpcReq := &pb.DeleteRequest{
		Key: Key,
	}

	_, err = c.client.Delete(ctx, grpcReq)
	if err != nil {
		return err
	}

	return nil
}

// Get implements IServiceClient.
func (c *GRPCClient) Get(ctx context.Context, key []byte) (value []byte, found bool, err error) {
	grpcReq := &pb.GetRequest{
		Key: key,
	}

	grpcResp, err := c.client.Get(ctx, grpcReq)

	if err != nil {
		return nil, false, err
	}

	return grpcResp.Value, grpcResp.Found, nil

}

// GetNodeInfo implements IServiceClient.
func (c *GRPCClient) GetNodeInfo(ctx context.Context) (*pb.GetNodeInfoResponse, error) {
	req := &pb.GetNodeInfoRequest{}
	resp, err := c.client.GetNodeInfo(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// GetStats implements IServiceClient.
func (c *GRPCClient) GetStats(ctx context.Context) (*pb.GetStatsResponse, error) {
	req := &pb.GetStatsRequest{}
	resp, err := c.client.GetStats(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Put implements IServiceClient.
func (c *GRPCClient) Put(ctx context.Context, key []byte, value []byte) (err error) {
	req := &pb.PutRequest{
		Key:   key,
		Value: value,
	}
	_, err = c.client.Put(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

// RollbackTransaction implements IServiceClient.
func (c *GRPCClient) RollbackTransaction(ctx context.Context) (err error) {
	_, err = c.client.RollbackTransaction(ctx, &pb.RollbackTransactionRequest{})
	if err != nil {
		return err
	}

	return nil
}

// Scan implements IServiceClient.
func (c *GRPCClient) Scan(
	ctx context.Context,
	prefix []byte, suffix []byte,
	startKey []byte, endKey []byte,
	limit int32,
) (*ScanResponse, error) {
	req := &pb.ScanRequest{
		Prefix:   prefix,
		Suffix:   suffix,
		StartKey: startKey,
		EndKey:   endKey,
		Limit:    limit,
	}

	respStream, err := c.client.Scan(ctx, req)

	if err != nil {
		return nil, err
	}

	return &ScanResponse{
		resp: respStream,
	}, nil
}

// Close closes the connection
func (c *GRPCClient) Close() error {
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		c.client = nil
		if c.sessionRenewalDone != nil {
			close(c.sessionRenewalDone)
		}
		// c.setStatus(false, nil)
		return err
	}
	return nil
}
