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
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/KevoDB/kevo/pkg/common"
	"github.com/KevoDB/kevo/pkg/engine"
	"github.com/KevoDB/kevo/pkg/grpc/interceptors"
	grpcservice "github.com/KevoDB/kevo/pkg/grpc/service"
	"github.com/KevoDB/kevo/pkg/replication"
	"github.com/KevoDB/kevo/pkg/sessreg"
	pb "github.com/KevoDB/kevo/proto/kevo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// Server represents the Kevo server
type Server struct {
	eng *engine.EngineFacade

	sessreg *sessreg.SessionRegistry
	// txRegistry         *txregold.TransactionRegistry
	listener           net.Listener
	grpcServer         *grpc.Server
	kevoService        *grpcservice.KevoServiceServer
	config             Config
	replicationManager *replication.Manager
}

// NewServer creates a new server instance
func NewServer(eng *engine.EngineFacade, config Config) *Server {
	return &Server{
		eng: eng,
		// txRegistry: txregold.NewRegistry(),
		config: config,
	}
}

// Start initializes and starts the server
func (s *Server) Start() error {
	// Create a listener on the specified address
	var err error
	s.listener, err = net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %s", s.config.ListenAddr, err)
	}

	fmt.Printf("Listening on %s\n", s.config.ListenAddr)

	// Configure gRPC server options
	var serverOpts []grpc.ServerOption

	// Add TLS if configured
	var tlsConfig *tls.Config
	if s.config.TLSEnabled {
		tlsConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}

		// Load server certificate if provided
		if s.config.TLSCertFile != "" && s.config.TLSKeyFile != "" {
			cert, err := tls.LoadX509KeyPair(s.config.TLSCertFile, s.config.TLSKeyFile)
			if err != nil {
				return fmt.Errorf("failed to load TLS certificate: %s", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		// Add credentials to server options
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}

	// Configure keepalive parameters
	kaProps := keepalive.ServerParameters{
		MaxConnectionIdle:     60 * time.Second,
		MaxConnectionAge:      5 * time.Minute,
		MaxConnectionAgeGrace: 5 * time.Second,
		Time:                  15 * time.Second,
		Timeout:               5 * time.Second,
	}

	kaPolicy := keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second,
		PermitWithoutStream: true,
	}

	// for verifying if the session was indeed created
	// by the server, and not the client.
	secret, err := common.GenerateRandom256Bit()
	if err != nil {
		return err
	}

	sreg := sessreg.NewSessionRegistryWithDefaults()
	s.sessreg = sreg
	err = sreg.StartAutoExpiry()
	if err != nil {
		return err
	}

	serverOpts = append(serverOpts,
		grpc.KeepaliveParams(kaProps),
		grpc.KeepaliveEnforcementPolicy(kaPolicy),
		grpc.UnaryInterceptor(interceptors.CreateServerUnaryInterceptor(secret, sreg)),
		grpc.StreamInterceptor(interceptors.CreateServerStreamInterceptor(secret, sreg)))

	// Create gRPC server with options
	s.grpcServer = grpc.NewServer(serverOpts...)

	// Initialize replication if enabled
	if s.config.ReplicationEnabled {
		// Create replication manager config
		replicationConfig := &replication.ManagerConfig{
			Enabled:       true,
			Mode:          s.config.ReplicationMode,
			PrimaryAddr:   s.config.PrimaryAddr,
			ListenAddr:    s.config.ReplicationAddr,
			TLSConfig:     tlsConfig,
			ForceReadOnly: true,
		}

		// Create the replication manager
		s.replicationManager, err = replication.NewManager(s.eng, replicationConfig)
		if err != nil {
			return fmt.Errorf("failed to create replication manager: %s", err)
		}

		// Start the replication service
		if err := s.replicationManager.Start(); err != nil {
			return fmt.Errorf("failed to start replication: %s", err)
		}

		fmt.Printf("Replication started in %s mode\n", s.config.ReplicationMode)

		// If in replica mode, the engine should now be read-only
		if s.config.ReplicationMode == "replica" {
			fmt.Println("Running as replica: database is in read-only mode")
		}
	}

	// Create and register the Kevo service implementation
	// Only pass replicationManager if it's properly initialized
	var repManager grpcservice.ReplicationInfoProvider
	if s.replicationManager != nil && s.config.ReplicationEnabled {
		fmt.Printf("DEBUG: Using replication manager for role %s\n", s.config.ReplicationMode)
		repManager = s.replicationManager
	} else {
		fmt.Printf("DEBUG: No replication manager available. ReplicationEnabled: %v, Manager nil: %v\n",
			s.config.ReplicationEnabled, s.replicationManager == nil)
	}

	s.kevoService = grpcservice.NewKevoServiceServer(s.eng, sreg, repManager)
	pb.RegisterKevoServiceServer(s.grpcServer, s.kevoService)

	fmt.Println("gRPC server initialized")
	return nil
}

// Serve starts serving requests (blocking)
func (s *Server) Serve() error {
	if s.grpcServer == nil {
		return fmt.Errorf("server not initialized, call Start() first")
	}

	fmt.Println("Starting gRPC server")
	return s.grpcServer.Serve(s.listener)
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown(ctx context.Context) error {
	// First, stop the replication manager if it exists
	if s.replicationManager != nil {
		fmt.Println("Stopping replication manager...")
		if err := s.replicationManager.Stop(); err != nil {
			fmt.Printf("Warning: Failed to stop replication manager: %v\n", err)
		} else {
			fmt.Println("Replication manager stopped")
		}
	}

	// Next, gracefully stop the gRPC server if it exists
	if s.grpcServer != nil {
		fmt.Println("Gracefully stopping gRPC server...")

		// Create a channel to signal when the server has stopped
		stopped := make(chan struct{})
		go func() {
			s.grpcServer.GracefulStop()
			close(stopped)
		}()

		// Wait for graceful stop or context deadline
		select {
		case <-stopped:
			fmt.Println("gRPC server stopped gracefully")
		case <-ctx.Done():
			fmt.Println("Context deadline exceeded, forcing server stop")
			s.grpcServer.Stop()
		}
	}

	// Shut down the listener if it's still open
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			return fmt.Errorf("failed to close listener: %s", err)
		}
	}

	s.sessreg.Close()

	return nil
}
