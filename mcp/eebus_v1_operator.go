package mcp

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
)

const eebusV1OperatorSocket = "/data/eebus/operator-mcp.sock"

type eebusV1PeerUIDResolver func(net.Conn) (int, error)

type eebusV1UIDListener struct {
	net.Listener
	resolve eebusV1PeerUIDResolver
	wantUID int
}

func (listener *eebusV1UIDListener) Accept() (net.Conn, error) {
	for {
		connection, err := listener.Listener.Accept()
		if err != nil {
			return nil, err
		}
		uid, err := listener.resolve(connection)
		if err == nil && uid == listener.wantUID {
			return connection, nil
		}
		_ = connection.Close()
	}
}

type eebusV1OperatorEndpoint struct {
	once     sync.Once
	server   *http.Server
	listener net.Listener
	done     chan struct{}
}

func (endpoint *eebusV1OperatorEndpoint) Close() error {
	var result error
	endpoint.once.Do(func() {
		if endpoint.server != nil {
			result = endpoint.server.Close()
		}
		if endpoint.listener != nil {
			result = errors.Join(result, endpoint.listener.Close())
		}
		close(endpoint.done)
	})
	return result
}

func (server *Server) eebusV1OperatorHandler() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		ctx := context.WithValue(request.Context(), eebusV1BoundaryContextKey{}, eebusV1OperatorBoundary)
		server.ServeHTTP(writer, request.WithContext(ctx))
	})
}

func (server *Server) eebusV1OperatorSocketPath() string {
	return eebusV1OperatorSocket
}

// StartEEBusV1OperatorEndpoint starts the fixed owner-only operator transport.
func (server *Server) StartEEBusV1OperatorEndpoint(ctx context.Context) (io.Closer, error) {
	resolve := eebusV1PlatformPeerUIDResolver()
	if resolve == nil {
		return nil, errors.New("eeBUS operator peer credential proof is unsupported")
	}
	return server.eebusV1ServeOperator(ctx, eebusV1OperatorSocket, resolve)
}

func (server *Server) eebusV1ServeOperator(ctx context.Context, socketPath string, resolve func(net.Conn) (int, error)) (io.Closer, error) {
	if server == nil {
		return nil, errors.New("eeBUS MCP server is nil")
	}
	server.eebusV1Mu.RLock()
	registered := server.eebusV1 != nil
	server.eebusV1Mu.RUnlock()
	if !registered {
		return nil, errors.New("eeBUS MCP provider is unavailable")
	}
	if resolve == nil {
		return nil, errors.New("eeBUS operator peer credential proof is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	base, err := eebusV1PlatformListenOperator(socketPath)
	if err != nil {
		return nil, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = base.Close()
		}
	}()

	guarded := &eebusV1UIDListener{Listener: base, resolve: resolve, wantUID: os.Geteuid()}
	httpServer := &http.Server{Handler: server.eebusV1OperatorHandler()}
	endpoint := &eebusV1OperatorEndpoint{
		server: httpServer, listener: guarded, done: make(chan struct{}),
	}
	cleanup = false
	go func() {
		_ = httpServer.Serve(guarded)
		_ = endpoint.Close()
	}()
	go func() {
		select {
		case <-ctx.Done():
			_ = endpoint.Close()
		case <-endpoint.done:
		}
	}()
	return endpoint, nil
}
