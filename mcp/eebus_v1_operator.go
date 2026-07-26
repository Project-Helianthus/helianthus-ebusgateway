package mcp

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
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
	once       sync.Once
	server     *http.Server
	listener   net.Listener
	socketPath string
	socketInfo os.FileInfo
	done       chan struct{}
}

func (endpoint *eebusV1OperatorEndpoint) Close() error {
	var result error
	endpoint.once.Do(func() {
		if endpoint.server != nil {
			result = endpoint.server.Close()
		} else if endpoint.listener != nil {
			result = endpoint.listener.Close()
		}
		if current, err := os.Lstat(endpoint.socketPath); err == nil &&
			endpoint.socketInfo != nil && os.SameFile(current, endpoint.socketInfo) {
			if removeErr := os.Remove(endpoint.socketPath); result == nil {
				result = removeErr
			}
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

	parent := filepath.Dir(socketPath)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return nil, err
	}
	if err := os.Chmod(parent, 0o700); err != nil {
		return nil, err
	}
	parentInfo, err := os.Stat(parent)
	if err != nil || !parentInfo.IsDir() || parentInfo.Mode().Perm() != 0o700 ||
		!eebusV1OwnedByEffectiveUID(parentInfo) {
		return nil, errors.New("eeBUS operator socket parent permission proof failed")
	}
	if err := eebusV1RemoveStaleSocket(socketPath); err != nil {
		return nil, err
	}

	base, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = base.Close()
			_ = os.Remove(socketPath)
		}
	}()
	if err := os.Chmod(socketPath, 0o600); err != nil {
		return nil, err
	}
	socketInfo, err := os.Lstat(socketPath)
	if err != nil || socketInfo.Mode()&os.ModeSocket == 0 || socketInfo.Mode().Perm() != 0o600 ||
		!eebusV1OwnedByEffectiveUID(socketInfo) {
		return nil, errors.New("eeBUS operator socket permission proof failed")
	}

	guarded := &eebusV1UIDListener{Listener: base, resolve: resolve, wantUID: os.Geteuid()}
	httpServer := &http.Server{Handler: server.eebusV1OperatorHandler()}
	endpoint := &eebusV1OperatorEndpoint{
		server: httpServer, listener: guarded, socketPath: socketPath, socketInfo: socketInfo, done: make(chan struct{}),
	}
	cleanup = false
	go func() {
		_ = httpServer.Serve(guarded)
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

func eebusV1RemoveStaleSocket(socketPath string) error {
	info, err := os.Lstat(socketPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSocket == 0 {
		return errors.New("eeBUS operator socket path is occupied by a non-socket")
	}
	if !eebusV1OwnedByEffectiveUID(info) {
		return errors.New("eeBUS operator stale socket is not owned by the effective UID")
	}
	connection, dialErr := net.DialTimeout("unix", socketPath, 100*time.Millisecond)
	if dialErr == nil {
		_ = connection.Close()
		return errors.New("eeBUS operator socket already has an active listener")
	}
	return os.Remove(socketPath)
}
