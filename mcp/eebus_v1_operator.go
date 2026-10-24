package mcp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

const (
	// EEBusV1OperatorSocketName is the file name of the owner-only operator
	// socket.
	EEBusV1OperatorSocketName = "operator-mcp.sock"
	// eebusV1DefaultStateRoot is the add-on eeBUS state root, used when no
	// valid state root is configured (eeBUS disabled or misconfigured).
	eebusV1DefaultStateRoot = "/data/eebus"
	// eebusV1OperatorDirectorySuffix names the socket directory beside the
	// state root.
	eebusV1OperatorDirectorySuffix = "-operator"
)

// EEBusV1OperatorSocketPathLimit is the longest operator socket path a client
// can dial: sockaddr_un.sun_path less its terminating NUL (107 bytes on Linux,
// 103 on macOS).
const EEBusV1OperatorSocketPathLimit = len(syscall.RawSockaddrUnix{}.Path) - 1

// ValidateEEBusV1OperatorSocketPath rejects a socket path that exceeds
// EEBusV1OperatorSocketPathLimit. The Linux listener binds through a short
// /proc/self/fd alias, so an over-long path would bind and still be
// undialable; it is refused before anything is bound. The error carries the
// lengths only, never the path.
func ValidateEEBusV1OperatorSocketPath(path string) error {
	if length := len(path); length > EEBusV1OperatorSocketPathLimit {
		return fmt.Errorf("eeBUS operator socket path is %d bytes; the AF_UNIX limit is %d", length, EEBusV1OperatorSocketPathLimit)
	}
	return nil
}

// EEBusV1OperatorSocketPath derives the operator socket from the eeBUS state
// root: <state-root>-operator/operator-mcp.sock. The socket directory is a
// sibling of the state root, never an entry inside it, because the eeBUS store
// accepts only its own entries there and rejects any other layout on open. An
// empty, relative or filesystem-root value derives from the add-on root
// /data/eebus, so the add-on socket is /data/eebus-operator/operator-mcp.sock.
func EEBusV1OperatorSocketPath(stateRoot string) string {
	root := filepath.Clean(stateRoot)
	if stateRoot == "" || !filepath.IsAbs(root) || root == filepath.Dir(root) {
		root = eebusV1DefaultStateRoot
	}
	return filepath.Join(root+eebusV1OperatorDirectorySuffix, EEBusV1OperatorSocketName)
}

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
		if request.Header.Get(m8SourceScopeHeader) == m8SourceScopeV1 {
			ctx = context.WithValue(ctx, m8SourceScopeContextKey{}, true)
		}
		server.ServeHTTP(writer, request.WithContext(ctx))
	})
}

// StartEEBusV1OperatorEndpoint starts the owner-only operator transport at
// socketPath, normally EEBusV1OperatorSocketPath of the configured state root.
func (server *Server) StartEEBusV1OperatorEndpoint(ctx context.Context, socketPath string) (io.Closer, error) {
	resolve := eebusV1PlatformPeerUIDResolver()
	if resolve == nil {
		return nil, errors.New("eeBUS operator peer credential proof is unsupported")
	}
	return server.eebusV1ServeOperator(ctx, socketPath, resolve)
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
	if err := ValidateEEBusV1OperatorSocketPath(socketPath); err != nil {
		return nil, err
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
