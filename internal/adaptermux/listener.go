package adaptermux

import (
	"context"
	"errors"
	"log"
	"net"
	"sync"
	"time"
)

// ProxyListener binds a TCP listener and registers each accepted
// connection as an ENH session on the adapter multiplexer. External
// ENH clients (e.g. ebusd) connect here to share the adapter.
type ProxyListener struct {
	mux      *Mux
	listener net.Listener
	logger   *log.Logger
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewProxyListener creates and starts a proxy listener on listenAddr.
// The listener runs until Close() is called or ctx is cancelled.
func NewProxyListener(ctx context.Context, mux *Mux, listenAddr string, logger *log.Logger) (*ProxyListener, error) {
	if mux == nil {
		return nil, errors.New("adaptermux: proxy listener requires non-nil mux")
	}
	if logger == nil {
		logger = log.Default()
	}

	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", listenAddr)
	if err != nil {
		return nil, err
	}

	plCtx, plCancel := context.WithCancel(ctx)
	pl := &ProxyListener{
		mux:      mux,
		listener: ln,
		logger:   logger,
		ctx:      plCtx,
		cancel:   plCancel,
	}

	pl.wg.Add(1)
	// goroutine: closes the TCP listener when the context is cancelled,
	// unblocking the accept loop. Lives until ctx is done.
	go pl.watchCtx()

	pl.wg.Add(1)
	// goroutine: accepts incoming TCP connections and registers them as
	// ENH sessions on the mux. Lives until the listener is closed.
	go pl.acceptLoop()

	return pl, nil
}

// Close stops accepting connections and shuts down the listener.
// It is safe to call multiple times.
func (pl *ProxyListener) Close() error {
	pl.cancel()
	err := pl.listener.Close()
	pl.wg.Wait()
	return err
}

// Addr returns the listener's network address (useful when bound to ":0").
func (pl *ProxyListener) Addr() net.Addr {
	return pl.listener.Addr()
}

// watchCtx closes the listener when the context is cancelled.
func (pl *ProxyListener) watchCtx() {
	defer pl.wg.Done()
	<-pl.ctx.Done()
	// Closing the listener unblocks Accept in the accept loop.
	_ = pl.listener.Close()
}

// acceptLoop accepts connections until the listener is closed.
func (pl *ProxyListener) acceptLoop() {
	defer pl.wg.Done()

	for {
		conn, err := pl.listener.Accept()
		if err != nil {
			// Check if shutdown is in progress.
			if pl.ctx.Err() != nil {
				return
			}
			// Transient errors (fd exhaustion, etc.): log and retry.
			// Do NOT exit the loop — the condition may be temporary.
			if isTemporaryAcceptError(err) {
				pl.logger.Printf("adaptermux: proxy accept transient error: %v", err)
				// Brief backoff to avoid tight-looping on persistent fd exhaustion.
				time.Sleep(5 * time.Millisecond)
				continue
			}
			// Non-transient error (listener closed, etc.): exit.
			pl.logger.Printf("adaptermux: proxy accept error (fatal): %v", err)
			return
		}

		// Set TCP options on the accepted connection.
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			if err := tcpConn.SetNoDelay(true); err != nil {
				pl.logger.Printf("adaptermux: proxy conn SetNoDelay: %v", err)
			}
			if err := tcpConn.SetKeepAlive(true); err != nil {
				pl.logger.Printf("adaptermux: proxy conn SetKeepAlive: %v", err)
			}
			if err := tcpConn.SetKeepAlivePeriod(30 * time.Second); err != nil {
				pl.logger.Printf("adaptermux: proxy conn SetKeepAlivePeriod: %v", err)
			}
		}

		pl.mux.AddSession(conn)
	}
}

// isTemporaryAcceptError returns true for transient accept errors that
// should be retried (fd exhaustion, resource temporarily unavailable).
func isTemporaryAcceptError(err error) bool {
	// net.Error.Temporary is deprecated but still the most reliable way
	// to detect transient accept failures from the standard library.
	var netErr net.Error
	if errors.As(err, &netErr) {
		//nolint:staticcheck // Temporary() is deprecated but functional
		return netErr.Temporary()
	}
	return false
}
