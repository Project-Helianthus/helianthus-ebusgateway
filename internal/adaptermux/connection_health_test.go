package adaptermux

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type managedFenceTransport struct {
	closeOnce  sync.Once
	closed     chan struct{}
	writeOnce  sync.Once
	writeStart chan struct{}
	startOnce  sync.Once
	startStart chan struct{}
	writes     atomic.Int32
	startCalls atomic.Int32
}

func newManagedFenceTransport() *managedFenceTransport {
	return &managedFenceTransport{
		closed:     make(chan struct{}),
		writeStart: make(chan struct{}),
		startStart: make(chan struct{}),
	}
}

func (transport *managedFenceTransport) ReadByte() (byte, error) {
	<-transport.closed
	return 0, net.ErrClosed
}

func (transport *managedFenceTransport) Write(payload []byte) (int, error) {
	transport.writes.Add(1)
	transport.writeOnce.Do(func() { close(transport.writeStart) })
	<-transport.closed
	return 0, net.ErrClosed
}

func (transport *managedFenceTransport) RequestStart(byte) error {
	transport.startCalls.Add(1)
	transport.startOnce.Do(func() { close(transport.startStart) })
	<-transport.closed
	return net.ErrClosed
}

func (transport *managedFenceTransport) Close() error {
	transport.closeOnce.Do(func() { close(transport.closed) })
	return nil
}

type blockingRemoteAddrConn struct {
	net.Conn
	once    sync.Once
	reached chan struct{}
	release chan struct{}
}

func (conn *blockingRemoteAddrConn) RemoteAddr() net.Addr {
	conn.once.Do(func() {
		close(conn.reached)
		<-conn.release
	})
	return conn.Conn.RemoteAddr()
}

func TestManagedConnectionLossDelegatesRecoveryAndFencesOldMux(t *testing.T) {
	reconnectTarget, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer func() {
		if closeErr := reconnectTarget.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			t.Errorf("listener Close() error = %v", closeErr)
		}
	}()

	mock := newP3MockTransport()
	mock.readTimeout = 2 * time.Millisecond
	mux := New(Config{
		Protocol:              "enh",
		Network:               "tcp",
		Address:               reconnectTarget.Addr().String(),
		DialTimeout:           20 * time.Millisecond,
		ReadTimeout:           2 * time.Millisecond,
		BlackholeThreshold:    time.Hour,
		ReconnectInitialDelay: 5 * time.Millisecond,
		ReconnectMaxDelay:     5 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		mux.wg.Wait()
	}()
	mux.ctx, mux.cancel = ctx, cancel
	mux.connMu.Lock()
	mux.upstream = mock
	mux.connMu.Unlock()

	lost := make(chan struct{}, 1)
	var reports atomic.Int32
	mux.SetConnectionLostCallback(func() {
		reports.Add(1)
		select {
		case lost <- struct{}{}:
		default:
		}
	})
	mux.wg.Add(1)
	readDone := make(chan struct{})
	go func() {
		mux.readLoop()
		close(readDone)
	}()

	select {
	case <-lost:
		t.Fatal("ordinary idle read timeout emitted connection loss")
	case <-time.After(25 * time.Millisecond):
	}

	if err := mock.Close(); err != nil {
		t.Fatalf("mock Close() error = %v", err)
	}
	select {
	case <-lost:
	case <-time.After(time.Second):
		t.Fatal("terminal read failure did not emit connection loss")
	}
	if got := reports.Load(); got != 1 {
		t.Fatalf("connection loss reports = %d, want 1", got)
	}

	select {
	case <-readDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("managed read loop did not retire after delegating recovery")
	}

	// A managed generation must not run the mux's legacy reconnect loop. A
	// listening endpoint makes any old-generation dial directly observable.
	tcpTarget, ok := reconnectTarget.(*net.TCPListener)
	if !ok {
		t.Fatalf("listener type = %T, want *net.TCPListener", reconnectTarget)
	}
	if err := tcpTarget.SetDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("SetDeadline() error = %v", err)
	}
	conn, acceptErr := reconnectTarget.Accept()
	if acceptErr == nil {
		_ = conn.Close()
		t.Fatal("retired managed mux dialed the adapter during manager-owned recovery")
	}
	var netErr net.Error
	if !errors.As(acceptErr, &netErr) || !netErr.Timeout() {
		t.Fatalf("Accept() error = %v, want timeout proving no reconnect dial", acceptErr)
	}

	// The generation-local proxy surface remains fenced while the manager is
	// in BACKOFF: new sessions are rejected and START cannot reach upstream.
	server, client := net.Pipe()
	if id := mux.AddSession(server); id != 0 {
		t.Fatalf("AddSession() id = %d after delegated loss, want 0", id)
	}
	_ = client.Close()
	result := <-mux.requestStartForSession(41, 0x31)
	if !errors.Is(result.err, errNotConnected) {
		t.Fatalf("old-generation START error = %v, want errNotConnected", result.err)
	}
	if starts := mock.getStartRequests(); len(starts) != 0 {
		t.Fatalf("old-generation START reached upstream after loss: %v", starts)
	}

	// Existing proxy sessions are fenced too. Simulate a previously granted
	// external owner and prove its SEND cannot reach the retired transport.
	mux.arb.mu.Lock()
	mux.arb.hasOwner = true
	mux.arb.currentOwner = 41
	mux.arb.mu.Unlock()
	if err := mux.doSend(41, 0x55); !errors.Is(err, errNotConnected) {
		t.Fatalf("old-generation SEND error = %v, want errNotConnected", err)
	}
	mock.mu.Lock()
	written := append([]byte(nil), mock.writtenBytes...)
	mock.mu.Unlock()
	if len(written) != 0 {
		t.Fatalf("old-generation SEND reached upstream after loss: % X", written)
	}

	// Retirement remains normally closable; manager replacement will call the
	// same Close path after its bounded backoff.
	closeDone := make(chan error, 1)
	go func() { closeDone <- mux.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Close() did not remain bounded after managed connection loss")
	}
}

func TestManagedConnectionLossLinearizesProxyAdmissionAndProviderUse(t *testing.T) {
	newMux := func(t *testing.T, upstream *managedFenceTransport, callback func()) (*Mux, context.CancelFunc) {
		t.Helper()
		mux := New(Config{Protocol: "enh", ReconnectInitialDelay: time.Hour})
		ctx, cancel := context.WithCancel(context.Background())
		mux.ctx, mux.cancel = ctx, cancel
		mux.connMu.Lock()
		mux.upstream = upstream
		mux.connMu.Unlock()
		mux.SetConnectionLostCallback(callback)
		return mux, cancel
	}

	t.Run("blocked write drains before BACKOFF publication", func(t *testing.T) {
		upstream := newManagedFenceTransport()
		writeDone := make(chan struct{})
		callback := make(chan struct{})
		mux, cancel := newMux(t, upstream, func() {
			select {
			case <-writeDone:
			default:
				t.Error("connection-loss callback crossed an admitted Write")
			}
			close(callback)
		})
		defer cancel()
		mux.arb.mu.Lock()
		mux.arb.hasOwner = true
		mux.arb.currentOwner = 41
		mux.arb.mu.Unlock()

		go func() {
			_ = mux.doSend(41, 0x55)
			close(writeDone)
		}()
		select {
		case <-upstream.writeStart:
		case <-time.After(time.Second):
			t.Fatal("Write did not reach the managed provider")
		}

		reconnectDone := make(chan error, 1)
		go func() {
			delegated, err := mux.reconnect()
			if !delegated && err == nil {
				err = errors.New("reconnect did not delegate")
			}
			reconnectDone <- err
		}()
		select {
		case <-callback:
		case <-time.After(time.Second):
			t.Fatal("managed loss did not publish after closing the blocked Write")
		}
		if err := <-reconnectDone; err != nil {
			t.Fatalf("reconnect() error = %v", err)
		}
		if err := mux.doSend(41, 0x56); !errors.Is(err, errNotConnected) {
			t.Fatalf("post-BACKOFF Write error = %v, want errNotConnected", err)
		}
		if got := upstream.writes.Load(); got != 1 {
			t.Fatalf("upstream Write calls = %d, want exactly pre-fence call", got)
		}
	})

	t.Run("blocked request start drains before BACKOFF publication", func(t *testing.T) {
		upstream := newManagedFenceTransport()
		startDone := make(chan struct{})
		callback := make(chan struct{})
		mux, cancel := newMux(t, upstream, func() {
			select {
			case <-startDone:
			default:
				t.Error("connection-loss callback crossed an admitted RequestStart")
			}
			close(callback)
		})
		defer cancel()

		go func() {
			result := <-mux.requestStartForSession(41, 0x31)
			if result.err == nil {
				t.Error("RequestStart unexpectedly succeeded across loss")
			}
			close(startDone)
		}()
		select {
		case <-upstream.startStart:
		case <-time.After(time.Second):
			t.Fatal("RequestStart did not reach the managed provider")
		}

		reconnectDone := make(chan error, 1)
		go func() {
			delegated, err := mux.reconnect()
			if !delegated && err == nil {
				err = errors.New("reconnect did not delegate")
			}
			reconnectDone <- err
		}()
		select {
		case <-callback:
		case <-time.After(time.Second):
			t.Fatal("managed loss did not publish after closing RequestStart")
		}
		if err := <-reconnectDone; err != nil {
			t.Fatalf("reconnect() error = %v", err)
		}
		result := <-mux.requestStartForSession(42, 0x32)
		if !errors.Is(result.err, errNotConnected) {
			t.Fatalf("post-BACKOFF START error = %v, want errNotConnected", result.err)
		}
		if got := upstream.startCalls.Load(); got != 1 {
			t.Fatalf("upstream RequestStart calls = %d, want exactly pre-fence call", got)
		}
	})

	t.Run("queued start is drained before BACKOFF and kick does not deadlock", func(t *testing.T) {
		upstream := newManagedFenceTransport()
		callback := make(chan struct{})
		mux, cancel := newMux(t, upstream, func() { close(callback) })
		defer cancel()

		// Hold stateMu so requestStartForSession acquires the managed R fence
		// and pauses immediately before the queue insertion.
		mux.stateMu.Lock()
		requestReturned := make(chan (<-chan startResult), 1)
		go func() { requestReturned <- mux.requestStartForSession(51, 0x33) }()
		deadline := time.Now().Add(time.Second)
		for mux.connectionUseMu.TryLock() {
			mux.connectionUseMu.Unlock()
			if time.Now().After(deadline) {
				mux.stateMu.Unlock()
				t.Fatal("START admission did not acquire managed read fence")
			}
			time.Sleep(time.Millisecond)
		}

		reconnectDone := make(chan error, 1)
		go func() {
			delegated, err := mux.reconnect()
			if !delegated && err == nil {
				err = errors.New("reconnect did not delegate")
			}
			reconnectDone <- err
		}()
		select {
		case <-upstream.closed:
		case <-time.After(time.Second):
			mux.stateMu.Unlock()
			t.Fatal("managed loss did not detach/close upstream")
		}

		// The request now inserts while still holding R. The queued loss writer
		// must acquire W next, fail that request, and publish BACKOFF before the
		// post-enqueue idle kick can acquire a fresh R admission.
		mux.stateMu.Unlock()
		select {
		case <-callback:
		case <-time.After(time.Second):
			t.Fatal("BACKOFF did not publish after draining queued START")
		}
		if err := <-reconnectDone; err != nil {
			t.Fatalf("reconnect() error = %v", err)
		}
		var resultCh <-chan startResult
		select {
		case resultCh = <-requestReturned:
		case <-time.After(time.Second):
			t.Fatal("requestStartForSession deadlocked in nested managed admission")
		}
		select {
		case result := <-resultCh:
			if result.err == nil {
				t.Fatal("pre-fence START survived managed BACKOFF")
			}
		case <-time.After(time.Second):
			t.Fatal("pre-fence START remained unresolved after failAllPending")
		}
		mux.stateMu.Lock()
		pendingStart := mux.pendingStart
		mux.stateMu.Unlock()
		mux.arb.mu.Lock()
		pendingGateway := mux.arb.pendingGateway
		pendingExternal := len(mux.arb.pendingExternal)
		mux.arb.mu.Unlock()
		if pendingStart != nil || pendingGateway != nil || pendingExternal != 0 {
			t.Fatalf("pending START residue after BACKOFF: active=%v gateway=%v external=%d", pendingStart != nil, pendingGateway != nil, pendingExternal)
		}
	})

	t.Run("in-flight AddSession completes before BACKOFF and later admission rejects", func(t *testing.T) {
		upstream := newManagedFenceTransport()
		addID := make(chan uint64, 1)
		addDone := make(chan struct{})
		callback := make(chan struct{})
		var mux *Mux
		var cancel context.CancelFunc
		mux, cancel = newMux(t, upstream, func() {
			mux.sessionsMu.Lock()
			sessionCount := len(mux.sessions)
			mux.sessionsMu.Unlock()
			if sessionCount != 1 {
				t.Errorf("connection-loss callback observed %d sessions, want completed pre-fence admission", sessionCount)
			}
			close(callback)
		})
		defer cancel()
		server, client := net.Pipe()
		wrapped := &blockingRemoteAddrConn{Conn: server, reached: make(chan struct{}), release: make(chan struct{})}
		go func() {
			addID <- mux.AddSession(wrapped)
			close(addDone)
		}()
		select {
		case <-wrapped.reached:
		case <-time.After(time.Second):
			t.Fatal("AddSession did not enter its admitted critical section")
		}

		reconnectDone := make(chan error, 1)
		go func() {
			delegated, err := mux.reconnect()
			if !delegated && err == nil {
				err = errors.New("reconnect did not delegate")
			}
			reconnectDone <- err
		}()
		select {
		case <-callback:
			t.Fatal("BACKOFF published before pre-fence AddSession completed")
		case <-time.After(20 * time.Millisecond):
		}
		close(wrapped.release)
		select {
		case <-callback:
		case <-time.After(time.Second):
			t.Fatal("BACKOFF did not publish after AddSession drained")
		}
		if err := <-reconnectDone; err != nil {
			t.Fatalf("reconnect() error = %v", err)
		}
		if id := <-addID; id == 0 {
			t.Fatal("pre-fence AddSession did not linearize before BACKOFF")
		}

		laterServer, laterClient := net.Pipe()
		if id := mux.AddSession(laterServer); id != 0 {
			t.Fatalf("post-BACKOFF AddSession id = %d, want 0", id)
		}
		_ = laterClient.Close()
		_ = client.Close()
		if err := mux.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
}

func TestManagedConnectionLossDoesNotDeadlockCancelledStartQueueAdvance(t *testing.T) {
	mock := &failingStartTransport{
		p3MockTransport: newP3MockTransport(),
		startGate:       make(chan struct{}),
		startErr:        errors.New("adapter disconnected"),
	}
	mux := New(Config{
		Protocol:        "enh",
		PendingStartTTL: time.Hour,
		SYNInterval:     time.Hour,
		StartDeadline:   time.Hour,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux.ctx, mux.cancel = ctx, cancel
	mux.connMu.Lock()
	mux.upstream = mock
	mux.connMu.Unlock()

	callback := make(chan struct{})
	var callbackOnce sync.Once
	mux.SetConnectionLostCallback(func() {
		callbackOnce.Do(func() { close(callback) })
	})
	var releaseOnce sync.Once
	releaseStart := func() { releaseOnce.Do(func() { close(mock.startGate) }) }
	defer releaseStart()

	firstResult := mux.arb.requestStart(41, 0x31)
	grantDone := make(chan struct{})
	go func() {
		mux.tryGrantAndStart()
		close(grantDone)
	}()

	deadline := time.Now().Add(time.Second)
	for {
		mux.stateMu.Lock()
		pending := mux.pendingStart != nil && mux.pendingStart.sessionID == 41
		mux.stateMu.Unlock()
		if pending {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first START did not enter managed provider use")
		}
		time.Sleep(time.Millisecond)
	}

	// Cancel A while its RequestStart still holds the managed R admission,
	// then queue B. When A later fails it will take the recursive queue-
	// advance branch that previously tried to nest another RLock.
	mux.cancelPendingStart(41)
	secondResult := mux.arb.requestStart(42, 0x32)

	reconnectDone := make(chan error, 1)
	go func() {
		delegated, err := mux.reconnect()
		if !delegated && err == nil {
			err = errors.New("reconnect did not delegate")
		}
		reconnectDone <- err
	}()

	// An existing reader does not block TryRLock. Failure here therefore
	// proves the managed-loss writer is queued behind the first START before
	// RequestStart is released, making the writer-preference deadlock exact.
	deadline = time.Now().Add(time.Second)
	for {
		if mux.connectionUseMu.TryRLock() {
			mux.connectionUseMu.RUnlock()
		} else {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("managed loss writer did not queue behind RequestStart")
		}
		time.Sleep(time.Millisecond)
	}
	releaseStart()

	select {
	case <-callback:
	case <-time.After(time.Second):
		t.Fatal("managed loss did not publish BACKOFF after cancelled START")
	}
	select {
	case err := <-reconnectDone:
		if err != nil {
			t.Fatalf("reconnect() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("managed reconnect deadlocked behind nested queue advance")
	}
	select {
	case <-grantDone:
	case <-time.After(time.Second):
		t.Fatal("tryGrantAndStart deadlocked after managed loss")
	}
	select {
	case result := <-firstResult:
		if !result.cancelled {
			t.Fatalf("first START result = %#v, want cancelled", result)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled first START remained unresolved")
	}
	select {
	case result := <-secondResult:
		if result.err == nil {
			t.Fatal("queued second START survived managed connection loss")
		}
	case <-time.After(time.Second):
		t.Fatal("queued second START remained unresolved after BACKOFF")
	}
	mux.stateMu.Lock()
	pendingStart := mux.pendingStart
	mux.stateMu.Unlock()
	mux.arb.mu.Lock()
	pendingGateway := mux.arb.pendingGateway
	pendingExternal := len(mux.arb.pendingExternal)
	mux.arb.mu.Unlock()
	if pendingStart != nil || pendingGateway != nil || pendingExternal != 0 {
		t.Fatalf("pending START residue after managed loss: active=%v gateway=%v external=%d", pendingStart != nil, pendingGateway != nil, pendingExternal)
	}
	if err := mux.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestUnmanagedConnectionLossKeepsLegacyReconnectLoop(t *testing.T) {
	mux := New(Config{Protocol: "enh", ReconnectInitialDelay: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	mux.ctx, mux.cancel = ctx, cancel
	mock := newP3MockTransport()
	mux.connMu.Lock()
	mux.upstream = mock
	mux.connMu.Unlock()

	done := make(chan struct {
		delegated bool
		err       error
	}, 1)
	go func() {
		delegated, err := mux.reconnect()
		done <- struct {
			delegated bool
			err       error
		}{delegated: delegated, err: err}
	}()
	select {
	case result := <-done:
		t.Fatalf("unmanaged reconnect returned early: delegated=%v err=%v", result.delegated, result.err)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	result := <-done
	if result.delegated || !errors.Is(result.err, context.Canceled) {
		t.Fatalf("unmanaged reconnect = delegated:%v err:%v, want false/context.Canceled", result.delegated, result.err)
	}
	if err := mock.Close(); err != nil {
		t.Fatalf("mock Close() error = %v", err)
	}
}
