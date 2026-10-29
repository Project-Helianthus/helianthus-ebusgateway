package adaptermux

import (
	"bytes"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/v8classifier"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

type backlogWrite struct {
	data []byte
	at   time.Time
}
type backlogConn struct {
	net.Conn
	mu      sync.Mutex
	writes  []backlogWrite
	written chan struct{}
}

func (c *backlogConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.writes = append(c.writes, backlogWrite{append([]byte(nil), p...), time.Now()})
	c.mu.Unlock()
	c.written <- struct{}{}
	return len(p), nil
}

func backlogSession(t *testing.T, frames []sessionFrame) (*session, *backlogConn) {
	t.Helper()
	mux, _, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeShadow)
	t.Cleanup(cleanup)
	conn := &backlogConn{written: make(chan struct{}, 256)}
	s := &session{id: 1, conn: conn, mux: mux, sendCh: make(chan sessionFrame, 256), done: make(chan struct{})}
	mux.ensureSessionPacer(s.id)
	for _, f := range frames {
		s.sendCh <- f
	}
	s.wg.Add(1)
	go s.writeLoop()
	t.Cleanup(func() { s.closed.Store(true); close(s.done); s.wg.Wait() })
	return s, conn
}
func awaitBacklogWrites(t *testing.T, c *backlogConn, n int) []backlogWrite {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for i := 0; i < n; i++ {
		select {
		case <-c.written:
		case <-timer.C:
			t.Fatalf("only %d of %d writes", i, n)
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]backlogWrite(nil), c.writes...)
}
func TestSessionPacer_BacklogBurst(t *testing.T) {
	for _, kind := range []sessionFrameKind{sessionFrameStarted, sessionFrameFailed} {
		t.Run(map[sessionFrameKind]string{sessionFrameStarted: "started", sessionFrameFailed: "failed"}[kind], func(t *testing.T) {
			frames := make([]sessionFrame, 100, 101)
			for i := range frames {
				frames[i] = sessionFrame{kind: sessionFrameReceived, payload: byte(i)}
			}
			frames = append(frames, sessionFrame{kind: kind, payload: 0x31})
			s, c := backlogSession(t, frames)
			writes := awaitBacklogWrites(t, c, 101)
			response := transport.ENHResStarted
			if kind == sessionFrameFailed {
				response = transport.ENHResFailed
			}
			encoded := transport.EncodeENH(response, 0x31)
			if !bytes.Equal(writes[100].data, encoded[:]) {
				t.Fatalf("last write %x; want notification %x", writes[100].data, encoded)
			}
			for i := 0; i < 100; i++ {
				if !bytes.Equal(writes[i].data, []byte{byte(i)}) {
					t.Fatalf("mirrored frame %d changed: %x", i, writes[i].data)
				}
			}
			if elapsed := writes[100].at.Sub(writes[0].at); elapsed > 5*time.Millisecond {
				t.Fatalf("burst took %v; want <=5ms", elapsed)
			}
			if got := s.backlogFlushes.Load(); got != 1 {
				t.Fatalf("flushes=%d; want 1", got)
			}
			pacer := s.mux.SessionPacer(s.id)
			// Write notification precedes its return and the cadence rebase.
			deadline := time.Now().Add(time.Second)
			last := pacer.LastScheduledEmit()
			for last.Before(writes[100].at) && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
				last = pacer.LastScheduledEmit()
			}
			if last.Before(writes[100].at) || last.After(time.Now()) {
				t.Fatalf("cadence anchor must follow final flush egress: %v", last)
			}
			next := pacer.Schedule(last)
			if !next.Equal(last.Add(v8classifier.TauWireByte)) {
				t.Fatalf("next slot %v; want one symbol after flush", next)
			}
		})
	}
}
func TestSessionPacer_BacklogKeepsResetBoundary(t *testing.T) {
	frames := []sessionFrame{{kind: sessionFrameResetted, payload: 1}, {kind: sessionFrameReceived, payload: 0x12}, {kind: sessionFrameStarted, payload: 0x31}}
	_, c := backlogSession(t, frames)
	writes := awaitBacklogWrites(t, c, len(frames))
	reset := transport.EncodeENH(transport.ENHResResetted, 1)
	if !bytes.Equal(writes[0].data, reset[:]) {
		t.Fatalf("notification crossed reset: %x", writes[0].data)
	}
}
func TestSessionPacer_PriorityDoesNotReserveCadence(t *testing.T) {
	mux, _, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeShadow)
	defer cleanup()
	mux.ensureSessionPacer(1)
	c := &backlogConn{written: make(chan struct{}, 4)}
	s := &session{id: 1, mux: mux, conn: c}
	pacer := mux.SessionPacer(1)
	future := time.Now().Add(time.Second)
	pacer.Schedule(future)
	for _, kind := range []sessionFrameKind{sessionFrameStarted, sessionFrameFailed} {
		if err := s.writeFrame(sessionFrame{kind: kind, payload: 0x31}); err != nil {
			t.Fatal(err)
		}
	}
	if got := pacer.LastScheduledEmit(); !got.Equal(future) {
		t.Fatalf("priority changed cadence: %v", got)
	}
}

func TestSessionPacer_NotificationInterruptsWait(t *testing.T) {
	for _, kind := range []sessionFrameKind{sessionFrameStarted, sessionFrameFailed} {
		for _, n := range []int{1, 2} {
			t.Run(fmt.Sprintf("kind_%d/symbols_%d", kind, n), func(t *testing.T) {
				s, c := backlogSession(t, nil)
				pacer := s.mux.SessionPacer(s.id)
				const tau = 200 * time.Millisecond
				pacer.SetTau(tau)
				anchor := time.Now()
				pacer.Schedule(anchor)
				for i := 0; i < n; i++ {
					s.sendCh <- sessionFrame{kind: sessionFrameReceived, payload: byte(0x12 + i)}
				}
				// Wait until the writer has taken the symbols into its pacing wait.
				deadline := time.Now().Add(tau / 2)
				for len(s.sendCh) != 0 && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
				if len(s.sendCh) != 0 {
					t.Fatal("writer did not consume pending symbols")
				}
				notificationAt := time.Now()
				s.sendCh <- sessionFrame{kind: kind, payload: 0x31}
				response := transport.ENHResStarted
				if kind == sessionFrameFailed {
					response = transport.ENHResFailed
				}
				encoded := transport.EncodeENH(response, 0x31)
				writes := awaitBacklogWrites(t, c, n+1)
				if !bytes.Equal(writes[n].data, encoded[:]) {
					t.Fatalf("write after older symbols %x; want notification %x", writes[n].data, encoded)
				}
				if !writes[n].at.Before(anchor.Add(tau / 2)) {
					t.Fatal("notification did not interrupt the pacing wait")
				}
				if gap := writes[n].at.Sub(notificationAt); gap > 5*time.Millisecond {
					t.Fatalf("notification took %v after enqueue; want <=5ms", gap)
				}
				for i := 0; i < n; i++ {
					if !bytes.Equal(writes[i].data, []byte{byte(0x12 + i)}) {
						t.Fatalf("symbol %d lost or reordered: %x", i, writes[i].data)
					}
					if !writes[i].at.Before(anchor.Add(tau / 2)) {
						t.Fatalf("symbol %d was held back by pacing", i)
					}
				}
				if gap := writes[n].at.Sub(writes[n-1].at); gap > 5*time.Millisecond {
					t.Fatalf("notification followed flushed prefix after %v; want <=5ms", gap)
				}
				// Only the older prefix is unpaced; the next symbol resumes cadence.
				s.sendCh <- sessionFrame{kind: sessionFrameReceived, payload: 0x42}
				writes = awaitBacklogWrites(t, c, 1)
				if !bytes.Equal(writes[n+1].data, []byte{0x42}) {
					t.Fatalf("post-notification symbol changed: %x", writes[n+1].data)
				}
				if gap := writes[n+1].at.Sub(writes[n].at); gap < tau {
					t.Fatalf("post-notification symbol lost cadence: %v; want >=%v", gap, tau)
				}
				if got := s.backlogFlushes.Load(); got != 0 {
					t.Fatalf("notification delivery triggered %d backlog flushes", got)
				}
			})
		}
	}
}

// An ENH client begins echo comparison as soon as STARTED is visible.
// Older traffic must be consumed while it is still waiting for the grant.
func TestSessionPacer_StartedPreservesClientEchoState(t *testing.T) {
	for _, n := range []int{1, 2} {
		t.Run(fmt.Sprintf("symbols_%d", n), func(t *testing.T) {
			frames := make([]sessionFrame, 0, n+1)
			for i := 0; i < n; i++ {
				frames = append(frames, sessionFrame{kind: sessionFrameReceived, payload: byte(0x12 + i)})
			}
			frames = append(frames, sessionFrame{kind: sessionFrameStarted, payload: 0x31})
			s, c := backlogSession(t, frames)
			writes := awaitBacklogWrites(t, c, len(frames))
			var parser transport.ENHParser
			awaitingEcho := false
			observed := 0
			const sent = 0x42
			consume := func(write backlogWrite) {
				messages, err := parser.Parse(write.data)
				if err != nil {
					t.Fatal(err)
				}
				for _, msg := range messages {
					switch msg.Command {
					case transport.ENHResStarted:
						if observed != n {
							t.Fatalf("client began SEND with %d older symbols still pending", n-observed)
						}
						awaitingEcho = true
						// Model the adapter echo of the client's first post-grant SEND.
						s.sendCh <- sessionFrame{kind: sessionFrameReceived, payload: sent}
					case transport.ENHResReceived:
						if awaitingEcho {
							if msg.Data != sent {
								t.Fatalf("client echo collision: received %x; sent %x", msg.Data, sent)
							}
							awaitingEcho = false
						} else {
							if msg.Data != byte(0x12+observed) {
								t.Fatalf("older symbol %d changed: %x", observed, msg.Data)
							}
							observed++
						}
					default:
						t.Fatalf("unexpected client message: %+v", msg)
					}
				}
			}
			for _, write := range writes {
				consume(write)
			}
			writes = awaitBacklogWrites(t, c, 1)
			consume(writes[n+1])
			if awaitingEcho || observed != n {
				t.Fatalf("client did not complete SEND echo: awaiting=%t older=%d", awaitingEcho, observed)
			}
		})
	}
}

func TestSessionPacer_NotificationKeepsControlBoundary(t *testing.T) {
	for _, kind := range []sessionFrameKind{sessionFrameStarted, sessionFrameFailed} {
		for _, boundary := range []sessionFrameKind{sessionFrameResetted, sessionFrameInfo, sessionFrameErrorEBUS, sessionFrameErrorHost} {
			t.Run(fmt.Sprintf("kind_%d/boundary_%d", kind, boundary), func(t *testing.T) {
				frames := []sessionFrame{
					{kind: sessionFrameReceived, payload: 0x12},
					{kind: boundary, payload: 1},
					{kind: kind, payload: 0x31},
				}
				_, c := backlogSession(t, frames)
				writes := awaitBacklogWrites(t, c, len(frames))
				expected := &backlogConn{written: make(chan struct{}, len(frames))}
				encoder := &session{conn: expected}
				for i, frame := range frames {
					if err := encoder.writeFrame(frame); err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(writes[i].data, expected.writes[i].data) {
						t.Fatalf("write %d=%x; want %x across control boundary", i, writes[i].data, expected.writes[i].data)
					}
				}
			})
		}
	}
}

func TestSessionPacer_BacklogBound(t *testing.T) {
	for _, n := range []int{2, 3} {
		t.Run(map[int]string{2: "at_bound", 3: "over_bound"}[n], func(t *testing.T) {
			frames := make([]sessionFrame, n)
			for i := range frames {
				frames[i] = sessionFrame{kind: sessionFrameReceived, payload: byte(i)}
			}
			start := time.Now()
			s, c := backlogSession(t, frames)
			writes := awaitBacklogWrites(t, c, n)
			want := uint64(0)
			if n > 2 {
				want = 1
			}
			if got := s.backlogFlushes.Load(); got != want {
				t.Fatalf("flushes=%d; want %d", got, want)
			}
			if n == 2 && writes[1].at.Sub(start) < v8classifier.TauWireByte {
				t.Fatal("cadence lost at backlog bound")
			}
		})
	}
}

func TestSessionPacer_MirroredBurst(t *testing.T) {
	frames := make([]sessionFrame, 100)
	for i := range frames {
		frames[i] = sessionFrame{kind: sessionFrameReceived, payload: byte(i)}
	}
	_, c := backlogSession(t, frames)
	writes := awaitBacklogWrites(t, c, 100)
	if elapsed := writes[99].at.Sub(writes[0].at); elapsed > 5*time.Millisecond {
		t.Fatalf("100 mirrored frames took %v; want <=5ms", elapsed)
	}
}

func TestSessionPacer_SteadyStreamCadence(t *testing.T) {
	s, c := backlogSession(t, nil)
	start := time.Now()
	for i := 0; i < 6; i++ {
		s.sendCh <- sessionFrame{kind: sessionFrameReceived, payload: byte(i)}
		writes := awaitBacklogWrites(t, c, 1)
		floor := start.Add(time.Duration(i) * v8classifier.TauWireByte)
		if writes[i].at.Before(floor) {
			t.Fatalf("frame %d preceded wire slot", i)
		}
	}
	if got := s.backlogFlushes.Load(); got != 0 {
		t.Fatalf("steady stream flushed %d times", got)
	}
}

func TestSessionPacer_ControlFramesDoNotFlush(t *testing.T) {
	frames := []sessionFrame{{kind: sessionFrameInfo, payload: 1}, {kind: sessionFrameInfo, payload: 2}, {kind: sessionFrameInfo, payload: 3}}
	s, c := backlogSession(t, frames)
	awaitBacklogWrites(t, c, len(frames))
	if got := s.backlogFlushes.Load(); got != 0 {
		t.Fatalf("control frames triggered %d flushes", got)
	}
	if !s.mux.SessionPacer(s.id).LastScheduledEmit().IsZero() {
		t.Fatal("control frames reserved cadence")
	}
}

// blockingBacklogConn holds the final flush write beyond a symbol period.
type blockingBacklogConn struct {
	backlogConn
	entered chan struct{}
	release chan struct{}
}

func (c *blockingBacklogConn) Write(p []byte) (int, error) {
	if p[0] == 0x12 {
		close(c.entered)
		<-c.release
	}
	return c.backlogConn.Write(p)
}

func TestSessionPacer_BacklogRecoveryAfterBlockedWrite(t *testing.T) {
	mux, _, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeShadow)
	defer cleanup()
	mux.ensureSessionPacer(1)
	pacer := mux.SessionPacer(1)
	const tau = 20 * time.Millisecond
	pacer.SetTau(tau)
	c := &blockingBacklogConn{
		backlogConn: backlogConn{written: make(chan struct{}, 4)},
		entered:     make(chan struct{}), release: make(chan struct{}),
	}
	s := &session{id: 1, mux: mux, conn: c, flushBacklog: true}
	finished := make(chan error, 1)
	go func() { finished <- s.writeFrame(sessionFrame{kind: sessionFrameReceived, payload: 0x12}) }()
	<-c.entered
	// Force the pre-write anchor to expire before completing final egress.
	time.Sleep(2 * tau)
	releasedAt := time.Now()
	close(c.release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if got := pacer.LastScheduledEmit(); got.Before(releasedAt) {
		t.Fatalf("cadence anchor %v precedes successful egress %v", got, releasedAt)
	}
	s.flushBacklog = false
	if err := s.writeFrame(sessionFrame{kind: sessionFrameReceived, payload: 0x13}); err != nil {
		t.Fatal(err)
	}
	writes := awaitBacklogWrites(t, &c.backlogConn, 2)
	if gap := writes[1].at.Sub(writes[0].at); gap < tau {
		t.Fatalf("ordinary frame followed blocked flush after %v; want >=%v", gap, tau)
	}
}

// snapshotBacklogConn blocks each original snapshot write so arrivals can be
// injected while the writer is flushing, rather than before it takes a snapshot.
type snapshotBacklogConn struct {
	backlogConn
	entered chan byte
	release chan struct{}
}

func (c *snapshotBacklogConn) Write(p []byte) (int, error) {
	if p[0] >= 0x10 && p[0] <= 0x12 {
		c.entered <- p[0]
		<-c.release
	}
	return c.backlogConn.Write(p)
}

func TestSessionPacer_BacklogFlushFreezesSnapshot(t *testing.T) {
	mux, _, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeShadow)
	t.Cleanup(cleanup)
	mux.ensureSessionPacer(1)
	const tau = 40 * time.Millisecond
	mux.SessionPacer(1).SetTau(tau)
	c := &snapshotBacklogConn{
		backlogConn: backlogConn{written: make(chan struct{}, 8)},
		entered:     make(chan byte, 3),
		release:     make(chan struct{}),
	}
	s := &session{id: 1, mux: mux, conn: c, sendCh: make(chan sessionFrame, 256), done: make(chan struct{})}
	for b := byte(0x10); b <= 0x12; b++ {
		s.sendCh <- sessionFrame{kind: sessionFrameReceived, payload: b}
	}
	s.wg.Add(1)
	go s.writeLoop()
	t.Cleanup(func() {
		s.closed.Store(true)
		close(s.done)
		close(c.release)
		s.wg.Wait()
	})
	for b := byte(0x10); b <= 0x12; b++ {
		select {
		case got := <-c.entered:
			if got != b {
				t.Fatalf("blocked write=%x; want %x", got, b)
			}
		case <-time.After(time.Second):
			t.Fatalf("snapshot write %x did not start", b)
		}
		if b < 0x12 {
			// Two new RECEIVED frames stay below the flush threshold.
			s.sendCh <- sessionFrame{kind: sessionFrameReceived, payload: b + 3}
		}
		c.release <- struct{}{}
	}
	writes := awaitBacklogWrites(t, &c.backlogConn, 5)
	for i, write := range writes {
		if !bytes.Equal(write.data, []byte{byte(0x10 + i)}) {
			t.Fatalf("write %d=%x; want %x", i, write.data, 0x10+i)
		}
		if i >= 3 {
			if gap := write.at.Sub(writes[i-1].at); gap < tau {
				t.Fatalf("post-snapshot frame %d gap=%v; want >=%v", i, gap, tau)
			}
		}
	}
	if got := s.backlogFlushes.Load(); got != 1 {
		t.Fatalf("flushes=%d; want 1", got)
	}
}

// The frozen flush snapshot stays FIFO; later notifications interrupt pacing.
func TestSessionPacer_NotificationArrivesDuringBacklogFlush(t *testing.T) {
	boundaries := []struct {
		name  string
		frame sessionFrame
	}{
		{"none", sessionFrame{}},
		{"reset", sessionFrame{kind: sessionFrameResetted, payload: 1}},
		{"info", sessionFrame{kind: sessionFrameInfo, payload: 1}},
		{"error_ebus", sessionFrame{kind: sessionFrameErrorEBUS}},
		{"error_host", sessionFrame{kind: sessionFrameErrorHost}},
		{"started", sessionFrame{kind: sessionFrameStarted, payload: 2}},
		{"failed", sessionFrame{kind: sessionFrameFailed, payload: 2}},
	}
	for _, kind := range []sessionFrameKind{sessionFrameStarted, sessionFrameFailed} {
		for _, boundary := range boundaries {
			for _, laterBoundary := range []bool{false, true} {
				t.Run(fmt.Sprintf("kind_%d/%s/later_%t", kind, boundary.name, laterBoundary), func(t *testing.T) {
					mux, _, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeShadow)
					t.Cleanup(cleanup)
					mux.ensureSessionPacer(1)
					const tau = 20 * time.Millisecond
					mux.SessionPacer(1).SetTau(tau)
					c := &snapshotBacklogConn{
						backlogConn: backlogConn{written: make(chan struct{}, 16)},
						entered:     make(chan byte, 3), release: make(chan struct{}),
					}
					s := &session{id: 1, mux: mux, conn: c, sendCh: make(chan sessionFrame, 256), done: make(chan struct{})}
					received := func(b byte) sessionFrame { return sessionFrame{kind: sessionFrameReceived, payload: b} }
					initial := []sessionFrame{received(0x10), received(0x11), received(0x12)}
					if boundary.name != "none" && !laterBoundary {
						initial = append(initial, boundary.frame)
					}
					for _, f := range initial {
						s.sendCh <- f
					}
					s.wg.Add(1)
					go s.writeLoop()
					t.Cleanup(func() { s.closed.Store(true); close(s.done); close(c.release); s.wg.Wait() })
					select {
					case <-c.entered:
					case <-time.After(time.Second):
						t.Fatal("flush did not start")
					}
					arrivals := []sessionFrame{received(0x13)}
					if boundary.name != "none" && laterBoundary {
						arrivals = append(arrivals, boundary.frame)
					}
					notification := sessionFrame{kind: kind, payload: 0x31}
					arrivals = append(arrivals, notification, received(0x14))
					for _, f := range arrivals {
						s.sendCh <- f
					}
					for i := 0; i < 3; i++ {
						select {
						case c.release <- struct{}{}:
						case <-time.After(time.Second):
							t.Fatal("flush write did not complete")
						}
					}
					want := append(append([]sessionFrame(nil), initial...), arrivals...)
					flushPrefix := boundary.name == "none" || !laterBoundary ||
						boundary.frame.kind == sessionFrameStarted || boundary.frame.kind == sessionFrameFailed
					writes := awaitBacklogWrites(t, &c.backlogConn, len(want))
					// Encode expectations through a separate, unpaced session.
					expected := &backlogConn{written: make(chan struct{}, 16)}
					encoder := &session{conn: expected}
					var previousMirror time.Time
					for i, f := range want {
						if err := encoder.writeFrame(f); err != nil {
							t.Fatal(err)
						}
						if !bytes.Equal(writes[i].data, expected.writes[i].data) {
							t.Fatalf("write %d=%x; want %x", i, writes[i].data, expected.writes[i].data)
						}
						if f.kind == sessionFrameReceived {
							if f.payload == 0x13 && flushPrefix {
								if gap := writes[i+1].at.Sub(writes[i].at); gap > 5*time.Millisecond {
									t.Fatalf("notification followed flushed prefix after %v; want <=5ms", gap)
								}
							} else if f.payload >= 0x13 && writes[i].at.Sub(previousMirror) < tau {
								t.Fatalf("later mirror %x inherited flush cadence", f.payload)
							}
							previousMirror = writes[i].at
						}
					}
					if got := s.backlogFlushes.Load(); got != 1 {
						t.Fatalf("flushes=%d; want 1", got)
					}
				})
			}
		}
	}
}

// A stalled TCP write must not free a second channel's worth of capacity.
func TestSessionPacer_BlockedWriteBoundsLookahead(t *testing.T) {
	mux, _, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeShadow)
	t.Cleanup(cleanup)
	mux.ensureSessionPacer(1)
	c := &snapshotBacklogConn{
		backlogConn: backlogConn{written: make(chan struct{}, 256)},
		entered:     make(chan byte, 3), release: make(chan struct{}),
	}
	const capacity = 128
	s := &session{id: 1, mux: mux, conn: c, sendCh: make(chan sessionFrame, capacity), done: make(chan struct{})}
	for i := 0; i < capacity; i++ {
		s.sendCh <- sessionFrame{kind: sessionFrameReceived, payload: 0x10}
	}
	s.wg.Add(1)
	go s.writeLoop()
	t.Cleanup(func() { s.closed.Store(true); close(s.done); close(c.release); s.wg.Wait() })
	select {
	case <-c.entered:
	case <-time.After(time.Second):
		t.Fatal("writer did not stall")
	}
	if got := len(s.sendCh); got != capacity-(maxSessionPacerBacklog+1) {
		t.Fatalf("stalled writer drained %d frames; want only three", capacity-got)
	}
}

func TestSessionPacer_FlushMetricIsSeparate(t *testing.T) {
	before := adaptermuxSessionBacklogFlushesTotal.Value()
	_, c := backlogSession(t, []sessionFrame{
		{kind: sessionFrameReceived, payload: 1},
		{kind: sessionFrameReceived, payload: 2},
		{kind: sessionFrameReceived, payload: 3},
	})
	awaitBacklogWrites(t, c, 3)
	if adaptermuxSessionBacklogFlushesTotal.Value() <= before {
		t.Fatal("flush counter did not increase")
	}
	if adaptermuxSessionFrameLatencyBucketTotal.Get("backlog_flushes") != nil {
		t.Fatal("flush counter polluted latency histogram")
	}
}

func TestSessionPacer_SessionBudgetIncludesLookahead(t *testing.T) {
	mux, _, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeShadow)
	t.Cleanup(cleanup)
	server, client := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	id := mux.AddSession(server)
	if id == 0 {
		t.Fatal("session rejected")
	}
	t.Cleanup(func() { mux.RemoveSession(id) })
	mux.sessionsMu.Lock()
	s := mux.sessions[id]
	mux.sessionsMu.Unlock()
	if got := cap(s.sendCh) + sessionWriterLookahead; got != defaultSessionSendBuffer {
		t.Fatalf("channel plus writer budget=%d; want %d", got, defaultSessionSendBuffer)
	}
}
