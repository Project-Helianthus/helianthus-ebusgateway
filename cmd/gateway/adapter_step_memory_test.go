package main

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// adapterStepFakeServer answers INIT with RESETTED and INFO at once, emits SYN
// every 15 ms, and answers every START with STARTED twice: the first STARTED
// grants the bid, the second arrives with nothing pending, so every START
// cycle is a bad cycle of the out-of-step rule.
type adapterStepFakeServer struct {
	ln   net.Listener
	done chan struct{}
	wg   sync.WaitGroup

	mu          sync.Mutex
	conns       []net.Conn
	connections int
}

func newAdapterStepFakeServer(t *testing.T) *adapterStepFakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen fake adapter: %v", err)
	}
	server := &adapterStepFakeServer{ln: ln, done: make(chan struct{})}
	server.wg.Add(2)
	go server.accept() // goroutine: ends when the listener closes at cleanup
	go server.syn()    // goroutine: ends when done closes at cleanup
	t.Cleanup(func() {
		close(server.done)
		_ = ln.Close()
		server.mu.Lock()
		for _, c := range server.conns {
			_ = c.Close()
		}
		server.mu.Unlock()
		server.wg.Wait()
	})
	return server
}

func (server *adapterStepFakeServer) accept() {
	defer server.wg.Done()
	for {
		c, err := server.ln.Accept()
		if err != nil {
			return
		}
		server.mu.Lock()
		server.conns = append(server.conns, c)
		server.connections++
		server.mu.Unlock()
		server.wg.Add(1)
		go server.serve(c) // goroutine: ends when the connection closes
	}
}

func (server *adapterStepFakeServer) write(c net.Conn, cmd transport.ENHCommand, data byte) {
	frame := transport.EncodeENH(cmd, data)
	server.mu.Lock()
	defer server.mu.Unlock()
	_ = c.SetWriteDeadline(time.Now().Add(time.Second))
	_, _ = c.Write(frame[:])
}

func (server *adapterStepFakeServer) serve(c net.Conn) {
	defer server.wg.Done()
	defer func() { _ = c.Close() }()
	var parser transport.ENHParser
	buf := make([]byte, 64)
	for {
		n, err := c.Read(buf)
		if err != nil {
			return
		}
		for _, b := range buf[:n] {
			msg, ok, perr := parser.Feed(b)
			if perr != nil || !ok {
				continue
			}
			switch msg.Command {
			case transport.ENHReqInit:
				server.write(c, transport.ENHResResetted, 0x01)
			case transport.ENHReqInfo:
				server.write(c, transport.ENHResInfo, 0x01)
				server.write(c, transport.ENHResInfo, 0x00)
			case transport.ENHReqStart:
				server.write(c, transport.ENHResStarted, msg.Data)
				server.write(c, transport.ENHResStarted, msg.Data)
			}
		}
	}
}

func (server *adapterStepFakeServer) syn() {
	defer server.wg.Done()
	ticker := time.NewTicker(15 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-server.done:
			return
		case <-ticker.C:
		}
		server.mu.Lock()
		conns := append([]net.Conn(nil), server.conns...)
		server.mu.Unlock()
		for _, c := range conns {
			server.write(c, transport.ENHResReceived, protocol.SymbolSyn)
		}
	}
}

func (server *adapterStepFakeServer) connectionCount() int {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.connections
}

// adapterStepGeneration builds one managed adapter-direct generation and
// returns its multiplexer and an idempotent retire that, as DriverRuntime
// does, cancels the generation's context, requests the close and waits for it.
func adapterStepGeneration(t *testing.T, cfg ebusgateway.Config, counter *ebusV8CumulativeCounter) (func(), *adaptermux.Mux) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	managed, err := buildManagedAdapterDirectGeneration(ctx, cfg, counter)
	if err != nil {
		cancel()
		t.Fatalf("build generation: %v", err)
	}
	generation, ok := managed.Transport.(*managedEBusGeneration)
	if !ok {
		t.Fatalf("generation transport %T", managed.Transport)
	}
	mux, ok := generation.Classifier().(*adaptermux.Mux)
	if !ok {
		t.Fatalf("generation classifier %T", generation.Classifier())
	}
	var once sync.Once
	retire := func() {
		once.Do(func() {
			cancel()
			close(generation.closeRequest)
		})
		<-generation.closed
	}
	t.Cleanup(retire)
	return retire, mux
}

// adapterStepBid bids for the gateway on mux until stop closes.
func adapterStepBid(mux *adaptermux.Mux, stop <-chan struct{}) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // goroutine: ends when stop closes and the last bid returns
		defer wg.Done()
		starter := mux.ActiveTransport().(interface{ StartArbitration(byte) error })
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := starter.StartArbitration(0x7F); err != nil {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()
	return &wg
}

func adapterStepWait(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Two adapter-direct generations built from one runtime counter share one
// out-of-step memory: the first spends the single link reset, the second goes
// from its first detection straight to the hold, and the stable classifier
// reports the memory's hold reason until the holding generation closes.
func TestAdapterStepMemorySharedAcrossManagedGenerations(t *testing.T) {
	server := newAdapterStepFakeServer(t)
	cfg := ebusgateway.DefaultConfig()
	cfg.BroadcastListen = false
	cfg.TransportConfig.Protocol = ebusgateway.TransportAdapterDirect
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = server.ln.Addr().String()
	cfg.TransportConfig.DialTimeout = time.Second
	counter := &ebusV8CumulativeCounter{}
	stable := &ebusStableClassifier{counter: counter}
	memory := counter.adapterStepMemory()

	retireFirst, firstMux := adapterStepGeneration(t, cfg, counter)
	if firstMux.AdapterStepMemory() != memory {
		t.Fatal("first generation does not use the runtime's memory")
	}
	stop := make(chan struct{})
	bids := adapterStepBid(firstMux, stop)
	adapterStepWait(t, "the first generation's link reset", func() bool {
		return firstMux.ActiveTxnSnapshot().AdapterLinkResets == 1
	})
	close(stop)
	retireFirst()
	bids.Wait()
	if got := stable.ActiveArbitrationHoldReason(); got != "" {
		t.Fatalf("hold reason %q after a link reset, want none", got)
	}

	retireSecond, secondMux := adapterStepGeneration(t, cfg, counter)
	if secondMux == firstMux || secondMux.AdapterStepMemory() != memory {
		t.Fatal("second generation does not share the runtime's memory")
	}
	stop = make(chan struct{})
	bids = adapterStepBid(secondMux, stop)
	adapterStepWait(t, "the second generation's hold", func() bool {
		return stable.ActiveArbitrationHoldReason() == "adapter_out_of_step"
	})
	snapshot := secondMux.ActiveTxnSnapshot()
	if snapshot.OutOfStepDetections != 1 || snapshot.AdapterLinkResets != 0 || snapshot.ActiveArbitrationHoldReason != "adapter_out_of_step" {
		t.Fatalf("second generation detections=%d link resets=%d hold=%q, want 1, 0, adapter_out_of_step",
			snapshot.OutOfStepDetections, snapshot.AdapterLinkResets, snapshot.ActiveArbitrationHoldReason)
	}
	if got := server.connectionCount(); got != 2 {
		t.Fatalf("fake adapter saw %d connections, want 2", got)
	}
	close(stop)
	retireSecond()
	bids.Wait()
	if got := stable.ActiveArbitrationHoldReason(); got != "" {
		t.Fatalf("hold reason %q after the holding generation closed", got)
	}
}

// A nil counter has no memory and reports no hold.
func TestAdapterStepMemoryNilCounterReportsNoHold(t *testing.T) {
	var counter *ebusV8CumulativeCounter
	if counter.adapterStepMemory() != nil {
		t.Fatal("nil counter returned a memory")
	}
	if got := (&ebusStableClassifier{}).ActiveArbitrationHoldReason(); got != "" {
		t.Fatalf("hold reason %q without a counter", got)
	}
}
