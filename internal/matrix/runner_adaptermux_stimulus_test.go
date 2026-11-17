package matrix

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// fakeMuxPeer models the mux ENH listener as an external session sees it.
// A bid stays queued until fakeInFlightArm has elapsed and its STARTED waits
// until fakeLateAfter, so a contender can replace or abandon it in flight.
// The late STARTED of a replaced bid is absorbed unless deliverStaleResult.
// Closing an in-flight bid arms the absorb barrier. A SEND is refused while
// another connection owns the bus unless forwardDuringActive. The first
// completed command stops mid-response when stopMidResponse is set.
type fakeMuxPeer struct {
	response []byte
	// deliverStaleResult hands the contender the replaced bid's STARTED.
	deliverStaleResult bool
	// absorbNeverExpires leaves the absorb barrier armed.
	absorbNeverExpires bool
	// forwardDuringActive echoes a SEND while another session owns the bus.
	forwardDuringActive bool
	// stopMidResponse ends the first response after its length byte and sends SYN.
	stopMidResponse bool
	// noClosingSyn never closes an open command with SYN.
	noClosingSyn bool

	mu                     sync.Mutex
	telegrams              [][]byte
	starts                 int
	startedSent            int
	rejectedDuringActive   []byte
	rejectedIdle           []byte
	closedInFlight         int
	closedQueued           int
	absorbArmed            int
	replacedInFlight       int
	staleDelivered         int
	replacementGranted     int
	grantsHeldForAbsorb    int
	midResponseSyn         int
	responseBytesBeforeSyn int
	responseBytesOwed      int
	synClosures            int
	dials                  int
	busOwner               net.Conn
	inFlight               *fakeBid
	bidGen                 int
	absorbDeadline         time.Time
	grantAfterAbsorb       time.Time
	grantedOnce            map[net.Conn]bool
	withheldOnce           bool
}

// fakeBid is one connection's outstanding START.
type fakeBid struct {
	initiator      byte
	staleInitiator byte
	replacement    byte
	conn           net.Conn
	generation     int
	queuedAt       time.Time
	replaced       bool
}

const (
	// fakeInFlightArm is the age at which a queued bid counts as in flight.
	fakeInFlightArm = 20 * time.Millisecond
	// fakeLateAfter is when that bid's STARTED or FAILED is delivered,
	// measured from the START. It stays after bidInFlightSettle.
	fakeLateAfter     = 200 * time.Millisecond
	fakeAbsorbBudget  = 250 * time.Millisecond
	fakeMuxGrantDelay = 10 * time.Millisecond
	fakeMuxIdleSyn    = 300 * time.Millisecond
)

func (bid *fakeBid) inFlightAt(now time.Time) bool {
	return bid != nil && !now.Before(bid.queuedAt.Add(fakeInFlightArm))
}

func (peer *fakeMuxPeer) observed() [][]byte {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	return append([][]byte(nil), peer.telegrams...)
}

// fakeMuxCounts is a copy of what the peer counted.
type fakeMuxCounts struct {
	telegrams              [][]byte
	starts                 int
	startedSent            int
	rejectedDuringActive   []byte
	rejectedIdle           []byte
	closedInFlight         int
	closedQueued           int
	absorbArmed            int
	replacedInFlight       int
	staleDelivered         int
	replacementGranted     int
	grantsHeldForAbsorb    int
	midResponseSyn         int
	responseBytesBeforeSyn int
	responseBytesOwed      int
	synClosures            int
	dials                  int
	absorbDeadline         time.Time
	grantAfterAbsorb       time.Time
}

func (peer *fakeMuxPeer) snapshot() fakeMuxCounts {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	return fakeMuxCounts{
		starts: peer.starts, startedSent: peer.startedSent,
		rejectedDuringActive: append([]byte(nil), peer.rejectedDuringActive...),
		rejectedIdle:         append([]byte(nil), peer.rejectedIdle...),
		closedInFlight:       peer.closedInFlight, closedQueued: peer.closedQueued,
		absorbArmed: peer.absorbArmed, replacedInFlight: peer.replacedInFlight,
		staleDelivered: peer.staleDelivered, replacementGranted: peer.replacementGranted,
		grantsHeldForAbsorb: peer.grantsHeldForAbsorb, midResponseSyn: peer.midResponseSyn,
		responseBytesBeforeSyn: peer.responseBytesBeforeSyn, responseBytesOwed: peer.responseBytesOwed,
		synClosures: peer.synClosures, dials: peer.dials,
		absorbDeadline: peer.absorbDeadline, grantAfterAbsorb: peer.grantAfterAbsorb,
		telegrams: append([][]byte(nil), peer.telegrams...),
	}
}

func (peer *fakeMuxPeer) serve(conn net.Conn) {
	const (
		idle = iota
		initiatorSend
		awaitAck
		awaitSyn
	)
	var (
		stateMu  sync.Mutex
		owner    bool
		state    = idle
		source   byte
		wire     []byte
		lastSend time.Time
		alive    atomic.Bool
	)
	alive.Store(true)
	done := make(chan struct{})
	defer func() {
		alive.Store(false)
		close(done)
		_ = conn.Close()
		stateMu.Lock()
		peer.mu.Lock()
		now := time.Now()
		if bid := peer.inFlight; bid != nil && bid.conn == conn {
			if bid.inFlightAt(now) && !bid.replaced {
				peer.closedInFlight++
				peer.absorbArmed++
				if peer.absorbNeverExpires {
					peer.absorbDeadline = now.Add(time.Hour)
				} else {
					peer.absorbDeadline = now.Add(fakeAbsorbBudget)
				}
			} else if !bid.replaced {
				peer.closedQueued++
			}
			peer.bidGen++
			peer.inFlight = nil
		}
		if peer.busOwner == conn {
			peer.busOwner = nil
			owner = false
		}
		peer.mu.Unlock()
		stateMu.Unlock()
	}()
	peer.mu.Lock()
	peer.dials++
	if peer.grantedOnce == nil {
		peer.grantedOnce = map[net.Conn]bool{}
	}
	peer.mu.Unlock()
	// write is called with stateMu held, so frames keep their order; a
	// writer goroutine drains the queue so the peer never blocks on the
	// synchronous pipe while the session is writing too.
	out := make(chan []byte, 4096)
	go func() {
		for {
			select {
			case <-done:
				return
			case frame := <-out:
				if _, err := conn.Write(frame); err != nil {
					return
				}
			}
		}
	}()
	write := func(command transport.ENHCommand, data byte) {
		frame := transport.EncodeENH(command, data)
		out <- frame[:]
	}
	// releaseBus clears this connection's ownership. Caller holds stateMu.
	releaseBus := func() {
		peer.mu.Lock()
		if peer.busOwner == conn {
			peer.busOwner = nil
		}
		peer.mu.Unlock()
		owner, state = false, idle
	}
	// grantNow delivers STARTED for initiator. held counts a grant that
	// waited out the absorb barrier. Caller must not hold either mutex.
	var grantNow func(initiator byte, held bool)
	grantNow = func(initiator byte, held bool) {
		if !alive.Load() {
			return
		}
		stateMu.Lock()
		defer stateMu.Unlock()
		if !alive.Load() {
			return
		}
		peer.mu.Lock()
		if peer.busOwner != nil && peer.busOwner != conn {
			peer.mu.Unlock()
			time.AfterFunc(5*time.Millisecond, func() { grantNow(initiator, held) })
			return
		}
		peer.grantedOnce[conn] = true
		peer.busOwner = conn
		peer.startedSent++
		if held {
			peer.grantsHeldForAbsorb++
			if peer.grantAfterAbsorb.IsZero() {
				peer.grantAfterAbsorb = time.Now()
			}
		}
		peer.mu.Unlock()
		owner, state, source, wire, lastSend = true, initiatorSend, initiator, nil, time.Now()
		write(transport.ENHResStarted, initiator)
	}
	// resolve delivers the outstanding bid's result once it is due.
	var resolve func(gen int)
	resolve = func(gen int) {
		if !alive.Load() {
			return
		}
		stateMu.Lock()
		defer stateMu.Unlock()
		if !alive.Load() {
			return
		}
		peer.mu.Lock()
		bid := peer.inFlight
		if bid == nil || bid.generation != gen || bid.conn != conn {
			peer.mu.Unlock()
			return
		}
		initiator := bid.initiator
		stale := false
		if bid.replaced && peer.deliverStaleResult {
			initiator = bid.staleInitiator
			stale = true
		} else if bid.replaced {
			initiator = bid.replacement
		}
		if !stale && peer.busOwner != nil && peer.busOwner != conn {
			peer.mu.Unlock()
			time.AfterFunc(5*time.Millisecond, func() { resolve(gen) })
			return
		}
		peer.inFlight = nil
		if stale {
			peer.staleDelivered++
			peer.mu.Unlock()
			write(transport.ENHResStarted, initiator)
			return
		}
		if bid.replaced {
			peer.replacementGranted++
		}
		peer.grantedOnce[conn] = true
		peer.busOwner = conn
		peer.startedSent++
		peer.mu.Unlock()
		owner, state, source, wire, lastSend = true, initiatorSend, initiator, nil, time.Now()
		write(transport.ENHResStarted, initiator)
	}
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
			}
			stateMu.Lock()
			if owner && !peer.noClosingSyn && time.Since(lastSend) > fakeMuxIdleSyn {
				peer.mu.Lock()
				peer.synClosures++
				if peer.busOwner == conn {
					peer.busOwner = nil
				}
				peer.mu.Unlock()
				owner, state = false, idle
				write(transport.ENHResReceived, protocol.SymbolSyn)
			}
			stateMu.Unlock()
		}
	}()

	var parser transport.ENHParser
	buffer := make([]byte, 64)
	for {
		count, err := conn.Read(buffer)
		if err != nil {
			return
		}
		messages, err := parser.Parse(buffer[:count])
		if err != nil {
			return
		}
		stateMu.Lock()
		for _, message := range messages {
			switch message.Command {
			case transport.ENHReqInit:
				write(transport.ENHResResetted, message.Data)
			case transport.ENHReqStart:
				initiator := message.Data
				peer.mu.Lock()
				peer.starts++
				if initiator == protocol.SymbolSyn {
					if peer.busOwner == conn {
						peer.busOwner = nil
					}
					peer.mu.Unlock()
					owner, state = false, idle
					continue
				}
				now := time.Now()
				if bid := peer.inFlight; bid != nil && bid.conn == conn {
					if bid.inFlightAt(now) && bid.initiator != initiator {
						bid.replaced = true
						bid.staleInitiator = bid.initiator
						bid.replacement = initiator
						peer.replacedInFlight++
					}
					peer.mu.Unlock()
					continue
				}
				if !peer.absorbDeadline.IsZero() && now.Before(peer.absorbDeadline) {
					never := peer.absorbNeverExpires
					deadline := peer.absorbDeadline
					peer.mu.Unlock()
					if !never {
						time.AfterFunc(time.Until(deadline), func() { grantNow(initiator, true) })
					}
					continue
				}
				if !peer.grantedOnce[conn] {
					peer.bidGen++
					gen := peer.bidGen
					peer.inFlight = &fakeBid{initiator: initiator, conn: conn, generation: gen, queuedAt: now}
					peer.mu.Unlock()
					time.AfterFunc(fakeLateAfter, func() { resolve(gen) })
					continue
				}
				peer.mu.Unlock()
				time.AfterFunc(fakeMuxGrantDelay, func() { grantNow(initiator, false) })
			case transport.ENHReqSend:
				peer.mu.Lock()
				owns := peer.busOwner == conn
				busy := peer.busOwner != nil && !owns
				forward := peer.forwardDuringActive
				if !owns {
					if busy && forward {
						peer.mu.Unlock()
						write(transport.ENHResReceived, message.Data)
						continue
					}
					if busy {
						peer.rejectedDuringActive = append(peer.rejectedDuringActive, message.Data)
					} else {
						peer.rejectedIdle = append(peer.rejectedIdle, message.Data)
					}
					peer.mu.Unlock()
					write(transport.ENHResErrorHost, 0x00)
					continue
				}
				peer.mu.Unlock()
				lastSend = time.Now()
				write(transport.ENHResReceived, message.Data)
				switch state {
				case initiatorSend:
					wire = append(wire, message.Data)
					logical, whole := unescapeSymbols(wire)
					if !whole || len(logical) < 4 || len(logical) != 4+int(logical[3])+1 {
						continue
					}
					response := append([]byte{byte(len(peer.response))}, peer.response...)
					response = append(response, protocol.CRC(response))
					peer.mu.Lock()
					peer.telegrams = append(peer.telegrams, append([]byte{source}, logical...))
					withhold := peer.stopMidResponse && !peer.withheldOnce
					if withhold {
						peer.withheldOnce = true
						peer.responseBytesBeforeSyn = 1
						peer.responseBytesOwed = len(response)
						peer.midResponseSyn++
						peer.synClosures++
						if peer.busOwner == conn {
							peer.busOwner = nil
						}
					}
					peer.mu.Unlock()
					write(transport.ENHResReceived, protocol.SymbolAck)
					if withhold {
						write(transport.ENHResReceived, response[0])
						owner, state = false, idle
						write(transport.ENHResReceived, protocol.SymbolSyn)
						continue
					}
					for _, symbol := range escapeSymbols(response) {
						write(transport.ENHResReceived, symbol)
					}
					state = awaitAck
				case awaitAck:
					state = awaitSyn
				case awaitSyn:
					releaseBus()
				}
			}
		}
		stateMu.Unlock()
	}
}

func readStimulusRecord(t *testing.T, outputDir string, caseID string) (StimulusRecord, string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(outputDir, caseID, "logs", "client.json"))
	if err != nil {
		t.Fatalf("read client record: %v", err)
	}
	var record struct {
		Record StimulusRecord `json:"record"`
		Error  string         `json:"error"`
	}
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatalf("decode client record: %v", err)
	}
	return record.Record, record.Error
}

func clientResultOf(verdict CaseVerdict) commandResult {
	for _, command := range verdict.Commands {
		if strings.HasPrefix(command.Name, "client-") {
			return command
		}
	}
	return commandResult{}
}

func TestEveryExecutablePXCellHasItsOwnStimulus(t *testing.T) {
	t.Parallel()

	seen := map[string]string{}
	for _, testCase := range GenerateAdapterMuxV2Cases("") {
		cell := testCase.AdapterMux
		if !cell.Executable() || cell.NorthboundClient != NorthboundSyntheticENH {
			continue
		}
		stimulus := contenderStimuli[cell.Invariant]
		if stimulus == "" {
			t.Fatalf("%s has no contender stimulus", testCase.ID)
		}
		if other, ok := seen[stimulus]; ok && other != cell.Invariant {
			t.Fatalf("%s and %s share the stimulus %s", other, cell.Invariant, stimulus)
		}
		seen[stimulus] = cell.Invariant
	}
	if len(seen) != len(contenderStimuli) {
		t.Fatalf("stimuli %v; want one per executable PX invariant %v", seen, contenderStimuli)
	}
}

func TestContenderStimulusMatchesTheInvariant(t *testing.T) {
	t.Parallel()

	if fakeInFlightArm >= bidInFlightSettle || bidInFlightSettle >= fakeLateAfter {
		t.Fatalf("in-flight window is arm %s, settle %s, late result %s", fakeInFlightArm, bidInFlightSettle, fakeLateAfter)
	}
	cases := []struct {
		invariant string
		observed  string
		prepare   func(peer *fakeMuxPeer)
		check     func(t *testing.T, peer fakeMuxCounts)
	}{
		{"PX01", "replaced bid absorbed and its late result did not arrive", nil, func(t *testing.T, peer fakeMuxCounts) {
			if peer.replacedInFlight != 1 || peer.staleDelivered != 0 || peer.replacementGranted != 1 {
				t.Fatalf("replaced %d, stale delivered %d, replacement granted %d; want 1, 0, 1",
					peer.replacedInFlight, peer.staleDelivered, peer.replacementGranted)
			}
		}},
		{"PX02", "in-flight bid left for the absorb barrier", nil, func(t *testing.T, peer fakeMuxCounts) {
			if peer.closedInFlight != 1 || peer.closedQueued != 0 || peer.absorbArmed != 1 || peer.dials != 2 {
				t.Fatalf("closed in flight %d, closed queued %d, absorb armed %d, sessions %d",
					peer.closedInFlight, peer.closedQueued, peer.absorbArmed, peer.dials)
			}
			if peer.grantsHeldForAbsorb < 1 || peer.grantAfterAbsorb.IsZero() || peer.grantAfterAbsorb.Before(peer.absorbDeadline) {
				t.Fatalf("grants held %d at %s, barrier %s; the next session was granted before the barrier expired",
					peer.grantsHeldForAbsorb, peer.grantAfterAbsorb, peer.absorbDeadline)
			}
		}},
		{"PX03", "bus closed the command before its CRC with SYN", nil, func(t *testing.T, peer fakeMuxCounts) {
			if peer.synClosures != 1 || peer.startedSent != contenderTelegramCount+1 {
				t.Fatalf("SYN closures %d, grants %d", peer.synClosures, peer.startedSent)
			}
		}},
		{"PX04", "SYN while response bytes were still missing", func(peer *fakeMuxPeer) { peer.stopMidResponse = true }, func(t *testing.T, peer fakeMuxCounts) {
			if peer.midResponseSyn != 1 || peer.responseBytesBeforeSyn <= 0 || peer.responseBytesBeforeSyn >= peer.responseBytesOwed {
				t.Fatalf("mid-response SYN %d after %d of %d response bytes",
					peer.midResponseSyn, peer.responseBytesBeforeSyn, peer.responseBytesOwed)
			}
			if peer.startedSent != contenderTelegramCount+1 || len(peer.telegrams) != contenderTelegramCount+1 {
				t.Fatalf("grants %d, telegrams %d; the bid after the mid-response SYN was not granted",
					peer.startedSent, len(peer.telegrams))
			}
		}},
		{"PX12", "non-owner SEND refused with ERROR_HOST during an active transaction", nil, func(t *testing.T, peer fakeMuxCounts) {
			if string(peer.rejectedDuringActive) != string([]byte{protocol.SymbolSyn}) || len(peer.rejectedIdle) != 0 || peer.dials != 2 {
				t.Fatalf("rejected during active % X, idle % X, sessions %d",
					peer.rejectedDuringActive, peer.rejectedIdle, peer.dials)
			}
		}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.invariant, func(t *testing.T) {
			t.Parallel()
			caseID := "ENS-" + tc.invariant
			fixture, err := newExecuteFixture(t, []string{caseID}, nil)
			if err != nil {
				t.Fatalf("fixture: %v", err)
			}
			if tc.prepare != nil {
				tc.prepare(fixture.enhPeer)
			}
			verdicts, err := fixture.runner.Run(context.Background())
			if err != nil {
				t.Fatalf("Run error = %v", err)
			}
			if verdicts[0].Outcome != caseOutcomePass {
				t.Fatalf("verdict = %+v", verdicts[0])
			}
			record, recordErr := readStimulusRecord(t, fixture.output, caseID)
			if recordErr != "" || record.Invariant != tc.invariant || record.Stimulus != contenderStimuli[tc.invariant] ||
				record.Observed != tc.observed || record.Workload.Completed != contenderTelegramCount {
				t.Fatalf("record = %+v (error %q)", record, recordErr)
			}
			peer := fixture.enhPeer.snapshot()
			tc.check(t, peer)
			for _, logical := range peer.telegrams {
				onWire := GuardTelegram{Target: logical[1], Primary: logical[2], Secondary: logical[3], Data: logical[5 : len(logical)-1]}
				if requestType, err := ClassifyTelegram(onWire); err != nil || requestType != RequestIdentification {
					t.Fatalf("contender sent % X (%q, %v)", logical, requestType, err)
				}
			}
		})
	}
}

func TestContenderStimulusFailsWhenTheMuxDoesNotAnswerAsRequired(t *testing.T) {
	t.Parallel()

	cases := []struct {
		invariant string
		mutate    func(peer *fakeMuxPeer)
		want      string
	}{
		{"PX01", func(peer *fakeMuxPeer) { peer.deliverStaleResult = true }, "stale arbitration result"},
		{"PX02", func(peer *fakeMuxPeer) { peer.absorbNeverExpires = true }, "await arbitration"},
		{"PX03", func(peer *fakeMuxPeer) { peer.noClosingSyn = true }, "await the closing SYN"},
		{"PX04", nil, "target delivered its response"},
		{"PX12", func(peer *fakeMuxPeer) { peer.forwardDuringActive = true }, "await the refusal of a non-owner SEND"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.invariant, func(t *testing.T) {
			t.Parallel()
			caseID := "ENH-" + tc.invariant
			fixture, err := newExecuteFixture(t, []string{caseID}, nil)
			if err != nil {
				t.Fatalf("fixture: %v", err)
			}
			if tc.mutate != nil {
				tc.mutate(fixture.enhPeer)
			}
			if tc.invariant == "PX02" {
				fixture.runner.options.CaseTimeout = 20 * time.Second
			}
			verdicts, err := fixture.runner.Run(context.Background())
			if err != nil {
				t.Fatalf("Run error = %v", err)
			}
			client := clientResultOf(verdicts[0])
			if verdicts[0].Outcome != caseOutcomeFail || client.Status != actualStatusFailed || !strings.Contains(client.Error, tc.want) {
				t.Fatalf("verdict %+v, client %+v; want a failed client naming %q", verdicts[0], client, tc.want)
			}
		})
	}
}

func TestClientStartsOnlyAfterTheSmokeBaseline(t *testing.T) {
	t.Parallel()

	var fixture *executeFixture
	var err error
	fixture, err = newExecuteFixture(t, []string{"ENS-PX02", "ENS-AD06"}, func(options *RunnerOptions) {
		options.SmokeCommand = "sleep 0.3; echo smoke-baseline-$MATRIX_INVARIANT >> \"$MATRIX_LOG_DIR/../order.log\"; " +
			": > \"$MATRIX_SMOKE_READY_FILE\"; sleep 0.5"
	})
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	dial := fixture.runner.dial
	fixture.runner.dial = func(ctx context.Context, address string) (net.Conn, error) {
		caseID := "ENS-PX02"
		if address == "ebusd.invalid:1" {
			caseID = "ENS-AD06"
		}
		appendLine(t, filepath.Join(fixture.output, caseID, "order.log"), "client-dial")
		return dial(ctx, address)
	}
	verdicts, err := fixture.runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}
	for _, verdict := range verdicts {
		if verdict.Outcome != caseOutcomePass {
			t.Fatalf("%s verdict = %+v", verdict.CaseID, verdict)
		}
		data, err := os.ReadFile(filepath.Join(fixture.output, verdict.CaseID, "order.log"))
		if err != nil {
			t.Fatalf("read order: %v", err)
		}
		lines := strings.Fields(string(data))
		if len(lines) < 2 || !strings.HasPrefix(lines[0], "smoke-baseline-") || lines[1] != "client-dial" {
			t.Fatalf("%s order = %v; the client dialled before the smoke baseline", verdict.CaseID, lines)
		}
	}
}

func appendLine(t *testing.T, path string, line string) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Errorf("open %s: %v", path, err)
		return
	}
	defer func() { _ = file.Close() }()
	_, _ = fmt.Fprintln(file, line)
}

func TestClientIsNotStartedWhenTheSmokeEndsWithoutBaseline(t *testing.T) {
	t.Parallel()

	fixture, err := newExecuteFixture(t, []string{"ENH-PX01"}, func(options *RunnerOptions) {
		options.SmokeCommand = "exit 4"
	})
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	verdicts, err := fixture.runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}
	client := clientResultOf(verdicts[0])
	if verdicts[0].Outcome != caseOutcomeFail || client.Status != actualStatusFailed ||
		!strings.Contains(client.Error, "ended before it signalled its baseline") {
		t.Fatalf("verdict %+v, client %+v", verdicts[0], client)
	}
	if peer := fixture.enhPeer.snapshot(); peer.dials != 0 {
		t.Fatalf("the contender dialled %d times without a baseline", peer.dials)
	}
}

func TestEbusdCommandAddressReachesTheCellCommands(t *testing.T) {
	fixture, err := newExecuteFixture(t, []string{"ENH-AD06"}, nil)
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	fixture.runner.options.StartEbusd = "echo ebusd-addr=$MATRIX_EBUSD_COMMAND_ADDR >> '" + fixture.marker + "'"
	if _, err := fixture.runner.Run(context.Background()); err != nil {
		t.Fatalf("Run error = %v", err)
	}
	if commands := strings.Join(fixture.commandsRun(t), " "); !strings.Contains(commands, "ebusd-addr=ebusd.invalid:1") {
		t.Fatalf("commands %q; want the --ebusd-command-addr value", commands)
	}

	for _, exported := range []string{"ebusd.invalid:1", " ebusd.invalid:1 "} {
		t.Setenv(ebusdCommandAddrEnv, exported)
		if _, err := newExecuteFixture(t, []string{"ENH-AD06"}, nil); err != nil {
			t.Fatalf("exported %q equal to the option: %v", exported, err)
		}
	}
	for _, exported := range []string{"ebusd.invalid:2", "other.invalid:1"} {
		t.Setenv(ebusdCommandAddrEnv, exported)
		_, err := newExecuteFixture(t, []string{"ENH-AD06"}, nil)
		if err == nil || !strings.Contains(err.Error(), "differs from --ebusd-command-addr") {
			t.Fatalf("exported %q: err = %v; want a mismatch error", exported, err)
		}
		if strings.Contains(err.Error(), exported) {
			t.Fatalf("the mismatch error prints the address: %v", err)
		}
	}
	t.Setenv(ebusdCommandAddrEnv, "ebusd.invalid:1")
	if _, err := newExecuteFixture(t, []string{"ENH-AD01"}, func(options *RunnerOptions) { options.EbusdCommandAddr = "" }); err == nil {
		t.Fatalf("an exported address without --ebusd-command-addr was accepted")
	}
}
