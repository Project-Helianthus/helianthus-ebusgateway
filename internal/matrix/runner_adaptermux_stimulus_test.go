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
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// fakeMuxPeer models the mux ENH listener as an external session sees it:
// a START is granted after a short delay and a replaced bid is answered
// once; a SEND without ownership is refused with ERROR_HOST; an owner that
// stops sending sees the bus close its telegram with SYN.
type fakeMuxPeer struct {
	response []byte
	// duplicateStarted answers every START, so a replaced bid's result
	// reaches the session (the mux would absorb it).
	duplicateStarted bool
	// forwardNonOwner echoes a SEND without ownership instead of refusing it.
	forwardNonOwner bool
	// noClosingSyn never closes an open telegram.
	noClosingSyn bool

	mu            sync.Mutex
	telegrams     [][]byte
	starts        int
	startedSent   int
	rejected      []byte
	closedWithBid int
	synClosures   int
	dials         int
}

const (
	fakeMuxGrantDelay = 10 * time.Millisecond
	fakeMuxIdleSyn    = 300 * time.Millisecond
)

func (peer *fakeMuxPeer) observed() [][]byte {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	return append([][]byte(nil), peer.telegrams...)
}

// fakeMuxCounts is a copy of what the peer counted.
type fakeMuxCounts struct {
	telegrams     [][]byte
	starts        int
	startedSent   int
	rejected      []byte
	closedWithBid int
	synClosures   int
	dials         int
}

func (peer *fakeMuxPeer) snapshot() fakeMuxCounts {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	return fakeMuxCounts{
		starts: peer.starts, startedSent: peer.startedSent, rejected: append([]byte(nil), peer.rejected...),
		closedWithBid: peer.closedWithBid, synClosures: peer.synClosures, dials: peer.dials,
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
		stateMu      sync.Mutex
		owner        bool
		grantPending bool
		state        = idle
		source       byte
		wire         []byte
		lastSend     time.Time
	)
	done := make(chan struct{})
	defer func() {
		close(done)
		_ = conn.Close()
		stateMu.Lock()
		if grantPending {
			peer.mu.Lock()
			peer.closedWithBid++
			peer.mu.Unlock()
		}
		stateMu.Unlock()
	}()
	peer.mu.Lock()
	peer.dials++
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
	grant := func(bid byte) {
		owner, grantPending, state, source, wire, lastSend = true, false, initiatorSend, bid, nil, time.Now()
		peer.mu.Lock()
		peer.startedSent++
		peer.mu.Unlock()
		write(transport.ENHResStarted, bid)
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
				owner, state = false, idle
				peer.mu.Lock()
				peer.synClosures++
				peer.mu.Unlock()
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
				peer.mu.Lock()
				peer.starts++
				peer.mu.Unlock()
				bid := message.Data
				switch {
				case peer.duplicateStarted:
					grant(bid)
				case grantPending:
					// Replacement: the pending bid is answered once.
				default:
					grantPending = true
					time.AfterFunc(fakeMuxGrantDelay, func() {
						stateMu.Lock()
						defer stateMu.Unlock()
						if grantPending {
							grant(bid)
						}
					})
				}
			case transport.ENHReqSend:
				if !owner {
					if peer.forwardNonOwner {
						write(transport.ENHResReceived, message.Data)
						continue
					}
					peer.mu.Lock()
					peer.rejected = append(peer.rejected, message.Data)
					peer.mu.Unlock()
					write(transport.ENHResErrorHost, 0x00)
					continue
				}
				lastSend = time.Now()
				write(transport.ENHResReceived, message.Data)
				switch state {
				case initiatorSend:
					wire = append(wire, message.Data)
					logical, whole := unescapeSymbols(wire)
					if !whole || len(logical) < 4 || len(logical) != 4+int(logical[3])+1 {
						continue
					}
					peer.mu.Lock()
					peer.telegrams = append(peer.telegrams, append([]byte{source}, logical...))
					peer.mu.Unlock()
					write(transport.ENHResReceived, protocol.SymbolAck)
					response := append([]byte{byte(len(peer.response))}, peer.response...)
					response = append(response, protocol.CRC(response))
					for _, symbol := range escapeSymbols(response) {
						write(transport.ENHResReceived, symbol)
					}
					state = awaitAck
				case awaitAck:
					state = awaitSyn
				case awaitSyn:
					owner, state = false, idle
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

	cases := []struct {
		invariant string
		observed  string
		check     func(t *testing.T, peer fakeMuxCounts)
	}{
		{"PX01", "replaced bid answered once", func(t *testing.T, peer fakeMuxCounts) {
			if peer.starts != contenderTelegramCount+1 || peer.startedSent != contenderTelegramCount {
				t.Fatalf("starts %d, STARTED sent %d; want %d bids answered %d times",
					peer.starts, peer.startedSent, contenderTelegramCount+1, contenderTelegramCount)
			}
		}},
		{"PX02", "session closed with its bid outstanding", func(t *testing.T, peer fakeMuxCounts) {
			if peer.closedWithBid != 1 || peer.dials != 2 {
				t.Fatalf("closed with bid %d over %d sessions; want 1 over 2", peer.closedWithBid, peer.dials)
			}
		}},
		{"PX03", "bus closed the command before its CRC with SYN", func(t *testing.T, peer fakeMuxCounts) {
			if peer.synClosures != 1 || peer.startedSent != contenderTelegramCount+1 {
				t.Fatalf("SYN closures %d, grants %d", peer.synClosures, peer.startedSent)
			}
		}},
		{"PX04", "bus closed the unacknowledged response with SYN", func(t *testing.T, peer fakeMuxCounts) {
			// The unacknowledged command is complete, so the peer saw one more.
			if peer.synClosures != 1 || len(peer.telegrams) != contenderTelegramCount+1 {
				t.Fatalf("SYN closures %d, telegrams %d", peer.synClosures, len(peer.telegrams))
			}
		}},
		{"PX12", "non-owner SEND refused with ERROR_HOST", func(t *testing.T, peer fakeMuxCounts) {
			if string(peer.rejected) != string([]byte{protocol.SymbolSyn}) {
				t.Fatalf("rejected % X; want one SYN", peer.rejected)
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
		{"PX01", func(peer *fakeMuxPeer) { peer.duplicateStarted = true }, "unexpected ENH command"},
		{"PX03", func(peer *fakeMuxPeer) { peer.noClosingSyn = true }, "await the closing SYN"},
		{"PX04", func(peer *fakeMuxPeer) { peer.noClosingSyn = true }, "await the closing SYN"},
		{"PX12", func(peer *fakeMuxPeer) { peer.forwardNonOwner = true }, "await the refusal of a non-owner SEND"},
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
			tc.mutate(fixture.enhPeer)
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
