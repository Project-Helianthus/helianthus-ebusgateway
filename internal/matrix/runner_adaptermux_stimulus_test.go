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
// It holds the INFO answer for a queued bid until that bid is forwarded,
// which is when the grant step returns from RequestStart. A queued bid's
// selecting SYN is delivered at fakeInFlightArm, before the bid is written
// to the adapter, as the mux delivers a SYN before its grant step; the bid
// is forwarded fakeForwardLag later, the held INFO answer is released, and
// the next received symbol follows that. Its STARTED waits until fakeLateAfter, so a contender can replace or
// abandon it once it is forwarded. The late STARTED of a replaced bid is
// absorbed unless deliverStaleResult. Closing an in-flight bid arms the
// absorb barrier, which the peer clears after fakeAbsorbBudget as the mux
// does once it absorbs the cancelled bid's result; with skipAbsorb the next
// bid is granted at once with that result. A SEND is refused while another
// connection owns the bus unless forwardDuringActive. The first completed
// command stops mid-response when stopMidResponse is set.
type fakeMuxPeer struct {
	response []byte
	// deliverStaleResult hands the contender the replaced bid's STARTED.
	deliverStaleResult bool
	// deliverStaleDuringWorkload hands the workload a STARTED of the replaced
	// initiator, or a FAILED of it when staleFailed, before the real grant.
	deliverStaleDuringWorkload bool
	// staleFailed sends FAILED of the replaced initiator during the workload,
	// or of staleWinner when it is set.
	staleFailed bool
	staleWinner byte
	// deliverStaleAfterWorkload sends the replaced initiator's STARTED after
	// the identification telegrams have completed.
	deliverStaleAfterWorkload bool
	// absorbNeverExpires leaves the absorb barrier armed.
	absorbNeverExpires bool
	// skipAbsorb closes an in-flight bid without arming the barrier and hands
	// the next bid the cancelled bid's STARTED as its grant.
	skipAbsorb bool
	// neverForward leaves only the first START queued: no SYN and no result.
	neverForward bool
	// promptResult answers the first START with STARTED and no in-flight
	// SYN; later STARTs are shown in flight as usual.
	promptResult bool
	// promptFailed answers the first START with FAILED and no in-flight SYN.
	promptFailed bool
	// forwardDuringActive echoes a SEND while another session owns the bus.
	forwardDuringActive bool
	// stopMidResponse ends the first response after its length byte and sends SYN.
	stopMidResponse bool
	// noClosingSyn never closes an open command with SYN.
	noClosingSyn bool
	// synBeforeFence delivers a SYN right after a START, before the INFO
	// answer, as a SYN the mux handled before it queued the bid.
	synBeforeFence bool
	// infoErrorHost answers INFO with ERROR_HOST, as a mux without a cached
	// adapter answer does.
	infoErrorHost bool
	// excludedSnapshot is a START queued after the snapshot of a SYN.
	// The INFO answer is written before that SYN is delivered, then the
	// SYN and one following symbol. The bid is never handed on.
	excludedSnapshot  bool
	excludedFollowing byte
	excludedDelivered bool
	// staleFailedWithGrant sends the replacement grant and a FAILED naming
	// the replaced initiator in one write, so they arrive in one read.
	staleFailedWithGrant bool
	// busChatter keeps delivering bus symbols to a session whose non-owner
	// SEND it neither refuses nor forwards.
	busChatter bool

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
	replacedInitiator      byte
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
	staleWorkloadSent      bool
	staleAfterScheduled    bool
	unabsorbed             byte
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
	signalled      bool
	forwarded      bool
	prompt         bool
	answerOwed     int
	answerHost     bool
}

const (
	// fakeInFlightArm is when a queued bid is shown in flight by a received SYN.
	fakeInFlightArm = 5 * time.Millisecond
	// fakeForwardLag is how long after that SYN the bid is written to the
	// adapter; a contender that acts on the SYN alone acts before it.
	fakeForwardLag = 5 * time.Millisecond
	// fakeLateAfter is when that bid's STARTED or FAILED is delivered,
	// measured from the START. It stays inside the old silence window so a
	// settle timer would treat the result as arriving before the bid was
	// in flight, while the SYN has already shown the forwarding boundary.
	fakeLateAfter = 55 * time.Millisecond
	// fakeAbsorbBudget is when the barrier clears: the mux clears it once
	// the cancelled bid's result is absorbed, well before the absorb bound.
	fakeAbsorbBudget = 250 * time.Millisecond
	// fakeStaleAfterWorkload is when a replaced bid's STARTED arrives after
	// the identification telegrams. It is past a short post-workload listen
	// and inside the absorb bound.
	fakeStaleAfterWorkload = time.Second
	fakeMuxGrantDelay      = 10 * time.Millisecond
	fakeMuxIdleSyn         = 300 * time.Millisecond
)

// inFlight is true once the bid has been written to the adapter.
func (bid *fakeBid) inFlight() bool {
	return bid != nil && bid.forwarded
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
			if bid.inFlight() && !bid.replaced {
				peer.closedInFlight++
				if peer.skipAbsorb {
					peer.unabsorbed = bid.initiator
				} else {
					peer.absorbArmed++
					if peer.absorbNeverExpires {
						peer.absorbDeadline = now.Add(time.Hour)
					} else {
						peer.absorbDeadline = now.Add(fakeAbsorbBudget)
					}
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
	writeInfo := func(host bool) {
		if host {
			write(transport.ENHResErrorHost, 0x00)
			return
		}
		write(transport.ENHResInfo, 0x02)
		write(transport.ENHResInfo, 0x01)
		write(transport.ENHResInfo, 0x02)
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
	// forward writes the bid to the adapter after its SYN was delivered and
	// then delivers the next bus symbol. The bid stays outstanding so it can
	// still be replaced or closed.
	forward := func(gen int) {
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
		if bid == nil || bid.generation != gen || bid.conn != conn || bid.forwarded {
			peer.mu.Unlock()
			return
		}
		bid.forwarded = true
		owed := bid.answerOwed
		host := bid.answerHost
		bid.answerOwed = 0
		peer.mu.Unlock()
		// The selecting SYN is already delivered. The INFO answer follows
		// the return from RequestStart, then the next symbol.
		for i := 0; i < owed; i++ {
			writeInfo(host)
		}
		write(transport.ENHResReceived, protocol.SymbolSyn)
	}
	// signalInFlight delivers the SYN that selects the bid; the bid is
	// forwarded only after it, as the mux's grant step follows the delivery.
	signalInFlight := func(gen int) {
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
		if bid == nil || bid.generation != gen || bid.conn != conn || bid.signalled {
			peer.mu.Unlock()
			return
		}
		bid.signalled = true
		peer.mu.Unlock()
		write(transport.ENHResReceived, protocol.SymbolSyn)
		time.AfterFunc(fakeForwardLag, func() { forward(gen) })
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
		withStaleFailed := bid.replaced && peer.staleFailedWithGrant
		promptFailed := bid.prompt && peer.promptFailed
		if !bid.prompt {
			peer.grantedOnce[conn] = true
		}
		if !promptFailed {
			peer.busOwner = conn
			peer.startedSent++
		}
		peer.mu.Unlock()
		if promptFailed {
			write(transport.ENHResFailed, initiator)
			return
		}
		owner, state, source, wire, lastSend = true, initiatorSend, initiator, nil, time.Now()
		if withStaleFailed {
			started := transport.EncodeENH(transport.ENHResStarted, initiator)
			failed := transport.EncodeENH(transport.ENHResFailed, bid.staleInitiator)
			out <- append(started[:], failed[:]...)
			return
		}
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
			case transport.ENHReqInfo:
				if peer.excludedSnapshot {
					peer.mu.Lock()
					first := !peer.excludedDelivered
					peer.excludedDelivered = true
					symbol := peer.excludedFollowing
					host := peer.infoErrorHost
					peer.mu.Unlock()
					if first {
						if host {
							write(transport.ENHResErrorHost, 0x00)
						} else {
							write(transport.ENHResInfo, 0x02)
							write(transport.ENHResInfo, 0x01)
							write(transport.ENHResInfo, 0x02)
						}
						// The answer is already on the wire when the
						// excluded SYN, and the symbol after it, arrive.
						write(transport.ENHResReceived, protocol.SymbolSyn)
						write(transport.ENHResReceived, symbol)
					}
					continue
				}
				peer.mu.Lock()
				bid := peer.inFlight
				park := bid != nil && bid.conn == conn && !bid.forwarded && !bid.prompt
				if park {
					bid.answerOwed++
					bid.answerHost = peer.infoErrorHost
					peer.mu.Unlock()
					continue
				}
				host := peer.infoErrorHost
				peer.mu.Unlock()
				writeInfo(host)
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
					if bid.inFlight() && bid.initiator != initiator {
						bid.replaced = true
						bid.staleInitiator = bid.initiator
						bid.replacement = initiator
						peer.replacedInitiator = bid.initiator
						peer.replacedInFlight++
					}
					peer.mu.Unlock()
					continue
				}
				if stale := peer.unabsorbed; stale != 0 {
					peer.unabsorbed = 0
					peer.mu.Unlock()
					time.AfterFunc(fakeMuxGrantDelay, func() { grantNow(stale, false) })
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
					prompt := (peer.promptResult || peer.promptFailed) && peer.starts == 1
					excluded := peer.excludedSnapshot
					peer.inFlight = &fakeBid{initiator: initiator, conn: conn, generation: gen, queuedAt: now, prompt: prompt}
					if excluded {
						// The snapshot already closed. This START stays
						// queued and is never marked forwarded.
						peer.mu.Unlock()
						continue
					}
					forwardBid := !peer.neverForward || peer.starts != 1
					earlySyn := peer.synBeforeFence
					peer.mu.Unlock()
					if earlySyn {
						write(transport.ENHResReceived, protocol.SymbolSyn)
					}
					if forwardBid {
						if !prompt {
							time.AfterFunc(fakeInFlightArm, func() { signalInFlight(gen) })
						}
						time.AfterFunc(fakeLateAfter, func() { resolve(gen) })
					}
					continue
				}
				if peer.deliverStaleDuringWorkload && !peer.staleWorkloadSent && peer.replacedInitiator != 0 {
					peer.staleWorkloadSent = true
					replaced := peer.replacedInitiator
					real := initiator
					failed := peer.staleFailed
					winner := replaced
					if peer.staleWinner != 0 {
						winner = peer.staleWinner
					}
					peer.mu.Unlock()
					if failed {
						write(transport.ENHResFailed, winner)
						time.AfterFunc(fakeMuxGrantDelay, func() { grantNow(real, false) })
					} else {
						time.AfterFunc(fakeMuxGrantDelay, func() { grantNow(replaced, false) })
					}
					continue
				}
				peer.mu.Unlock()
				time.AfterFunc(fakeMuxGrantDelay, func() { grantNow(initiator, false) })
			case transport.ENHReqSend:
				peer.mu.Lock()
				owns := peer.busOwner == conn
				busy := peer.busOwner != nil && !owns
				forwardSend := peer.forwardDuringActive
				chatter := peer.busChatter
				if !owns {
					if busy && forwardSend {
						peer.mu.Unlock()
						write(transport.ENHResReceived, message.Data)
						continue
					}
					if busy && chatter {
						peer.mu.Unlock()
						go func() {
							ticker := time.NewTicker(20 * time.Millisecond)
							defer ticker.Stop()
							for {
								select {
								case <-done:
									return
								case <-ticker.C:
								}
								stateMu.Lock()
								if alive.Load() {
									write(transport.ENHResReceived, 0x10)
								}
								stateMu.Unlock()
							}
						}()
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
					scheduleStale := false
					var staleInitiator byte
					if peer.deliverStaleAfterWorkload && !peer.staleAfterScheduled &&
						len(peer.telegrams) == contenderTelegramCount && peer.replacedInitiator != 0 {
						peer.staleAfterScheduled = true
						scheduleStale = true
						staleInitiator = peer.replacedInitiator
					}
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
					if scheduleStale {
						time.AfterFunc(fakeStaleAfterWorkload, func() {
							if !alive.Load() {
								return
							}
							stateMu.Lock()
							defer stateMu.Unlock()
							if !alive.Load() {
								return
							}
							write(transport.ENHResStarted, staleInitiator)
						})
					}
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

	if fakeInFlightArm+fakeForwardLag >= fakeLateAfter || fakeLateAfter >= 60*time.Millisecond {
		t.Fatalf("selecting SYN at %s, forwarded %s later, late result at %s; the result must follow the forwarding and fall inside 60ms",
			fakeInFlightArm, fakeForwardLag, fakeLateAfter)
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
		name      string
		invariant string
		mutate    func(peer *fakeMuxPeer)
		want      string
		check     func(t *testing.T, peer fakeMuxCounts)
	}{
		{name: "stale result", invariant: "PX01", mutate: func(peer *fakeMuxPeer) { peer.deliverStaleResult = true }, want: "stale arbitration result"},
		{name: "queued", invariant: "PX01", mutate: func(peer *fakeMuxPeer) { peer.neverForward = true }, want: "stayed queued",
			check: func(t *testing.T, peer fakeMuxCounts) {
				if peer.starts != 1 {
					t.Fatalf("starts %d; a queued bid was replaced", peer.starts)
				}
			}},
		{name: "queued", invariant: "PX02", mutate: func(peer *fakeMuxPeer) { peer.neverForward = true }, want: "stayed queued",
			check: func(t *testing.T, peer fakeMuxCounts) {
				if peer.dials != 1 || peer.starts != 1 {
					t.Fatalf("dials %d starts %d; a queued bid was closed and the next session ran", peer.dials, peer.starts)
				}
			}},
		{name: "barrier not armed", invariant: "PX02", mutate: func(peer *fakeMuxPeer) { peer.skipAbsorb = true }, want: "stale arbitration result"},
		{name: "barrier never expires", invariant: "PX02", mutate: func(peer *fakeMuxPeer) { peer.absorbNeverExpires = true }, want: "await arbitration"},
		{name: "stale during workload", invariant: "PX01", mutate: func(peer *fakeMuxPeer) { peer.deliverStaleDuringWorkload = true }, want: "stale arbitration result"},
		{name: "stale failed during workload", invariant: "PX01", mutate: func(peer *fakeMuxPeer) {
			peer.deliverStaleDuringWorkload = true
			peer.staleFailed = true
		}, want: "stale arbitration result"},
		{name: "stale failed naming another winner during workload", invariant: "PX01", mutate: func(peer *fakeMuxPeer) {
			peer.deliverStaleDuringWorkload = true
			peer.staleFailed = true
			peer.staleWinner = 0x10
		}, want: "no outstanding START explains"},
		{name: "stale failed with the replacement grant", invariant: "PX01", mutate: func(peer *fakeMuxPeer) { peer.staleFailedWithGrant = true }, want: "stale arbitration result"},
		{name: "stale after workload", invariant: "PX01", mutate: func(peer *fakeMuxPeer) { peer.deliverStaleAfterWorkload = true }, want: "stale arbitration result"},
		{name: "no closing syn", invariant: "PX03", mutate: func(peer *fakeMuxPeer) { peer.noClosingSyn = true }, want: "await the closing SYN"},
		{name: "whole response", invariant: "PX04", want: "target delivered its response"},
		{name: "forwarded send", invariant: "PX12", mutate: func(peer *fakeMuxPeer) { peer.forwardDuringActive = true }, want: "await the refusal of a non-owner SEND"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.invariant+"/"+tc.name, func(t *testing.T) {
			t.Parallel()
			caseID := "ENH-" + tc.invariant
			fixture, err := newExecuteFixture(t, []string{caseID}, nil)
			if err != nil {
				t.Fatalf("fixture: %v", err)
			}
			if tc.mutate != nil {
				tc.mutate(fixture.enhPeer)
			}
			verdicts, err := fixture.runner.Run(context.Background())
			if err != nil {
				t.Fatalf("Run error = %v", err)
			}
			client := clientResultOf(verdicts[0])
			if verdicts[0].Outcome != caseOutcomeFail || client.Status != actualStatusFailed || !strings.Contains(client.Error, tc.want) {
				t.Fatalf("verdict %+v, client %+v; want a failed client naming %q", verdicts[0], client, tc.want)
			}
			if tc.check != nil {
				tc.check(t, fixture.enhPeer.snapshot())
			}
		})
	}
}

// The mux delivers the SYN that selects a bid before it writes the bid to
// the adapter, and it releases the INFO answer only after that write
// returns. PX01 replaces and PX02 abandons the bid only after that answer
// completes, with the answer carried as INFO data or as ERROR_HOST. A SYN
// the mux handled before it queued the bid is not enough on its own.
func TestStimulusActsOnlyAfterTheBidReachedTheAdapter(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		mutate func(peer *fakeMuxPeer)
	}{
		{"selecting SYN before the grant step", nil},
		{"SYN before the INFO answer", func(peer *fakeMuxPeer) { peer.synBeforeFence = true }},
		{"INFO answered with ERROR_HOST", func(peer *fakeMuxPeer) { peer.infoErrorHost = true }},
	}
	for _, invariant := range []string{"PX01", "PX02"} {
		for _, tc := range cases {
			invariant, tc := invariant, tc
			t.Run(invariant+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				caseID := "ENH-" + invariant
				fixture, err := newExecuteFixture(t, []string{caseID}, nil)
				if err != nil {
					t.Fatalf("fixture: %v", err)
				}
				if tc.mutate != nil {
					tc.mutate(fixture.enhPeer)
				}
				verdicts, err := fixture.runner.Run(context.Background())
				if err != nil {
					t.Fatalf("Run error = %v", err)
				}
				if verdicts[0].Outcome != caseOutcomePass {
					t.Fatalf("verdict %+v, client %+v", verdicts[0], clientResultOf(verdicts[0]))
				}
				peer := fixture.enhPeer.snapshot()
				if invariant == "PX01" && (peer.replacedInFlight != 1 || peer.staleDelivered != 0 || peer.replacementGranted != 1) {
					t.Fatalf("replaced in flight %d, stale delivered %d, replacement granted %d; the bid was replaced before it reached the adapter",
						peer.replacedInFlight, peer.staleDelivered, peer.replacementGranted)
				}
				if invariant == "PX02" && (peer.closedInFlight != 1 || peer.closedQueued != 0 || peer.absorbArmed != 1) {
					t.Fatalf("closed in flight %d, closed queued %d, absorb armed %d; the session closed before the bid reached the adapter",
						peer.closedInFlight, peer.closedQueued, peer.absorbArmed)
				}
			})
		}
	}
}

// A non-owner SEND that the mux neither refuses nor forwards fails PX12
// within one client step, while bus symbols keep arriving, and the gateway
// stop command still runs.
func TestNonOwnerSendWaitIsBoundedWhileBusTrafficArrives(t *testing.T) {
	t.Parallel()

	fixture, err := newExecuteFixture(t, []string{"ENH-PX12"}, nil)
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	fixture.enhPeer.busChatter = true
	started := time.Now()
	verdicts, err := fixture.runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}
	elapsed := time.Since(started)
	client := clientResultOf(verdicts[0])
	if verdicts[0].Outcome != caseOutcomeFail || client.Status != actualStatusFailed ||
		!strings.Contains(client.Error, "await the refusal of a non-owner SEND") ||
		!strings.Contains(client.Error, "while bus symbols kept arriving") {
		t.Fatalf("verdict %+v, client %+v; want a failed client naming the missing refusal", verdicts[0], client)
	}
	if limit := clientStepTimeout + 3*time.Second; elapsed > limit {
		t.Fatalf("the case took %s; the refusal wait must end within one client step (%s), not the case timeout", elapsed, clientStepTimeout)
	}
	stopped := false
	for _, command := range verdicts[0].Commands {
		if command.Name == "gateway-stop" && command.Status == actualStatusPassed {
			stopped = true
		}
	}
	if !stopped {
		t.Fatalf("commands %+v; the gateway stop command did not run", verdicts[0].Commands)
	}
}

// A mux that answers the first START before it is shown in flight does not
// end the stimulus: the contender bids again and replaces
// or abandons the bid that is in flight.
func TestPromptResultDoesNotEndTheStimulus(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		invariant string
		mutate    func(peer *fakeMuxPeer)
	}{
		{"started", "PX01", func(peer *fakeMuxPeer) { peer.promptResult = true }},
		{"failed", "PX01", func(peer *fakeMuxPeer) { peer.promptFailed = true }},
		{"started", "PX02", func(peer *fakeMuxPeer) { peer.promptResult = true }},
		{"failed", "PX02", func(peer *fakeMuxPeer) { peer.promptFailed = true }},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.invariant+"/"+tc.name, func(t *testing.T) {
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
			if verdicts[0].Outcome != caseOutcomePass {
				t.Fatalf("verdict %+v, client %+v", verdicts[0], clientResultOf(verdicts[0]))
			}
			peer := fixture.enhPeer.snapshot()
			if tc.invariant == "PX01" && (peer.replacedInFlight != 1 || peer.staleDelivered != 0) {
				t.Fatalf("replaced in flight %d, stale delivered %d; the prompt result ended the stimulus before the replacement",
					peer.replacedInFlight, peer.staleDelivered)
			}
			if tc.invariant == "PX02" && (peer.closedInFlight != 1 || peer.absorbArmed != 1 || peer.dials != 2) {
				t.Fatalf("closed in flight %d, absorb armed %d, sessions %d; the prompt result ended the stimulus before the close",
					peer.closedInFlight, peer.absorbArmed, peer.dials)
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
