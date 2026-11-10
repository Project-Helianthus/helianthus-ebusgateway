package adaptermux

import (
	"reflect"
	"testing"
)

// The reference selector preserves the pre-extraction implementation.
func (a *arbitrator) selectOriginalBids(busIdle bool, bids map[*startRequest]chan startResult) (*startRequest, []*startRequest) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.hasOwner || !busIdle {
		return nil, nil
	}
	eligible := func(req *startRequest) bool {
		return req != nil && (bids == nil || bids[req] == req.notify)
	}
	var winner *startRequest
	if eligible(a.pendingGateway) {
		winner = a.pendingGateway
	}
	for _, req := range a.pendingExternal {
		if eligible(req) && (winner == nil || originalHigherPriority(req, winner)) {
			winner = req
		}
	}
	if winner == nil {
		return nil, nil
	}
	var losers []*startRequest
	lose := func(req *startRequest) {
		if eligible(req) && req != winner {
			losers = append(losers, req)
			if req.sessionID == gatewaySessionID {
				a.deferredGatewayLoser = req
			}
		}
	}
	lose(a.pendingGateway)
	for _, req := range a.pendingExternal {
		lose(req)
	}
	if eligible(a.pendingGateway) {
		a.pendingGateway = nil
	}
	remaining := a.pendingExternal[:0]
	for _, req := range a.pendingExternal {
		if !eligible(req) {
			remaining = append(remaining, req)
		}
	}
	clear(a.pendingExternal[len(remaining):])
	a.pendingExternal = remaining
	return winner, losers
}

func originalHigherPriority(a, b *startRequest) bool {
	if a.initiator&0x0f != b.initiator&0x0f {
		return a.initiator&0x0f < b.initiator&0x0f
	}
	if a.initiator != b.initiator {
		return a.initiator>>4 < b.initiator>>4
	}
	return a.sessionID < b.sessionID
}

func TestArbitrationDecisionExhaustive(t *testing.T) {
	addresses := []byte{0x10, 0x01, 0x11}
	cases := 0
	for count := 0; count <= 3; count++ {
		combinations := 1
		for i := 0; i < count; i++ {
			combinations *= len(addresses) * 2
		}
		for code := 0; code < combinations; code++ {
			refs := make([]*startRequest, count)
			n := code
			for i := range refs {
				refs[i] = &startRequest{sessionID: uint64(1 + n%2), initiator: addresses[(n/2)%3], notify: make(chan startResult, 1)}
				n /= 6
			}
			for gateway := 0; gateway < 2; gateway++ {
				var gw *startRequest
				if gateway == 1 {
					gw = &startRequest{sessionID: 0, initiator: 0x11, notify: make(chan startResult, 1)}
				}
				for mask := -1; mask < (1 << (count + gateway)); mask++ {
					var bids map[*startRequest]chan startResult
					if mask >= 0 {
						bids = make(map[*startRequest]chan startResult)
						for i, r := range refs {
							if mask&(1<<i) != 0 {
								bids[r] = r.notify
							}
						}
						if gw != nil && mask&(1<<count) != 0 {
							bids[gw] = gw.notify
						}
					}
					for state := 0; state < 4; state++ {
						a := &arbitrator{pendingGateway: gw, pendingExternal: append([]*startRequest(nil), refs...), hasOwner: state&1 != 0}
						b := &arbitrator{pendingGateway: gw, pendingExternal: append([]*startRequest(nil), refs...), hasOwner: state&1 != 0}
						want, wl := b.selectOriginalBids(state&2 == 0, bids)
						got, gl := a.selectBidsAtSYN(state&2 == 0, bids)
						if got != want || !reflect.DeepEqual(gl, wl) || a.pendingGateway != b.pendingGateway || !reflect.DeepEqual(a.pendingExternal, b.pendingExternal) || a.deferredGatewayLoser != b.deferredGatewayLoser {
							t.Fatalf("count=%d code=%d gateway=%d mask=%d state=%d: selection or ordered state differs", count, code, gateway, mask, state)
						}
						cases++
					}
				}
			}
		}
	}
	if cases == 0 {
		t.Fatal("empty enumeration")
	}
	t.Logf("compared %d ordered selection states", cases)
}

func TestArbitrationDecisionBoundaryIdentity(t *testing.T) {
	for _, mode := range []string{"channel", "request", "cancel", "late"} {
		t.Run(mode, func(t *testing.T) {
			a := newArbitrator()
			old := a.requestStart(3, 0x11)
			req := a.pendingExternal[0]
			bids := a.snapshotBids()
			var current <-chan startResult
			switch mode {
			case "channel":
				current = a.requestStart(3, 0x11)
			case "request":
				current = a.requestStart(3, 0x01)
			case "cancel":
				a.cancelStart(3)
			case "late":
				current = a.requestStart(4, 0x01)
			}
			if mode != "late" {
				result := <-old
				if !result.cancelled || result.initiator != 0x11 {
					t.Fatal("old waiter must receive its cancellation")
				}
				select {
				case <-old:
					t.Fatal("duplicate cancellation")
				default:
				}
			}
			winner, losers := a.selectBidsAtSYN(true, bids)
			if mode == "late" {
				if winner != req || len(losers) != 0 || len(a.pendingExternal) != 1 || a.pendingExternal[0].sessionID != 4 {
					t.Fatal("late bid crossed SYN boundary")
				}
			} else if winner != nil || len(losers) != 0 {
				t.Fatal("stale or cancelled identity was admitted")
			}
			if current != nil {
				select {
				case <-current:
					t.Fatal("retained waiter was resolved")
				default:
				}
				winner, losers = a.selectAtSYN(true)
				if winner == nil || len(losers) != 0 || winner.notify != current {
					t.Fatal("retained bid not selected at next SYN")
				}
			}
		})
	}
}

func TestArbitrationDecisionPriorityAndDeferred(t *testing.T) {
	a := newArbitrator()
	gw := a.requestStart(0, 0x11)
	a.requestStart(9, 0x10)
	a.requestStart(2, 0x10)
	winner, losers := a.selectAtSYN(true)
	if winner == nil || winner.sessionID != 2 || len(losers) != 2 || losers[0].sessionID != 0 || losers[1].sessionID != 9 {
		t.Fatal("priority or loser ordering changed")
	}
	if a.hasOwner || a.deferredGatewayLoser != losers[0] {
		t.Fatal("selection committed ownership or lost deferred gateway")
	}
	select {
	case <-gw:
		t.Fatal("gateway resolved before adapter outcome")
	default:
	}
	if !a.takeDeferredGatewayLoser(losers[0]) || a.takeDeferredGatewayLoser(losers[0]) {
		t.Fatal("deferred result must be taken once")
	}
	losers[0].notify <- startResult{initiator: winner.initiator}
	if result := <-gw; result.granted || result.initiator != 0x10 {
		t.Fatal("wrong deferred result")
	}
	select {
	case <-gw:
		t.Fatal("duplicate result")
	default:
	}
	for _, tc := range []struct {
		left, right byte
		want        uint64
	}{{0x10, 0x01, 5}, {0x21, 0x11, 6}, {0x11, 0x11, 5}} {
		a := newArbitrator()
		a.requestStart(5, tc.left)
		a.requestStart(6, tc.right)
		w, _ := a.selectAtSYN(true)
		if w.sessionID != tc.want {
			t.Fatal("nibble/session priority changed")
		}
	}
}

func TestArbitrationDecisionCopiedState(t *testing.T) {
	snapshot := arbitrationSnapshot{busIdle: true, bids: []arbitrationBid{
		{sessionID: 0, initiator: 0x11, eligible: true},
		{sessionID: 8, initiator: 0x10, eligible: false},
		{sessionID: 4, initiator: 0x10, eligible: true},
		{sessionID: 2, initiator: 0x10, eligible: true},
	}}
	original := snapshot
	original.bids = append([]arbitrationBid(nil), snapshot.bids...)
	want := arbitrationDecision{winner: 3, losers: []int{0, 2}, retained: []int{1}}
	for i := 0; i < 2; i++ {
		if got := decideArbitration(snapshot); !reflect.DeepEqual(got, want) {
			t.Fatalf("decision: got %+v want %+v", got, want)
		}
		if !reflect.DeepEqual(snapshot, original) {
			t.Fatal("decision mutated snapshot")
		}
	}
	for _, tc := range []struct{ owner, idle bool }{{true, true}, {false, false}, {true, false}} {
		snapshot.hasOwner = tc.owner
		snapshot.busIdle = tc.idle
		got := decideArbitration(snapshot)
		if got.winner != -1 || len(got.losers) != 0 || !reflect.DeepEqual(got.retained, []int{0, 1, 2, 3}) {
			t.Fatal("blocked state must retain every bid in order")
		}
	}
}

func TestArbitrationDecisionRequestIdentity(t *testing.T) {
	a := newArbitrator()
	a.requestStart(2, 0x11)
	original := a.pendingExternal[0]
	bids := a.snapshotBids()
	// Reuse the channel and all value fields, varying only the request identity.
	replacement := &startRequest{sessionID: original.sessionID, initiator: original.initiator, notify: original.notify}
	a.pendingExternal[0] = replacement
	if w, l := a.selectBidsAtSYN(true, bids); w != nil || len(l) != 0 {
		t.Fatal("request pointer omitted from eligibility key")
	}
	// Vary only session identity in a priority tie.
	snapshot := arbitrationSnapshot{busIdle: true, bids: []arbitrationBid{
		{sessionID: 8, initiator: 0x11, eligible: true}, {sessionID: 2, initiator: 0x11, eligible: true},
	}}
	if got := decideArbitration(snapshot); got.winner != 1 {
		t.Fatal("session tie order ignored")
	}
	snapshot.bids[0].sessionID = 1
	if got := decideArbitration(snapshot); got.winner != 0 {
		t.Fatal("session identity change ignored")
	}
}
