package adaptermux

// arbitrationBid is a copied value. The arbitrator resolves request and channel
// identity into eligibility while holding its mutex; decisions never touch them.
type arbitrationBid struct {
	sessionID uint64
	initiator byte
	eligible  bool
}

// arbitrationSnapshot lists the gateway first, when present, followed by the
// external pending list. Indices are local to this single locked selection.
type arbitrationSnapshot struct {
	hasOwner bool
	busIdle  bool
	bids     []arbitrationBid
}

type arbitrationDecision struct {
	winner   int // -1 when selection cannot proceed
	losers   []int
	retained []int
}

// decideArbitration reads only copied state and returns ordered bid indices.
// The caller owns all mutation, result delivery and ownership commits.
func decideArbitration(snapshot arbitrationSnapshot) arbitrationDecision {
	decision := arbitrationDecision{winner: -1}
	if !snapshot.hasOwner && snapshot.busIdle {
		for i, bid := range snapshot.bids {
			if bid.eligible && (decision.winner < 0 || higherPriority(bid, snapshot.bids[decision.winner])) {
				decision.winner = i
			}
		}
	}
	for i, bid := range snapshot.bids {
		if decision.winner < 0 || !bid.eligible {
			decision.retained = append(decision.retained, i)
		} else if i != decision.winner {
			decision.losers = append(decision.losers, i)
		}
	}
	return decision
}

func higherPriority(a, b arbitrationBid) bool {
	if a.initiator&0x0f != b.initiator&0x0f {
		return a.initiator&0x0f < b.initiator&0x0f
	}
	if a.initiator != b.initiator {
		return a.initiator>>4 < b.initiator>>4
	}
	return a.sessionID < b.sessionID
}
