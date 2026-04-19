package execution_policy

import (
	ebusstd "github.com/Project-Helianthus/helianthus-ebusreg/catalog/ebus_standard"
)

// nmWhitelistEntry is a partial identity tuple used to match catalog
// identity keys against the system_nm_runtime compile-time whitelist
// defined in architecture/ebus_standard/05-execution-safety.md §
// "system_nm_runtime Whitelist".
//
// Matching MUST be on the full 14-axis identity, not on PB/SB alone.
// Adjacent variants with the same PB/SB remain denied.
type nmWhitelistEntry struct {
	pb                    uint8
	sb                    uint8
	telegramClass         ebusstd.TelegramClass
	direction             ebusstd.Direction
	requestOrResponseRole ebusstd.RequestOrResponseRole
	broadcastOrAddressed  ebusstd.BroadcastOrAddressed
	answerPolicy          ebusstd.AnswerPolicy
	serviceVariant        string
	version               string
}

// nmWhitelist is the compile-time whitelist. Widening requires a new
// locked-plan decision, not a code change.
var nmWhitelist = []nmWhitelistEntry{
	{
		pb: 0xFF, sb: 0x00,
		telegramClass:         ebusstd.TelegramClassBroadcast,
		direction:             ebusstd.DirectionRequest,
		requestOrResponseRole: "initiator_broadcast_emit",
		broadcastOrAddressed:  ebusstd.AddressedBroadcast,
		answerPolicy:          "no-answer",
		serviceVariant:        "nm_reset_status_broadcast",
		version:               "v1.0-locked",
	},
	{
		pb: 0xFF, sb: 0x02,
		telegramClass:         ebusstd.TelegramClassBroadcast,
		direction:             ebusstd.DirectionRequest,
		requestOrResponseRole: "initiator_broadcast_emit",
		broadcastOrAddressed:  ebusstd.AddressedBroadcast,
		answerPolicy:          "no-answer",
		serviceVariant:        "nm_failure_broadcast",
		version:               "v1.0-locked",
	},
	{
		pb: 0xFF, sb: 0x03,
		telegramClass:         "initiator-target",
		direction:             ebusstd.DirectionResponse,
		requestOrResponseRole: "responder_reply",
		broadcastOrAddressed:  ebusstd.AddressedDirect,
		answerPolicy:          "answer-required",
		serviceVariant:        "nm_net_status_response",
		version:               "v1.0-locked",
	},
	{
		pb: 0xFF, sb: 0x04,
		telegramClass:         "initiator-target",
		direction:             ebusstd.DirectionResponse,
		requestOrResponseRole: "responder_reply",
		broadcastOrAddressed:  ebusstd.AddressedDirect,
		answerPolicy:          "answer-required",
		serviceVariant:        "nm_monitored_participants_response",
		version:               "v1.0-locked",
	},
	{
		pb: 0xFF, sb: 0x05,
		telegramClass:         "initiator-target",
		direction:             ebusstd.DirectionResponse,
		requestOrResponseRole: "responder_reply",
		broadcastOrAddressed:  ebusstd.AddressedDirect,
		answerPolicy:          "answer-required",
		serviceVariant:        "nm_failed_nodes_response",
		version:               "v1.0-locked",
	},
	{
		pb: 0xFF, sb: 0x06,
		telegramClass:         "initiator-target",
		direction:             ebusstd.DirectionResponse,
		requestOrResponseRole: "responder_reply",
		broadcastOrAddressed:  ebusstd.AddressedDirect,
		answerPolicy:          "answer-required",
		serviceVariant:        "nm_required_services_response",
		version:               "v1.0-locked",
	},
	{
		pb: 0x07, sb: 0xFF,
		telegramClass:         ebusstd.TelegramClassBroadcast,
		direction:             ebusstd.DirectionRequest,
		requestOrResponseRole: "initiator_broadcast_emit",
		broadcastOrAddressed:  ebusstd.AddressedBroadcast,
		answerPolicy:          "no-answer",
		serviceVariant:        "sign_of_life_broadcast",
		version:               "v1.0-locked",
	},
}

func nmWhitelistContains(id ebusstd.IdentityKey) bool {
	if id.PB == nil || id.SB == nil {
		return false
	}
	for _, entry := range nmWhitelist {
		if entry.pb != *id.PB || entry.sb != *id.SB {
			continue
		}
		if entry.telegramClass != id.TelegramClass {
			continue
		}
		if entry.direction != id.Direction {
			continue
		}
		if entry.requestOrResponseRole != id.RequestOrResponseRole {
			continue
		}
		if entry.broadcastOrAddressed != id.BroadcastOrAddressed {
			continue
		}
		if entry.answerPolicy != id.AnswerPolicy {
			continue
		}
		if entry.serviceVariant != id.ServiceVariant {
			continue
		}
		if entry.version != id.Version {
			continue
		}
		return true
	}
	return false
}

// NMWhitelistSize exposes the whitelist cardinality for tests.
func NMWhitelistSize() int { return len(nmWhitelist) }
