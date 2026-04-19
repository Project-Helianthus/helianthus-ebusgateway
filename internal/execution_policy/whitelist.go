package execution_policy

import (
	"reflect"

	ebusstd "github.com/Project-Helianthus/helianthus-ebusreg/catalog/ebus_standard"
)

// nmWhitelistEntry carries the FULL 14-axis identity tuple used to match
// catalog identity keys against the system_nm_runtime compile-time whitelist
// defined in architecture/ebus_standard/05-execution-safety.md §
// "system_nm_runtime Whitelist".
//
// AD09 INVARIANT: matching MUST be on the full 14-axis identity. Partial-axis
// matches (e.g. PB/SB + service_variant alone) are forbidden — they can
// widen the accept-set to future catalog commands that happen to share a
// subset of axes. Every axis present in ebusstd.IdentityKey is compared
// exactly, including the nullable SelectorPath and the ordered slice
// TransportCapabilityRequirements.
//
// Widening the whitelist requires a new locked-plan decision, not a code
// change.
type nmWhitelistEntry struct {
	namespace                       string
	pb                              uint8
	sb                              uint8
	selectorPath                    string
	telegramClass                   ebusstd.TelegramClass
	direction                       ebusstd.Direction
	requestOrResponseRole           ebusstd.RequestOrResponseRole
	broadcastOrAddressed            ebusstd.BroadcastOrAddressed
	answerPolicy                    ebusstd.AnswerPolicy
	lengthPrefixMode                ebusstd.LengthPrefixMode
	selectorDecoder                 string
	serviceVariant                  string
	transportCapabilityRequirements []string
	version                         string
}

// nmWhitelist is the compile-time whitelist. All 14 axes MUST be populated
// on every entry — the init() block at the bottom of this file enforces
// completeness at process start.
var nmWhitelist = []nmWhitelistEntry{
	{
		namespace:                       "ebus_standard",
		pb:                              0xFF,
		sb:                              0x00,
		selectorPath:                    "",
		telegramClass:                   ebusstd.TelegramClassBroadcast,
		direction:                       ebusstd.DirectionRequest,
		requestOrResponseRole:           "initiator_broadcast_emit",
		broadcastOrAddressed:            ebusstd.AddressedBroadcast,
		answerPolicy:                    "no-answer",
		lengthPrefixMode:                ebusstd.LengthPrefixNone,
		selectorDecoder:                 "none",
		serviceVariant:                  "nm_reset_status_broadcast",
		transportCapabilityRequirements: []string{"initiator+broadcast"},
		version:                         "v1.0-locked",
	},
	{
		namespace:                       "ebus_standard",
		pb:                              0xFF,
		sb:                              0x02,
		selectorPath:                    "",
		telegramClass:                   ebusstd.TelegramClassBroadcast,
		direction:                       ebusstd.DirectionRequest,
		requestOrResponseRole:           "initiator_broadcast_emit",
		broadcastOrAddressed:            ebusstd.AddressedBroadcast,
		answerPolicy:                    "no-answer",
		lengthPrefixMode:                ebusstd.LengthPrefixNone,
		selectorDecoder:                 "none",
		serviceVariant:                  "nm_failure_broadcast",
		transportCapabilityRequirements: []string{"initiator+broadcast"},
		version:                         "v1.0-locked",
	},
	{
		namespace:                       "ebus_standard",
		pb:                              0xFF,
		sb:                              0x03,
		selectorPath:                    "",
		telegramClass:                   "initiator-target",
		direction:                       ebusstd.DirectionResponse,
		requestOrResponseRole:           "responder_reply",
		broadcastOrAddressed:            ebusstd.AddressedDirect,
		answerPolicy:                    "answer-required",
		lengthPrefixMode:                ebusstd.LengthPrefixNone,
		selectorDecoder:                 "none",
		serviceVariant:                  "nm_net_status_response",
		transportCapabilityRequirements: []string{"responder+addressed"},
		version:                         "v1.0-locked",
	},
	{
		namespace:                       "ebus_standard",
		pb:                              0xFF,
		sb:                              0x04,
		selectorPath:                    "",
		telegramClass:                   "initiator-target",
		direction:                       ebusstd.DirectionResponse,
		requestOrResponseRole:           "responder_reply",
		broadcastOrAddressed:            ebusstd.AddressedDirect,
		answerPolicy:                    "answer-required",
		lengthPrefixMode:                ebusstd.LengthPrefixNone,
		selectorDecoder:                 "none",
		serviceVariant:                  "nm_monitored_participants_response",
		transportCapabilityRequirements: []string{"responder+addressed"},
		version:                         "v1.0-locked",
	},
	{
		namespace:                       "ebus_standard",
		pb:                              0xFF,
		sb:                              0x05,
		selectorPath:                    "",
		telegramClass:                   "initiator-target",
		direction:                       ebusstd.DirectionResponse,
		requestOrResponseRole:           "responder_reply",
		broadcastOrAddressed:            ebusstd.AddressedDirect,
		answerPolicy:                    "answer-required",
		lengthPrefixMode:                ebusstd.LengthPrefixNone,
		selectorDecoder:                 "none",
		serviceVariant:                  "nm_failed_nodes_response",
		transportCapabilityRequirements: []string{"responder+addressed"},
		version:                         "v1.0-locked",
	},
	{
		namespace:                       "ebus_standard",
		pb:                              0xFF,
		sb:                              0x06,
		selectorPath:                    "",
		telegramClass:                   "initiator-target",
		direction:                       ebusstd.DirectionResponse,
		requestOrResponseRole:           "responder_reply",
		broadcastOrAddressed:            ebusstd.AddressedDirect,
		answerPolicy:                    "answer-required",
		lengthPrefixMode:                ebusstd.LengthPrefixNone,
		selectorDecoder:                 "none",
		serviceVariant:                  "nm_required_services_response",
		transportCapabilityRequirements: []string{"responder+addressed"},
		version:                         "v1.0-locked",
	},
	{
		namespace:                       "ebus_standard",
		pb:                              0x07,
		sb:                              0xFF,
		selectorPath:                    "",
		telegramClass:                   ebusstd.TelegramClassBroadcast,
		direction:                       ebusstd.DirectionRequest,
		requestOrResponseRole:           "initiator_broadcast_emit",
		broadcastOrAddressed:            ebusstd.AddressedBroadcast,
		answerPolicy:                    "no-answer",
		lengthPrefixMode:                ebusstd.LengthPrefixNone,
		selectorDecoder:                 "none",
		serviceVariant:                  "sign_of_life_broadcast",
		transportCapabilityRequirements: []string{"initiator+broadcast"},
		version:                         "v1.0-locked",
	},
}

// init enforces that every whitelist entry has every axis populated. A
// missing axis would allow the full-identity check below to pass on an
// adjacent variant that shares the populated subset. Panicking at startup
// is the locked-plan response: the invariant is compile-time, not runtime.
func init() {
	for i, e := range nmWhitelist {
		if e.namespace == "" ||
			e.telegramClass == "" ||
			e.direction == "" ||
			e.requestOrResponseRole == "" ||
			e.broadcastOrAddressed == "" ||
			e.answerPolicy == "" ||
			e.lengthPrefixMode == "" ||
			e.selectorDecoder == "" ||
			e.serviceVariant == "" ||
			e.version == "" ||
			e.transportCapabilityRequirements == nil {
			panic("execution_policy: nmWhitelist entry has incomplete identity axes (index " +
				itoa(i) + ") — AD09 invariant violation")
		}
	}
}

// itoa is a tiny local integer formatter to avoid an fmt import in init.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// nmWhitelistContains reports whether id matches ANY whitelist entry on ALL
// 14 identity axes. Any axis mismatch → deny.
func nmWhitelistContains(id ebusstd.IdentityKey) bool {
	if id.PB == nil || id.SB == nil {
		return false
	}
	for _, entry := range nmWhitelist {
		if entry.namespace != id.Namespace {
			continue
		}
		if entry.pb != *id.PB || entry.sb != *id.SB {
			continue
		}
		if entry.selectorPath != id.SelectorPath {
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
		if entry.lengthPrefixMode != id.LengthPrefixMode {
			continue
		}
		if entry.selectorDecoder != id.SelectorDecoder {
			continue
		}
		if entry.serviceVariant != id.ServiceVariant {
			continue
		}
		if !reflect.DeepEqual(entry.transportCapabilityRequirements,
			id.TransportCapabilityRequirements) {
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
