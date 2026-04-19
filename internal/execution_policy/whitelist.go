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
// CATALOG-DRIVEN INVARIANT (issue #505 r3106832678): every axis literal MUST
// be the EXACT enum value emitted by the embedded ebusreg catalog YAML. No
// approximations, no placeholders. Drift between catalog and whitelist is
// caught by the catalog-grounded integration regression test in
// whitelist_catalog_integration_test.go.
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
//
// The literal values are sourced from ebusreg@30aa69a
// catalog/ebus_standard/catalog.yaml — service 0xFF (Network Management) and
// the 0x07 0xFF Sign of Life broadcast under service 0x07 (System Data).
var nmWhitelist = []nmWhitelistEntry{
	{
		// FF 00 — Reset Status NM (catalog: ebus_standard.nm.reset_status)
		namespace:                       "ebus_standard",
		pb:                              0xFF,
		sb:                              0x00,
		selectorPath:                    "",
		telegramClass:                   ebusstd.TelegramClassBroadcast,
		direction:                       ebusstd.DirectionRequest,
		requestOrResponseRole:           ebusstd.RoleOriginator,
		broadcastOrAddressed:            ebusstd.AddressedBroadcast,
		answerPolicy:                    ebusstd.AnswerNone,
		lengthPrefixMode:                ebusstd.LengthPrefixFixed,
		selectorDecoder:                 "none",
		serviceVariant:                  "reset_status",
		transportCapabilityRequirements: []string{"broadcast_send"},
		version:                         "v1.0-locked",
	},
	{
		// FF 02 — Failure Message (catalog: ebus_standard.nm.failure_message)
		namespace:                       "ebus_standard",
		pb:                              0xFF,
		sb:                              0x02,
		selectorPath:                    "",
		telegramClass:                   ebusstd.TelegramClassBroadcast,
		direction:                       ebusstd.DirectionRequest,
		requestOrResponseRole:           ebusstd.RoleOriginator,
		broadcastOrAddressed:            ebusstd.AddressedBroadcast,
		answerPolicy:                    ebusstd.AnswerNone,
		lengthPrefixMode:                ebusstd.LengthPrefixFixed,
		selectorDecoder:                 "none",
		serviceVariant:                  "failure_message",
		transportCapabilityRequirements: []string{"broadcast_send"},
		version:                         "v1.0-locked",
	},
	{
		// FF 03 — Net Status Query Response (catalog: ebus_standard.nm.net_status_query_response)
		namespace:                       "ebus_standard",
		pb:                              0xFF,
		sb:                              0x03,
		selectorPath:                    "",
		telegramClass:                   ebusstd.TelegramClassAddressed,
		direction:                       ebusstd.DirectionResponse,
		requestOrResponseRole:           ebusstd.RoleResponder,
		broadcastOrAddressed:            ebusstd.AddressedDirect,
		answerPolicy:                    ebusstd.AnswerRequired,
		lengthPrefixMode:                ebusstd.LengthPrefixFixed,
		selectorDecoder:                 "none",
		serviceVariant:                  "net_status_query",
		transportCapabilityRequirements: []string{"responder"},
		version:                         "v1.0-locked",
	},
	{
		// FF 04 — Monitored Participants Query Response
		// (catalog: ebus_standard.nm.monitored_participants_query_response)
		namespace:                       "ebus_standard",
		pb:                              0xFF,
		sb:                              0x04,
		selectorPath:                    "",
		telegramClass:                   ebusstd.TelegramClassAddressed,
		direction:                       ebusstd.DirectionResponse,
		requestOrResponseRole:           ebusstd.RoleResponder,
		broadcastOrAddressed:            ebusstd.AddressedDirect,
		answerPolicy:                    ebusstd.AnswerRequired,
		lengthPrefixMode:                ebusstd.LengthPrefixByte,
		selectorDecoder:                 "none",
		serviceVariant:                  "monitored_participants_query",
		transportCapabilityRequirements: []string{"responder"},
		version:                         "v1.0-locked",
	},
	{
		// FF 05 — Failed Nodes Query Response (catalog: ebus_standard.nm.failed_nodes_query_response)
		namespace:                       "ebus_standard",
		pb:                              0xFF,
		sb:                              0x05,
		selectorPath:                    "",
		telegramClass:                   ebusstd.TelegramClassAddressed,
		direction:                       ebusstd.DirectionResponse,
		requestOrResponseRole:           ebusstd.RoleResponder,
		broadcastOrAddressed:            ebusstd.AddressedDirect,
		answerPolicy:                    ebusstd.AnswerRequired,
		lengthPrefixMode:                ebusstd.LengthPrefixByte,
		selectorDecoder:                 "none",
		serviceVariant:                  "failed_nodes_query",
		transportCapabilityRequirements: []string{"responder"},
		version:                         "v1.0-locked",
	},
	{
		// FF 06 — Required Services Query Response
		// (catalog: ebus_standard.nm.required_services_query_response)
		namespace:                       "ebus_standard",
		pb:                              0xFF,
		sb:                              0x06,
		selectorPath:                    "",
		telegramClass:                   ebusstd.TelegramClassAddressed,
		direction:                       ebusstd.DirectionResponse,
		requestOrResponseRole:           ebusstd.RoleResponder,
		broadcastOrAddressed:            ebusstd.AddressedDirect,
		answerPolicy:                    ebusstd.AnswerRequired,
		lengthPrefixMode:                ebusstd.LengthPrefixByte,
		selectorDecoder:                 "none",
		serviceVariant:                  "required_services_query",
		transportCapabilityRequirements: []string{"responder"},
		version:                         "v1.0-locked",
	},
	{
		// 07 FF — Sign of Life broadcast (catalog: ebus_standard.system_data.sign_of_life)
		namespace:                       "ebus_standard",
		pb:                              0x07,
		sb:                              0xFF,
		selectorPath:                    "",
		telegramClass:                   ebusstd.TelegramClassBroadcast,
		direction:                       ebusstd.DirectionRequest,
		requestOrResponseRole:           ebusstd.RoleOriginator,
		broadcastOrAddressed:            ebusstd.AddressedBroadcast,
		answerPolicy:                    ebusstd.AnswerNone,
		lengthPrefixMode:                ebusstd.LengthPrefixNone,
		selectorDecoder:                 "none",
		serviceVariant:                  "sign_of_life",
		transportCapabilityRequirements: []string{"broadcast_send"},
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
