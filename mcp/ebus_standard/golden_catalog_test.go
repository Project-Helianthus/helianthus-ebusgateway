package ebus_standard_test

import (
	ebusstd "github.com/Project-Helianthus/helianthus-ebusreg/catalog/ebus_standard"
)

// syntheticGoldenCatalog builds a tiny deterministic catalog used by the
// envelope-golden tests. Pinning to a synthetic catalog (rather than the
// full embedded catalog) keeps fixtures small and decouples the golden
// envelope contract from legitimate catalog content churn.
func syntheticGoldenCatalog() ebusstd.Catalog {
	u := func(v uint8) *uint8 { return &v }
	pb03 := uint8(0x03)
	return ebusstd.Catalog{
		Namespace:  "ebus_standard",
		Version:    "v1.0-locked",
		PlanSHA256: "9e0a29bb76d99f551904b05749e322aafd3972621858aa6d1acbe49b9ef37305",
		Services: []ebusstd.Service{{
			PB:          &pb03,
			Name:        "Service Data",
			Description: "golden fixture",
			Commands: []ebusstd.Command{{
				ID:          "ebus_standard.golden.alpha",
				Name:        "Alpha",
				Description: "alpha command for golden tests",
				SafetyClass: ebusstd.SafetyReadOnlyBusLoad,
				Identity: ebusstd.IdentityKey{
					Namespace:                       "ebus_standard",
					PB:                              u(0x03),
					SB:                              u(0x04),
					SelectorPath:                    "",
					TelegramClass:                   ebusstd.TelegramClassAddressed,
					Direction:                       ebusstd.DirectionRequest,
					RequestOrResponseRole:           "initiator",
					BroadcastOrAddressed:            ebusstd.AddressedDirect,
					AnswerPolicy:                    "answer_required",
					LengthPrefixMode:                ebusstd.LengthPrefixNone,
					SelectorDecoder:                 "none",
					ServiceVariant:                  "start_counts",
					TransportCapabilityRequirements: []string{"master_slave"},
					Version:                         "v1.0-locked",
				},
			}},
		}},
	}
}
