package main

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/promotioncapture"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
)

var (
	errEEBusInboundNotReady       = errors.New("eeBUS inbound snapshot is not ready")
	errEEBusInboundBindingChanged = errors.New("eeBUS remote binding changed during the inbound read")
)

// eebusInboundZoneTemplatePrefix names the semantic paths of the zone-1
// candidate definitions. They are the template for every heating zone.
const eebusInboundZoneTemplatePrefix = "/zones/zone_1/"

// eebusInboundValue is one decoded inbound value together with the native read
// that produced it, so a later consumer can publish from the same read.
type eebusInboundValue struct {
	CandidateID  string
	SemanticPath string
	// Zone is the 1-based heating zone; it is 0 for hot-water and system values.
	Zone        int
	Raw         promotioncapture.TypedValue
	Value       promotioncapture.TypedValue
	Unit        *string
	Observation eebusraw.ReadObservationV1
}

// eebusInboundRead is the result of one inbound cycle. Every value and every
// admission read of the cycle share Binding. Unread lists the semantic paths
// whose source profile was admitted but whose value read or decode failed.
type eebusInboundRead struct {
	Binding eebusraw.RuntimeBindingV1
	Values  []eebusInboundValue
	Unread  []string
}

type eebusInboundTarget struct {
	candidate promotioncapture.CandidateDefinition
	slots     leafPromotionSlots
	path      string
	zone      int
}

// readEEBusInboundValues reads every eeBUS-sourced registry value from the
// entities of one ready snapshot. A candidate whose entity, feature or source
// profile is not admitted yields no value. A read whose remote binding differs
// from the rest of the cycle fails the whole cycle.
func readEEBusInboundValues(
	ctx context.Context,
	runtime leafPromotionEEBusRuntime,
	snapshot eebusruntime.SnapshotV1,
) (eebusInboundRead, error) {
	if runtime == nil || snapshot.Validate() != nil || snapshot.Meta.MaskTier != eebusraw.MaskTierRaw ||
		snapshot.Status.State != eebusruntime.ObservedRuntimeStateV1Ready || snapshot.Meta.LocalSKI == "" {
		return eebusInboundRead{}, errEEBusInboundNotReady
	}
	registry, err := promotioncapture.DefaultRegistry()
	if err != nil {
		return eebusInboundRead{}, err
	}
	targets := eebusInboundTargets(registry, eebusInboundResolveEntities(snapshot))
	source := &leafPromotionLiveSource{eebus: runtime}
	featureCache := make(map[string]eebusraw.FeaturesGetDataV1)
	functionCache := make(map[string]eebusraw.ReadObservationV1)
	result := eebusInboundRead{}
	bind := func(binding eebusraw.RuntimeBindingV1) error {
		if result.Binding == (eebusraw.RuntimeBindingV1{}) {
			result.Binding = binding
			return nil
		}
		if binding != result.Binding {
			return errEEBusInboundBindingChanged
		}
		return nil
	}
	for _, target := range targets {
		if err := ctx.Err(); err != nil {
			return eebusInboundRead{}, err
		}
		profile := *target.candidate.EEBusSource
		locator, err := leafPromotionLocatorForSource(snapshot, target.slots, profile)
		if err != nil || len(profile.ValueFunctions) == 0 {
			continue
		}
		inventory, err := source.readFeatureInventory(ctx, locator, featureCache)
		if err != nil {
			continue
		}
		if err := bind(inventory.Runtime); err != nil {
			return eebusInboundRead{}, err
		}
		if err := source.verifySourceProfile(ctx, target.candidate, locator, inventory, functionCache); err != nil {
			if err := eebusInboundCacheBinding(functionCache, bind); err != nil {
				return eebusInboundRead{}, err
			}
			continue
		}
		observation, err := source.readEEBusFunction(ctx, locator, profile.ValueFunctions[0], functionCache)
		if err != nil {
			result.Unread = append(result.Unread, target.path)
			continue
		}
		if err := bind(observation.Runtime); err != nil {
			return eebusInboundRead{}, err
		}
		raw, value, unit, err := leafPromotionDecodeEEBus(target.candidate, observation.Value.Value())
		if err != nil {
			result.Unread = append(result.Unread, target.path)
			continue
		}
		result.Values = append(result.Values, eebusInboundValue{
			CandidateID: target.candidate.CandidateID, SemanticPath: target.path, Zone: target.zone,
			Raw: raw, Value: value, Unit: unit, Observation: observation.Clone(),
		})
	}
	if err := eebusInboundCacheBinding(functionCache, bind); err != nil {
		return eebusInboundRead{}, err
	}
	if err := ctx.Err(); err != nil {
		return eebusInboundRead{}, err
	}
	return result, nil
}

func eebusInboundCacheBinding(cache map[string]eebusraw.ReadObservationV1, bind func(eebusraw.RuntimeBindingV1) error) error {
	for _, observation := range cache {
		if err := bind(observation.Runtime); err != nil {
			return err
		}
	}
	return nil
}

// eebusInboundTargets expands the registry into one read target per value.
// The zone-1 definitions are applied to every resolved zone; the definitions
// of other fixed zones are not used, so a zone is read once.
func eebusInboundTargets(registry *promotioncapture.Registry, entities eebusInboundEntities) []eebusInboundTarget {
	var targets []eebusInboundTarget
	for _, candidate := range registry.Candidates() {
		if candidate.SemanticPath == nil || candidate.EEBusSource == nil ||
			(candidate.ProtocolEligibility != promotioncapture.ProtocolCrossProtocol &&
				candidate.ProtocolEligibility != promotioncapture.ProtocolEEBusNative) {
			continue
		}
		path, slot := *candidate.SemanticPath, candidate.EEBusSource.EntitySlot
		switch slot {
		case "device_information", "dhw_circuit", "outside_sensor":
			entity, ok := entities.single[slot]
			if !ok || strings.HasPrefix(path, "/zones/") {
				continue
			}
			targets = append(targets, eebusInboundTarget{
				candidate: candidate, path: path,
				slots: leafPromotionSlots{byName: map[string]eebusruntime.EntityV1{slot: entity}},
			})
		case "zone_1", "zone_1_room":
			field := strings.TrimPrefix(path, eebusInboundZoneTemplatePrefix)
			if field == path || field == "" || strings.Contains(field, "/") {
				continue
			}
			for index, zone := range entities.zones {
				number := index + 1
				targets = append(targets, eebusInboundTarget{
					candidate: candidate, zone: number,
					path: "/zones/zone_" + strconv.Itoa(number) + "/" + field,
					slots: leafPromotionSlots{byName: map[string]eebusruntime.EntityV1{
						"zone_1": zone.zone, "zone_1_room": zone.room,
					}},
				})
			}
		}
	}
	return targets
}

type eebusInboundZone struct {
	zone eebusruntime.EntityV1
	room eebusruntime.EntityV1
}

type eebusInboundEntities struct {
	single map[string]eebusruntime.EntityV1
	zones  []eebusInboundZone
}

// eebusInboundResolveEntities resolves entities by type. Device information,
// the hot-water circuit and the outdoor temperature sensor resolve only when
// exactly one entity of their type exists. The n-th room and the n-th heating
// zone in entity-address order form zone n; unequal counts leave every zone
// unresolved because the pairing would be ambiguous.
func eebusInboundResolveEntities(snapshot eebusruntime.SnapshotV1) eebusInboundEntities {
	byType := make(map[string][]eebusruntime.EntityV1)
	keys := make(map[string][]uint64, len(snapshot.Entities))
	invalid := make(map[string]bool)
	for _, entity := range snapshot.Entities {
		address, ok := eebusInboundEntityAddress(entity)
		if !ok {
			invalid[entity.Type] = true
			continue
		}
		keys[entity.DeviceAddress+"\x00"+entity.EntityAddress] = address
		byType[entity.Type] = append(byType[entity.Type], entity)
	}
	for entityType := range byType {
		list := byType[entityType]
		sort.SliceStable(list, func(left, right int) bool {
			if list[left].DeviceAddress != list[right].DeviceAddress {
				return list[left].DeviceAddress < list[right].DeviceAddress
			}
			return eebusInboundAddressLess(
				keys[list[left].DeviceAddress+"\x00"+list[left].EntityAddress],
				keys[list[right].DeviceAddress+"\x00"+list[right].EntityAddress],
			)
		})
	}
	result := eebusInboundEntities{single: make(map[string]eebusruntime.EntityV1, 3)}
	for slot, entityType := range map[string]string{
		"device_information": "DeviceInformation", "dhw_circuit": "DHWCircuit", "outside_sensor": "TemperatureSensor",
	} {
		if !invalid[entityType] && len(byType[entityType]) == 1 {
			result.single[slot] = byType[entityType][0]
		}
	}
	zones, rooms := byType["HeatingZone"], byType["HVACRoom"]
	if invalid["HeatingZone"] || invalid["HVACRoom"] || len(zones) != len(rooms) {
		return result
	}
	for index := range zones {
		result.zones = append(result.zones, eebusInboundZone{zone: zones[index], room: rooms[index]})
	}
	return result
}

func eebusInboundEntityAddress(entity eebusruntime.EntityV1) ([]uint64, bool) {
	prefix := entity.DeviceAddress + ":["
	if entity.DeviceAddress == "" || !strings.HasPrefix(entity.EntityAddress, prefix) || !strings.HasSuffix(entity.EntityAddress, "]:") {
		return nil, false
	}
	body := strings.TrimSuffix(strings.TrimPrefix(entity.EntityAddress, prefix), "]:")
	parts := strings.Split(body, ",")
	address := make([]uint64, len(parts))
	for index, part := range parts {
		value, err := strconv.ParseUint(part, 10, 64)
		if err != nil || strconv.FormatUint(value, 10) != part {
			return nil, false
		}
		address[index] = value
	}
	return address, true
}

func eebusInboundAddressLess(left, right []uint64) bool {
	for index := 0; index < len(left) && index < len(right); index++ {
		if left[index] != right[index] {
			return left[index] < right[index]
		}
	}
	return len(left) < len(right)
}
