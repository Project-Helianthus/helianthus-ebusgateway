package main

import (
	"context"
	"encoding/binary"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	vaillantsystem "github.com/Project-Helianthus/helianthus-ebusreg/vaillant/system"
)

// The numeric guards below are conservative gateway policy, not additional
// OP00 protocol semantics or installation qualification. Records are reusable
// for at most five minutes, while the controller observation, session, source and
// registry identity still match.

type b524InterfaceIdentityRecord struct {
	identity             vaillantsystem.B524ControllerIdentity
	address, source      byte
	session              uint64
	controllerObservedAt time.Time
	acquiredAt           time.Time
	raw                  [2][4]byte
}

func (record b524InterfaceIdentityRecord) fresh(now time.Time) bool {
	age := now.Sub(record.acquiredAt)
	return !record.acquiredAt.IsZero() && age >= 0 && age <= 5*time.Minute
}

type b524InterfaceIdentityStore struct {
	mu       sync.Mutex
	sessions map[byte]uint64
	records  map[byte]b524InterfaceIdentityRecord
}

// Registries are runtime-scoped; a different registry can never inherit records.
// Each store is bounded to the six canonical regulator target slots.
var b524InterfaceIdentityStores sync.Map

func b524InterfaceIdentities(reg *registry.DeviceRegistry) *b524InterfaceIdentityStore {
	store, _ := b524InterfaceIdentityStores.LoadOrStore(reg, &b524InterfaceIdentityStore{
		sessions: make(map[byte]uint64), records: make(map[byte]b524InterfaceIdentityRecord),
	})
	return store.(*b524InterfaceIdentityStore)
}

func b524InterfaceQualifier(data []byte) (string, bool) {
	if len(data) != 4 {
		return "", false
	}
	value := float64(math.Float32frombits(binary.LittleEndian.Uint32(data)))
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 1<<24 || math.Trunc(value) != value {
		return "", false
	}
	return strconv.FormatUint(uint64(value), 10), true
}

type b524RegistryRevision struct {
	generation uint64
	observedAt time.Time
}

// Snapshot identity and the address observation must be coherent. The global
// generation fences snapshot reads only; unrelated inventory changes do not
// retire controller evidence. Re-observing the controller requires fresh OP00.
func b524RegistryIdentity(reg *registry.DeviceRegistry, address byte) (vaillantsystem.B524ControllerIdentity, b524RegistryRevision, bool) {
	if reg == nil {
		return vaillantsystem.B524ControllerIdentity{}, b524RegistryRevision{}, false
	}
	var before, after uint64
	reg.WithObservationGeneration(func(g uint64) { before = g })
	snap, found := reg.LookupEntrySnapshot(address)
	slot, slotFound := reg.LookupSlotSnapshot(address)
	reg.WithObservationGeneration(func(g uint64) { after = g })
	revision := b524RegistryRevision{generation: after, observedAt: slot.LastObservedAt}
	if !found || !slotFound || !slot.DeviceAttached || slot.LastObservedAt.IsZero() || before != after || !strings.EqualFold(snap.Manufacturer, "Vaillant") {
		return vaillantsystem.B524ControllerIdentity{}, revision, false
	}
	return graphql.ControllerIdentityFromSnapshot(snap), revision, true
}

func enrichB524InterfaceIdentity(ctx context.Context, reg *registry.DeviceRegistry, bus registry.ScanBus, source, companion byte) {
	if ctx == nil || reg == nil || bus == nil {
		return
	}
	store := b524InterfaceIdentities(reg)
	// Keep the same target priority as systemconfig.ControllerIdentityFromRegistry.
	for _, address := range [...]byte{0x15, 0x35, 0x75, 0xF5, 0x76, 0xF6} {
		if ctx.Err() != nil {
			return
		}
		if address == companion {
			continue
		}
		identity, revision, ok := b524RegistryIdentity(reg, address)
		if !ok || identity.EID == "" || identity.SoftwareRaw == "" || identity.HardwareRaw == "" {
			continue
		}
		store.mu.Lock()
		previous, exists := store.records[address]
		base := previous.identity
		base.APIVersion, base.Revision = "", ""
		if exists && base == identity && previous.identity.APIVersion != "" && previous.identity.Revision != "" &&
			previous.address == address && previous.source == source && previous.session == store.sessions[address] &&
			previous.controllerObservedAt.Equal(revision.observedAt) && previous.fresh(time.Now()) {
			store.mu.Unlock()
			continue
		}
		store.sessions[address]++
		session := store.sessions[address]
		delete(store.records, address)
		store.mu.Unlock()

		record := b524InterfaceIdentityRecord{
			identity: identity, address: address, source: source, session: session,
			controllerObservedAt: revision.observedAt,
		}
		complete := true
		for index, id := range [...]byte{0x06, 0x07} {
			if ctx.Err() != nil {
				complete = false
				break
			}
			reply, err := bus.Send(ctx, protocol.Frame{
				Source: source, Target: address, Primary: 0xB5, Secondary: 0x24,
				Data: []byte{0x00, id, 0x00},
			})
			if err != nil || reply == nil {
				complete = false
				continue
			}
			text, valid := b524InterfaceQualifier(reply.Data)
			if !valid {
				complete = false
				continue
			}
			copy(record.raw[index][:], reply.Data)
			if index == 0 {
				record.identity.APIVersion = text
			} else {
				record.identity.Revision = text
			}
		}
		if !complete || ctx.Err() != nil {
			continue
		}
		record.acquiredAt = time.Now()
		currentIdentity, currentRevision, valid := b524RegistryIdentity(reg, address)
		if !valid || currentIdentity != identity || currentRevision.observedAt != revision.observedAt {
			continue
		}
		reg.WithObservationGeneration(func(current uint64) {
			store.mu.Lock()
			defer store.mu.Unlock()
			if current == currentRevision.generation && store.sessions[address] == session {
				store.records[address] = record
			}
		})
	}
}

func storedB524InterfaceIdentity(reg *registry.DeviceRegistry, address, source byte, identity vaillantsystem.B524ControllerIdentity, revision b524RegistryRevision) vaillantsystem.B524ControllerIdentity {
	value, found := b524InterfaceIdentityStores.Load(reg)
	if !found {
		return identity
	}
	store := value.(*b524InterfaceIdentityStore)
	reg.WithObservationGeneration(func(current uint64) {
		store.mu.Lock()
		defer store.mu.Unlock()
		record, ok := store.records[address]
		base := record.identity
		base.APIVersion, base.Revision = "", ""
		if ok && record.identity.APIVersion != "" && record.identity.Revision != "" && base == identity && record.address == address && record.source == source &&
			record.controllerObservedAt.Equal(revision.observedAt) && current == revision.generation &&
			record.session == store.sessions[address] && record.fresh(time.Now()) {
			identity = record.identity
		}
	})
	return identity
}
