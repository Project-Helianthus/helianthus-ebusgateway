package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"strings"
	"time"

	"github.com/d3vi1/helianthus-ebusgateway"
	"github.com/d3vi1/helianthus-ebusgateway/graphql"
	"github.com/d3vi1/helianthus-ebusgo/protocol"
	"github.com/d3vi1/helianthus-ebusreg/registry"
)

const (
	vaillantExtRegisterPrimary   = byte(0xB5)
	vaillantExtRegisterSecondary = byte(0x24)
	vaillantB524OpcodeLocal      = byte(0x02)
	vaillantB524OpRead           = byte(0x00)

	vaillantGroupZones = byte(0x03)

	zoneRegName        = uint16(0x0016)
	zoneRegIndex       = uint16(0x001C)
	zoneRegCurrentTemp = uint16(0x000F)
	zoneRegTargetTemp  = uint16(0x0022)
)

func startVaillantSemanticPolling(ctx context.Context, cfg ebusgateway.Config, gateway *ebusgateway.Gateway, provider *graphql.LiveSemanticProvider, hub *graphql.BroadcastHub) {
	if gateway == nil || gateway.Bus == nil || gateway.Registry == nil || provider == nil {
		return
	}

	interval := cfg.SemanticInterval
	if interval <= 0 {
		interval = 10 * time.Second
	}

	go func() {
		pollCtx := ctx
		if pollCtx == nil {
			pollCtx = context.Background()
		}

		// First poll quickly so HA can create entities on initial coordinator refresh.
		pollVaillantZonesOnce(pollCtx, cfg.ScanSource, gateway.Registry, gateway.Bus, provider, hub)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-pollCtx.Done():
				return
			case <-ticker.C:
				pollVaillantZonesOnce(pollCtx, cfg.ScanSource, gateway.Registry, gateway.Bus, provider, hub)
			}
		}
	}()
}

func pollVaillantZonesOnce(ctx context.Context, source byte, reg *registry.DeviceRegistry, bus *protocol.Bus, provider *graphql.LiveSemanticProvider, hub *graphql.BroadcastHub) {
	if bus == nil || provider == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	controller, ok := findDeviceAddressByPrefix(reg, "BASV")
	if !ok {
		return
	}

	zones := make([]graphql.Zone, 0, 3)
	for instance := byte(0x00); instance <= 0x0A; instance++ {
		indexBytes, ok := readB524Value(ctx, bus, source, controller, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegIndex)
		if !ok || len(indexBytes) < 1 || indexBytes[0] == 0xFF {
			continue
		}

		name := fmt.Sprintf("Zone %d", instance+1)
		if rawName, ok := readB524CString(ctx, bus, source, controller, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegName); ok {
			if trimmed := strings.TrimSpace(rawName); trimmed != "" {
				name = trimmed
			}
		}

		var currentPtr *float64
		if value, ok := readB524Float32LE(ctx, bus, source, controller, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegCurrentTemp); ok {
			currentPtr = &value
		}

		var targetPtr *float64
		if value, ok := readB524Float32LE(ctx, bus, source, controller, vaillantB524OpcodeLocal, vaillantGroupZones, instance, zoneRegTargetTemp); ok {
			targetPtr = &value
		}

		zone := graphql.Zone{
			ID:           fmt.Sprintf("zone-%d", instance+1),
			Name:         name,
			CurrentTempC: currentPtr,
			TargetTempC:  targetPtr,
		}
		zones = append(zones, zone)
	}

	provider.SetZones(zones)
	if hub != nil {
		for _, zone := range zones {
			hub.PublishZoneUpdate(zone)
		}
	}
}

func findDeviceAddressByPrefix(reg *registry.DeviceRegistry, wantedPrefix string) (byte, bool) {
	if reg == nil {
		return 0, false
	}
	wantedPrefix = normalizeDeviceID(wantedPrefix)
	if wantedPrefix == "" {
		return 0, false
	}

	var addr byte
	var found bool
	reg.Iterate(func(entry registry.DeviceEntry) bool {
		if entry == nil {
			return true
		}
		if strings.HasPrefix(normalizeDeviceID(entry.DeviceID()), wantedPrefix) {
			addr = entry.Address()
			found = true
			return false
		}
		return true
	})
	return addr, found
}

func normalizeDeviceID(value string) string {
	normalized := strings.ToUpper(strings.TrimSpace(value))
	normalized = strings.ReplaceAll(normalized, "_", "")
	normalized = strings.ReplaceAll(normalized, "-", "")
	normalized = strings.ReplaceAll(normalized, " ", "")
	return normalized
}

func readB524Value(ctx context.Context, bus *protocol.Bus, source, target, opcode, group, instance byte, addr uint16) ([]byte, bool) {
	if bus == nil {
		return nil, false
	}
	if ctx == nil {
		ctx = context.Background()
	}

	request := protocol.Frame{
		Source:    source,
		Target:    target,
		Primary:   vaillantExtRegisterPrimary,
		Secondary: vaillantExtRegisterSecondary,
		Data:      []byte{opcode, vaillantB524OpRead, group, instance, byte(addr), byte(addr >> 8)},
	}

	response, err := bus.Send(ctx, request)
	if err != nil || response == nil {
		return nil, false
	}
	payload := response.Data
	if len(payload) == 1 {
		return nil, false
	}
	if len(payload) < 4 {
		return nil, false
	}

	tt := payload[0]
	if tt == 0x00 {
		return nil, false
	}
	if len(payload) <= 4 {
		return nil, false
	}

	replyGroup := payload[1]
	replyAddr := uint16(payload[2]) | uint16(payload[3])<<8
	if replyGroup != group || replyAddr != addr {
		log.Printf("b524 read mismatch: want group=0x%02x addr=0x%04x; got tt=0x%02x group=0x%02x addr=0x%04x", group, addr, tt, replyGroup, replyAddr)
	}
	return payload[4:], true
}

func readB524Float32LE(ctx context.Context, bus *protocol.Bus, source, target, opcode, group, instance byte, addr uint16) (float64, bool) {
	raw, ok := readB524Value(ctx, bus, source, target, opcode, group, instance, addr)
	if !ok || len(raw) < 4 {
		return 0, false
	}
	bits := binary.LittleEndian.Uint32(raw[:4])
	value := float64(math.Float32frombits(bits))
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, false
	}
	return value, true
}

func readB524CString(ctx context.Context, bus *protocol.Bus, source, target, opcode, group, instance byte, addr uint16) (string, bool) {
	raw, ok := readB524Value(ctx, bus, source, target, opcode, group, instance, addr)
	if !ok || len(raw) == 0 {
		return "", false
	}
	trimmed := raw
	for i, b := range trimmed {
		if b == 0x00 {
			trimmed = trimmed[:i]
			break
		}
	}
	return string(trimmed), true
}
