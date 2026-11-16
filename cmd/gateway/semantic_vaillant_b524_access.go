package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"strings"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

func semanticReadB524WatchDescriptorPolicy(key ebusgateway.B524WatchKey) (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	switch key.Opcode {
	case vaillantB524OpcodeLocal:
		return semanticReadB524LocalDescriptorPolicy(key.Group, key.RegisterAddress)
	case vaillantB524OpcodeRead:
		return semanticReadB524RemoteDescriptorPolicy(key.Group, key.RegisterAddress)
	default:
		return semanticReadStateFastDescriptorPolicy()
	}
}

func semanticReadB524LocalDescriptorPolicy(group byte, addr uint16) (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	switch group {
	case localZones.group:
		return semanticReadB524ZoneDescriptorPolicy(addr)
	case localDHW.group:
		return semanticReadB524DHWDescriptorPolicy(addr)
	case localCircuits.group:
		return semanticReadB524CircuitDescriptorPolicy(addr)
	case localRegulator.group:
		return semanticReadB524RegulatorDescriptorPolicy(addr)
	case localSolar.group:
		return semanticReadB524SolarDescriptorPolicy(addr)
	case localCylinders.group:
		return semanticReadB524CylinderDescriptorPolicy(addr)
	default:
		return semanticReadStateFastDescriptorPolicy()
	}
}

func semanticReadB524RegulatorDescriptorPolicy(addr uint16) (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	switch addr {
	case system_off,
		system_water_pressure,
		system_flow_temperature,
		system_outdoor_temperature,
		system_maintenance_due,
		system_hwc_cylinder_temperature_top,
		system_hwc_cylinder_temperature_bottom:
		return semanticReadStateFastDescriptorPolicy()
	case system_outdoor_temperature_avg_24h,
		energy_fuel_sum_hc,
		energy_electricity_sum_hc,
		energy_electricity_sum_hwc,
		energy_fuel_sum_hwc,
		energy_fuel_sum_hc_this_month,
		energy_electricity_sum_hc_this_month,
		energy_electricity_sum_hwc_this_month,
		energy_fuel_sum_hwc_this_month,
		energy_fuel_sum_hc_last_month,
		energy_electricity_sum_hc_last_month,
		energy_electricity_sum_hwc_last_month,
		energy_fuel_sum_hwc_last_month:
		return semanticReadStateSlowDescriptorPolicy()
	case system_adaptive_heating_curve,
		system_alternative_point,
		system_heating_circuit_bivalence_point,
		system_dhw_bivalence_point,
		system_hc_emergency_temperature,
		system_hwc_max_flow_temp_desired,
		system_max_room_humidity,
		system_maintenance_date,
		system_installer_name_1,
		system_installer_name_2,
		system_installer_phone_1,
		system_installer_phone_2,
		system_installer_menu_code:
		return semanticReadConfigDescriptorPolicy()
	case system_scheme, system_module_configuration_vr71:
		return semanticReadDiscoveryDescriptorPolicy()
	default:
		return semanticReadStateFastDescriptorPolicy()
	}
}

func semanticReadB524ZoneDescriptorPolicy(addr uint16) (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	switch addr {
	case zone_current_temp, zone_special_function, zone_valve_status:
		return semanticReadStateFastDescriptorPolicy()
	case zone_current_humidity, zone_quick_veto_end_time, zone_quick_veto_end_date:
		return semanticReadStateSlowDescriptorPolicy()
	case zone_heating_op_mode,
		zone_target_temp,
		zone_fallback_manual_temp,
		zone_room_temperature_zone_mapping_raw,
		zone_quick_veto_temp,
		zone_quick_veto_duration,
		zone_holiday_start_date,
		zone_holiday_end_date,
		zone_holiday_setpoint,
		zone_holiday_end_time,
		zone_holiday_start_time:
		return semanticReadConfigDescriptorPolicy()
	case zone_name, zone_name_prefix, zone_name_suffix, zone_index:
		return semanticReadDiscoveryDescriptorPolicy()
	default:
		return semanticReadStateFastDescriptorPolicy()
	}
}

func semanticReadB524DHWDescriptorPolicy(addr uint16) (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	switch addr {
	case dhw_current_temp, dhw_special_function:
		return semanticReadStateFastDescriptorPolicy()
	case dhw_operation_mode, dhw_target_temp, dhw_holiday_start_date, dhw_holiday_end_date:
		return semanticReadConfigDescriptorPolicy()
	default:
		return semanticReadStateFastDescriptorPolicy()
	}
}

func semanticReadB524CircuitDescriptorPolicy(addr uint16) (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	switch addr {
	case circuit_flow_setpoint,
		circuit_flow_temp,
		circuit_circuit_state,
		circuit_pump_status,
		circuit_calc_flow_temp,
		circuit_mixer_position:
		return semanticReadStateFastDescriptorPolicy()
	case circuit_humidity,
		circuit_dew_point,
		circuit_pump_hours,
		circuit_pump_starts:
		return semanticReadStateSlowDescriptorPolicy()
	case circuit_type,
		circuit_cooling_enabled,
		circuit_heating_curve,
		circuit_flow_temp_max,
		circuit_flow_temp_min,
		circuit_summer_limit,
		circuit_room_temp_control,
		circuit_special_function_mode:
		return semanticReadConfigDescriptorPolicy()
	default:
		return semanticReadStateFastDescriptorPolicy()
	}
}

func semanticReadB524SolarDescriptorPolicy(addr uint16) (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	switch addr {
	case solar_collector_temp, solar_return_temp, solar_pump_active, solar_current_yield:
		return semanticReadStateFastDescriptorPolicy()
	case solar_pump_hours:
		return semanticReadStateSlowDescriptorPolicy()
	case solar_enabled, solar_function_mode:
		return semanticReadConfigDescriptorPolicy()
	default:
		return semanticReadStateFastDescriptorPolicy()
	}
}

func semanticReadB524CylinderDescriptorPolicy(addr uint16) (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	switch addr {
	case cylinder_temperature:
		return semanticReadStateFastDescriptorPolicy()
	case cylinder_max_setpoint, cylinder_charge_hysteresis, cylinder_charge_offset:
		return semanticReadConfigDescriptorPolicy()
	default:
		return semanticReadStateFastDescriptorPolicy()
	}
}

func semanticReadB524RemoteDescriptorPolicy(group byte, addr uint16) (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	switch group {
	case remoteRegulators.group, remoteThermostats.group, remoteFunctionalModules.group:
		switch addr {
		case device_slot_connected:
			return semanticReadStateSlowDescriptorPolicy()
		case device_slot_room_humidity,
			device_slot_room_temperature,
			device_slot_paired,
			device_slot_reception_strength:
			return semanticReadStateSlowDescriptorPolicy()
		case device_slot_class_address,
			device_slot_firmware,
			device_slot_remote_control_address,
			device_slot_hardware_identifier,
			device_slot_zone_assignment:
			return semanticReadDiscoveryDescriptorPolicy()
		default:
			return semanticReadStateFastDescriptorPolicy()
		}
	default:
		return semanticReadStateFastDescriptorPolicy()
	}
}

func (p *vaillantSemanticPoller) readB524Value(ctx context.Context, opcode, group, instance byte, addr uint16) ([]byte, bool) {
	return p.readB524ValueWithMaxAge(ctx, opcode, group, instance, addr, -1)
}

// readB524ValueWithMaxAge is readB524Value with a cache-age override: a
// non-negative maxAgeOverride replaces the descriptor TTL (0 never consults the
// scheduler or shadow cache); a negative override keeps the descriptor TTL.
func (p *vaillantSemanticPoller) readB524ValueWithMaxAge(ctx context.Context, opcode, group, instance byte, addr uint16, maxAgeOverride time.Duration) ([]byte, bool) {
	if p == nil || (p.bus == nil && p.sendFrameFn == nil) {
		return nil, false
	}
	if ctx == nil {
		ctx = context.Background()
	}

	p.mu.Lock()
	source := p.source
	target := p.controller
	timeout := p.requestTimeout
	p.mu.Unlock()

	if target == 0 {
		return nil, false
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	watchKey := ebusgateway.NewB524WatchKey(target, opcode, group, instance, addr)
	watchRuntime := p.prepareSemanticReadWatchRuntime(watchKey)
	maxAge := watchRuntime.maxAge
	if maxAgeOverride >= 0 {
		maxAge = maxAgeOverride
	}
	value, stats, err := p.scheduler.GetWatchWithStats(ctx, watchKey, maxAge, func(ctx context.Context) ([]byte, error) {
		var lastErr error
		for attempt := 0; attempt < 3; attempt++ {
			p.readMu.Lock()

			reqCtx, cancel := context.WithTimeout(ctx, timeout)
			request := protocol.Frame{
				FrameType: protocol.FrameTypeInitiatorTarget,
				Source:    source,
				Target:    target,
				Primary:   vaillantExtRegisterPrimary,
				Secondary: vaillantExtRegisterSecondary,
				Data:      buildB524ReadSelector(opcode, group, instance, addr),
			}
			response, err := p.sendSemanticFrame(reqCtx, request)
			cancel()
			p.readMu.Unlock()

			if err != nil {
				lastErr = err
			} else if response == nil {
				lastErr = fmt.Errorf("b524 read returned nil response")
			} else {
				payload, ok := parseB524ReadPayload(response.Data, opcode, group, instance, addr)
				if ok {
					return payload, nil
				}
				lastErr = fmt.Errorf(
					"b524 read failed: opcode=0x%02x group=0x%02x instance=0x%02x addr=0x%04x",
					opcode,
					group,
					instance,
					addr,
				)
			}

			if attempt < 2 && !p.pause(ctx, 75*time.Millisecond) {
				return nil, ctx.Err()
			}
		}
		if lastErr == nil {
			lastErr = fmt.Errorf("b524 read failed")
		}
		return nil, lastErr
	})
	p.emitWatchReadEfficiency(watchRuntime, maxAge, stats)
	if stats.ActiveFetchSucceeded {
		p.pause(ctx, 25*time.Millisecond)
	}
	if err != nil {
		return nil, false
	}
	return value, true
}

func semanticReadBreakerKey(target, opcode, group, instance byte, addr uint16) string {
	return ebusgateway.NewB524WatchKey(target, opcode, group, instance, addr).Canonical()
}

func buildB524ReadSelector(opcode, group, instance byte, addr uint16) []byte {
	return []byte{
		opcode,
		vaillantB524OpRead,
		group,
		instance,
		byte(addr),
		byte(addr >> 8),
	}
}

func buildB524WriteSelector(opcode, group, instance byte, addr uint16, data []byte) []byte {
	selector := []byte{
		opcode,
		vaillantB524OpWrite,
		group,
		instance,
		byte(addr),
		byte(addr >> 8),
	}
	return append(selector, data...)
}

func parseB524ReadPayload(payload []byte, opcode, group, instance byte, addr uint16) ([]byte, bool) {
	if len(payload) == 0 {
		return nil, false
	}

	if len(payload) == 1 && payload[0] == 0x00 {
		return nil, false
	}
	if len(payload) < 4 {
		return nil, false
	}

	replyKind := payload[0]

	if len(payload) >= 5 {
		replyInstance := payload[1]
		replyGroup := payload[2]
		replyAddr := uint16(payload[3]) | uint16(payload[4])<<8
		if replyGroup == group && replyAddr == addr {
			if !matchesB524ReplyInstance(replyInstance, instance) {
				log.Printf(
					"b524 read mismatch: want opcode=0x%02x group=0x%02x instance=0x%02x addr=0x%04x; got reply-instance=0x%02x (group=0x%02x addr=0x%04x)",
					opcode,
					group,
					instance,
					addr,
					replyInstance,
					replyGroup,
					replyAddr,
				)
				return nil, false
			}
			if len(payload) == 5 {
				return nil, false
			}
			return payload[5:], true
		}
	}

	replyGroup := payload[1]
	replyAddr := uint16(payload[2]) | uint16(payload[3])<<8
	if replyGroup != group || replyAddr != addr {
		log.Printf(
			"b524 read mismatch: want opcode=0x%02x group=0x%02x instance=0x%02x addr=0x%04x; got kind=0x%02x group=0x%02x addr=0x%04x len=%d",
			opcode,
			group,
			instance,
			addr,
			replyKind,
			replyGroup,
			replyAddr,
			len(payload),
		)
		return nil, false
	}
	if len(payload) == 4 {
		return nil, false
	}
	return payload[4:], true
}

// isB524ProbeCoherent checks if a B524 read response proves the target
// understands B524 register reads. Unlike parseB524ReadPayload, this accepts
// header-only responses (no value bytes) as coherent — the device returned a
// valid B524 structure which proves capability.
func isB524ProbeCoherent(payload []byte, group byte, addr uint16) bool {
	return ebusgateway.IsB524ResponseCoherent(payload, group, addr)
}

// probeB524Register sends a single B524 read request to target and checks
// if the response is coherent. Retries up to 3 times with 200ms backoff
// to handle adapter-direct bus contention.
func (p *vaillantSemanticPoller) probeB524Register(ctx context.Context, target, opcode, group, instance byte, addr uint16) bool {
	if p == nil || p.bus == nil {
		return false
	}

	p.mu.Lock()
	source := p.source
	timeout := p.requestTimeout
	p.mu.Unlock()

	// B524 probes need at least 5s per attempt in adapter-direct mode:
	// arbitration(~200ms) + frame(~220ms) + adapter-timeout(~550ms) +
	// collision-retry(~200ms) + second-attempt(~970ms) ≈ 2.1s minimum.
	// The config default SemanticRequestTimeout=2s is insufficient.
	const minB524ProbeTimeout = 5 * time.Second
	if timeout < minB524ProbeTimeout {
		timeout = minB524ProbeTimeout
	}

	// Retry probes up to 3 times — adapter-direct mode has bus contention
	// that can cause individual probes to timeout on busy buses.
	for attempt := 0; attempt < 3; attempt++ {
		// Back off between retries to avoid colliding with the same
		// bus contention pattern (50ms arbitration + waitForSyn).
		if attempt > 0 && !p.pause(ctx, 200*time.Millisecond) {
			return false
		}

		data := buildB524ReadSelector(opcode, group, instance, addr)
		frame := protocol.Frame{
			FrameType: protocol.FrameTypeInitiatorTarget,
			Source:    source,
			Target:    target,
			Primary:   vaillantExtRegisterPrimary,
			Secondary: vaillantExtRegisterSecondary,
			Data:      data,
		}

		p.readMu.Lock()
		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		response, err := p.sendSemanticFrame(reqCtx, frame)
		cancel()
		p.readMu.Unlock()

		if err != nil {
			if ctx.Err() != nil {
				return false // parent context cancelled
			}
			// Don't retry definitive rejections (NACK, no device) —
			// these are permanent, not transient bus contention.
			if ebuserrors.IsDefinitive(err) {
				return false
			}
			continue // retry on transient errors
		}
		if response == nil {
			continue
		}
		return isB524ProbeCoherent(response.Data, group, addr)
	}
	return false
}

func matchesB524ReplyInstance(replyInstance, requestedInstance byte) bool {
	if replyInstance == requestedInstance {
		return true
	}
	if requestedInstance < 0xFF && replyInstance == requestedInstance+1 {
		return true
	}
	return false
}

func (p *vaillantSemanticPoller) writeB524Value(ctx context.Context, opcode, group, instance byte, addr uint16, data []byte) error {
	if p == nil || (p.bus == nil && p.sendFrameFn == nil) {
		return fmt.Errorf("b524 write unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	p.mu.Lock()
	source := p.source
	target := p.controller
	timeout := p.requestTimeout
	p.mu.Unlock()
	if target == 0 {
		return fmt.Errorf("b524 write target is zero")
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		p.readMu.Lock()
		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		request := protocol.Frame{
			FrameType: protocol.FrameTypeInitiatorTarget,
			Source:    source,
			Target:    target,
			Primary:   vaillantExtRegisterPrimary,
			Secondary: vaillantExtRegisterSecondary,
			Data:      buildB524WriteSelector(opcode, group, instance, addr, data),
		}
		response, err := p.sendSemanticFrame(reqCtx, request)
		cancel()
		p.readMu.Unlock()

		if err != nil {
			lastErr = err
		} else if response != nil {
			// B524 write responses are typically short ACKs (< 4 bytes payload).
			return nil
		} else {
			lastErr = fmt.Errorf("b524 write returned nil response")
		}

		if attempt < 2 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(75 * time.Millisecond):
			}
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("b524 write failed")
	}
	return lastErr
}

func (p *vaillantSemanticPoller) readB524ValueLive(ctx context.Context, opcode, group, instance byte, addr uint16) ([]byte, bool) {
	if p == nil || (p.bus == nil && p.sendFrameFn == nil) {
		return nil, false
	}
	if ctx == nil {
		ctx = context.Background()
	}

	p.mu.Lock()
	source := p.source
	target := p.controller
	timeout := p.requestTimeout
	p.mu.Unlock()
	if target == 0 {
		return nil, false
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	for attempt := 0; attempt < 3; attempt++ {
		p.readMu.Lock()
		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		request := protocol.Frame{
			FrameType: protocol.FrameTypeInitiatorTarget,
			Source:    source,
			Target:    target,
			Primary:   vaillantExtRegisterPrimary,
			Secondary: vaillantExtRegisterSecondary,
			Data:      buildB524ReadSelector(opcode, group, instance, addr),
		}
		response, err := p.sendSemanticFrame(reqCtx, request)
		cancel()
		p.readMu.Unlock()

		if err == nil && response != nil {
			payload, ok := parseB524ReadPayload(response.Data, opcode, group, instance, addr)
			if ok {
				return payload, true
			}
		}

		if attempt < 2 {
			select {
			case <-ctx.Done():
				return nil, false
			case <-time.After(75 * time.Millisecond):
			}
		}
	}
	return nil, false
}

// Reader kinds of the fixed-width B524 readers, used as keys of
// semantic_b524_width_mismatch_total.
const (
	b524ReaderKindU8       = "u8"
	b524ReaderKindUint16   = "uint16"
	b524ReaderKindFloat32  = "float32le"
	b524ReaderKindUint32   = "uint32le"
	b524ReaderKindDateHDA3 = "date_hda3"

	// b524WidthMismatchLogInterval bounds the width mismatch log to one
	// line per selector in this interval.
	b524WidthMismatchLogInterval = 10 * time.Minute
)

// b524WidthMismatchSelector is the selector the width mismatch line names.
type b524WidthMismatchSelector struct {
	opcode byte
	group  byte
	addr   uint16
}

// b524ReplyWidthMatches reports whether a reply of got bytes fits a reader
// that accepts exactly one of the widths in accepted. A mismatch is counted
// under kind and logged at most once per selector per log interval; the
// caller then returns no value. want is the first accepted width.
func (p *vaillantSemanticPoller) b524ReplyWidthMatches(kind string, opcode, group byte, addr uint16, got int, accepted ...int) bool {
	for _, width := range accepted {
		if got == width {
			return true
		}
	}
	semanticB524WidthMismatchTotal.Add(kind, 1)
	if p == nil {
		return false
	}
	selector := b524WidthMismatchSelector{opcode: opcode, group: group, addr: addr}
	now := p.now()
	p.b524WidthMismatchMu.Lock()
	last, seen := p.b524WidthMismatchLoggedAt[selector]
	logLine := !seen || now.Sub(last) >= b524WidthMismatchLogInterval
	if logLine {
		if p.b524WidthMismatchLoggedAt == nil {
			p.b524WidthMismatchLoggedAt = make(map[b524WidthMismatchSelector]time.Time)
		}
		p.b524WidthMismatchLoggedAt[selector] = now
	}
	p.b524WidthMismatchMu.Unlock()
	if logLine {
		want := 0
		if len(accepted) > 0 {
			want = accepted[0]
		}
		log.Printf("semantic: b524 reply width mismatch opcode=0x%02X group=0x%02X register=0x%04X want=%d got=%d", opcode, group, addr, want, got)
	}
	return false
}

func (p *vaillantSemanticPoller) readB524Float32LE(ctx context.Context, opcode, group, instance byte, addr uint16) (float64, bool) {
	// The circuit RR0020 selector is raw evidence in the native catalog.
	// Reject it before acquisition or cache lookup, regardless of payload
	// width; this rejection is not a width mismatch.
	if opcode == localCircuits.opcode && group == localCircuits.group && addr == circuit_calc_flow_temp {
		return 0, false
	}
	raw, ok := p.readB524Value(ctx, opcode, group, instance, addr)
	if !ok || !p.b524ReplyWidthMatches(b524ReaderKindFloat32, opcode, group, addr, len(raw), 4) {
		return 0, false
	}
	bits := binary.LittleEndian.Uint32(raw[:4])
	value := float64(math.Float32frombits(bits))
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, false
	}
	return value, true
}

func (p *vaillantSemanticPoller) readB524U8(ctx context.Context, opcode, group, instance byte, addr uint16) *uint8 {
	raw, ok := p.readB524Value(ctx, opcode, group, instance, addr)
	if !ok || !p.b524ReplyWidthMatches(b524ReaderKindU8, opcode, group, addr, len(raw), 1) {
		return nil
	}
	if raw[0] == 0xFF {
		return nil
	}
	v := raw[0]
	return &v
}

func (p *vaillantSemanticPoller) readB524U16(ctx context.Context, opcode, group, instance byte, addr uint16) *uint16 {
	v, ok := p.readB524Uint16(ctx, opcode, group, instance, addr)
	if !ok || v == nil {
		return nil
	}
	if *v == 0xFFFF {
		return nil
	}
	return v
}

func (p *vaillantSemanticPoller) readB524F32(ctx context.Context, opcode, group, instance byte, addr uint16) *float64 {
	v, ok := p.readB524Float32LE(ctx, opcode, group, instance, addr)
	if !ok {
		return nil
	}
	return &v
}

func (p *vaillantSemanticPoller) readB524Bool(ctx context.Context, opcode, group, instance byte, addr uint16) *bool {
	v := p.readB524U8(ctx, opcode, group, instance, addr)
	if v == nil {
		return nil
	}
	if *v > 1 {
		return nil
	}
	value := *v == 1
	return &value
}

func (p *vaillantSemanticPoller) readB524Firmware(ctx context.Context, opcode, group, instance byte, addr uint16) *string {
	raw, ok := p.readB524Value(ctx, opcode, group, instance, addr)
	if !ok {
		return nil
	}
	return decodeB524FirmwareVersion(raw)
}

func (p *vaillantSemanticPoller) readB524CString(ctx context.Context, opcode, group, instance byte, addr uint16) (string, bool) {
	raw, ok := p.readB524Value(ctx, opcode, group, instance, addr)
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

func (p *vaillantSemanticPoller) readB524CStringSanitized(ctx context.Context, opcode, group, instance byte, addr uint16) (string, bool) {
	value, ok := p.readB524CString(ctx, opcode, group, instance, addr)
	if !ok {
		return "", false
	}
	sanitized := make([]byte, len(value))
	for i := 0; i < len(value); i++ {
		if value[i] > 0x7F {
			sanitized[i] = '?'
		} else {
			sanitized[i] = value[i]
		}
	}
	return string(sanitized), true
}

func (p *vaillantSemanticPoller) readB524DateHDA3(ctx context.Context, opcode, group, instance byte, addr uint16) (string, bool) {
	raw, ok := p.readB524Value(ctx, opcode, group, instance, addr)
	if !ok || !p.b524ReplyWidthMatches(b524ReaderKindDateHDA3, opcode, group, addr, len(raw), 3) {
		return "", false
	}
	day, month, yearOffset := int(raw[0]), int(raw[1]), int(raw[2])
	if day < 1 || day > 31 || month < 1 || month > 12 || yearOffset > 99 {
		return "", false
	}
	year := 2000 + yearOffset
	iso := fmt.Sprintf("%04d-%02d-%02d", year, month, day)
	return iso, true
}

func (p *vaillantSemanticPoller) readB524ZoneNamePart(ctx context.Context, instance byte, addr uint16) (string, bool) {
	raw, ok := p.readB524CString(ctx, localZones.opcode, localZones.group, instance, addr)
	if !ok {
		return "", false
	}
	return strings.TrimSpace(raw), true
}

func (p *vaillantSemanticPoller) readB524Uint16(ctx context.Context, opcode, group, instance byte, addr uint16) (*uint16, bool) {
	raw, ok := p.readB524Value(ctx, opcode, group, instance, addr)
	// The codec takes the two-byte form and the one-byte form.
	if !ok || !p.b524ReplyWidthMatches(b524ReaderKindUint16, opcode, group, addr, len(raw), 2, 1) {
		return nil, false
	}
	parsed, ok := decodeB524Uint16(raw)
	if !ok {
		return nil, false
	}
	return &parsed, true
}

func (p *vaillantSemanticPoller) readB524Uint32LE(ctx context.Context, opcode, group, instance byte, addr uint16) (uint32, bool) {
	raw, ok := p.readB524Value(ctx, opcode, group, instance, addr)
	if !ok || !p.b524ReplyWidthMatches(b524ReaderKindUint32, opcode, group, addr, len(raw), 4) {
		return 0, false
	}
	v := binary.LittleEndian.Uint32(raw[:4])
	if v == 0xFFFFFFFF {
		return 0, false
	}
	return v, true
}
