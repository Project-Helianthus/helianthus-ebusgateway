package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"net"
	"strings"
	"time"

	"github.com/d3vi1/helianthus-ebusgateway"
)

const (
	ebusdGrabCommand               = "grab result all\n"
	ebusdGrabFollowupReadWindow    = 350 * time.Millisecond
	ebusdGrabMinimumInitialTimeout = 2 * time.Second
)

func (p *vaillantSemanticPoller) refreshFromEbusdGrab(ctx context.Context) bool {
	if p == nil {
		return false
	}
	if !isEbusdTCPTransport(p.transportConfig) {
		return false
	}

	var controller byte
	p.mu.Lock()
	controller = p.controller
	p.mu.Unlock()
	if controller == 0 {
		if found, ok := findDeviceAddressByPrefix(p.reg, "BASV"); ok {
			controller = found
		}
	}

	zones, ok := readB524ZonesFromEbusdGrab(ctx, p.transportConfig, controller)
	if !ok || len(zones) == 0 {
		return false
	}

	p.mu.Lock()
	if p.controller == 0 && controller != 0 {
		p.controller = controller
	}
	for instance, incoming := range zones {
		entry := p.zones[instance]
		if entry == nil {
			entry = &vaillantZoneSnapshot{Instance: instance}
			p.zones[instance] = entry
		}
		entry.Present = true
		if strings.TrimSpace(incoming.Name) != "" {
			entry.Name = incoming.Name
		}
		if incoming.OperatingMode != "" {
			entry.OperatingMode = incoming.OperatingMode
		}
		if incoming.Preset != "" {
			entry.Preset = incoming.Preset
		}
		if incoming.CurrentTempC != nil {
			value := *incoming.CurrentTempC
			entry.CurrentTempC = &value
		}
		if incoming.TargetTempC != nil {
			value := *incoming.TargetTempC
			entry.TargetTempC = &value
		}
		if incoming.HumidityPct != nil {
			value := *incoming.HumidityPct
			entry.HumidityPct = &value
		}
		if incoming.HvacAction != "" {
			entry.HvacAction = incoming.HvacAction
		}
		if incoming.OperatingMode != "" {
			entry.OperatingMode = incoming.OperatingMode
		}
		if incoming.Preset != "" {
			entry.Preset = incoming.Preset
		}
		if len(incoming.AllowedModes) > 0 {
			entry.AllowedModes = append([]string(nil), incoming.AllowedModes...)
		}
		if incoming.StateSpecialFunction != "" {
			entry.StateSpecialFunction = incoming.StateSpecialFunction
		}
		entry.ConfigurationAssociatedCircuitRaw = incoming.ConfigurationAssociatedCircuitRaw
		entry.ConfigurationCircuitTypeRaw = incoming.ConfigurationCircuitTypeRaw
		entry.StateValveStatusRaw = incoming.StateValveStatusRaw
		entry.ConfigurationHeatingOperationMode = incoming.ConfigurationHeatingOperationMode
	}
	p.mu.Unlock()
	return true
}

func isEbusdTCPTransport(cfg ebusgateway.TransportConfig) bool {
	if strings.TrimSpace(strings.ToLower(cfg.Network)) != "tcp" {
		return false
	}
	return strings.TrimSpace(strings.ToLower(string(cfg.Protocol))) == "ebusd-tcp"
}

func readB524ZonesFromEbusdGrab(ctx context.Context, cfg ebusgateway.TransportConfig, controller byte) (map[byte]*vaillantZoneSnapshot, bool) {
	candidates := ebusdScanTargetCandidates(cfg)
	for _, candidate := range candidates {
		lines, err := ebusdCommandLines(ctx, candidate, ebusdGrabCommand)
		if err != nil || len(lines) == 0 {
			continue
		}
		zones := parseB524ZonesFromGrab(lines, controller)
		if len(zones) > 0 {
			return zones, true
		}
	}
	return nil, false
}

func (p *vaillantSemanticPoller) refreshDHWFromEbusdGrab(ctx context.Context) bool {
	if p == nil {
		return false
	}
	if !isEbusdTCPTransport(p.transportConfig) {
		return false
	}
	var controller byte
	p.mu.Lock()
	controller = p.controller
	p.mu.Unlock()
	if controller == 0 {
		if found, ok := findDeviceAddressByPrefix(p.reg, "BASV"); ok {
			controller = found
		}
	}

	candidates := ebusdScanTargetCandidates(p.transportConfig)
	for _, candidate := range candidates {
		lines, err := ebusdCommandLines(ctx, candidate, ebusdGrabCommand)
		if err != nil || len(lines) == 0 {
			continue
		}
		dhw, ok := parseB524DhwFromGrab(lines, controller)
		if !ok || dhw == nil {
			continue
		}
		p.mu.Lock()
		p.dhw = dhw
		p.mu.Unlock()
		return true
	}
	return false
}

func ebusdCommandLines(ctx context.Context, cfg ebusgateway.TransportConfig, command string) ([]string, error) {
	command = strings.TrimSpace(command)
	if command == "" || cfg.Address == "" || strings.TrimSpace(strings.ToLower(cfg.Network)) != "tcp" {
		return nil, nil
	}

	dial := cfg.Dial
	if dial == nil {
		dial = dialContext
	}

	dialCtx := ctx
	cancel := func() {}
	if dialCtx == nil {
		dialCtx = context.Background()
	}
	if cfg.DialTimeout > 0 {
		dialCtx, cancel = context.WithTimeout(dialCtx, cfg.DialTimeout)
	}
	defer cancel()

	conn, err := dial(dialCtx, cfg.Network, cfg.Address, cfg.DialTimeout)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()

	reader := bufio.NewReader(conn)
	if _, err := io.WriteString(conn, command+"\n"); err != nil {
		return nil, err
	}

	initialTimeout := cfg.ReadTimeout
	if initialTimeout < ebusdGrabMinimumInitialTimeout {
		initialTimeout = ebusdGrabMinimumInitialTimeout
	}
	if initialTimeout > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(initialTimeout))
	}

	lines := make([]string, 0, 256)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if isNetTimeout(err) || errors.Is(err, io.EOF) {
				break
			}
			return lines, err
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		lines = append(lines, trimmed)
		if initialTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(ebusdGrabFollowupReadWindow))
		}
	}

	return lines, nil
}

func parseB524ZonesFromGrab(lines []string, controller byte) map[byte]*vaillantZoneSnapshot {
	type zoneNameParts struct {
		primary string
		prefix  string
		suffix  string
	}
	type zoneState struct {
		snapshot                   *vaillantZoneSnapshot
		nameParts                  zoneNameParts
		hasIndex                   bool
		indexPresent               bool
		configurationOperationMode *uint16
		stateSpecialFunction       *uint16
		stateValveStatus           *uint16
		configurationCircuitRaw    *uint16
	}

	states := make(map[byte]*zoneState)
	zones := make(map[byte]*vaillantZoneSnapshot)
	circuitTypeByInstance := make(map[byte]*uint16)

	for _, line := range lines {
		group, instance, addr, payload, ok := parseB524GrabLine(line, controller)
		if !ok {
			continue
		}
		if group == vaillantGroupCircuits && addr == circuitRegType {
			if value, ok := decodeB524Uint16(payload); ok {
				typed := value
				circuitTypeByInstance[instance] = &typed
			}
			continue
		}
		if group != vaillantGroupZones {
			continue
		}
		state := states[instance]
		if state == nil {
			state = &zoneState{
				snapshot: &vaillantZoneSnapshot{
					Instance: instance,
				},
			}
			states[instance] = state
		}
		snapshot := state.snapshot

		switch addr {
		case zoneRegIndex:
			state.hasIndex = true
			if len(payload) > 0 && payload[0] != 0xFF {
				state.indexPresent = true
			}
		case zoneRegName:
			parts := state.nameParts
			parts.primary = decodeCString(payload)
			state.nameParts = parts
		case zoneRegNamePrefix:
			parts := state.nameParts
			parts.prefix = decodeCString(payload)
			state.nameParts = parts
		case zoneRegNameSuffix:
			parts := state.nameParts
			parts.suffix = decodeCString(payload)
			state.nameParts = parts
		case zoneRegHeatingOpMode:
			if value, ok := decodeB524Uint16(payload); ok {
				typed := value
				state.configurationOperationMode = &typed
				snapshot.ConfigurationHeatingOperationMode = formatUintToken(value)
			}
		case zoneRegSpecialFunction:
			if value, ok := decodeB524Uint16(payload); ok {
				typed := value
				state.stateSpecialFunction = &typed
				snapshot.StateSpecialFunction = formatUintToken(value)
			}
		case zoneRegValveStatus:
			if value, ok := decodeB524Uint16(payload); ok {
				typed := value
				state.stateValveStatus = &typed
				snapshot.StateValveStatusRaw = &typed
			}
		case zoneRegAssociatedCircuitRaw:
			if value, ok := decodeB524Uint16(payload); ok {
				typed := value
				state.configurationCircuitRaw = &typed
				snapshot.ConfigurationAssociatedCircuitRaw = &typed
			}
		case zoneRegCurrentTemp:
			if value, ok := decodeFloat32LE(payload); ok {
				v := value
				snapshot.CurrentTempC = &v
			}
		case zoneRegTargetTemp:
			if value, ok := decodeFloat32LE(payload); ok {
				v := value
				snapshot.TargetTempC = &v
			}
		case zoneRegFallbackManualTemp:
			if snapshot.TargetTempC == nil {
				if value, ok := decodeFloat32LE(payload); ok {
					v := value
					snapshot.TargetTempC = &v
				}
			}
		case zoneRegCurrentHumidity:
			if value, ok := decodeFloat32LE(payload); ok {
				v := value
				snapshot.HumidityPct = &v
			}
		}
	}

	for instance, state := range states {
		if state == nil || state.snapshot == nil {
			continue
		}
		circuitInstance := resolveCircuitInstance(state.configurationCircuitRaw, instance)
		circuitType, hasCircuitType := circuitTypeByInstance[circuitInstance]
		if hasCircuitType && circuitType != nil {
			state.snapshot.ConfigurationCircuitTypeRaw = circuitType
		}

		mode, preset, allowedModes := deriveZoneModeAndPreset(state.configurationOperationMode, state.stateSpecialFunction, circuitType, hasCircuitType)
		hvacAction := deriveZoneHvacAction(state.stateValveStatus, circuitType, hasCircuitType)
		if mode != "" {
			state.snapshot.OperatingMode = mode
		}
		if preset != "" {
			state.snapshot.Preset = preset
		}
		if hvacAction != "" {
			state.snapshot.HvacAction = hvacAction
		}
		if len(allowedModes) > 0 {
			state.snapshot.AllowedModes = allowedModes
		}

		name := composeZoneName(state.nameParts.primary, state.nameParts.prefix, state.nameParts.suffix)
		if strings.TrimSpace(name) != "" {
			state.snapshot.Name = name
		}
		meaningful := strings.TrimSpace(state.snapshot.Name) != "" ||
			state.snapshot.OperatingMode != "" ||
			state.snapshot.Preset != "" ||
			state.snapshot.CurrentTempC != nil ||
			state.snapshot.TargetTempC != nil ||
			state.snapshot.HumidityPct != nil
		if state.hasIndex {
			if !state.indexPresent {
				continue
			}
		} else if !meaningful {
			continue
		}
		state.snapshot.Present = true
		zones[instance] = state.snapshot
	}

	return zones
}

func parseB524DhwFromGrab(lines []string, controller byte) (*vaillantDhwSnapshot, bool) {
	var (
		opModeRaw *uint16
		sfModeRaw *uint16
		current   *float64
		target    *float64
	)

	for _, line := range lines {
		group, instance, addr, payload, ok := parseB524GrabLine(line, controller)
		if !ok || group != vaillantGroupDHW || instance != dhwInstance {
			continue
		}
		switch addr {
		case dhwRegOperationMode:
			if value, ok := decodeB524Uint16(payload); ok {
				typed := value
				opModeRaw = &typed
			}
		case dhwRegSpecialFunction:
			if value, ok := decodeB524Uint16(payload); ok {
				typed := value
				sfModeRaw = &typed
			}
		case dhwRegCurrentTemp:
			if value, ok := decodeFloat32LE(payload); ok {
				typed := value
				current = &typed
			}
		case dhwRegTargetTemp:
			if value, ok := decodeFloat32LE(payload); ok {
				typed := value
				target = &typed
			}
		}
	}

	operatingMode, preset := deriveDhwModeAndPreset(opModeRaw, sfModeRaw)
	if operatingMode == "" && preset == "" && current == nil && target == nil {
		return nil, false
	}

	dhw := &vaillantDhwSnapshot{
		OperatingMode: operatingMode,
		Preset:        preset,
		CurrentTempC:  current,
		TargetTempC:   target,
	}
	if opModeRaw != nil {
		dhw.ConfigurationDHWOperationMode = formatUintToken(*opModeRaw)
	}
	if sfModeRaw != nil {
		dhw.StateSpecialFunction = formatUintToken(*sfModeRaw)
	}
	return dhw, true
}

func parseB524GrabLine(line string, controller byte) (byte, byte, uint16, []byte, bool) {
	line = strings.TrimSpace(line)
	if line == "" {
		return 0, 0, 0, nil, false
	}

	slash := strings.Index(line, "/")
	if slash <= 0 {
		return 0, 0, 0, nil, false
	}

	reqText := strings.TrimSpace(line[:slash])
	reqFields := strings.Fields(reqText)
	if len(reqFields) == 0 {
		return 0, 0, 0, nil, false
	}
	reqBytes, ok := decodeHexField(reqFields[0])
	if !ok || len(reqBytes) < 11 {
		return 0, 0, 0, nil, false
	}
	if reqBytes[2] != vaillantExtRegisterPrimary || reqBytes[3] != vaillantExtRegisterSecondary {
		return 0, 0, 0, nil, false
	}
	if reqBytes[4] != vaillantB524OpcodeRead ||
		reqBytes[5] != vaillantB524OpcodeLocal ||
		reqBytes[6] != vaillantB524OpRead {
		return 0, 0, 0, nil, false
	}

	target := reqBytes[1]
	if controller != 0 && target != controller {
		return 0, 0, 0, nil, false
	}

	group := reqBytes[7]
	instance := reqBytes[8]
	addr := uint16(reqBytes[9]) | uint16(reqBytes[10])<<8

	respPart := strings.TrimSpace(line[slash+1:])
	if respPart == "" {
		return 0, 0, 0, nil, false
	}
	if equal := strings.Index(respPart, "="); equal >= 0 {
		respPart = strings.TrimSpace(respPart[:equal])
	}
	respFields := strings.Fields(respPart)
	if len(respFields) == 0 {
		return 0, 0, 0, nil, false
	}
	if strings.HasPrefix(strings.ToLower(respFields[0]), "err") {
		return 0, 0, 0, nil, false
	}

	respBytes, ok := decodeHexField(respFields[0])
	if !ok || len(respBytes) == 0 {
		return 0, 0, 0, nil, false
	}
	if int(respBytes[0]) == len(respBytes)-1 {
		respBytes = respBytes[1:]
	}

	payload, ok := parseB524ReadPayload(respBytes, vaillantB524OpcodeLocal, group, instance, addr)
	if !ok {
		return 0, 0, 0, nil, false
	}
	return group, instance, addr, payload, true
}

func decodeHexField(value string) ([]byte, bool) {
	normalized := strings.TrimSpace(value)
	if normalized == "" {
		return nil, false
	}
	normalized = strings.ReplaceAll(normalized, " ", "")
	normalized = strings.ReplaceAll(normalized, "\t", "")
	if len(normalized)%2 != 0 {
		return nil, false
	}
	bytes, err := hex.DecodeString(normalized)
	if err != nil {
		return nil, false
	}
	return bytes, true
}

func decodeCString(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	trimmed := raw
	for index, value := range trimmed {
		if value == 0x00 {
			trimmed = trimmed[:index]
			break
		}
	}
	return strings.TrimSpace(string(trimmed))
}

func decodeFloat32LE(raw []byte) (float64, bool) {
	if len(raw) < 4 {
		return 0, false
	}
	bits := uint32(raw[0]) | uint32(raw[1])<<8 | uint32(raw[2])<<16 | uint32(raw[3])<<24
	value := float64(math.Float32frombits(bits))
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, false
	}
	return value, true
}

func isNetTimeout(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
