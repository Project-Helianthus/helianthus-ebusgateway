package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

type semanticReadWatchRuntime struct {
	key           ebusgateway.WatchKey
	descriptor    ebusgateway.WatchDescriptor
	hasDescriptor bool
	maxAge        time.Duration
}

func (p *vaillantSemanticPoller) prepareSemanticReadWatch(key ebusgateway.WatchKey) time.Duration {
	return p.prepareSemanticReadWatchRuntime(key).maxAge
}

func (p *vaillantSemanticPoller) prepareSemanticReadWatchRuntime(key ebusgateway.WatchKey) semanticReadWatchRuntime {
	return p.resolveSemanticReadWatchRuntime(
		key,
		semanticReadWatchDescriptorDefault,
		[]ebusgateway.WatchActivationSource{ebusgateway.WatchActivationSourcePoller},
		semanticReadActivationSources,
	)
}

func (p *vaillantSemanticPoller) bootstrapPassiveSharedWatchKey(key ebusgateway.WatchKey) semanticReadWatchRuntime {
	return p.resolveSemanticReadWatchRuntime(
		key,
		semanticReadWatchDescriptorPassiveFallback,
		[]ebusgateway.WatchActivationSource{ebusgateway.WatchActivationSourceTooling},
		semanticReadPassiveActivationSources,
	)
}

func (p *vaillantSemanticPoller) resolveSemanticReadWatchRuntime(
	key ebusgateway.WatchKey,
	fallback func(ebusgateway.WatchKey) ebusgateway.WatchDescriptor,
	defaultSources []ebusgateway.WatchActivationSource,
	sourceResolver func([]ebusgateway.WatchActivationSource) []ebusgateway.WatchActivationSource,
) semanticReadWatchRuntime {
	runtime := semanticReadWatchRuntime{
		key: key,
	}
	if key == nil {
		return runtime
	}

	descriptor := fallback(key)
	hasDescriptor := false
	sources := defaultSources
	if sourceResolver == nil {
		sourceResolver = semanticReadActivationSources
	}
	if p != nil && p.watchObserver != nil {
		observation := p.watchObserver.Observe(key)
		if observation.HasDescriptor {
			descriptor = semanticReadWatchDescriptorFromObservation(key, observation.Descriptor)
			hasDescriptor = true
		}
		sources = sourceResolver(observation.Sources)
	}

	maxAge, err := descriptor.EffectiveFreshnessTTL()
	if err != nil {
		descriptor = fallback(key)
		maxAge, _ = descriptor.EffectiveFreshnessTTL()
		hasDescriptor = false
	}
	if maxAge < 0 {
		maxAge = 0
	}

	if p != nil && p.shadow != nil {
		if err := p.shadow.BootstrapRuntimeDescriptor(descriptor, sources...); err != nil {
			log.Printf("semantic_read_shadow_bootstrap_failed key=%q err=%v", key.Canonical(), err)
		}
	}

	runtime.descriptor = descriptor
	runtime.hasDescriptor = hasDescriptor
	runtime.maxAge = maxAge
	return runtime
}

func (p *vaillantSemanticPoller) emitWatchReadEfficiency(runtime semanticReadWatchRuntime, maxAge time.Duration, stats ebusgateway.SemanticReadExecutionStats) {
	if p == nil || p.watchEfficiency == nil || runtime.key == nil {
		return
	}
	observedAt := p.now()
	p.watchEfficiency.ObserveWatchRead(ebusgateway.WatchEfficiencyReadEvent{
		Key:           runtime.key,
		Descriptor:    runtime.descriptor,
		HasDescriptor: runtime.hasDescriptor,
		MaxAge:        maxAge,
		Stats:         stats,
		ObservedAt:    observedAt,
	})
}

func (p *vaillantSemanticPoller) emitWatchDirectApplyEfficiency(runtime semanticReadWatchRuntime, observedAt time.Time, candidateEvaluated bool, accepted bool) {
	if p == nil || p.watchEfficiency == nil || runtime.key == nil {
		return
	}
	if observedAt.IsZero() {
		observedAt = p.now()
	}
	p.watchEfficiency.ObserveWatchDirectApply(ebusgateway.WatchEfficiencyDirectApplyEvent{
		Key:                runtime.key,
		Descriptor:         runtime.descriptor,
		HasDescriptor:      runtime.hasDescriptor,
		ObservedAt:         observedAt,
		CandidateEvaluated: candidateEvaluated,
		Accepted:           accepted,
	})
}

func semanticReadWatchDescriptorDefault(key ebusgateway.WatchKey) ebusgateway.WatchDescriptor {
	return semanticReadWatchDescriptorWithDecoderID(key, "semantic.read.poller")
}

func semanticReadWatchDescriptorPassiveFallback(key ebusgateway.WatchKey) ebusgateway.WatchDescriptor {
	return semanticReadWatchDescriptorWithDecoderID(key, "semantic.read.passive_fallback")
}

func semanticReadWatchDescriptorWithDecoderID(key ebusgateway.WatchKey, decoderID string) ebusgateway.WatchDescriptor {
	semanticClass, freshnessProfile, directApplyPolicy := semanticReadWatchDescriptorPolicy(key)
	return ebusgateway.WatchDescriptor{
		Key:               key,
		SemanticClass:     semanticClass,
		FreshnessProfile:  freshnessProfile,
		DecoderID:         decoderID,
		CorrelationPolicy: ebusgateway.WatchCorrelationPolicyRequestResponse,
		DirectApplyPolicy: directApplyPolicy,
	}
}

func semanticReadWatchDescriptorPolicy(key ebusgateway.WatchKey) (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	switch typed := key.(type) {
	case ebusgateway.B524WatchKey:
		return semanticReadB524WatchDescriptorPolicy(typed)
	case *ebusgateway.B524WatchKey:
		if typed != nil {
			return semanticReadB524WatchDescriptorPolicy(*typed)
		}
	}
	return semanticReadStateFastDescriptorPolicy()
}

func semanticReadStateFastDescriptorPolicy() (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	return ebusgateway.WatchSemanticClassState, ebusgateway.WatchFreshnessProfileStateFast, ebusgateway.WatchDirectApplyPolicyStateDefault
}

func semanticReadStateSlowDescriptorPolicy() (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	return ebusgateway.WatchSemanticClassState, ebusgateway.WatchFreshnessProfileStateSlow, ebusgateway.WatchDirectApplyPolicyStateDefault
}

func semanticReadConfigDescriptorPolicy() (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	return ebusgateway.WatchSemanticClassConfig, ebusgateway.WatchFreshnessProfileConfig, ebusgateway.WatchDirectApplyPolicyConfigOptIn
}

func semanticReadDiscoveryDescriptorPolicy() (ebusgateway.WatchSemanticClass, ebusgateway.WatchFreshnessProfile, ebusgateway.WatchDirectApplyPolicy) {
	return ebusgateway.WatchSemanticClassDiscovery, ebusgateway.WatchFreshnessProfileDiscovery, ebusgateway.WatchDirectApplyPolicyNever
}

func semanticReadWatchDescriptorFromObservation(key ebusgateway.WatchKey, observed ebusgateway.WatchDescriptor) ebusgateway.WatchDescriptor {
	normalized := observed
	normalized.Key = key
	if normalized.SemanticClass == "" {
		normalized.SemanticClass = ebusgateway.WatchSemanticClassState
	}
	if normalized.FreshnessProfile == "" {
		normalized.FreshnessProfile = ebusgateway.WatchFreshnessProfileStateFast
	}
	if normalized.DecoderID == "" {
		normalized.DecoderID = "semantic.read.poller"
	}
	if normalized.CorrelationPolicy == "" {
		normalized.CorrelationPolicy = ebusgateway.WatchCorrelationPolicyRequestResponse
	}
	if normalized.DirectApplyPolicy == "" {
		normalized.DirectApplyPolicy = ebusgateway.WatchDirectApplyPolicyStateDefault
	}
	return normalized
}

func semanticReadActivationSources(sources []ebusgateway.WatchActivationSource) []ebusgateway.WatchActivationSource {
	if len(sources) == 0 {
		return []ebusgateway.WatchActivationSource{ebusgateway.WatchActivationSourcePoller}
	}

	out := make([]ebusgateway.WatchActivationSource, 0, len(sources)+1)
	seen := make(map[ebusgateway.WatchActivationSource]struct{}, len(sources)+1)
	for _, source := range sources {
		if source == "" {
			continue
		}
		if _, ok := seen[source]; ok {
			continue
		}
		seen[source] = struct{}{}
		out = append(out, source)
	}
	if _, ok := seen[ebusgateway.WatchActivationSourcePoller]; !ok {
		out = append(out, ebusgateway.WatchActivationSourcePoller)
	}
	return out
}

func semanticReadPassiveActivationSources(sources []ebusgateway.WatchActivationSource) []ebusgateway.WatchActivationSource {
	if len(sources) == 0 {
		return []ebusgateway.WatchActivationSource{ebusgateway.WatchActivationSourceTooling}
	}

	seen := make(map[ebusgateway.WatchActivationSource]struct{}, len(sources))
	out := make([]ebusgateway.WatchActivationSource, 0, len(sources))
	for _, source := range sources {
		if source == "" {
			continue
		}
		if _, ok := seen[source]; ok {
			continue
		}
		seen[source] = struct{}{}
		out = append(out, source)
	}
	if len(out) == 0 {
		return []ebusgateway.WatchActivationSource{ebusgateway.WatchActivationSourceTooling}
	}
	return out
}

func (p *vaillantSemanticPoller) readB509Value(ctx context.Context, target byte, addr uint16) ([]byte, bool) {
	return p.readB509ValueWithMaxAge(ctx, target, addr, -1)
}

func (p *vaillantSemanticPoller) readB509ValueLive(ctx context.Context, target byte, addr uint16) ([]byte, bool) {
	return p.readB509ValueWithMaxAge(ctx, target, addr, 0)
}

func (p *vaillantSemanticPoller) readB509ValueWithMaxAge(ctx context.Context, target byte, addr uint16, maxAgeOverride time.Duration) ([]byte, bool) {
	if p == nil || (p.bus == nil && p.sendFrameFn == nil) || target == 0 {
		return nil, false
	}
	if ctx == nil {
		ctx = context.Background()
	}

	p.mu.Lock()
	source := p.source
	timeout := p.requestTimeout
	p.mu.Unlock()
	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	watchKey := ebusgateway.NewB509WatchKey(target, addr)
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
				Primary:   vaillantB509Primary,
				Secondary: vaillantB509Secondary,
				Data:      buildB509ReadSelector(addr),
			}
			response, err := p.sendSemanticFrame(reqCtx, request)
			cancel()
			p.readMu.Unlock()
			if addr == boiler_b509_flow_temperature {
				p.retainBoilerThermalRead(request, response, err)
			}

			if err != nil {
				lastErr = err
			} else if response == nil {
				lastErr = fmt.Errorf("b509 read returned nil response")
			} else if payload, ok := parseB509ReadPayload(response.Data, addr); ok {
				return payload, nil
			} else {
				lastErr = fmt.Errorf("b509 read failed: target=0x%02x addr=0x%04x", target, addr)
			}

			if attempt < 2 && !p.pause(ctx, 75*time.Millisecond) {
				return nil, ctx.Err()
			}
		}
		if lastErr == nil {
			lastErr = fmt.Errorf("b509 read failed")
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

func (p *vaillantSemanticPoller) writeB509Value(ctx context.Context, target byte, addr uint16, value []byte) error {
	if p == nil || (p.bus == nil && p.sendFrameFn == nil) {
		return fmt.Errorf("b509 write unavailable")
	}
	if target == 0 {
		return fmt.Errorf("b509 write target is zero")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	p.mu.Lock()
	source := p.source
	timeout := p.requestTimeout
	p.mu.Unlock()
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
			Primary:   vaillantB509Primary,
			Secondary: vaillantB509Secondary,
			Data:      buildB509WriteSelector(addr, value),
		}
		response, err := p.sendSemanticFrame(reqCtx, request)
		cancel()
		p.readMu.Unlock()

		if err != nil {
			lastErr = err
		} else if response == nil {
			lastErr = fmt.Errorf("b509 write returned nil response")
		} else if parseB509WriteAck(response.Data, addr) {
			return nil
		} else {
			lastErr = fmt.Errorf("b509 write ack invalid: target=0x%02x addr=0x%04x", target, addr)
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
		lastErr = fmt.Errorf("b509 write failed")
	}
	return lastErr
}

func (p *vaillantSemanticPoller) sendSemanticFrame(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	if p == nil {
		return nil, fmt.Errorf("semantic poller unavailable")
	}
	if p.sendFrameFn != nil {
		return p.sendFrameFn(ctx, frame)
	}
	if p.bus == nil {
		return nil, fmt.Errorf("semantic bus unavailable")
	}
	return p.bus.Send(ctx, frame)
}

func (p *vaillantSemanticPoller) readB509DATA2b(ctx context.Context, target byte, addr uint16) *float64 {
	raw, ok := p.readB509Value(ctx, target, addr)
	if !ok {
		return nil
	}
	value, ok := decodeDATA2b(raw)
	if !ok {
		return nil
	}
	return &value
}

func (p *vaillantSemanticPoller) readB509DATA2c(ctx context.Context, target byte, addr uint16) *float64 {
	raw, ok := p.readB509Value(ctx, target, addr)
	if !ok {
		return nil
	}
	value, ok := decodeDATA2c(raw)
	if !ok {
		return nil
	}
	return &value
}

func (p *vaillantSemanticPoller) readB509OnOff(ctx context.Context, target byte, addr uint16) *bool {
	raw, ok := p.readB509Value(ctx, target, addr)
	if !ok {
		return nil
	}
	value, ok := decodeOnOff(raw)
	if !ok {
		return nil
	}
	return &value
}

func (p *vaillantSemanticPoller) readB509UINInt(ctx context.Context, target byte, addr uint16) *int {
	raw, ok := p.readB509Value(ctx, target, addr)
	if !ok {
		return nil
	}
	value, ok := decodeUIN(raw)
	if !ok {
		return nil
	}
	out := int(value)
	return &out
}

func (p *vaillantSemanticPoller) readB509SINScaled(ctx context.Context, target byte, addr uint16, divisor float64) *float64 {
	raw, ok := p.readB509Value(ctx, target, addr)
	if !ok {
		return nil
	}
	value, ok := decodeSIN(raw)
	if !ok {
		return nil
	}
	out := float64(value)
	if divisor != 0 {
		out /= divisor
	}
	return &out
}

func (p *vaillantSemanticPoller) readB509UIN100(ctx context.Context, target byte, addr uint16) *float64 {
	raw, ok := p.readB509Value(ctx, target, addr)
	if !ok {
		return nil
	}
	value, ok := decodeUIN100(raw)
	if !ok {
		return nil
	}
	return &value
}

func (p *vaillantSemanticPoller) readB509Percent0(ctx context.Context, target byte, addr uint16) *float64 {
	raw, ok := p.readB509Value(ctx, target, addr)
	if !ok {
		return nil
	}
	value, ok := decodePercent0(raw)
	if !ok {
		return nil
	}
	out := float64(value)
	return &out
}

func (p *vaillantSemanticPoller) readB509Hoursum2(ctx context.Context, target byte, addr uint16) *float64 {
	raw, ok := p.readB509Value(ctx, target, addr)
	if !ok {
		return nil
	}
	value, ok := decodeHoursum2(raw)
	if !ok {
		return nil
	}
	return &value
}

func (p *vaillantSemanticPoller) readB509Hoursum2Int(ctx context.Context, target byte, addr uint16) *int {
	value := p.readB509Hoursum2(ctx, target, addr)
	if value == nil {
		return nil
	}
	v := int(*value)
	return &v
}

func (p *vaillantSemanticPoller) readB509PhoneBCD(ctx context.Context, target byte, addr uint16) *string {
	raw, ok := p.readB509Value(ctx, target, addr)
	if !ok || len(raw) == 0 {
		return nil
	}
	s := decodeBCDPhone(raw)
	if s == "" {
		return nil
	}
	return &s
}

func decodeBCDPhone(b []byte) string {
	var buf strings.Builder
	for _, v := range b {
		hi, lo := v>>4, v&0x0F
		if hi <= 9 {
			buf.WriteByte('0' + hi)
		} else {
			break
		}
		if lo <= 9 {
			buf.WriteByte('0' + lo)
		} else {
			break
		}
	}
	return buf.String()
}

func (p *vaillantSemanticPoller) readB509UCHInt(ctx context.Context, target byte, addr uint16) *int {
	raw, ok := p.readB509Value(ctx, target, addr)
	if !ok {
		return nil
	}
	value, ok := decodeUCH(raw)
	if !ok {
		return nil
	}
	out := int(value)
	return &out
}

func (p *vaillantSemanticPoller) readB509UCHFloat(ctx context.Context, target byte, addr uint16) *float64 {
	raw, ok := p.readB509Value(ctx, target, addr)
	if !ok {
		return nil
	}
	value, ok := decodeUCH(raw)
	if !ok {
		return nil
	}
	out := float64(value)
	return &out
}

func buildB509ReadSelector(addr uint16) []byte {
	return []byte{
		vaillantB509OpcodeRead,
		byte(addr >> 8),
		byte(addr),
	}
}

func buildB509WriteSelector(addr uint16, value []byte) []byte {
	data := make([]byte, 0, 3+len(value))
	data = append(data, vaillantB509OpcodeWrite, byte(addr>>8), byte(addr))
	data = append(data, value...)
	return data
}

func parseB509ReadPayload(payload []byte, addr uint16) ([]byte, bool) {
	if len(payload) == 0 {
		return nil, false
	}

	addrHi := byte(addr >> 8)
	addrLo := byte(addr)

	if len(payload) >= 3 && payload[0] == vaillantB509OpcodeRead && payload[1] == addrHi && payload[2] == addrLo {
		if len(payload) == 3 {
			return nil, false
		}
		return payload[3:], true
	}
	if len(payload) >= 2 && payload[0] == addrHi && payload[1] == addrLo {
		if len(payload) == 2 {
			return nil, false
		}
		return payload[2:], true
	}
	return payload, true
}

func parseB509WriteAck(payload []byte, addr uint16) bool {
	if len(payload) == 0 {
		return true
	}
	if len(payload) == 1 {
		return payload[0] != 0x00
	}

	addrHi := byte(addr >> 8)
	addrLo := byte(addr)

	if len(payload) >= 3 && payload[0] == vaillantB509OpcodeWrite && payload[1] == addrHi && payload[2] == addrLo {
		if len(payload) == 3 {
			return true
		}
		return payload[3] != 0x00
	}
	if len(payload) >= 2 && payload[0] == addrHi && payload[1] == addrLo {
		if len(payload) == 2 {
			return true
		}
		return payload[2] != 0x00
	}
	return payload[0] != 0x00
}

var (
	// Reserved for GW-2 wiring; keep implementations linked and lint-clean.
	_ = (*vaillantSemanticPoller).readB509Value
	_ = (*vaillantSemanticPoller).writeB509Value
)

// readB555Frame sends a B555 frame and returns the responder payload.
func (p *vaillantSemanticPoller) readB555Frame(ctx context.Context, data []byte) ([]byte, bool) {
	if p == nil || p.bus == nil {
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

	p.readMu.Lock()
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	request := protocol.Frame{
		FrameType: protocol.FrameTypeInitiatorTarget,
		Source:    source,
		Target:    target,
		Primary:   vaillantB555Primary,
		Secondary: vaillantB555Secondary,
		Data:      data,
	}
	response, err := p.bus.Send(reqCtx, request)
	cancel()
	p.readMu.Unlock()

	if err != nil || response == nil {
		return nil, false
	}
	return response.Data, true
}

func (p *vaillantSemanticPoller) writeB555Frame(ctx context.Context, data []byte) ([]byte, bool) {
	if p == nil || p.bus == nil {
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

	p.readMu.Lock()
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	request := protocol.Frame{
		FrameType: protocol.FrameTypeInitiatorTarget,
		Source:    source,
		Target:    target,
		Primary:   vaillantB555Primary,
		Secondary: vaillantB555Secondary,
		Data:      data,
	}
	response, err := p.bus.Send(reqCtx, request)
	cancel()
	p.readMu.Unlock()

	if err != nil || response == nil {
		return nil, false
	}
	return response.Data, true
}

func (p *vaillantSemanticPoller) writeB555TimerSlot(ctx context.Context, zone, hc, weekday, slotIdx, slotCount, startHour, startMinute, endHour, endMinute byte, temperatureRaw uint16) (byte, error) {
	data := []byte{
		b555OpcodeTimerWrite,
		zone, hc, weekday,
		slotIdx, slotCount,
		startHour, startMinute,
		endHour, endMinute,
		byte(temperatureRaw & 0xFF), byte(temperatureRaw >> 8),
	}
	resp, ok := p.writeB555Frame(ctx, data)
	if !ok {
		return 0xFF, fmt.Errorf("b555 timer write: no response")
	}
	if len(resp) < 1 {
		return 0xFF, fmt.Errorf("b555 timer write: empty response")
	}
	return resp[0], nil
}
