package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	modbus "github.com/Project-Helianthus/helianthus-modbus"
)

type huaweiWireKey struct {
	family   modbusTCPFamily
	function byte
	outcome  modbus.TCPTransportEventKind
}
type huaweiWireCounterStore struct {
	mu     sync.Mutex
	totals map[huaweiWireKey]uint64
}

var huaweiWireCounters huaweiWireCounterStore

type huaweiTransportSink struct {
	family   modbusTCPFamily
	upstream modbus.TCPTransportEventSink
}

func (s huaweiTransportSink) RecordTCPTransportEvent(event modbus.TCPTransportEvent) {
	huaweiWireCounters.record(s.family, event)
	if s.upstream != nil {
		s.upstream.RecordTCPTransportEvent(event)
	}
}

// Only closed categories reach the existing metrics route. Counts aggregate
// all instances, with no endpoint, label, unit, raw bytes or device label.
func (s *huaweiWireCounterStore) record(family modbusTCPFamily, e modbus.TCPTransportEvent) {
	if family != modbusTCPFamilyHuaweiSmartLogger && family != modbusTCPFamilyHuaweiSDongle {
		return
	}
	if e.RequestedFunction != modbus.FunctionReadHoldingRegisters && e.RequestedFunction != modbus.FunctionEncapsulatedInterface {
		return
	}
	switch e.Kind {
	case modbus.TCPEventWriteInvocation, modbus.TCPEventResponseReceive, modbus.TCPEventResponseTimerFire, modbus.TCPEventCallerCancellation:
	default:
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.totals == nil {
		s.totals = map[huaweiWireKey]uint64{}
	}
	key := huaweiWireKey{family, byte(e.RequestedFunction), e.Kind}
	if s.totals[key] != ^uint64(0) {
		s.totals[key]++
	}
}
func (s *huaweiWireCounterStore) text() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.totals) == 0 {
		return ""
	}
	lines := make([]string, 0, len(s.totals))
	for key, total := range s.totals {
		function := "fc03"
		if key.function == 43 {
			function = "fc2b_mei0e"
		}
		lines = append(lines, fmt.Sprintf("helianthus_huawei_transport_events_total{family=%q,function=%q,outcome=%q} %d\n", key.family, function, key.outcome, total))
	}
	sort.Strings(lines)
	return "# HELP helianthus_huawei_transport_events_total Read-only Huawei native transport events.\n# TYPE helianthus_huawei_transport_events_total counter\n" + strings.Join(lines, "")
}
