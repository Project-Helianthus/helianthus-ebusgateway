package main

// Synthetic site for the Vaillant publication goldens (issue #1480).
//
// The site holds a regulator-class controller that answers B524 and B555, a
// boiler-class heat source that answers B509, a functional module of the FM5
// class and two radio slots. Every value is synthetic: register selectors come
// from the poller's own constants, payload widths follow the poller's readers,
// and serial numbers wrap a public product-catalog part number in zero date,
// plant and sequence fields (see controllerlessSerial). No identifier of a real
// installation is used.
//
// B524 and B509 frames reach the site through the poller's sendFrameFn seam.
// The B555 timer protocol is sent on the gateway bus itself, so the site also
// answers on the wire: vaillantPublicationWire is a plain raw transport that
// echoes every written symbol and, after a complete initiator telegram,
// queues the target's ACK, NN, data and CRC. Every frame costs a fixed amount
// of simulated time; nothing reads the wall clock.

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

const (
	vaillantPublicationSource     = byte(0xF0)
	vaillantPublicationController = byte(0x15)
	vaillantPublicationBoiler     = byte(0x08)
	vaillantPublicationModule     = byte(0x26)
	// vaillantPublicationFrameCost is the simulated bus time of one frame.
	vaillantPublicationFrameCost = 20 * time.Millisecond
	// vaillantPublicationThermostatClass is a radio class address the
	// poller has no model name for.
	vaillantPublicationThermostatClass = byte(0x3A)
)

// vaillantPublicationEpoch is the simulated start instant: a Monday.
var vaillantPublicationEpoch = time.Date(2026, time.January, 5, 6, 0, 0, 0, time.UTC)

type vaillantPublicationClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *vaillantPublicationClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *vaillantPublicationClock) Advance(d time.Duration) {
	if d <= 0 {
		return
	}
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// pause keeps the poller's read pauses on the simulated clock.
func (c *vaillantPublicationClock) pause(ctx context.Context, d time.Duration) bool {
	if ctx.Err() != nil {
		return false
	}
	c.Advance(d)
	return true
}

type vaillantPublicationSelector struct {
	opcode   byte
	group    byte
	instance byte
	addr     uint16
}

type vaillantPublicationProgram struct {
	zone byte
	hc   byte
}

type vaillantPublicationTimer struct {
	program vaillantPublicationProgram
	weekday byte
	slot    byte
}

// vaillantPublicationSite is the synthetic installation. Writes change the
// stored registers and timers, so read-backs and later reads see them.
type vaillantPublicationSite struct {
	clock *vaillantPublicationClock

	mu       sync.Mutex
	b524     map[vaillantPublicationSelector][]byte
	b509     map[uint16][]byte
	configs  map[vaillantPublicationProgram][]byte
	slots    map[vaillantPublicationProgram][]byte
	timers   map[vaillantPublicationTimer][]byte
	failB524 map[vaillantPublicationSelector]bool
	failB509 map[uint16]bool
}

func vpF32(v float64) []byte {
	out := make([]byte, 4)
	binary.LittleEndian.PutUint32(out, math.Float32bits(float32(v)))
	return out
}

func vpU16(v uint16) []byte { return []byte{byte(v), byte(v >> 8)} }

func vpU32(v uint32) []byte {
	out := make([]byte, 4)
	binary.LittleEndian.PutUint32(out, v)
	return out
}

func vpCString(s string) []byte { return append([]byte(s), 0x00) }

func newVaillantPublicationSite() *vaillantPublicationSite {
	site := &vaillantPublicationSite{
		clock:    &vaillantPublicationClock{now: vaillantPublicationEpoch},
		b524:     make(map[vaillantPublicationSelector][]byte),
		b509:     make(map[uint16][]byte),
		configs:  make(map[vaillantPublicationProgram][]byte),
		slots:    make(map[vaillantPublicationProgram][]byte),
		timers:   make(map[vaillantPublicationTimer][]byte),
		failB524: make(map[vaillantPublicationSelector]bool),
		failB509: make(map[uint16]bool),
	}
	site.addRegulatorRegisters()
	site.addZoneRegisters()
	site.addCircuitRegisters()
	site.addFM5Registers()
	site.addRemoteRegisters()
	site.addBoilerRegisters()
	site.addTimerPrograms()
	return site
}

func (s *vaillantPublicationSite) local(group, instance byte, addr uint16, value []byte) {
	s.b524[vaillantPublicationSelector{opcode: vaillantB524OpcodeLocal, group: group, instance: instance, addr: addr}] = value
}

func (s *vaillantPublicationSite) remote(group, instance byte, addr uint16, value []byte) {
	s.b524[vaillantPublicationSelector{opcode: vaillantB524OpcodeRead, group: group, instance: instance, addr: addr}] = value
}

func (s *vaillantPublicationSite) addRegulatorRegisters() {
	g, i := localRegulator.group, regulatorInstance
	for addr, value := range map[uint16][]byte{
		system_off:                             {0x00},
		system_water_pressure:                  vpF32(1.625),
		system_flow_temperature:                vpF32(42.5),
		system_outdoor_temperature:             vpF32(7.25),
		system_outdoor_temperature_avg_24h:     vpF32(6.5),
		system_maintenance_due:                 {0x00},
		system_hwc_cylinder_temperature_top:    vpF32(55.5),
		system_hwc_cylinder_temperature_bottom: vpF32(35.25),
		system_adaptive_heating_curve:          {0x01},
		system_alternative_point:               vpF32(-5),
		system_heating_circuit_bivalence_point: vpF32(-3),
		system_dhw_bivalence_point:             vpF32(-2),
		system_hc_emergency_temperature:        vpF32(45),
		system_hwc_max_flow_temp_desired:       vpF32(70),
		system_max_room_humidity:               vpU16(60),
		system_maintenance_date:                {1, 6, 27},
		system_installer_name_1:                vpCString("Synthe"),
		system_installer_name_2:                vpCString("tic"),
		system_installer_phone_1:               vpCString("000000"),
		system_installer_phone_2:               vpCString("00"),
		system_installer_menu_code:             vpU16(17),
		system_scheme:                          vpU16(1),
		system_module_configuration_vr71:       vpU16(1),
	} {
		s.local(g, i, addr, value)
	}
	energy := []uint16{
		energy_fuel_sum_hc_this_month, energy_electricity_sum_hc_this_month,
		energy_electricity_sum_hwc_this_month, energy_fuel_sum_hwc_this_month,
		energy_fuel_sum_hc_last_month, energy_electricity_sum_hc_last_month,
		energy_electricity_sum_hwc_last_month, energy_fuel_sum_hwc_last_month,
		energy_fuel_sum_hc, energy_electricity_sum_hc, energy_electricity_sum_hwc, energy_fuel_sum_hwc,
	}
	for idx, addr := range energy {
		s.local(g, i, addr, vpU32(uint32(100*(idx+1)+idx)))
	}

	d := localDHW.group
	s.local(d, dhwInstance, dhw_current_temp, vpF32(48.5))
	s.local(d, dhwInstance, dhw_target_temp, vpF32(50))
	s.local(d, dhwInstance, dhw_operation_mode, vpU16(1))
	s.local(d, dhwInstance, dhw_special_function, vpU16(0))
	s.local(d, dhwInstance, dhw_holiday_start_date, []byte{10, 2, 26})
	s.local(d, dhwInstance, dhw_holiday_end_date, []byte{17, 2, 26})
}

func (s *vaillantPublicationSite) addZoneRegisters() {
	g := localZones.group
	for instance := byte(0x00); instance <= 0x08; instance++ {
		if instance > 0x01 {
			s.local(g, instance, zone_index, []byte{0xFF})
			continue
		}
		f := float64(instance)
		s.local(g, instance, zone_index, []byte{instance})
		s.local(g, instance, zone_current_temp, vpF32(20.5+f))
		s.local(g, instance, zone_target_temp, vpF32(21+f))
		s.local(g, instance, zone_fallback_manual_temp, vpF32(19.5))
		s.local(g, instance, zone_current_humidity, vpF32(45+f))
		s.local(g, instance, zone_quick_veto_temp, vpF32(22))
		s.local(g, instance, zone_quick_veto_duration, vpF32(3))
		s.local(g, instance, zone_holiday_setpoint, vpF32(12))
		s.local(g, instance, zone_special_function, vpU16(0))
		s.local(g, instance, zone_valve_status, vpU16(uint16(instance)+1))
		s.local(g, instance, zone_heating_op_mode, vpU16(1))
		s.local(g, instance, zone_room_temperature_zone_mapping_raw, vpU16(uint16(1-instance)))
		s.local(g, instance, zone_quick_veto_end_time, []byte{6, 30})
		s.local(g, instance, zone_quick_veto_end_date, []byte{6, 1, 26})
		s.local(g, instance, zone_holiday_start_date, []byte{10, 2, 26})
		s.local(g, instance, zone_holiday_end_date, []byte{17, 2, 26})
		s.local(g, instance, zone_holiday_start_time, []byte{8, 0})
		s.local(g, instance, zone_holiday_end_time, []byte{18, 0})
		s.local(g, instance, zone_name, vpCString(fmt.Sprintf("Zone %d", instance+1)))
		s.local(g, instance, zone_name_prefix, vpCString(""))
		s.local(g, instance, zone_name_suffix, vpCString(""))
	}
}

func (s *vaillantPublicationSite) addCircuitRegisters() {
	g := localCircuits.group
	for instance := byte(0x00); instance <= 0x01; instance++ {
		f := float64(instance)
		s.local(g, instance, circuit_type, vpU16(uint16(instance)+1))
		s.local(g, instance, circuit_cooling_enabled, []byte{0x00})
		s.local(g, instance, circuit_heating_curve, vpF32(1.2+f/10))
		s.local(g, instance, circuit_flow_temp_max, vpF32(75))
		s.local(g, instance, circuit_flow_temp_min, vpF32(25))
		s.local(g, instance, circuit_summer_limit, vpF32(21))
		s.local(g, instance, circuit_room_temp_control, vpU16(1))
		s.local(g, instance, circuit_special_function_mode, []byte{0x00})
		s.local(g, instance, circuit_flow_setpoint, vpF32(40+f))
		s.local(g, instance, circuit_flow_temp, vpF32(38.5+f))
		s.local(g, instance, circuit_pump_status, vpU16(1))
		s.local(g, instance, circuit_circuit_state, vpU16(1))
		s.local(g, instance, circuit_calc_flow_temp, vpF32(39))
		s.local(g, instance, circuit_mixer_position, vpF32(50))
		s.local(g, instance, circuit_humidity, vpF32(44))
		s.local(g, instance, circuit_dew_point, vpF32(9.5))
		s.local(g, instance, circuit_pump_hours, vpU32(1200+uint32(instance)))
		s.local(g, instance, circuit_pump_starts, vpU32(340+uint32(instance)))
	}
}

func (s *vaillantPublicationSite) addFM5Registers() {
	sg := localSolar.group
	s.local(sg, solarInstance, solar_enabled, []byte{0x01})
	s.local(sg, solarInstance, solar_function_mode, []byte{0x01})
	s.local(sg, solarInstance, solar_collector_temp, vpF32(55.5))
	s.local(sg, solarInstance, solar_return_temp, vpF32(30.25))
	s.local(sg, solarInstance, solar_pump_active, []byte{0x01})
	s.local(sg, solarInstance, solar_current_yield, vpF32(1.5))
	s.local(sg, solarInstance, solar_pump_hours, vpU32(1234))
	cg := localCylinders.group
	for instance := byte(0x00); instance <= 0x01; instance++ {
		f := float64(instance)
		s.local(cg, instance, cylinder_max_setpoint, vpF32(60-5*f))
		s.local(cg, instance, cylinder_charge_hysteresis, vpF32(5))
		s.local(cg, instance, cylinder_charge_offset, vpF32(2))
		s.local(cg, instance, cylinder_temperature, vpF32(52.5-12*f))
	}
}

// addRemoteRegisters answers the OP=0x06 slot namespace: one connected
// thermostat, one connected functional module, every other slot answering as
// not connected.
func (s *vaillantPublicationSite) addRemoteRegisters() {
	for _, grp := range remoteDeviceGroups {
		for instance := byte(0x00); instance <= 0x0A; instance++ {
			s.remote(grp.group, instance, device_slot_connected, []byte{0x00})
			s.remote(grp.group, instance, device_slot_class_address, []byte{0xFF})
			s.remote(grp.group, instance, device_slot_firmware, []byte{0xFF, 0xFF, 0xFF})
			s.remote(grp.group, instance, device_slot_hardware_identifier, []byte{0xFF, 0xFF})
		}
	}
	fm := remoteFunctionalModules.group
	s.remote(fm, 0x00, device_slot_connected, []byte{0x01})
	s.remote(fm, 0x00, device_slot_class_address, []byte{circuitManagingDeviceVR71Address})
	s.remote(fm, 0x00, device_slot_firmware, []byte{2, 1, 0})
	s.remote(fm, 0x00, device_slot_hardware_identifier, vpU16(0x0201))

	th := remoteThermostats.group
	s.remote(th, 0x01, device_slot_connected, []byte{0x01})
	s.remote(th, 0x01, device_slot_class_address, []byte{vaillantPublicationThermostatClass})
	s.remote(th, 0x01, device_slot_firmware, []byte{1, 2, 3})
	s.remote(th, 0x01, device_slot_hardware_identifier, vpU16(0x0102))
	s.remote(th, 0x01, device_slot_room_temperature, vpF32(20.75))
	s.remote(th, 0x01, device_slot_room_humidity, vpF32(48))
	s.remote(th, 0x01, device_slot_paired, []byte{0x01})
	s.remote(th, 0x01, device_slot_reception_strength, []byte{0x07})
	s.remote(th, 0x01, device_slot_remote_control_address, []byte{0x01})
	s.remote(th, 0x01, device_slot_zone_assignment, []byte{0x00})
}

func (s *vaillantPublicationSite) addBoilerRegisters() {
	s.b509[boiler_b509_water_pressure] = []byte{0x80, 0x01}     // DATA2b 1.5 bar
	s.b509[boiler_b509_flame_active] = []byte{0x0F}             // on
	s.b509[boiler_b509_flowset_hc_max_c] = []byte{0xB0, 0x04}   // DATA2c 75 °C
	s.b509[boiler_b509_flowset_hwc_max_c] = []byte{0x80, 0x03}  // DATA2c 56 °C
	s.b509[boiler_b509_dhw_temp_desired_c] = []byte{0x20, 0x03} // DATA2c 50 °C
	s.b509[boiler_b509_flow_temperature] = []byte{0x80, 0x02}   // DATA2c 40 °C
	s.b509[boiler_b509_phone_number] = []byte{0xFF}             // no BCD digit
}

// addTimerPrograms answers the zone 0 and zone 1 heating programs and the
// hot-water program; every other program reports the timer unavailable.
func (s *vaillantPublicationSite) addTimerPrograms() {
	programs := []vaillantPublicationProgram{
		{zone: 0x00, hc: b555HCHeating},
		{zone: 0x01, hc: b555HCHeating},
		{zone: b555ZoneDHW, hc: b555HCDHW},
	}
	for idx, program := range programs {
		hasTemp := byte(1)
		if program.hc == b555HCDHW {
			hasTemp = 0
		}
		// max_slots time_res min_dur has_temp temp_slots min_temp max_temp pad
		s.configs[program] = []byte{3, 10, 10, hasTemp, hasTemp, 5, 30, 0}
		s.slots[program] = []byte{1, 1, 1, 1, 1, 2, 2, 0}
		for weekday := byte(0); weekday < 7; weekday++ {
			temp := vpU16(uint16(200 + 5*idx))
			if hasTemp == 0 {
				temp = []byte{0xFF, 0xFF}
			}
			first := append([]byte{6, 0, 22, 0}, temp...)
			s.timers[vaillantPublicationTimer{program: program, weekday: weekday, slot: 0}] = first
			if weekday >= 5 {
				second := append([]byte{8, 30, 23, 0}, temp...)
				s.timers[vaillantPublicationTimer{program: program, weekday: weekday, slot: 1}] = second
			}
		}
	}
}

// failReads marks the selectors the site stops answering.
func (s *vaillantPublicationSite) failReads(b524 []vaillantPublicationSelector, b509 []uint16) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, selector := range b524 {
		s.failB524[selector] = true
	}
	for _, addr := range b509 {
		s.failB509[addr] = true
	}
}

// send is the poller's sendFrameFn: every B524 and B509 frame arrives here.
func (s *vaillantPublicationSite) send(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.clock.Advance(vaillantPublicationFrameCost)
	reply, ok := s.respond(frame)
	if !ok {
		return nil, fmt.Errorf("synthetic site: no answer from 0x%02X: %w", frame.Target, ebuserrors.ErrTimeout)
	}
	return &protocol.Frame{Source: frame.Target, Target: frame.Source, Primary: frame.Primary, Secondary: frame.Secondary, Data: reply}, nil
}

func (s *vaillantPublicationSite) respond(frame protocol.Frame) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case frame.Primary == vaillantExtRegisterPrimary && frame.Secondary == vaillantExtRegisterSecondary:
		if frame.Target != vaillantPublicationController {
			return nil, false
		}
		return s.respondB524(frame.Data)
	case frame.Primary == vaillantB509Primary && frame.Secondary == vaillantB509Secondary:
		if frame.Target != vaillantPublicationBoiler {
			return nil, false
		}
		return s.respondB509(frame.Data)
	case frame.Primary == vaillantB555Primary && frame.Secondary == vaillantB555Secondary:
		if frame.Target != vaillantPublicationController {
			return nil, false
		}
		return s.respondB555(frame.Data), true
	}
	return nil, false
}

// respondB524 answers a read with the stored value, or with the single 0x00
// byte of an unknown register, and stores the value of a write.
func (s *vaillantPublicationSite) respondB524(data []byte) ([]byte, bool) {
	if len(data) < 6 {
		return []byte{0x00}, true
	}
	selector := vaillantPublicationSelector{opcode: data[0], group: data[2], instance: data[3], addr: uint16(data[4]) | uint16(data[5])<<8}
	if data[1] == vaillantB524OpWrite {
		s.b524[selector] = slices.Clone(data[6:])
		return []byte{0x01}, true
	}
	if s.failB524[selector] {
		return nil, false
	}
	value, ok := s.b524[selector]
	if !ok {
		return []byte{0x00}, true
	}
	reply := []byte{0x01, selector.instance, selector.group, data[4], data[5]}
	return append(reply, value...), true
}

// respondB509 answers a read with the stored value or two zero bytes, and
// stores the value of a write.
func (s *vaillantPublicationSite) respondB509(data []byte) ([]byte, bool) {
	if len(data) < 3 {
		return nil, false
	}
	addr := uint16(data[1])<<8 | uint16(data[2])
	switch data[0] {
	case vaillantB509OpcodeWrite:
		s.b509[addr] = slices.Clone(data[3:])
		return []byte{0x01}, true
	case vaillantB509OpcodeRead:
		if s.failB509[addr] {
			return nil, false
		}
		value, ok := s.b509[addr]
		if !ok {
			value = []byte{0x00, 0x00}
		}
		return append([]byte{vaillantB509OpcodeRead, data[1], data[2]}, value...), true
	}
	return nil, false
}

// respondB555 answers the timer protocol. Status 0x03 reports a timer the
// site does not have.
func (s *vaillantPublicationSite) respondB555(data []byte) []byte {
	unavailable := []byte{0x03, 0, 0, 0, 0, 0, 0, 0, 0}
	if len(data) < 3 {
		return unavailable
	}
	program := vaillantPublicationProgram{zone: data[1], hc: data[2]}
	switch data[0] {
	case b555OpcodeConfigRead:
		if config, ok := s.configs[program]; ok {
			return append([]byte{0x00}, config...)
		}
	case b555OpcodeSlotsRead:
		if slotsPerDay, ok := s.slots[program]; ok {
			return append([]byte{0x00}, slotsPerDay...)
		}
	case b555OpcodeTimerRead:
		if len(data) >= 5 {
			if timer, ok := s.timers[vaillantPublicationTimer{program: program, weekday: data[3], slot: data[4]}]; ok {
				return append([]byte{0x00}, timer...)
			}
		}
	case b555OpcodeTimerWrite:
		if _, ok := s.configs[program]; ok && len(data) >= 12 {
			key := vaillantPublicationTimer{program: program, weekday: data[3], slot: data[4]}
			s.timers[key] = slices.Clone(data[6:12])
			if slotsPerDay := s.slots[program]; int(data[3]) < len(slotsPerDay) {
				slotsPerDay[data[3]] = data[5]
			}
			return []byte{0x00}
		}
	}
	return unavailable
}

// vaillantPublicationWire is the plain raw transport under the gateway bus.
type vaillantPublicationWire struct {
	site   *vaillantPublicationSite
	notify chan struct{}

	mu       sync.Mutex
	pending  []byte
	telegram []byte
	escaped  bool
	answered bool
}

func newVaillantPublicationWire(site *vaillantPublicationSite) *vaillantPublicationWire {
	return &vaillantPublicationWire{site: site, notify: make(chan struct{}, 1)}
}

func (w *vaillantPublicationWire) take() (byte, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.pending) == 0 {
		return 0, false
	}
	b := w.pending[0]
	w.pending = w.pending[1:]
	return b, true
}

// ReadByte returns the next echoed or answered symbol. An idle wire reports
// a timeout after a short wait so the bus loop does not spin.
func (w *vaillantPublicationWire) ReadByte() (byte, error) {
	if b, ok := w.take(); ok {
		return b, nil
	}
	select {
	case <-w.notify:
	case <-time.After(5 * time.Millisecond):
	}
	if b, ok := w.take(); ok {
		return b, nil
	}
	return 0, ebuserrors.ErrTimeout
}

func (w *vaillantPublicationWire) Write(payload []byte) (int, error) {
	w.mu.Lock()
	for _, b := range payload {
		w.pending = append(w.pending, b)
		w.feed(b)
	}
	w.mu.Unlock()
	select {
	case w.notify <- struct{}{}:
	default:
	}
	return len(payload), nil
}

func (w *vaillantPublicationWire) Close() error { return nil }

// feed parses the initiator telegram QQ ZZ PB SB NN DATA CRC; a SYN ends the
// exchange. Callers hold w.mu.
func (w *vaillantPublicationWire) feed(b byte) {
	if b == protocol.SymbolSyn {
		w.telegram, w.escaped, w.answered = w.telegram[:0], false, false
		return
	}
	if w.answered {
		return
	}
	if w.escaped {
		w.escaped = false
		b = map[bool]byte{true: protocol.SymbolEscape, false: protocol.SymbolSyn}[b == 0x00]
	} else if b == protocol.SymbolEscape {
		w.escaped = true
		return
	}
	w.telegram = append(w.telegram, b)
	if len(w.telegram) < 5 || len(w.telegram) != 5+int(w.telegram[4])+1 {
		return
	}
	w.answered = true
	t := w.telegram
	frame := protocol.Frame{Source: t[0], Target: t[1], Primary: t[2], Secondary: t[3], Data: slices.Clone(t[5 : len(t)-1])}
	w.site.clock.Advance(vaillantPublicationFrameCost)
	reply, ok := w.site.respond(frame)
	if !ok {
		return
	}
	segment := append([]byte{byte(len(reply))}, reply...)
	w.pending = append(w.pending, protocol.SymbolAck)
	for _, symbol := range append(segment, protocol.CRC(segment)) {
		switch symbol {
		case protocol.SymbolEscape:
			w.pending = append(w.pending, protocol.SymbolEscape, 0x00)
		case protocol.SymbolSyn:
			w.pending = append(w.pending, protocol.SymbolEscape, 0x01)
		default:
			w.pending = append(w.pending, symbol)
		}
	}
}
