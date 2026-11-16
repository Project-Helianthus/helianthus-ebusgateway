package main

import (
	"context"
	"fmt"
	"math"
)

// The B555 reads and the time-program writes of the poller use the native
// schedule types below; the publication owner maps them to the GraphQL
// schedules and the writer adapters map them to the MCP time-program forms.

// vaillantScheduleTimerSlot is one timer slot read from B555 A5.
// TemperatureRaw holds the raw deci-degree value; both temperature fields
// are nil when the slot carries the unavailable sentinel.
type vaillantScheduleTimerSlot struct {
	StartHour      int
	StartMinute    int
	EndHour        int
	EndMinute      int
	TemperatureC   *float64
	TemperatureRaw *int
}

// vaillantScheduleDay is the timer slots of one weekday of a program.
type vaillantScheduleDay struct {
	Weekday string
	Slots   []vaillantScheduleTimerSlot
}

// vaillantScheduleConfig is the B555 A3 configuration of a program.
type vaillantScheduleConfig struct {
	MaxSlots       int
	TimeResolution int
	MinDuration    int
	HasTemperature bool
	TempSlots      int
	MinTempC       *float64
	MaxTempC       *float64
}

// vaillantScheduleProgram is one (zone, heating circuit kind) program with
// its configuration, slots per day and day programs.
type vaillantScheduleProgram struct {
	Zone      int
	HC        string
	Config    *vaillantScheduleConfig
	SlotsUsed []int
	Days      []vaillantScheduleDay
}

// vaillantTimeProgramSlot is one slot of a time-program write request.
type vaillantTimeProgramSlot struct {
	StartHour    int
	StartMinute  int
	EndHour      int
	EndMinute    int
	TemperatureC *float64
}

// vaillantTimeProgramSlotResult is the B555 outcome of one written slot.
type vaillantTimeProgramSlotResult struct {
	SlotIndex int
	Accepted  bool
	ErrorCode int
	ErrorDesc string
}

// vaillantTimeProgramWriteResult is the outcome of a time-program write.
type vaillantTimeProgramWriteResult struct {
	Success     bool
	SlotResults []vaillantTimeProgramSlotResult
	Error       string
}

// vaillantScheduleWriter writes zone and hot water time programs in native
// form; the poller implements it.
type vaillantScheduleWriter interface {
	SetZoneTimeProgram(ctx context.Context, zone int, weekday int, slots []vaillantTimeProgramSlot) (*vaillantTimeProgramWriteResult, error)
	SetDhwTimeProgram(ctx context.Context, weekday int, slots []vaillantTimeProgramSlot) (*vaillantTimeProgramWriteResult, error)
}

var b555ErrorDescriptions = map[byte]string{
	0x00: "accepted",
	0x01: "parameter out of range",
	0x03: "timer unavailable",
	0x06: "validation failure",
}

func b555ErrorDesc(code byte) string {
	if desc, ok := b555ErrorDescriptions[code]; ok {
		return desc
	}
	return fmt.Sprintf("unknown error 0x%02X", code)
}

func (p *vaillantSemanticPoller) setZoneTimeProgram(ctx context.Context, zone int, weekday int, slots []vaillantTimeProgramSlot) (*vaillantTimeProgramWriteResult, error) {
	if p == nil {
		return nil, fmt.Errorf("schedule writer unavailable")
	}
	if zone < 0 || zone > 2 {
		return &vaillantTimeProgramWriteResult{Success: false, Error: "zone must be 0-2"}, nil
	}
	if weekday < 0 || weekday > 6 {
		return &vaillantTimeProgramWriteResult{Success: false, Error: "weekday must be 0-6"}, nil
	}
	if len(slots) == 0 {
		return &vaillantTimeProgramWriteResult{Success: false, Error: "slots array must not be empty"}, nil
	}

	cfg := p.readB555Config(ctx, byte(zone), b555HCHeating)
	if cfg == nil {
		return &vaillantTimeProgramWriteResult{Success: false, Error: "unable to read B555 config for zone heating"}, nil
	}

	maxSlots := cfg.MaxSlots
	if maxSlots <= 0 {
		maxSlots = 3
	}
	if len(slots) > maxSlots {
		return &vaillantTimeProgramWriteResult{
			Success: false,
			Error:   fmt.Sprintf("slot count %d exceeds max_slots %d", len(slots), maxSlots),
		}, nil
	}

	minTemp := 5.0
	maxTemp := 30.0
	if cfg.MinTempC != nil {
		minTemp = *cfg.MinTempC
	}
	if cfg.MaxTempC != nil {
		maxTemp = *cfg.MaxTempC
	}

	for i, slot := range slots {
		if slot.StartHour < 0 || slot.StartHour > 24 {
			return &vaillantTimeProgramWriteResult{Success: false, Error: fmt.Sprintf("slot %d: start_hour %d out of range 0-24", i, slot.StartHour)}, nil
		}
		if slot.StartMinute < 0 || slot.StartMinute > 59 {
			return &vaillantTimeProgramWriteResult{Success: false, Error: fmt.Sprintf("slot %d: start_minute %d out of range 0-59", i, slot.StartMinute)}, nil
		}
		if slot.EndHour < 0 || slot.EndHour > 24 {
			return &vaillantTimeProgramWriteResult{Success: false, Error: fmt.Sprintf("slot %d: end_hour %d out of range 0-24", i, slot.EndHour)}, nil
		}
		if slot.EndMinute < 0 || slot.EndMinute > 59 {
			return &vaillantTimeProgramWriteResult{Success: false, Error: fmt.Sprintf("slot %d: end_minute %d out of range 0-59", i, slot.EndMinute)}, nil
		}
		if slot.TemperatureC == nil {
			return &vaillantTimeProgramWriteResult{Success: false, Error: fmt.Sprintf("slot %d: temperature_c is required for heating", i)}, nil
		}
		if *slot.TemperatureC < minTemp || *slot.TemperatureC > maxTemp {
			return &vaillantTimeProgramWriteResult{Success: false, Error: fmt.Sprintf("slot %d: temperature_c %.1f out of range [%.0f, %.0f]", i, *slot.TemperatureC, minTemp, maxTemp)}, nil
		}
	}

	result := &vaillantTimeProgramWriteResult{
		Success:     true,
		SlotResults: make([]vaillantTimeProgramSlotResult, len(slots)),
	}

	for i, slot := range slots {
		tempRaw := uint16(math.Round(*slot.TemperatureC * 10))
		code, err := p.writeB555TimerSlot(ctx,
			byte(zone), b555HCHeating, byte(weekday),
			byte(i), byte(len(slots)),
			byte(slot.StartHour), byte(slot.StartMinute),
			byte(slot.EndHour), byte(slot.EndMinute),
			tempRaw,
		)
		if err != nil {
			result.SlotResults[i] = vaillantTimeProgramSlotResult{
				SlotIndex: i,
				Accepted:  false,
				ErrorCode: 0xFF,
				ErrorDesc: err.Error(),
			}
			result.Success = false
			continue
		}
		accepted := code == 0x00
		if !accepted {
			result.Success = false
		}
		result.SlotResults[i] = vaillantTimeProgramSlotResult{
			SlotIndex: i,
			Accepted:  accepted,
			ErrorCode: int(code),
			ErrorDesc: b555ErrorDesc(code),
		}
	}

	go p.refreshScheduleForProgram(context.Background(), byte(zone), b555HCHeating, byte(weekday))

	return result, nil
}

func (p *vaillantSemanticPoller) setDhwTimeProgram(ctx context.Context, weekday int, slots []vaillantTimeProgramSlot) (*vaillantTimeProgramWriteResult, error) {
	if p == nil {
		return nil, fmt.Errorf("schedule writer unavailable")
	}
	if weekday < 0 || weekday > 6 {
		return &vaillantTimeProgramWriteResult{Success: false, Error: "weekday must be 0-6"}, nil
	}
	if len(slots) == 0 {
		return &vaillantTimeProgramWriteResult{Success: false, Error: "slots array must not be empty"}, nil
	}

	cfg := p.readB555Config(ctx, b555ZoneDHW, b555HCDHW)
	if cfg == nil {
		return &vaillantTimeProgramWriteResult{Success: false, Error: "unable to read B555 config for DHW"}, nil
	}

	maxSlots := cfg.MaxSlots
	if maxSlots <= 0 {
		maxSlots = 3
	}
	if len(slots) > maxSlots {
		return &vaillantTimeProgramWriteResult{
			Success: false,
			Error:   fmt.Sprintf("slot count %d exceeds max_slots %d", len(slots), maxSlots),
		}, nil
	}

	minTemp := 35.0
	maxTemp := 65.0
	if cfg.MinTempC != nil {
		minTemp = *cfg.MinTempC
	}
	if cfg.MaxTempC != nil {
		maxTemp = *cfg.MaxTempC
	}

	for i, slot := range slots {
		if slot.StartHour < 0 || slot.StartHour > 24 {
			return &vaillantTimeProgramWriteResult{Success: false, Error: fmt.Sprintf("slot %d: start_hour %d out of range 0-24", i, slot.StartHour)}, nil
		}
		if slot.StartMinute < 0 || slot.StartMinute > 59 {
			return &vaillantTimeProgramWriteResult{Success: false, Error: fmt.Sprintf("slot %d: start_minute %d out of range 0-59", i, slot.StartMinute)}, nil
		}
		if slot.EndHour < 0 || slot.EndHour > 24 {
			return &vaillantTimeProgramWriteResult{Success: false, Error: fmt.Sprintf("slot %d: end_hour %d out of range 0-24", i, slot.EndHour)}, nil
		}
		if slot.EndMinute < 0 || slot.EndMinute > 59 {
			return &vaillantTimeProgramWriteResult{Success: false, Error: fmt.Sprintf("slot %d: end_minute %d out of range 0-59", i, slot.EndMinute)}, nil
		}
		if slot.TemperatureC != nil {
			if *slot.TemperatureC < minTemp || *slot.TemperatureC > maxTemp {
				return &vaillantTimeProgramWriteResult{Success: false, Error: fmt.Sprintf("slot %d: temperature_c %.1f out of range [%.0f, %.0f]", i, *slot.TemperatureC, minTemp, maxTemp)}, nil
			}
		}
	}

	result := &vaillantTimeProgramWriteResult{
		Success:     true,
		SlotResults: make([]vaillantTimeProgramSlotResult, len(slots)),
	}

	for i, slot := range slots {
		var tempRaw uint16
		if slot.TemperatureC != nil {
			tempRaw = uint16(math.Round(*slot.TemperatureC * 10))
		} else {
			tempRaw = 0xFFFF
		}
		code, err := p.writeB555TimerSlot(ctx,
			b555ZoneDHW, b555HCDHW, byte(weekday),
			byte(i), byte(len(slots)),
			byte(slot.StartHour), byte(slot.StartMinute),
			byte(slot.EndHour), byte(slot.EndMinute),
			tempRaw,
		)
		if err != nil {
			result.SlotResults[i] = vaillantTimeProgramSlotResult{
				SlotIndex: i,
				Accepted:  false,
				ErrorCode: 0xFF,
				ErrorDesc: err.Error(),
			}
			result.Success = false
			continue
		}
		accepted := code == 0x00
		if !accepted {
			result.Success = false
		}
		result.SlotResults[i] = vaillantTimeProgramSlotResult{
			SlotIndex: i,
			Accepted:  accepted,
			ErrorCode: int(code),
			ErrorDesc: b555ErrorDesc(code),
		}
	}

	go p.refreshScheduleForProgram(context.Background(), b555ZoneDHW, b555HCDHW, byte(weekday))

	return result, nil
}

func (p *vaillantSemanticPoller) SetZoneTimeProgram(ctx context.Context, zone int, weekday int, slots []vaillantTimeProgramSlot) (*vaillantTimeProgramWriteResult, error) {
	return p.setZoneTimeProgram(ctx, zone, weekday, slots)
}

func (p *vaillantSemanticPoller) SetDhwTimeProgram(ctx context.Context, weekday int, slots []vaillantTimeProgramSlot) (*vaillantTimeProgramWriteResult, error) {
	return p.setDhwTimeProgram(ctx, weekday, slots)
}

// refreshScheduleForProgram rereads one weekday of a program after a write
// and hands it to the publication owner.
func (p *vaillantSemanticPoller) refreshScheduleForProgram(ctx context.Context, zone, hc, weekday byte) {
	if p == nil || p.bus == nil || p.liveProvider() == nil {
		return
	}

	p.mu.Lock()
	controller := p.controller
	p.mu.Unlock()
	if controller == 0 {
		return
	}

	cfg := p.readB555Config(ctx, zone, hc)
	if cfg == nil {
		return
	}

	maxSlots := cfg.MaxSlots
	if maxSlots <= 0 {
		maxSlots = 3
	}

	slotsPerDay, _ := p.readB555Slots(ctx, zone, hc)

	slotCount := maxSlots
	if slotsPerDay != nil && int(weekday) < len(slotsPerDay) && slotsPerDay[weekday] < slotCount {
		slotCount = slotsPerDay[weekday]
	}

	var timerSlots []vaillantScheduleTimerSlot
	for ss := 0; ss < slotCount; ss++ {
		slot := p.readB555Timer(ctx, zone, hc, weekday, byte(ss))
		if slot == nil {
			break
		}
		timerSlots = append(timerSlots, *slot)
	}

	hcName := b555HCNames[hc]
	if hcName == "" {
		hcName = fmt.Sprintf("unknown_%d", hc)
	}
	zoneIdx := int(zone)
	if zone == b555ZoneDHW {
		zoneIdx = 255
	}

	p.publication.publishScheduleDay(zoneIdx, hcName, int(weekday), vaillantScheduleDay{
		Weekday: b555WeekdayNames[weekday],
		Slots:   timerSlots,
	}, slotsPerDay)
}

// readB555Config reads a B555 A3 config response for a zone+HC.
func (p *vaillantSemanticPoller) readB555Config(ctx context.Context, zone, hc byte) *vaillantScheduleConfig {
	data, ok := p.readB555Frame(ctx, []byte{b555OpcodeConfigRead, zone, hc})
	if !ok || len(data) < 9 {
		return nil
	}
	status := data[0]
	if status != 0x00 {
		return nil
	}
	cfg := &vaillantScheduleConfig{
		MaxSlots:       int(data[1]),
		TimeResolution: int(data[2]),
		MinDuration:    int(data[3]),
		HasTemperature: data[4] != 0,
		TempSlots:      int(data[5]),
	}
	// A3 layout: status(1)+max_slots(1)+time_res(1)+min_dur(1)+has_temp(1)+temp_slots(1)+min_temp(1)+max_temp(1)+pad(1) = 9 bytes
	// min_temp and max_temp are UCH (whole °C), sentinel 0xFF = unavailable.
	if data[6] != 0xFF {
		v := float64(data[6])
		cfg.MinTempC = &v
	}
	if data[7] != 0xFF {
		v := float64(data[7])
		cfg.MaxTempC = &v
	}
	return cfg
}

// readB555Slots reads B555 A4 slots-per-day response for a zone+HC.
func (p *vaillantSemanticPoller) readB555Slots(ctx context.Context, zone, hc byte) ([]int, bool) {
	data, ok := p.readB555Frame(ctx, []byte{b555OpcodeSlotsRead, zone, hc})
	if !ok || len(data) < 9 {
		return nil, false
	}
	status := data[0]
	if status != 0x00 {
		return nil, false
	}
	// A4 layout: status(1)+Mon(1)+Tue(1)+Wed(1)+Thu(1)+Fri(1)+Sat(1)+Sun(1)+pad(1) = 9 bytes
	slots := make([]int, 7)
	for i := 0; i < 7 && i+1 < len(data); i++ {
		slots[i] = int(data[i+1])
	}
	return slots, true
}

// readB555Timer reads a single timer slot from B555 A5.
func (p *vaillantSemanticPoller) readB555Timer(ctx context.Context, zone, hc, weekday, slotIdx byte) *vaillantScheduleTimerSlot {
	data, ok := p.readB555Frame(ctx, []byte{b555OpcodeTimerRead, zone, hc, weekday, slotIdx})
	if !ok || len(data) < 7 {
		return nil
	}
	status := data[0]
	if status != 0x00 {
		return nil
	}
	slot := &vaillantScheduleTimerSlot{
		StartHour:   int(data[1]),
		StartMinute: int(data[2]),
		EndHour:     int(data[3]),
		EndMinute:   int(data[4]),
	}
	tempRaw := uint16(data[5]) | uint16(data[6])<<8
	if tempRaw != 0xFFFF {
		v := float64(tempRaw) / 10.0
		slot.TemperatureC = &v
		rawInt := int(tempRaw)
		slot.TemperatureRaw = &rawInt
	}
	return slot
}

// b555ProgramSpec defines a (zone, hc) combination to poll.
type b555ProgramSpec struct {
	zone byte
	hc   byte
}

// refreshSchedules polls B555 timer schedules and hands the native programs
// to the publication owner.
func (p *vaillantSemanticPoller) refreshSchedules(ctx context.Context) {
	if p == nil || p.bus == nil || p.liveProvider() == nil {
		return
	}

	p.mu.Lock()
	controller := p.controller
	p.mu.Unlock()
	if controller == 0 {
		return
	}

	// Define which (zone, HC) combinations to poll.
	// Per-zone programs: zone 0-2 × heating/cooling
	// Zone-agnostic programs: 0xFF × DHW/CC/Silent
	specs := []b555ProgramSpec{
		{zone: 0x00, hc: b555HCHeating},
		{zone: 0x01, hc: b555HCHeating},
		{zone: 0x02, hc: b555HCHeating},
		{zone: 0x00, hc: b555HCCooling},
		{zone: 0x01, hc: b555HCCooling},
		{zone: 0x02, hc: b555HCCooling},
		{zone: b555ZoneDHW, hc: b555HCDHW},
		{zone: b555ZoneDHW, hc: b555HCCC},
		{zone: b555ZoneDHW, hc: b555HCSilent},
	}

	var programs []vaillantScheduleProgram

	for _, spec := range specs {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Step 1: Read config (A3)
		cfg := p.readB555Config(ctx, spec.zone, spec.hc)
		if cfg == nil {
			continue
		}

		hcName := b555HCNames[spec.hc]
		if hcName == "" {
			hcName = fmt.Sprintf("unknown_%d", spec.hc)
		}
		zoneIdx := int(spec.zone)
		if spec.zone == b555ZoneDHW {
			zoneIdx = 255
		}

		prog := vaillantScheduleProgram{
			Zone:   zoneIdx,
			HC:     hcName,
			Config: cfg,
		}

		// Step 2: Read slots per day (A4)
		slotsPerDay, ok := p.readB555Slots(ctx, spec.zone, spec.hc)
		if ok {
			prog.SlotsUsed = slotsPerDay
		}

		// Step 3: Read timer entries (A5) for each day/slot
		maxSlots := cfg.MaxSlots
		if maxSlots <= 0 {
			maxSlots = 3
		}

		days := make([]vaillantScheduleDay, 7)
		for dd := byte(0); dd < 7; dd++ {
			select {
			case <-ctx.Done():
				return
			default:
			}

			day := vaillantScheduleDay{
				Weekday: b555WeekdayNames[dd],
			}

			slotCount := maxSlots
			if ok && int(dd) < len(slotsPerDay) && slotsPerDay[dd] < slotCount {
				slotCount = slotsPerDay[dd]
			}

			for ss := 0; ss < slotCount; ss++ {
				slot := p.readB555Timer(ctx, spec.zone, spec.hc, dd, byte(ss))
				if slot == nil {
					break
				}
				day.Slots = append(day.Slots, *slot)
			}
			days[dd] = day
		}
		prog.Days = days

		programs = append(programs, prog)
	}

	if len(programs) > 0 {
		p.publication.publishSchedulePrograms(programs)
	}
}
