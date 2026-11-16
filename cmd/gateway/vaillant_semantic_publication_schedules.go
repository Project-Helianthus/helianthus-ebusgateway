package main

import (
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

// The publication owner maps the native schedule programs that the poller
// reads over B555 to the GraphQL schedules of the provider. The writer
// adapters map MCP time-program slots to native slots and the native write
// result back to its MCP form. A nil slice stays nil in every mapping so the
// published and returned forms are unchanged.

// publishSchedulePrograms replaces the provider's schedules with the given
// native programs.
func (o *vaillantSemanticPublication) publishSchedulePrograms(programs []vaillantScheduleProgram) {
	if o == nil || o.provider == nil {
		return
	}
	o.provider.SetSchedules(&graphql.ScheduleStatus{Programs: scheduleProgramsFromNative(programs)})
}

// publishScheduleDay replaces one weekday, and the slots per day when they
// were read, of the published program that matches zone and hc. It publishes
// nothing when the provider holds no schedules or no matching program.
func (o *vaillantSemanticPublication) publishScheduleDay(zone int, hc string, weekday int, day vaillantScheduleDay, slotsUsed []int) {
	if o == nil || o.provider == nil {
		return
	}
	status := o.provider.Schedules()
	if status == nil {
		return
	}
	for i, prog := range status.Programs {
		if prog.Zone != zone || prog.HC != hc {
			continue
		}
		if weekday < len(prog.Days) {
			status.Programs[i].Days[weekday] = scheduleDayFromNative(day)
		}
		if slotsUsed != nil {
			status.Programs[i].SlotsUsed = slotsUsed
		}
		o.provider.SetSchedules(status)
		return
	}
}

func scheduleProgramsFromNative(programs []vaillantScheduleProgram) []graphql.ScheduleProgram {
	if programs == nil {
		return nil
	}
	out := make([]graphql.ScheduleProgram, len(programs))
	for i, prog := range programs {
		out[i] = graphql.ScheduleProgram{
			Zone:      prog.Zone,
			HC:        prog.HC,
			Config:    scheduleConfigFromNative(prog.Config),
			SlotsUsed: prog.SlotsUsed,
		}
		if prog.Days != nil {
			out[i].Days = make([]graphql.ScheduleDayProgram, len(prog.Days))
			for d, day := range prog.Days {
				out[i].Days[d] = scheduleDayFromNative(day)
			}
		}
	}
	return out
}

func scheduleConfigFromNative(cfg *vaillantScheduleConfig) *graphql.ScheduleConfig {
	if cfg == nil {
		return nil
	}
	return &graphql.ScheduleConfig{
		MaxSlots:       cfg.MaxSlots,
		TimeResolution: cfg.TimeResolution,
		MinDuration:    cfg.MinDuration,
		HasTemperature: cfg.HasTemperature,
		TempSlots:      cfg.TempSlots,
		MinTempC:       cloneFloatPtr(cfg.MinTempC),
		MaxTempC:       cloneFloatPtr(cfg.MaxTempC),
	}
}

func scheduleDayFromNative(day vaillantScheduleDay) graphql.ScheduleDayProgram {
	out := graphql.ScheduleDayProgram{Weekday: day.Weekday}
	if day.Slots != nil {
		out.Slots = make([]graphql.ScheduleTimerSlot, len(day.Slots))
		for i, slot := range day.Slots {
			out.Slots[i] = graphql.ScheduleTimerSlot{
				StartHour:      slot.StartHour,
				StartMinute:    slot.StartMinute,
				EndHour:        slot.EndHour,
				EndMinute:      slot.EndMinute,
				TemperatureC:   cloneFloatPtr(slot.TemperatureC),
				TemperatureRaw: cloneIntPtr(slot.TemperatureRaw),
			}
		}
	}
	return out
}

// timeProgramSlotsFromMCP maps the MCP slots of a time-program write request
// to native slots.
func timeProgramSlotsFromMCP(slots []mcp.TimeProgramSlot) []vaillantTimeProgramSlot {
	if slots == nil {
		return nil
	}
	out := make([]vaillantTimeProgramSlot, len(slots))
	for i, slot := range slots {
		out[i] = vaillantTimeProgramSlot{
			StartHour:    slot.StartHour,
			StartMinute:  slot.StartMinute,
			EndHour:      slot.EndHour,
			EndMinute:    slot.EndMinute,
			TemperatureC: slot.TemperatureC,
		}
	}
	return out
}

// mcpTimeProgramWriteResult maps a native time-program write result and its
// error to the MCP form; a nil result stays nil.
func mcpTimeProgramWriteResult(result *vaillantTimeProgramWriteResult, err error) (*mcp.TimeProgramWriteResult, error) {
	if result == nil {
		return nil, err
	}
	out := &mcp.TimeProgramWriteResult{Success: result.Success, Error: result.Error}
	if result.SlotResults != nil {
		out.SlotResults = make([]mcp.TimeProgramSlotResult, len(result.SlotResults))
		for i, slot := range result.SlotResults {
			out.SlotResults[i] = mcp.TimeProgramSlotResult{
				SlotIndex: slot.SlotIndex,
				Accepted:  slot.Accepted,
				ErrorCode: slot.ErrorCode,
				ErrorDesc: slot.ErrorDesc,
			}
		}
	}
	return out, err
}
