package main

import (
	"context"
	"encoding/binary"
	"log"
	"time"
)

// --- Energy registers: GG=0x00/OP=0x02 (shared with system/regulator) ---
// energy4 type = ULG (unsigned 32-bit LE) in kWh.
const (
	// STATE registers (read-only counters)
	energy_fuel_sum_hc                    = uint16(0x0056) // PrFuelSumHc: total gas consumption heating
	energy_electricity_sum_hc             = uint16(0x0057) // PrEnergySumHc: total electricity consumption heating
	energy_electricity_sum_hwc            = uint16(0x0058) // PrEnergySumHwc: total electricity consumption hot water
	energy_fuel_sum_hwc                   = uint16(0x0059) // PrFuelSumHwc: total gas consumption hot water
	energy_fuel_sum_hc_this_month         = uint16(0x004E) // PrFuelSumHcThisMonth: gas heating this month
	energy_electricity_sum_hc_this_month  = uint16(0x004F) // PrEnergySumHcThisMonth: electricity heating this month
	energy_electricity_sum_hwc_this_month = uint16(0x0050) // PrEnergySumHwcThisMonth: electricity hot water this month
	energy_fuel_sum_hwc_this_month        = uint16(0x0051) // PrFuelSumHwcThisMonth: gas hot water this month
	energy_fuel_sum_hc_last_month         = uint16(0x0052) // PrFuelSumHcLastMonth: gas heating last month
	energy_electricity_sum_hc_last_month  = uint16(0x0053) // PrEnergySumHcLastMonth: electricity heating last month
	energy_electricity_sum_hwc_last_month = uint16(0x0054) // PrEnergySumHwcLastMonth: electricity hot water last month
	energy_fuel_sum_hwc_last_month        = uint16(0x0055) // PrFuelSumHwcLastMonth: gas hot water last month
)

type b524EnergyQuery struct {
	addr     uint16 // B5.24 register address
	channel  string // EnergyMergeKey.Channel
	usage    string // EnergyMergeKey.Usage
	period   string // EnergyMergeKey.Period ("year", "month")
	yearKind string // relative window label ("current", "previous"), resolved to a dated EnergyMergeKey via provider.EnergyWindowKey at read time
}

var b524EnergyQueries = []b524EnergyQuery{
	// All-time (lifetime) totals: undated "all" window, never keyed by the
	// reference year; projected to the current yearly slot by the provider.
	{energy_fuel_sum_hc, "gas", "climate", "all", ""},
	{energy_fuel_sum_hwc, "gas", "hot_water", "all", ""},
	{energy_electricity_sum_hc, "electricity", "climate", "all", ""},
	{energy_electricity_sum_hwc, "electricity", "hot_water", "all", ""},
	// Monthly: this month (month/current).
	{energy_fuel_sum_hc_this_month, "gas", "climate", "month", "current"},
	{energy_fuel_sum_hwc_this_month, "gas", "hot_water", "month", "current"},
	{energy_electricity_sum_hc_this_month, "electricity", "climate", "month", "current"},
	{energy_electricity_sum_hwc_this_month, "electricity", "hot_water", "month", "current"},
	// Monthly: last month (month/previous).
	{energy_fuel_sum_hc_last_month, "gas", "climate", "month", "previous"},
	{energy_fuel_sum_hwc_last_month, "gas", "hot_water", "month", "previous"},
	{energy_electricity_sum_hc_last_month, "electricity", "climate", "month", "previous"},
	{energy_electricity_sum_hwc_last_month, "electricity", "hot_water", "month", "previous"},
}

func (p *vaillantSemanticPoller) refreshEnergy(ctx context.Context) {
	if p == nil || p.provider == nil {
		return
	}

	p.mu.Lock()
	controller := p.controller
	p.mu.Unlock()
	if controller == 0 {
		p.refreshFM5Semantic(ctx)
		return
	}

	p.acquireEnergy(func(q b524EnergyQuery, _ time.Time) (uint32, bool) {
		return p.readEnergyRegister(ctx, q)
	})
}

// acquireEnergy performs one register acquisition. The reference time is
// captured once and every dated key of the acquisition derives from it. If the
// provider clock crosses a month or year boundary during the acquisition, the
// reply read after the crossing is discarded (its relative window would be
// mislabelled) and the acquisition stops; the next poll re-acquires.
func (p *vaillantSemanticPoller) acquireEnergy(read func(b524EnergyQuery, time.Time) (uint32, bool)) {
	ref := p.provider.EnergyReferenceTime()
	accepted, failed := 0, 0
	for _, q := range b524EnergyQueries {
		val, ok := read(q, ref)
		if !ok {
			failed++
			continue
		}
		if now := p.provider.EnergyReferenceTime(); now.Year() != ref.Year() || now.Month() != ref.Month() {
			log.Printf("semantic energy b524: window boundary crossed during acquisition (ref=%s now=%s); discarding remaining replies", ref.Format("2006-01"), now.Format("2006-01"))
			failed++
			break
		}
		kwh := float64(val)

		key := p.provider.EnergyLifetimeKey(q.channel, q.usage)
		if q.period != "all" {
			key = p.provider.EnergyWindowKeyAt(q.channel, q.usage, q.period, q.yearKind, ref)
		}
		if p.provider.ApplyEnergyFromRegister(key, kwh) {
			accepted++
		}
	}
	if accepted > 0 || failed > 0 {
		log.Printf("semantic energy b524: accepted=%d failed=%d", accepted, failed)
	}
	if accepted > 0 {
		p.publishEnergyTotals()
	}
}

// readEnergyRegister reads one B524 energy register. Relative-window (monthly:
// this-month / last-month) replies are never served from the scheduler or
// shadow cache: their window label depends on when the value was acquired, so
// they always read fresh from the bus (maxAge 0). Lifetime totals are undated
// and keep the normal TTL.
func (p *vaillantSemanticPoller) readEnergyRegister(ctx context.Context, q b524EnergyQuery) (uint32, bool) {
	maxAgeOverride := time.Duration(-1)
	if q.period == "month" {
		maxAgeOverride = 0
	}
	raw, ok := p.readB524ValueWithMaxAge(ctx, vaillantB524OpcodeLocal, localRegulator.group, regulatorInstance, q.addr, maxAgeOverride)
	if !ok || !p.b524ReplyWidthMatches(b524ReaderKindUint32, vaillantB524OpcodeLocal, localRegulator.group, q.addr, len(raw), 4) {
		return 0, false
	}
	v := binary.LittleEndian.Uint32(raw[:4])
	if v == 0xFFFFFFFF {
		return 0, false
	}
	return v, true
}
