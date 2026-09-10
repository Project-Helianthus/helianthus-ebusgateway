package main

import (
	"testing"
	"time"

	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

func TestGrowattStorageDispositionsUseExactNativeLossDefinitionIDs(t *testing.T) {
	status := modbusreg.GrowattBMSTypedReadOnlyStatus{
		OperatingState:              modbusreg.GrowattBMSStateCharging,
		PackVoltageVolts:            52,
		PackCurrentAmps:             -12.5,
		SOCPercent:                  75,
		TemperatureCelsius:          24,
		CumulativeChargeAmpHours:    123.5,
		CumulativeDischargeAmpHours: 42.25,
	}
	_, dispositions := growattStorageDispositions(
		status, "asset:growatt-bms-a", "binding:test", "growatt-bms-a", "source-epoch-1", "1",
		growattStorageEvidence("native.fixture", "fixture"), growattWall(time.Unix(1_800_000_000, 0)),
		semreg.MonotonicPoint{ClockEpochID: growattBMSRS485ClockEpoch, Nanoseconds: "1"},
	)
	want := map[semreg.DefinitionID]semreg.DefinitionID{
		"storage.pack.current":       "native.growatt.bms.rs485.v202.pack_current_amps",
		"storage.capacity.charge":    "native.growatt.bms.rs485.v202.cumulative_charge_amp_hours",
		"storage.capacity.discharge": "native.growatt.bms.rs485.v202.cumulative_discharge_amp_hours",
		"storage.status.operating":   "native.growatt.bms.rs485.v202.operating_state",
	}
	for _, disposition := range dispositions {
		if expected, ok := want[disposition.item.ItemID]; ok {
			if len(disposition.item.Loss) != 1 || len(disposition.item.Loss[0].SourceItems) != 1 || disposition.item.Loss[0].SourceItems[0] != expected {
				t.Fatalf("%s source items=%#v; want %q", disposition.item.ItemID, disposition.item.Loss, expected)
			}
			delete(want, disposition.item.ItemID)
			continue
		}
		if len(disposition.item.Loss) != 0 {
			t.Fatalf("exact disposition %s declares loss=%#v", disposition.item.ItemID, disposition.item.Loss)
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing transformed dispositions=%#v", want)
	}
}
