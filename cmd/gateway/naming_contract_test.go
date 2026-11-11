package main

import (
	"encoding/json"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/thermal"
	"strings"
	"testing"
)

func TestNamingContractLibrary(t *testing.T) {
	b, err := json.Marshal(thermal.Metadata())
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, old := range []string{"thermal.measurement.humidity_relative", `"thermal.setpoint.temperature"`, `"thermal.setpoint.dhw_temperature"`} {
		if strings.Contains(s, old) {
			t.Errorf("old member %s", old)
		}
	}
	_ = semreg.ValueQuantity
}
