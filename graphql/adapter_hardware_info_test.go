package graphql

import (
	"testing"
	"time"

	graphqlgo "github.com/graphql-go/graphql"
)

type adapterHardwareInfoProvider struct {
	staticSemanticProvider
	info *AdapterHardwareInfo
}

func (p adapterHardwareInfoProvider) AdapterHardwareInfo() *AdapterHardwareInfo {
	return p.info
}

func adapterHardwareInfoSchema(t *testing.T, info *AdapterHardwareInfo) graphqlgo.Schema {
	t.Helper()
	builder := NewBuilder(nil, nil)
	builder.SetSemanticProvider(adapterHardwareInfoProvider{info: info})
	schema, err := NewQuerySchema(builder)
	if err != nil {
		t.Fatalf("query schema: %v", err)
	}
	return schema
}

func queryAdapterHardwareInfo(t *testing.T, schema graphqlgo.Schema, selection string) (map[string]any, []error) {
	t.Helper()
	result := graphqlgo.Do(graphqlgo.Params{
		Schema:        schema,
		RequestString: "{ adapter_hardware_info { " + selection + " } }",
	})
	errs := make([]error, 0, len(result.Errors))
	for _, err := range result.Errors {
		errs = append(errs, err)
	}
	if result.Data == nil {
		return nil, errs
	}
	data, ok := result.Data.(map[string]any)
	if !ok {
		t.Fatalf("result data type %T", result.Data)
	}
	object := data["adapter_hardware_info"]
	if object == nil {
		return nil, errs
	}
	fields, ok := object.(map[string]any)
	if !ok {
		t.Fatalf("adapter_hardware_info type %T", object)
	}
	return fields, errs
}

func syntheticAdapterHardwareInfo() AdapterHardwareInfo {
	temperature := 36.5
	supply := 3310
	busMax := 241
	busMin := 187
	resetCause := "brownout"
	resetCode := byte(4)
	restarts := byte(9)
	rssi := -63
	identityAt := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	telemetryAt := time.Date(2024, 7, 8, 9, 10, 11, 0, time.UTC)
	return AdapterHardwareInfo{
		FirmwareVersion:    "fw-ver-17",
		FirmwareChecksum:   "fw-sum-29",
		BootloaderVersion:  "bl-ver-31",
		BootloaderChecksum: "bl-sum-43",
		HardwareID:         "hw-id-59",
		HardwareConfig:     "hw-cfg-61",
		Features:           0x15,
		Jumpers:            0x2A,
		JumperFlags:        []string{"flag-alpha", "flag-beta"},
		IsWiFi:             true,
		IsEthernet:         false,
		TemperatureC:       &temperature,
		SupplyVoltageMV:    &supply,
		BusVoltageMaxDV:    &busMax,
		BusVoltageMinDV:    &busMin,
		ResetCause:         &resetCause,
		ResetCauseCode:     &resetCode,
		RestartCount:       &restarts,
		WiFiRSSIDBm:        &rssi,
		LastIdentityQuery:  &identityAt,
		LastTelemetryQuery: &telemetryAt,
		VersionResponseLen: 18,
		InfoSupported:      false,
	}
}

func TestAdapterHardwareInfo(t *testing.T) {
	populated := syntheticAdapterHardwareInfo()
	schema := adapterHardwareInfoSchema(t, &populated)

	t.Run("populated", func(t *testing.T) {
		checksum, errs := queryAdapterHardwareInfo(t, schema, "firmware_checksum")
		gotChecksum, _ := checksum["firmware_checksum"].(string)
		if checksum == nil || gotChecksum != populated.FirmwareChecksum {
			t.Fatalf("firmware_checksum = %#v while Go field FirmwareChecksum = %q; errors: %v", checksum["firmware_checksum"], populated.FirmwareChecksum, errs)
		}

		fields, errs := queryAdapterHardwareInfo(t, schema, `
			firmware_version
			firmware_checksum
			bootloader_version
			bootloader_checksum
			hardware_id
			hardware_config
			features
			jumpers
			jumper_flags
			is_wifi
			is_ethernet
			temperature_c
			supply_voltage_mv
			bus_voltage_max_dv
			bus_voltage_min_dv
			reset_cause
			reset_cause_code
			restart_count
			wifi_rssi_dbm
			last_identity_query
			last_telemetry_query
			version_response_len
			info_supported
		`)
		if len(errs) != 0 {
			t.Fatalf("populated query errors: %v", errs)
		}
		want := map[string]any{
			"firmware_version":     populated.FirmwareVersion,
			"firmware_checksum":    populated.FirmwareChecksum,
			"bootloader_version":   populated.BootloaderVersion,
			"bootloader_checksum":  populated.BootloaderChecksum,
			"hardware_id":          populated.HardwareID,
			"hardware_config":      populated.HardwareConfig,
			"features":             int(populated.Features),
			"jumpers":              int(populated.Jumpers),
			"jumper_flags":         []any{"flag-alpha", "flag-beta"},
			"is_wifi":              populated.IsWiFi,
			"is_ethernet":          populated.IsEthernet,
			"temperature_c":        *populated.TemperatureC,
			"supply_voltage_mv":    *populated.SupplyVoltageMV,
			"bus_voltage_max_dv":   *populated.BusVoltageMaxDV,
			"bus_voltage_min_dv":   *populated.BusVoltageMinDV,
			"reset_cause":          *populated.ResetCause,
			"reset_cause_code":     int(*populated.ResetCauseCode),
			"restart_count":        int(*populated.RestartCount),
			"wifi_rssi_dbm":        *populated.WiFiRSSIDBm,
			"last_identity_query":  populated.LastIdentityQuery.UTC().Format("2006-01-02T15:04:05Z"),
			"last_telemetry_query": populated.LastTelemetryQuery.UTC().Format("2006-01-02T15:04:05Z"),
			"version_response_len": populated.VersionResponseLen,
			"info_supported":       populated.InfoSupported,
		}
		if len(fields) != len(want) {
			t.Fatalf("field count = %d, want %d; got %#v", len(fields), len(want), fields)
		}
		for name, expected := range want {
			if !adapterHardwareInfoValueEqual(fields[name], expected) {
				t.Fatalf("%s = %#v (%T), want %#v (%T)", name, fields[name], fields[name], expected, expected)
			}
		}
	})

	t.Run("nil_optional", func(t *testing.T) {
		partial := syntheticAdapterHardwareInfo()
		partial.TemperatureC = nil
		partial.IsWiFi = false
		partial.IsEthernet = true
		partial.InfoSupported = true
		partialSchema := adapterHardwareInfoSchema(t, &partial)
		fields, errs := queryAdapterHardwareInfo(t, partialSchema, "temperature_c firmware_version firmware_checksum is_wifi is_ethernet info_supported")
		if fields == nil || len(errs) != 0 {
			t.Fatalf("nil optional nulled the object: fields=%#v errors=%v", fields, errs)
		}
		if _, present := fields["temperature_c"]; !present || fields["temperature_c"] != nil {
			t.Fatalf("temperature_c = %#v, want null", fields["temperature_c"])
		}
		if fields["firmware_version"] != partial.FirmwareVersion {
			t.Fatalf("firmware_version = %#v, want %q", fields["firmware_version"], partial.FirmwareVersion)
		}
		if fields["firmware_checksum"] != partial.FirmwareChecksum {
			t.Fatalf("firmware_checksum = %#v, want %q", fields["firmware_checksum"], partial.FirmwareChecksum)
		}
		if fields["is_wifi"] != false || fields["is_ethernet"] != true || fields["info_supported"] != true {
			t.Fatalf("bool fields = wifi %#v ethernet %#v info %#v", fields["is_wifi"], fields["is_ethernet"], fields["info_supported"])
		}

		distinct := partial
		distinct.IsEthernet = false
		distinctFields, distinctErrs := queryAdapterHardwareInfo(t, adapterHardwareInfoSchema(t, &distinct), "temperature_c firmware_version is_wifi is_ethernet info_supported")
		if distinctFields == nil || len(distinctErrs) != 0 || distinctFields["temperature_c"] != nil {
			t.Fatalf("nil optional nulled the object: fields=%#v errors=%v", distinctFields, distinctErrs)
		}
		if distinctFields["firmware_version"] != distinct.FirmwareVersion || distinctFields["is_wifi"] != false || distinctFields["is_ethernet"] != false || distinctFields["info_supported"] != true {
			t.Fatalf("bool fields = version %#v wifi %#v ethernet %#v info %#v", distinctFields["firmware_version"], distinctFields["is_wifi"], distinctFields["is_ethernet"], distinctFields["info_supported"])
		}
	})
}

func adapterHardwareInfoValueEqual(got, want any) bool {
	switch expected := want.(type) {
	case []any:
		switch actual := got.(type) {
		case []any:
			if len(actual) != len(expected) {
				return false
			}
			for i := range expected {
				if actual[i] != expected[i] {
					return false
				}
			}
			return true
		default:
			return false
		}
	default:
		return got == want
	}
}
