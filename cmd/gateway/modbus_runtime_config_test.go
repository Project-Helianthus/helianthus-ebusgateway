package main

import (
	"flag"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
)

func TestMapModbusRuntimeConfigDisabledIsInert(t *testing.T) {
	config, err := mapModbusRuntimeConfig(ebusgateway.ModbusTCPConfig{})
	if err != nil {
		t.Fatalf("map disabled config: %v", err)
	}
	if config.Enabled {
		t.Fatal("disabled config became enabled")
	}
}

func TestBindFlagsModbusTCP(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("modbus", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{
		"-modbus-tcp-enabled",
		"-modbus-tcp-endpoint", "tcp://192.0.2.10:502",
		"-modbus-tcp-dial-timeout", "3s",
	}); err != nil {
		t.Fatalf("parse Modbus flags: %v", err)
	}
	if !cfg.ModbusTCPConfig.Enabled || cfg.ModbusTCPConfig.Endpoint != "tcp://192.0.2.10:502" || cfg.ModbusTCPConfig.DialTimeout != 3*time.Second {
		t.Fatalf("Modbus config = %+v", cfg.ModbusTCPConfig)
	}
}

func TestBindFlagsGrowattBMSRS485KeepsLifecycleIdentityExplicit(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("growatt-bms", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{
		"-growatt-bms-rs485-enabled",
		"-growatt-bms-rs485-source-id", "growatt-bms-a",
		"-growatt-bms-rs485-source-epoch", "epoch-7",
		"-growatt-bms-rs485-driver-generation", "3",
		"-growatt-bms-rs485-unit-id", "7",
		"-growatt-bms-rs485-serial-path", "/dev/fixture",
		"-growatt-bms-rs485-baud", "9600",
		"-growatt-bms-rs485-parity", "even",
		"-growatt-bms-rs485-stop-bits", "1",
		"-growatt-bms-rs485-response-timeout", "1s",
		"-growatt-bms-rs485-max-response-delay", "100ms",
		"-growatt-bms-rs485-max-quiescence", "200ms",
	}); err != nil {
		t.Fatal(err)
	}
	config := cfg.ModbusTCPConfig.GrowattBMSRS485
	if !config.Enabled || config.SourceID != "growatt-bms-a" || config.SourceEpoch != "epoch-7" ||
		config.DriverGeneration != 3 || config.UnitID != 7 || config.Baud != 9600 || config.Parity != "even" || config.StopBits != 1 {
		t.Fatalf("Growatt BMS config = %+v", config)
	}
}

func TestMapModbusRuntimeConfigRejectsActiveFieldsWhileDisabled(t *testing.T) {
	_, err := mapModbusRuntimeConfig(ebusgateway.ModbusTCPConfig{
		Endpoint: "tcp://127.0.0.1:502",
	})
	if err == nil {
		t.Fatal("disabled config with endpoint was accepted")
	}
}

func TestMapModbusRuntimeConfigBuildsFiniteReadOnlyBounds(t *testing.T) {
	config, err := mapModbusRuntimeConfig(ebusgateway.ModbusTCPConfig{
		Enabled:     true,
		Endpoint:    "tcp://192.0.2.10:502",
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatalf("map enabled config: %v", err)
	}
	if !config.Enabled || config.Endpoint.Endpoint != "tcp://192.0.2.10:502" {
		t.Fatalf("mapped config = %+v", config)
	}
	if config.Endpoint.SchedulerLimits.MaxActiveAdmissionKeys < 2 ||
		config.Endpoint.SchedulerLimits.ProtectedSlotsPerKey < 1 ||
		config.Endpoint.SchedulerLimits.TotalQueued < 2 ||
		config.Endpoint.MaxRequestDeadline <= 0 ||
		config.Endpoint.MaxResponseDeadline <= 0 ||
		config.Endpoint.RuntimeAcquisitionSource == nil {
		t.Fatalf("mapped bounds are incomplete: %+v", config.Endpoint)
	}
}

func TestDefaultConfigLeavesModbusDisabled(t *testing.T) {
	config := ebusgateway.DefaultConfig().ModbusTCPConfig
	if config.Enabled || config.Endpoint != "" {
		t.Fatalf("default Modbus config is active: %+v", config)
	}
}
