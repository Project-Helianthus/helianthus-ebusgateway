package main

import (
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
