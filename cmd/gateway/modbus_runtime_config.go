package main

import (
	"errors"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
)

type zeroModbusJitter struct{}

func (zeroModbusJitter) Next(time.Duration) time.Duration { return 0 }

func mapModbusRuntimeConfig(config ebusgateway.ModbusTCPConfig) (modbusadapter.Config, error) {
	return mapModbusInstanceRuntimeConfig(config, nil)
}

// mapModbusInstanceRuntimeConfig maps one instance. A nil pacing is the
// configuration v1 mapping with its transport limits unchanged. A configuration
// v2 instance holds one connection with exactly one request in flight, its
// reconnect backoff between the pacing floor and ceiling, and the adapter
// pacing gate for the gap and the settle wait.
func mapModbusInstanceRuntimeConfig(config ebusgateway.ModbusTCPConfig, pacing *ebusgateway.ModbusTCPPacing) (modbusadapter.Config, error) {
	if !config.Enabled {
		if config.Endpoint != "" || config.DialTimeout != 0 {
			return modbusadapter.Config{}, errors.New("disabled Modbus TCP configuration contains active fields")
		}
		return modbusadapter.Config{}, nil
	}
	if config.Endpoint == "" || config.DialTimeout <= 0 {
		return modbusadapter.Config{}, errors.New("enabled Modbus TCP configuration is incomplete")
	}

	clock := modbus.NewRealTCPMonotonicClock()
	source, err := modbus.NewRuntimeAcquisitionSource(modbus.RuntimeAcquisitionConfig{
		Limits: modbus.RuntimeAcquisitionLimits{
			MaxLiveCapabilities:                        256,
			MaxAttempts:                                64,
			MaxMembersPerAttempt:                       32,
			AttemptKeyMaxUTF8Bytes:                     256,
			SourceEvidenceIDMaxUTF8Bytes:               512,
			NormalizationRecordMaxEncodedBytes:         16 << 10,
			NormalizationRequiredStringMaxUTF8Bytes:    512,
			NormalizationExtensionCountMax:             32,
			NormalizationExtensionKeyMaxUTF8Bytes:      256,
			NormalizationExtensionValueMaxEncodedBytes: 4 << 10,
			RetainedDiagnosticCountPerObjectMax:        32,
			RetainedDiagnosticMaxUTF8Bytes:             512,
			CapabilityTombstoneLimit:                   512,
			CapabilityTombstoneMaxEncodedBytes:         256,
		},
		ClaimLifetime: time.Minute,
		Clock:         clock,
	})
	if err != nil {
		return modbusadapter.Config{}, err
	}

	mapped := modbusadapter.Config{
		Enabled:     true,
		DialTimeout: config.DialTimeout,
		Endpoint: modbus.TCPEndpointConfig{
			Endpoint: config.Endpoint,
			PoolLimits: modbus.EndpointPoolLimits{
				MaxConnections: 1,
				Connection: modbus.ConnectionLimits{
					MaxInFlight:   4,
					MaxTombstones: 32,
				},
			},
			SchedulerLimits: modbus.SchedulerLimits{
				MaxActiveAdmissionKeys:         16,
				ProtectedSlotsPerKey:           1,
				SharedBurstSlots:               16,
				TotalQueued:                    32,
				MaxQueuedPerKey:                8,
				MaxQueuedPerAuthorizationScope: 16,
				MaxCoalescedDependentsPerKey:   8,
				MaxRetryAttempts:               2,
				MaxInFlightRequests:            4,
			},
			Backoff: modbus.BackoffConfig{
				Floor:             time.Second,
				Ceiling:           30 * time.Second,
				MaxAttempts:       5,
				Jitter:            zeroModbusJitter{},
				JitterAlgorithmID: "gateway-zero-jitter",
				JitterVersion:     "v1",
				JitterEvidence:    "deterministic-zero-schedule",
			},
			MaxBufferedBytes:         260,
			MaxRequestDeadline:       30 * time.Second,
			MaxResponseDeadline:      10 * time.Second,
			Clock:                    clock,
			RuntimeAcquisitionSource: source,
		},
	}
	if pacing != nil {
		if !validModbusTCPPacing(*pacing) {
			return modbusadapter.Config{}, errors.New("enabled Modbus TCP instance pacing is invalid")
		}
		mapped.Endpoint.PoolLimits.Connection.MaxInFlight = 1
		mapped.Endpoint.SchedulerLimits.SharedBurstSlots = 1
		mapped.Endpoint.SchedulerLimits.MaxInFlightRequests = 1
		mapped.Endpoint.Backoff.Floor = pacing.BackoffFloor
		mapped.Endpoint.Backoff.Ceiling = pacing.BackoffCeiling
		mapped.Pacing = &modbusadapter.Pacing{Gap: pacing.Gap, Settle: pacing.Settle}
	}
	return mapped, nil
}
