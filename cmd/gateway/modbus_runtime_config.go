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

	return modbusadapter.Config{
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
	}, nil
}
