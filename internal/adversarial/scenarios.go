package adversarial

import "time"

// DefaultScenarios returns the predefined adversarial runtime scenarios
// for the Helianthus gateway. Each scenario targets a specific failure
// mode that can occur in production and defines explicit thresholds
// for acceptable recovery behaviour.
func DefaultScenarios() []Scenario {
	return []Scenario{
		{
			ID:          "ADV-01",
			Name:        "HA restart while gateway stable",
			Description: "Gateway is in LIVE_READY, HA supervisor restarts the add-on container",
			Duration:    3 * time.Minute,
			Thresholds: ScenarioThresholds{
				MaxRecoveryTime: 90 * time.Second,
				MinLiveEpoch:    2,
				RequireZones:    true,
				RequireDHW:      true,
				MaxCollisions:   5,
			},
		},
		{
			ID:          "ADV-02",
			Name:        "Bus reset mid-flight",
			Description: "eBUS adapter is power-cycled while gateway is polling",
			Duration:    2 * time.Minute,
			Thresholds: ScenarioThresholds{
				MaxRecoveryTime: 120 * time.Second,
				MinLiveEpoch:    2,
				RequireZones:    true,
				RequireDHW:      false, // DHW may expire during bus outage
				MaxCollisions:   20,
			},
		},
		{
			ID:          "ADV-03",
			Name:        "60s partition and recovery",
			Description: "Network partition for 60 seconds, then recovery",
			Duration:    3 * time.Minute,
			Thresholds: ScenarioThresholds{
				MaxRecoveryTime: 90 * time.Second,
				MinLiveEpoch:    2,
				RequireZones:    true,
				RequireDHW:      false, // DHW may expire during network partition
				MaxCollisions:   10,
			},
		},
		{
			ID:          "ADV-04",
			Name:        "Cache corruption boot path",
			Description: "Gateway boots with corrupted/truncated semantic_cache.json",
			Duration:    2 * time.Minute,
			Thresholds: ScenarioThresholds{
				MaxRecoveryTime: 120 * time.Second,
				MinLiveEpoch:    2,
				RequireZones:    true,
				RequireDHW:      true,
				MaxCollisions:   5,
			},
		},
	}
}
