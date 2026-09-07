package adversarial

const (
	ReportSchemaURL  = "https://raw.githubusercontent.com/Project-Helianthus/helianthus-docs-ebus/main/docs/platform/schemas/adversarial-runtime-report-v1.schema.json"
	FixtureSchemaURL = "https://raw.githubusercontent.com/Project-Helianthus/helianthus-docs-ebus/main/docs/platform/schemas/adversarial-runtime-offline-fixture-v1.schema.json"
	SuiteID          = "helianthus.adversarial.ADV01-04"
)

// Definition is an immutable v1 catalog entry. It deliberately contains no
// command, network, or device-control field.
type Definition struct {
	ScenarioID, Name, TriggerKind, RecoveryTarget, RecoveryAnchor, RecoveryEvent string
	ExpectedEvents                                                               []string
	BaselinePhase, EndPhase                                                      string
	MaximumRecoveryMS, MaximumCollisionsDelta                                    int64
	ZonesRequired, DHWRequired                                                   bool
}

// Catalog returns a copy so callers cannot mutate the runner's contract.
func Catalog() []Definition {
	return []Definition{
		{"ADV-01", "HA Core/integration consumer restart while gateway stays stable", "ha_consumer_restart", "ha_consumer_synchronized", "consumer_stopped", "ha_consumer_synchronized", []string{"restart_requested", "consumer_stopped", "consumer_started", "ha_consumer_synchronized"}, "LIVE_READY", "LIVE_READY", 90000, 5, true, true},
		{"ADV-02", "eBUS adapter reset while polling", "adapter_reset", "gateway_live_ready", "reset_started", "gateway_live_ready", []string{"reset_requested", "reset_started", "transport_unavailable", "transport_available", "gateway_live_ready"}, "LIVE_READY", "LIVE_READY", 120000, 20, true, false},
		{"ADV-03", "60 second gateway-to-adapter transport partition and recovery", "transport_partition", "gateway_live_ready", "partition_cleared", "gateway_live_ready", []string{"partition_requested", "partition_active", "partition_cleared", "gateway_live_ready"}, "LIVE_READY", "LIVE_READY", 90000, 10, true, false},
		{"ADV-04", "fresh isolated gateway boot with corrupted cache fixture", "isolated_corrupt_cache_boot", "gateway_live_ready", "runtime_started", "gateway_live_ready", []string{"isolated_cache_staged", "runtime_started", "gateway_live_ready"}, "BOOT_INIT", "LIVE_READY", 120000, 5, true, true},
	}
}
