package adversarial

const (
	ReportSchemaURL   = "https://raw.githubusercontent.com/Project-Helianthus/helianthus-docs-ebus/main/docs/platform/schemas/adversarial-runtime-report-v1.schema.json"
	FixtureSchemaURL  = "https://raw.githubusercontent.com/Project-Helianthus/helianthus-docs-ebus/main/docs/platform/schemas/adversarial-runtime-offline-fixture-v1.schema.json"
	SuiteID           = "helianthus.adversarial.ADV01-04"
	subjectRepository = "Project-Helianthus/helianthus-ebusgateway"
	fixtureSetDigest  = "d7fbe89d068b1b5c0d41fe51176d9e9263a441ee0ed752c9e8cae794f5a8346a"
)

var catalogV1 = []Definition{
	{ScenarioID: "ADV-01", Name: "HA Core/integration consumer restart while gateway stays stable", DurationLimitMS: 180000, TriggerKind: "ha_consumer_restart", RecoveryTarget: "ha_consumer_synchronized", MinimumLiveEpochDelta: 2, ZonesRequired: true, DHWRequired: true, MaximumCollisionsDelta: 5, MaximumRecoveryMS: 90000, RecoveryAnchor: "consumer_stopped", RecoveryEvent: "ha_consumer_synchronized", ExpectedEvents: []string{"restart_requested", "consumer_stopped", "consumer_started", "ha_consumer_synchronized"}, BaselinePhase: "LIVE_READY", EndPhase: "LIVE_READY"},
	{ScenarioID: "ADV-02", Name: "eBUS adapter reset while polling", DurationLimitMS: 180000, TriggerKind: "adapter_reset", RecoveryTarget: "gateway_live_ready", MinimumLiveEpochDelta: 2, ZonesRequired: true, DHWRequired: false, MaximumCollisionsDelta: 20, MaximumRecoveryMS: 120000, RecoveryAnchor: "reset_started", RecoveryEvent: "gateway_live_ready", ExpectedEvents: []string{"reset_requested", "reset_started", "transport_unavailable", "transport_available", "gateway_live_ready"}, BaselinePhase: "LIVE_READY", EndPhase: "LIVE_READY"},
	{ScenarioID: "ADV-03", Name: "60 second gateway-to-adapter transport partition and recovery", DurationLimitMS: 180000, TriggerKind: "transport_partition", RecoveryTarget: "gateway_live_ready", MinimumLiveEpochDelta: 2, ZonesRequired: true, DHWRequired: false, MaximumCollisionsDelta: 10, MaximumRecoveryMS: 90000, RecoveryAnchor: "partition_cleared", RecoveryEvent: "gateway_live_ready", ExpectedEvents: []string{"partition_requested", "partition_active", "partition_cleared", "gateway_live_ready"}, BaselinePhase: "LIVE_READY", EndPhase: "LIVE_READY"},
	{ScenarioID: "ADV-04", Name: "fresh isolated gateway boot with corrupted cache fixture", DurationLimitMS: 180000, TriggerKind: "isolated_corrupt_cache_boot", RecoveryTarget: "gateway_live_ready", MinimumLiveEpochDelta: 2, ZonesRequired: true, DHWRequired: true, MaximumCollisionsDelta: 5, MaximumRecoveryMS: 120000, RecoveryAnchor: "runtime_started", RecoveryEvent: "gateway_live_ready", ExpectedEvents: []string{"isolated_cache_staged", "runtime_started", "gateway_live_ready"}, BaselinePhase: "BOOT_INIT", EndPhase: "LIVE_READY"},
}

// Catalog returns a deep copy so callers cannot alter the contract.
func Catalog() []Definition {
	out := make([]Definition, len(catalogV1))
	copy(out, catalogV1)
	for i := range out {
		out[i].ExpectedEvents = append([]string(nil), catalogV1[i].ExpectedEvents...)
	}
	return out
}
