package ebusgateway

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestConfigDeclaresIndependentDisabledPortalPVCapabilities(t *testing.T) {
	field, ok := reflect.TypeOf(Config{}).FieldByName("PortalPV")
	if !ok {
		t.Fatal("Config has no PortalPV capability configuration")
	}
	value := reflect.New(field.Type).Elem()
	for _, name := range []string{"SemanticEnabled", "RawReadEnabled"} {
		capability := value.FieldByName(name)
		if !capability.IsValid() || capability.Kind() != reflect.Bool || capability.Bool() {
			t.Fatalf("PortalPV.%s must exist and default disabled", name)
		}
	}
}

func TestConfigCrossValidatesPortalPVAgainstDedicatedM2MListener(t *testing.T) {
	type portalPVValidator interface{ ValidatePortalPV() error }
	validM2M := M2MGraphQLConfig{
		ListenAddr: "127.0.0.1:8443", ServerName: "m2m.gateway.test", ClientCAFile: "ca.pem",
		ServerCertFile: "server.pem", ServerKeyFile: "server-key.pem", AllowedAssets: []string{"pv-allowed"},
	}
	portal := PortalPVConfig{
		SemanticEnabled: true, M2MURL: "https://127.0.0.1:8443/graphql/m2m/v1", M2MServerName: "m2m.gateway.test",
		M2MCAFile: "ca.pem", M2MClientCert: "client.pem", M2MClientKey: "client-key.pem", AssetRef: "pv-allowed",
	}
	for _, cfg := range []*Config{
		{PortalPV: portal},
		{M2MGraphQL: validM2M, PortalPV: func() PortalPVConfig { value := portal; value.AssetRef = "pv-forbidden"; return value }()},
	} {
		validator, ok := any(cfg).(portalPVValidator)
		if !ok {
			t.Fatal("Config does not provide cross-configuration Portal PV validation")
		}
		if err := validator.ValidatePortalPV(); err == nil {
			t.Fatalf("cross-configuration mismatch accepted: %+v", cfg)
		}
	}
	valid := &Config{M2MGraphQL: validM2M, PortalPV: portal}
	validator, ok := any(valid).(portalPVValidator)
	if !ok || validator.ValidatePortalPV() != nil {
		t.Fatalf("valid cross-configuration rejected: %+v", valid)
	}
}

func TestConfigCrossValidatesPortalStorageAgainstGrowattProducer(t *testing.T) {
	m2m := M2MGraphQLConfig{ListenAddr: "127.0.0.1:8443", ServerName: "m2m.gateway.test", ClientCAFile: "ca.pem", ServerCertFile: "server.pem", ServerKeyFile: "server-key.pem", AllowedAssets: []string{"asset:storage-a"}}
	portal := PortalStorageConfig{SemanticEnabled: true, M2MURL: "https://127.0.0.1:8443/graphql/m2m/v1", M2MServerName: "m2m.gateway.test", M2MCAFile: "ca.pem", M2MClientCert: "client.pem", M2MClientKey: "client-key.pem", AssetRef: "asset:storage-a"}
	valid := Config{M2MGraphQL: m2m, PortalStorage: portal, ModbusTCPConfig: ModbusTCPConfig{GrowattBMSRS485: GrowattBMSRS485Config{Enabled: true, AssetID: "asset:storage-a", ResponseTimeout: time.Second, MaxQuiescence: 200 * time.Millisecond}}}
	if err := valid.ValidatePortalStorage(); err != nil {
		t.Fatalf("exact matching storage producer rejected: %v", err)
	}
	for name, mutate := range map[string]func(*Config){
		"disabled-producer":       func(cfg *Config) { cfg.ModbusTCPConfig.GrowattBMSRS485.Enabled = false },
		"different-asset":         func(cfg *Config) { cfg.ModbusTCPConfig.GrowattBMSRS485.AssetID = "asset:storage-b" },
		"allowed-assets-mismatch": func(cfg *Config) { cfg.M2MGraphQL.AllowedAssets = []string{"asset:storage-b"} },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			if err := candidate.ValidatePortalStorage(); err == nil {
				t.Fatal("invalid storage startup configuration accepted")
			}
		})
	}
	if err := (Config{}).ValidatePortalStorage(); err != nil {
		t.Fatalf("fully disabled storage config rejected: %v", err)
	}
	raw := valid
	raw.PortalStorage.RawReadEnabled = true
	if err := raw.ValidatePortalStorage(); err == nil {
		t.Fatal("Portal Storage raw-read setting accepted")
	}
	boundary := valid
	boundary.ModbusTCPConfig.GrowattBMSRS485.MaxQuiescence = 200 * time.Millisecond
	boundary.ModbusTCPConfig.GrowattBMSRS485.ResponseTimeout = (4500*time.Millisecond - 200*time.Millisecond - time.Nanosecond) / 4
	if err := boundary.ValidatePortalStorage(); err != nil {
		t.Fatalf("near-bound Portal storage timeout rejected: %v", err)
	}
	for _, timeout := range []time.Duration{(4500*time.Millisecond - 200*time.Millisecond) / 4, time.Duration(1<<63 - 1)} {
		candidate := valid
		candidate.ModbusTCPConfig.GrowattBMSRS485.MaxQuiescence = 200 * time.Millisecond
		candidate.ModbusTCPConfig.GrowattBMSRS485.ResponseTimeout = timeout
		if err := candidate.ValidatePortalStorage(); err == nil {
			t.Fatalf("Portal storage timeout accepted: %s", timeout)
		}
	}
	direct := valid
	direct.PortalStorage = PortalStorageConfig{}
	direct.ModbusTCPConfig.GrowattBMSRS485.MaxQuiescence = 200 * time.Millisecond
	direct.ModbusTCPConfig.GrowattBMSRS485.ResponseTimeout = (9250*time.Millisecond - 200*time.Millisecond - time.Nanosecond) / 4
	if err := direct.ValidatePortalStorage(); err != nil {
		t.Fatalf("near-bound direct GraphQL storage timeout rejected: %v", err)
	}
	direct.ModbusTCPConfig.GrowattBMSRS485.ResponseTimeout = (9250*time.Millisecond - 200*time.Millisecond) / 4
	if err := direct.ValidatePortalStorage(); err == nil {
		t.Fatal("direct GraphQL storage timeout without server headroom accepted")
	}
	overflow := valid
	overflow.ModbusTCPConfig.GrowattBMSRS485.MaxQuiescence = time.Duration(1<<63 - 1)
	overflow.ModbusTCPConfig.GrowattBMSRS485.ResponseTimeout = time.Nanosecond
	if err := overflow.ValidatePortalStorage(); err == nil {
		t.Fatal("overflowing public storage operation accepted")
	}
	nativeOnly := valid
	nativeOnly.PortalStorage = PortalStorageConfig{}
	nativeOnly.M2MGraphQL = M2MGraphQLConfig{}
	nativeOnly.ModbusTCPConfig.GrowattBMSRS485.ResponseTimeout = 3 * time.Second
	if err := nativeOnly.ValidatePortalStorage(); err != nil {
		t.Fatalf("native-only timeout rejected: %v", err)
	}
}

func TestConfigCrossValidatesPortalEVSEAgainstDedicatedM2MListener(t *testing.T) {
	m2m := M2MGraphQLConfig{ListenAddr: "127.0.0.1:8443", ServerName: "m2m.gateway.test", ClientCAFile: "ca.pem", ServerCertFile: "server.pem", ServerKeyFile: "server-key.pem", AllowedAssets: []string{"asset:evse-a"}}
	portal := PortalEVSEConfig{SemanticEnabled: true, M2MURL: "https://127.0.0.1:8443/graphql/m2m/v1", M2MServerName: "m2m.gateway.test", M2MCAFile: "ca.pem", M2MClientCert: "client.pem", M2MClientKey: "client-key.pem", AssetRef: "asset:evse-a"}
	valid := Config{M2MGraphQL: m2m, PortalEVSE: portal}
	if err := valid.ValidatePortalEVSE(); err != nil {
		t.Fatalf("valid EVSE BFF configuration rejected: %v", err)
	}
	for name, mutate := range map[string]func(*Config){
		"raw read":         func(cfg *Config) { cfg.PortalEVSE.RawReadEnabled = true },
		"unknown asset":    func(cfg *Config) { cfg.PortalEVSE.AssetRef = "asset:other" },
		"different server": func(cfg *Config) { cfg.PortalEVSE.M2MServerName = "other.gateway.test" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			if err := candidate.ValidatePortalEVSE(); err == nil {
				t.Fatal("invalid EVSE BFF configuration accepted")
			}
		})
	}
}

func TestConfigPinsPortalPVURLToLoopbackDedicatedListenerPort(t *testing.T) {
	base := Config{
		M2MGraphQL: M2MGraphQLConfig{
			ListenAddr: "0.0.0.0:8443", ServerName: "m2m.gateway.test", ClientCAFile: "ca.pem",
			ServerCertFile: "server.pem", ServerKeyFile: "server-key.pem", AllowedAssets: []string{"pv-allowed"},
		},
		PortalPV: PortalPVConfig{
			SemanticEnabled: true, M2MURL: "https://127.0.0.1:8443/graphql/m2m/v1", M2MServerName: "m2m.gateway.test",
			M2MCAFile: "ca.pem", M2MClientCert: "client.pem", M2MClientKey: "client-key.pem", AssetRef: "pv-allowed",
		},
	}
	if err := base.ValidatePortalPV(); err != nil {
		t.Fatalf("loopback URL for wildcard listener rejected: %v", err)
	}
	ipv6 := base
	ipv6.M2MGraphQL.ListenAddr = "[::]:8443"
	ipv6.PortalPV.M2MURL = "https://[::1]:8443/graphql/m2m/v1"
	if err := ipv6.ValidatePortalPV(); err != nil {
		t.Fatalf("IPv6 loopback URL for wildcard listener rejected: %v", err)
	}
	for _, mutate := range []func(*Config){
		func(cfg *Config) { cfg.PortalPV.M2MURL = "https://192.0.2.10:8443/graphql/m2m/v1" },
		func(cfg *Config) { cfg.PortalPV.M2MURL = "https://127.0.0.1:9443/graphql/m2m/v1" },
	} {
		candidate := base
		mutate(&candidate)
		if err := candidate.ValidatePortalPV(); err == nil {
			t.Fatalf("unpinned Portal PV URL accepted: %s", candidate.PortalPV.M2MURL)
		}
	}
}

func TestDefaultConfig_DisablesDumpUploadPath(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.DumpUploadPath != "" {
		t.Fatalf("expected DumpUploadPath empty by default, got %q", cfg.DumpUploadPath)
	}
}

func TestApplyDefaults_DoesNotEnableDumpUploadPath(t *testing.T) {
	cfg := applyDefaults(Config{})
	if cfg.DumpUploadPath != "" {
		t.Fatalf("expected DumpUploadPath empty after defaults, got %q", cfg.DumpUploadPath)
	}
}

func TestDefaultConfig_SetsScanSource(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.ScanSource != 0xF0 {
		t.Fatalf("expected ScanSource=0xf0 by default, got 0x%02x", cfg.ScanSource)
	}
}

func TestApplyDefaults_SetsScanSource(t *testing.T) {
	cfg := applyDefaults(Config{})
	if cfg.ScanSource != 0xF0 {
		t.Fatalf("expected ScanSource=0xf0 after defaults, got 0x%02x", cfg.ScanSource)
	}
}

func TestApplyDefaults_PreservesAutoScanSource(t *testing.T) {
	cfg := applyDefaults(Config{
		ScanSource:     0x00,
		ScanSourceAuto: true,
	})
	if cfg.ScanSource != 0x00 {
		t.Fatalf("expected ScanSource=0x00 for auto mode, got 0x%02x", cfg.ScanSource)
	}
}

func TestDefaultConfig_SetsBootLiveTimeout(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.BootLiveTimeout != 2*time.Minute {
		t.Fatalf("expected BootLiveTimeout=2m by default, got %s", cfg.BootLiveTimeout)
	}
}

func TestApplyDefaults_SetsBootLiveTimeout(t *testing.T) {
	cfg := applyDefaults(Config{})
	if cfg.BootLiveTimeout != 2*time.Minute {
		t.Fatalf("expected BootLiveTimeout=2m after defaults, got %s", cfg.BootLiveTimeout)
	}
}

func TestDefaultConfig_SetsSemanticCachePath(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.SemanticCachePath != "./semantic_cache.json" {
		t.Fatalf("expected SemanticCachePath=./semantic_cache.json by default, got %q", cfg.SemanticCachePath)
	}
}

func TestApplyDefaults_SetsSemanticCachePath(t *testing.T) {
	cfg := applyDefaults(Config{})
	if cfg.SemanticCachePath != "./semantic_cache.json" {
		t.Fatalf("expected SemanticCachePath=./semantic_cache.json after defaults, got %q", cfg.SemanticCachePath)
	}
}

func TestDefaultConfig_SetsSemanticReadBreakerDefaults(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.SemanticReadBreakerFailureBudget != DefaultSemanticReadFailureBudget {
		t.Fatalf("expected SemanticReadBreakerFailureBudget=%d by default, got %d", DefaultSemanticReadFailureBudget, cfg.SemanticReadBreakerFailureBudget)
	}
	if cfg.SemanticReadBreakerOpenCooldown != DefaultSemanticReadOpenCooldown {
		t.Fatalf("expected SemanticReadBreakerOpenCooldown=%s by default, got %s", DefaultSemanticReadOpenCooldown, cfg.SemanticReadBreakerOpenCooldown)
	}
	if cfg.SemanticReadBreakerHalfOpenProbeLimit != DefaultSemanticReadHalfOpenProbeLimit {
		t.Fatalf("expected SemanticReadBreakerHalfOpenProbeLimit=%d by default, got %d", DefaultSemanticReadHalfOpenProbeLimit, cfg.SemanticReadBreakerHalfOpenProbeLimit)
	}
}

func TestApplyDefaults_SetsSemanticReadBreakerDefaults(t *testing.T) {
	cfg := applyDefaults(Config{})
	if cfg.SemanticReadBreakerFailureBudget != DefaultSemanticReadFailureBudget {
		t.Fatalf("expected SemanticReadBreakerFailureBudget=%d after defaults, got %d", DefaultSemanticReadFailureBudget, cfg.SemanticReadBreakerFailureBudget)
	}
	if cfg.SemanticReadBreakerOpenCooldown != DefaultSemanticReadOpenCooldown {
		t.Fatalf("expected SemanticReadBreakerOpenCooldown=%s after defaults, got %s", DefaultSemanticReadOpenCooldown, cfg.SemanticReadBreakerOpenCooldown)
	}
	if cfg.SemanticReadBreakerHalfOpenProbeLimit != DefaultSemanticReadHalfOpenProbeLimit {
		t.Fatalf("expected SemanticReadBreakerHalfOpenProbeLimit=%d after defaults, got %d", DefaultSemanticReadHalfOpenProbeLimit, cfg.SemanticReadBreakerHalfOpenProbeLimit)
	}
}

func TestApplyDefaults_PreservesExplicitDisabledSemanticReadBreakerBudget(t *testing.T) {
	cfg := applyDefaults(Config{
		SemanticReadBreakerFailureBudget:    0,
		SemanticReadBreakerFailureBudgetSet: true,
	})
	if cfg.SemanticReadBreakerFailureBudget != 0 {
		t.Fatalf("expected SemanticReadBreakerFailureBudget=0 to remain disabled, got %d", cfg.SemanticReadBreakerFailureBudget)
	}
}

func TestApplyDefaults_UsesDefaultBudgetForPartialBreakerOverrides(t *testing.T) {
	cfg := applyDefaults(Config{
		SemanticReadBreakerOpenCooldown: 20 * time.Second,
	})
	if cfg.SemanticReadBreakerFailureBudget != DefaultSemanticReadFailureBudget {
		t.Fatalf("expected SemanticReadBreakerFailureBudget=%d with partial override, got %d", DefaultSemanticReadFailureBudget, cfg.SemanticReadBreakerFailureBudget)
	}
	if cfg.SemanticReadBreakerOpenCooldown != 20*time.Second {
		t.Fatalf("expected SemanticReadBreakerOpenCooldown=20s, got %s", cfg.SemanticReadBreakerOpenCooldown)
	}
}

func TestDefaultConfig_SetsSemanticZonePresenceThresholdDefaults(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.SemanticZonePresenceMissThreshold != DefaultSemanticZonePresenceMissThreshold {
		t.Fatalf("expected SemanticZonePresenceMissThreshold=%d by default, got %d", DefaultSemanticZonePresenceMissThreshold, cfg.SemanticZonePresenceMissThreshold)
	}
	if cfg.SemanticZonePresenceHitThreshold != DefaultSemanticZonePresenceHitThreshold {
		t.Fatalf("expected SemanticZonePresenceHitThreshold=%d by default, got %d", DefaultSemanticZonePresenceHitThreshold, cfg.SemanticZonePresenceHitThreshold)
	}
}

func TestApplyDefaults_SetsSemanticZonePresenceThresholdDefaults(t *testing.T) {
	cfg := applyDefaults(Config{})
	if cfg.SemanticZonePresenceMissThreshold != DefaultSemanticZonePresenceMissThreshold {
		t.Fatalf("expected SemanticZonePresenceMissThreshold=%d after defaults, got %d", DefaultSemanticZonePresenceMissThreshold, cfg.SemanticZonePresenceMissThreshold)
	}
	if cfg.SemanticZonePresenceHitThreshold != DefaultSemanticZonePresenceHitThreshold {
		t.Fatalf("expected SemanticZonePresenceHitThreshold=%d after defaults, got %d", DefaultSemanticZonePresenceHitThreshold, cfg.SemanticZonePresenceHitThreshold)
	}
}

func TestApplyDefaults_PreservesExplicitSemanticZonePresenceThresholds(t *testing.T) {
	cfg := applyDefaults(Config{
		SemanticZonePresenceMissThreshold: 5,
		SemanticZonePresenceHitThreshold:  4,
	})
	if cfg.SemanticZonePresenceMissThreshold != 5 {
		t.Fatalf("expected SemanticZonePresenceMissThreshold=5, got %d", cfg.SemanticZonePresenceMissThreshold)
	}
	if cfg.SemanticZonePresenceHitThreshold != 4 {
		t.Fatalf("expected SemanticZonePresenceHitThreshold=4, got %d", cfg.SemanticZonePresenceHitThreshold)
	}
}

func TestDefaultConfig_SetsSemanticDHWStaleTTL(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.SemanticDHWStaleTTL != DefaultSemanticDHWStaleTTL {
		t.Fatalf("expected SemanticDHWStaleTTL=%s by default, got %s", DefaultSemanticDHWStaleTTL, cfg.SemanticDHWStaleTTL)
	}
}

func TestApplyDefaults_SetsSemanticDHWStaleTTL(t *testing.T) {
	cfg := applyDefaults(Config{})
	if cfg.SemanticDHWStaleTTL != DefaultSemanticDHWStaleTTL {
		t.Fatalf("expected SemanticDHWStaleTTL=%s after defaults, got %s", DefaultSemanticDHWStaleTTL, cfg.SemanticDHWStaleTTL)
	}
}

func TestApplyDefaults_PreservesExplicitSemanticDHWStaleTTL(t *testing.T) {
	cfg := applyDefaults(Config{
		SemanticDHWStaleTTL: 42 * time.Minute,
	})
	if cfg.SemanticDHWStaleTTL != 42*time.Minute {
		t.Fatalf("expected SemanticDHWStaleTTL=42m, got %s", cfg.SemanticDHWStaleTTL)
	}
}

func TestDefaultConfig_SetsSemanticEnergyInterval(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.SemanticEnergyInterval != DefaultSemanticEnergyInterval {
		t.Fatalf("expected SemanticEnergyInterval=%s by default, got %s", DefaultSemanticEnergyInterval, cfg.SemanticEnergyInterval)
	}
}

func TestApplyDefaults_SetsSemanticEnergyInterval(t *testing.T) {
	cfg := applyDefaults(Config{})
	if cfg.SemanticEnergyInterval != DefaultSemanticEnergyInterval {
		t.Fatalf("expected SemanticEnergyInterval=%s after defaults, got %s", DefaultSemanticEnergyInterval, cfg.SemanticEnergyInterval)
	}
}

func TestApplyDefaults_PreservesExplicitSemanticEnergyInterval(t *testing.T) {
	cfg := applyDefaults(Config{
		SemanticEnergyInterval: 12 * time.Minute,
	})
	if cfg.SemanticEnergyInterval != 12*time.Minute {
		t.Fatalf("expected SemanticEnergyInterval=12m, got %s", cfg.SemanticEnergyInterval)
	}
}

func TestDefaultConfig_SetsPortalPath(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.PortalPath != "/portal" {
		t.Fatalf("expected PortalPath=/portal by default, got %q", cfg.PortalPath)
	}
}

func TestIssue817ConfigHasNoEEBusSpecificAdminOrCredentialSurface(t *testing.T) {
	content, err := os.ReadFile("config.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(content)
	for _, forbidden := range []string{
		"type EEBusAdminConfig struct",
		"DefaultEEBusAdminConfig",
		"EEBusAdminConfig",
	} {
		if strings.Contains(source, forbidden) {
			t.Errorf("config.go retains eeBUS-specific auth/config symbol %q", forbidden)
		}
	}
}

func TestApplyDefaults_SetsPortalPath(t *testing.T) {
	cfg := applyDefaults(Config{})
	if cfg.PortalPath != "/portal" {
		t.Fatalf("expected PortalPath=/portal after defaults, got %q", cfg.PortalPath)
	}
}

func TestDefaultConfig_DisablesM2MGraphQL(t *testing.T) {
	config := DefaultConfig().M2MGraphQL
	if !config.Disabled() || config.Validate() != nil {
		t.Fatalf("default M2M GraphQL config is not inert: %+v", config)
	}
}

func TestM2MGraphQLConfig_RejectsIncompleteActiveConfiguration(t *testing.T) {
	config := M2MGraphQLConfig{ListenAddr: "127.0.0.1:8443", AllowedAssets: []string{"pv-one"}}
	if err := config.Validate(); err == nil {
		t.Fatal("incomplete M2M GraphQL config was accepted")
	}
}

func TestM2MGraphQLConfig_RejectsMalformedOrDuplicateAuthority(t *testing.T) {
	base := M2MGraphQLConfig{
		ListenAddr: "127.0.0.1:8443", ServerName: "m2m.gateway.test", ClientCAFile: "ca.pem",
		ServerCertFile: "server.pem", ServerKeyFile: "server-key.pem", AllowedAssets: []string{"pv-one"},
	}
	malformed := base
	malformed.DeniedPrincipalFingerprints = []string{strings.Repeat("z", 64)}
	duplicate := base
	duplicate.AllowedAssets = []string{"pv-one", "pv-one"}
	for _, config := range []M2MGraphQLConfig{malformed, duplicate} {
		if err := config.Validate(); err == nil {
			t.Fatalf("invalid M2M GraphQL authority accepted: %+v", config)
		}
	}
}
