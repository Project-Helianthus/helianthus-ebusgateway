package main

import (
	"context"
	"log"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
)

func initRuntimeStateManager(ctx context.Context, cfg ebusgateway.Config, buildInfo gatewayBuildInfo) (*runtimestate.Manager, *runtimestate.State) {
	mgr := runtimestate.New(runtimestate.Options{
		Path:         cfg.RuntimeStatePath,
		GatewayBuild: gatewayBuildString(buildInfo),
		AddonVersion: buildInfo.ReleaseVersion,
	})
	state, err := mgr.Load(ctx)
	if err != nil {
		log.Printf("runtime_state: load returned error (continuing with empty state): %v", err)
		state = &runtimestate.State{}
	}

	// AD08 eager persist: when -instance-guid is supplied, durably write
	// meta.{schema_version, instance_guid, written_at} within ~1 s so the
	// crash-before-first-periodic-persist window is closed. Provenance per
	// AD27.
	if cfg.InstanceGUID != "" {
		source := identitySourceFromCfg(cfg.InstanceGUIDSource)
		if perr := mgr.EagerPersistInstanceGUID(ctx, cfg.InstanceGUID, source); perr != nil {
			log.Printf("runtime_state: eager-persist instance_guid failed (continuing): %v", perr)
		}
	} else if perr := mgr.Flush(ctx); perr != nil {
		log.Printf("runtime_state: startup metadata persist failed (continuing): %v", perr)
	}

	if serr := mgr.Start(ctx); serr != nil {
		log.Printf("runtime_state: start failed (continuing without periodic persister): %v", serr)
	}

	return mgr, state
}

func projectGatewayReadiness(ebusProxyReadiness func() string, snapshot eebusRuntimeLifecycleSnapshot) portal.RuntimeReadiness {
	eebusReadiness := eebusReadinessForLifecycle(snapshot)
	proxyReadiness := ebusProxyReadinessDisabled
	if ebusProxyReadiness != nil {
		proxyReadiness = ebusProxyReadiness()
	}
	return portal.RuntimeReadiness{
		ProcessReadiness:    string(eebusReadiness.ProcessReadiness),
		HTTPReadiness:       "READY",
		ProxyReadiness:      proxyReadiness,
		EEBusReadiness:      string(eebusReadiness.EEBusReadiness),
		EEBusDegradedReason: string(eebusReadiness.EEBusDegradedReason),
	}
}

// identitySourceFromCfg maps the CLI -instance-guid-source flag value to the
// runtimestate.IdentitySource enum, with AD27 deprecation log when the flag
// is absent. The well-formed value set is enforced here; unknown values fall
// back to "cli-override" with a warning rather than failing startup.
func identitySourceFromCfg(raw string) runtimestate.IdentitySource {
	switch raw {
	case string(runtimestate.IdentitySourceRuntimeState):
		return runtimestate.IdentitySourceRuntimeState
	case string(runtimestate.IdentitySourceLegacyMigrated):
		return runtimestate.IdentitySourceLegacyMigrated
	case string(runtimestate.IdentitySourceGenerated):
		return runtimestate.IdentitySourceGenerated
	case string(runtimestate.IdentitySourceCLIOverride):
		return runtimestate.IdentitySourceCLIOverride
	case "":
		log.Print("runtime_state: -instance-guid-source absent; defaulting to cli-override (deprecated; pass -instance-guid-source explicitly)")
		return runtimestate.IdentitySourceCLIOverride
	default:
		log.Printf("runtime_state: -instance-guid-source=%q is not a recognised AD27 value; treating as cli-override", raw)
		return runtimestate.IdentitySourceCLIOverride
	}
}
