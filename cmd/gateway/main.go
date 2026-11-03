package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
)

func main() {
	// The local worker branch is selected from argv alone, before any gateway
	// configuration, flag, listener or job exists.
	if code, selected := dispatchEBusLocalWorker(os.Args, newEBusLocalWorkerChildPorts); selected {
		os.Exit(code)
	}

	cfg := ebusgateway.DefaultConfig()
	inputs := bindFlags(flag.CommandLine, &cfg)
	matterLibrary := bindMatterLibraryFlags(flag.CommandLine)
	flag.Parse()
	if err := resolveModbusEndpointFile(&cfg.ModbusTCPConfig, inputs.modbusEndpointFile); err != nil {
		log.Fatalf("gateway: %v", err)
	}
	applyTransportSourcePolicy(&cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// No real Matter library factory is linked: an enabled option resolves to
	// unavailable and the ordinary lifecycle runs unchanged.
	run := runWithMatterLibrary(newMatterLibraryComposition(matterLibrary, nil))
	if err := run(ctx, cfg); err != nil {
		if inputs.modbusEndpointFile != "" {
			err = redactFileSourcedModbusError(err, cfg.ModbusTCPConfig.Endpoint)
		}
		log.Fatalf("gateway: %v", err)
	}
}

func run(ctx context.Context, cfg ebusgateway.Config) error {
	return runGatewayLifecycle(ctx, cfg)
}

// runWithMatterLibrary wraps the ordinary lifecycle with the Matter library
// owner.
func runWithMatterLibrary(owner matterLibraryOwner) func(context.Context, ebusgateway.Config) error {
	return func(ctx context.Context, cfg ebusgateway.Config) error {
		return runGatewayLifecycleWithMatterLibrary(ctx, cfg, owner, run)
	}
}
