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
	// The NO_SEND Tesla observation run is likewise selected from argv alone.
	if code, selected := dispatchTeslaObserve(os.Args, newTeslaObservePorts); selected {
		os.Exit(code)
	}

	cfg := ebusgateway.DefaultConfig()
	inputs := bindFlags(flag.CommandLine, &cfg)
	profile := bindMatterProfile(flag.CommandLine)
	flag.Parse()
	if err := resolveModbusEndpointFile(&cfg.ModbusTCPConfig, inputs.modbusEndpointFile); err != nil {
		log.Fatalf("gateway: %v", err)
	}
	applyTransportSourcePolicy(&cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	run := profile(run)
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
