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
	cfg := ebusgateway.DefaultConfig()
	inputs := bindFlags(flag.CommandLine, &cfg)
	flag.Parse()
	if err := resolveModbusEndpointFile(&cfg.ModbusTCPConfig, inputs.modbusEndpointFile); err != nil {
		log.Fatalf("gateway: %v", err)
	}
	applyTransportSourcePolicy(&cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
