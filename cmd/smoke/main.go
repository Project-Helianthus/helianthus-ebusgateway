package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/d3vi1/helianthus-ebusgateway"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := ebusgateway.RunSmokeFromEnv(ctx, ebusgateway.SmokeOptions{}); err != nil {
		log.Fatalf("smoke: %v", err)
	}
}
