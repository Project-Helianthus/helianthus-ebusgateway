package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/d3vi1/helianthus-ebusgateway"
	"github.com/d3vi1/helianthus-ebusgateway/graphql"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts := ebusgateway.SmokeOptions{
		OnGatewayReady: func(ctx context.Context, gateway *ebusgateway.Gateway, logger *log.Logger) {
			hub := graphql.NewBroadcastHub(nil)
			runtime := graphql.NewSemanticRuntime(gateway.Router, graphql.NewLiveSemanticProvider(), hub)
			runtime.Start(ctx)

			energyCh, err := hub.SubscribeEnergy(ctx)
			if err != nil {
				logger.Printf("energy subscription error: %v", err)
				return
			}
			go func() {
				for payload := range energyCh {
					totals, ok := payload.(*graphql.EnergyTotals)
					if !ok || totals == nil {
						continue
					}
					logger.Printf("energy totals update: %+v", *totals)
				}
			}()
		},
	}

	if err := ebusgateway.RunSmokeFromEnv(ctx, opts); err != nil {
		log.Fatalf("smoke: %v", err)
	}
}
