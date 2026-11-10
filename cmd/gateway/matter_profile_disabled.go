//go:build nomatter

package main

import (
	"context"
	"flag"
	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/catalogv1"
)

const matterProfileEnabled = false

func matterProfileAvailable(ebusgateway.Config) bool { return false }

func bindMatterProfile(*flag.FlagSet) func(func(context.Context, ebusgateway.Config) error) func(context.Context, ebusgateway.Config) error {
	return func(next func(context.Context, ebusgateway.Config) error) func(context.Context, ebusgateway.Config) error {
		return next
	}
}

func newProfileM2MRuntime(cfg ebusgateway.Config, tesla *teslaHSCRetainedOwner, _ catalogv1.SourceCapture, bindings *managedSidecarBindings) (*m2mGraphQLRuntime, error) {
	return newM2MGraphQLRuntimeWithRoutes(cfg, nil, nil, tesla, nil, bindings)
}
