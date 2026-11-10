//go:build !nomatter

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"net/http"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/m2mgraphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/matter/feedv1"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/catalogv1"
)

const matterProfileEnabled = true

func matterProfileAvailable(cfg ebusgateway.Config) bool { return !cfg.M2MGraphQL.Disabled() }

func bindMatterProfile(fs *flag.FlagSet) func(func(context.Context, ebusgateway.Config) error) func(context.Context, ebusgateway.Config) error {
	owner := newMatterLibraryComposition(bindMatterLibraryFlags(fs), nil)
	return func(next func(context.Context, ebusgateway.Config) error) func(context.Context, ebusgateway.Config) error {
		return func(ctx context.Context, cfg ebusgateway.Config) error {
			return runGatewayLifecycleWithMatterLibrary(ctx, cfg, owner, next)
		}
	}
}

func newProfileM2MRuntime(cfg ebusgateway.Config, tesla *teslaHSCRetainedOwner, source catalogv1.SourceCapture, bindings *managedSidecarBindings) (*m2mGraphQLRuntime, error) {
	provider, err := newGatewayMatterBindingFeedProvider(source, cfg.M2MGraphQL.AllowedAssets)
	if err != nil {
		return nil, fmt.Errorf("matter binding feed provider: %w", err)
	}
	return newM2MGraphQLRuntimeWithMatter(cfg, nil, nil, tesla, provider, bindings)
}

func newM2MGraphQLRuntimeWithMatter(config ebusgateway.Config, adapter *modbusadapter.Adapter, growatt *growattBMSRS485ProductionProvider, tesla *teslaHSCRetainedOwner, provider feedv1.Provider, managed ...*managedSidecarBindings) (*m2mGraphQLRuntime, error) {
	var routes func(http.Handler) (http.Handler, error)
	if provider != nil {
		routes = func(graphqlHandler http.Handler) (http.Handler, error) {
			return matterProfileRoutes(graphqlHandler, provider)
		}
	}
	return newM2MGraphQLRuntimeWithRoutes(config, adapter, growatt, tesla, routes, managed...)
}

func matterProfileRoutes(graphqlHandler http.Handler, matterProvider feedv1.Provider) (http.Handler, error) {
	instance, err := newMatterBindingFeedInstanceID()
	if err != nil {
		return nil, errors.New("matter binding feed instance identity is unavailable")
	}
	matterHandler, err := feedv1.New(matterProvider, feedv1.Options{InstanceID: instance, ReplayLimit: 64})
	if err != nil {
		return nil, errors.New("matter binding feed handler configuration is invalid")
	}
	// The Matter routes spend the same per-host request budget as GraphQL
	// before the serialized capture; a refusal is the feed's own error.
	budgetedMatter, err := m2mgraphql.RequestBudget(graphqlHandler, http.StripPrefix("/matter-binding", matterHandler), feedv1.WriteUnavailable)
	if err != nil {
		return nil, errors.New("matter binding feed request budget is unavailable")
	}
	mux := http.NewServeMux()
	mux.Handle("/graphql/m2m/v1", graphqlHandler)
	mux.Handle("/matter-binding/", budgetedMatter)
	return mux, nil
}

func newMatterBindingFeedInstanceID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return "matter-" + hex.EncodeToString(value[:]), nil
}
