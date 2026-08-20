package main

import (
	"errors"
	"flag"
	"sort"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
)

func validateM2MGraphQLConfig(config ebusgateway.M2MGraphQLConfig) error {
	if err := config.Validate(); err != nil {
		return errors.New("M2M GraphQL configuration is invalid")
	}
	return nil
}

func bindM2MGraphQLFlags(fs *flag.FlagSet, cfg *ebusgateway.Config) {
	if fs == nil || cfg == nil {
		return
	}
	fs.StringVar(&cfg.M2MGraphQL.ListenAddr, "m2m-graphql-listen", cfg.M2MGraphQL.ListenAddr, "dedicated M2M GraphQL TLS listen address")
	fs.StringVar(&cfg.M2MGraphQL.ServerName, "m2m-graphql-server-name", cfg.M2MGraphQL.ServerName, "dedicated M2M GraphQL server certificate identity")
	fs.StringVar(&cfg.M2MGraphQL.ClientCAFile, "m2m-graphql-client-ca", cfg.M2MGraphQL.ClientCAFile, "client CA file for the dedicated M2M GraphQL listener")
	fs.StringVar(&cfg.M2MGraphQL.ServerCertFile, "m2m-graphql-server-cert", cfg.M2MGraphQL.ServerCertFile, "server certificate file for the dedicated M2M GraphQL listener")
	fs.StringVar(&cfg.M2MGraphQL.ServerKeyFile, "m2m-graphql-server-key", cfg.M2MGraphQL.ServerKeyFile, "server private-key file for the dedicated M2M GraphQL listener")
	fs.Func("m2m-graphql-allowed-assets", "comma-separated opaque asset allowlist", func(value string) error {
		cfg.M2MGraphQL.AllowedAssets = normalizeEEBusList(value, false)
		sort.Strings(cfg.M2MGraphQL.AllowedAssets)
		return nil
	})
	fs.Func("m2m-graphql-known-assets", "comma-separated assets known independently of retained snapshots", func(value string) error {
		cfg.M2MGraphQL.KnownAssets = normalizeEEBusList(value, false)
		sort.Strings(cfg.M2MGraphQL.KnownAssets)
		return nil
	})
	fs.Func("m2m-graphql-denied-principals", "comma-separated SHA-256 client certificate fingerprints", func(value string) error {
		cfg.M2MGraphQL.DeniedPrincipalFingerprints = normalizeEEBusList(value, true)
		sort.Strings(cfg.M2MGraphQL.DeniedPrincipalFingerprints)
		return nil
	})
	fs.BoolVar(&cfg.PortalPV.SemanticEnabled, "portal-pv-semantic-enabled", cfg.PortalPV.SemanticEnabled, "enable Portal PV semantic BFF")
	fs.BoolVar(&cfg.PortalPV.RawReadEnabled, "portal-modbus-raw-read-enabled", cfg.PortalPV.RawReadEnabled, "enable Portal raw Modbus diagnostics")
	fs.StringVar(&cfg.PortalPV.M2MURL, "portal-pv-m2m-url", cfg.PortalPV.M2MURL, "dedicated M2M GraphQL URL for Portal PV")
	fs.StringVar(&cfg.PortalPV.M2MServerName, "portal-pv-m2m-server-name", cfg.PortalPV.M2MServerName, "dedicated M2M GraphQL certificate identity for Portal PV")
	fs.StringVar(&cfg.PortalPV.M2MCAFile, "portal-pv-m2m-ca", cfg.PortalPV.M2MCAFile, "M2M GraphQL CA file for Portal PV")
	fs.StringVar(&cfg.PortalPV.M2MClientCert, "portal-pv-m2m-client-cert", cfg.PortalPV.M2MClientCert, "M2M GraphQL client certificate for Portal PV")
	fs.StringVar(&cfg.PortalPV.M2MClientKey, "portal-pv-m2m-client-key", cfg.PortalPV.M2MClientKey, "M2M GraphQL client private key for Portal PV")
	fs.StringVar(&cfg.PortalPV.AssetRef, "portal-pv-asset-ref", cfg.PortalPV.AssetRef, "fixed canonical PV asset reference for Portal PV")
}
