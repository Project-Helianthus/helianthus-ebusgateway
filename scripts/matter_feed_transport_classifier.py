#!/usr/bin/env python3
"""Accept only the detached Matter feed's exact M2M composition diff."""
from __future__ import annotations

import difflib
import subprocess
import sys
from pathlib import Path


EXPECTED: dict[str, tuple[list[str], list[str]]] = {
    "cmd/gateway/gateway_run_lifecycle.go": (
        [
            "	m2mRuntime, err := newM2MGraphQLRuntimeWithTesla(cfg, modbusAdapter, growattBMSRuntime, teslaHSCRetained)",
            "	portalCatalogSource := newGatewayPortalCatalogSource(modbusAdapter, gatewayPortalStoragePublication(growattBMSRuntime), teslaHSCRetained, portalContributions)",
        ],
        [
            "	portalCatalogSource := newGatewayPortalCatalogSource(modbusAdapter, gatewayPortalStoragePublication(growattBMSRuntime), teslaHSCRetained, portalContributions)",
            "	matterFeedProvider, err := newGatewayMatterBindingFeedProvider(portalCatalogSource, cfg.M2MGraphQL.AllowedAssets)",
            "	if err != nil {",
            '		return fmt.Errorf("matter binding feed provider: %w", err)',
            "	}",
            "	m2mRuntime, err := newM2MGraphQLRuntimeWithMatter(cfg, modbusAdapter, growattBMSRuntime, teslaHSCRetained, matterFeedProvider)",
        ],
    ),
    "cmd/gateway/m2m_graphql_config.go": (
        [
            '	fs.StringVar(&cfg.M2MGraphQL.ListenAddr, "m2m-graphql-listen", cfg.M2MGraphQL.ListenAddr, "dedicated M2M GraphQL TLS listen address")',
            '	fs.StringVar(&cfg.M2MGraphQL.ServerName, "m2m-graphql-server-name", cfg.M2MGraphQL.ServerName, "dedicated M2M GraphQL server certificate identity")',
            '	fs.StringVar(&cfg.M2MGraphQL.ClientCAFile, "m2m-graphql-client-ca", cfg.M2MGraphQL.ClientCAFile, "client CA file for the dedicated M2M GraphQL listener")',
            '	fs.StringVar(&cfg.M2MGraphQL.ServerCertFile, "m2m-graphql-server-cert", cfg.M2MGraphQL.ServerCertFile, "server certificate file for the dedicated M2M GraphQL listener")',
            '	fs.StringVar(&cfg.M2MGraphQL.ServerKeyFile, "m2m-graphql-server-key", cfg.M2MGraphQL.ServerKeyFile, "server private-key file for the dedicated M2M GraphQL listener")',
        ],
        [
            '	fs.StringVar(&cfg.M2MGraphQL.ListenAddr, "m2m-graphql-listen", cfg.M2MGraphQL.ListenAddr, "dedicated M2M API TLS listen address")',
            '	fs.StringVar(&cfg.M2MGraphQL.ServerName, "m2m-graphql-server-name", cfg.M2MGraphQL.ServerName, "dedicated M2M API server certificate identity")',
            '	fs.StringVar(&cfg.M2MGraphQL.ClientCAFile, "m2m-graphql-client-ca", cfg.M2MGraphQL.ClientCAFile, "client CA file for the dedicated M2M API listener")',
            '	fs.StringVar(&cfg.M2MGraphQL.ServerCertFile, "m2m-graphql-server-cert", cfg.M2MGraphQL.ServerCertFile, "server certificate file for the dedicated M2M API listener")',
            '	fs.StringVar(&cfg.M2MGraphQL.ServerKeyFile, "m2m-graphql-server-key", cfg.M2MGraphQL.ServerKeyFile, "server private-key file for the dedicated M2M API listener")',
        ],
    ),
    "cmd/gateway/m2m_graphql_runtime.go": (
        [
            "	handler, err := m2mgraphql.NewHandler(m2mgraphql.Config{",
            "		handler.ServeHTTP(response, request.WithContext(m2mgraphql.WithMTLSPrincipal(request.Context(), m2mFingerprint(request.TLS.PeerCertificates[0].Raw))))",
        ],
        [
            '	"crypto/rand"',
            '	"github.com/Project-Helianthus/helianthus-ebusgateway/matter/feedv1"',
            "	return newM2MGraphQLRuntimeWithMatter(config, adapter, growatt, tesla, nil)",
            "}",
            "",
            "func newM2MGraphQLRuntimeWithMatter(config ebusgateway.Config, adapter *modbusadapter.Adapter, growatt *growattBMSRS485ProductionProvider, tesla *teslaHSCRetainedOwner, matterProvider feedv1.Provider) (*m2mGraphQLRuntime, error) {",
            "	graphqlHandler, err := m2mgraphql.NewHandler(m2mgraphql.Config{",
            "	handler := graphqlHandler",
            "	if matterProvider != nil {",
            "		instance, err := newMatterBindingFeedInstanceID()",
            "		if err != nil {",
            '			return nil, errors.New("matter binding feed instance identity is unavailable")',
            "		}",
            "		matterHandler, err := feedv1.New(matterProvider, feedv1.Options{InstanceID: instance, ReplayLimit: 64})",
            "		if err != nil {",
            '			return nil, errors.New("matter binding feed handler configuration is invalid")',
            "		}",
            "		mux := http.NewServeMux()",
            '		mux.Handle("/graphql/m2m/v1", graphqlHandler)',
            '		mux.Handle("/matter-binding/", http.StripPrefix("/matter-binding", matterHandler))',
            "		handler = mux",
            "	}",
            "		principal := m2mFingerprint(request.TLS.PeerCertificates[0].Raw)",
            "		requestContext := m2mgraphql.WithMTLSPrincipal(request.Context(), principal)",
            "		requestContext = feedv1.WithPrincipal(requestContext, principal)",
            "		handler.ServeHTTP(response, request.WithContext(requestContext))",
            "}",
            "",
            "func newMatterBindingFeedInstanceID() (string, error) {",
            "	var value [16]byte",
            "	if _, err := rand.Read(value[:]); err != nil {",
            '		return "", err',
            "	}",
            '	return "matter-" + hex.EncodeToString(value[:]), nil',
        ],
    ),
    "config.go": (
        ["// M2MGraphQLConfig configures the dedicated public SemReg PV listener."],
        [
            "// M2MGraphQLConfig configures the dedicated public M2M API listener. The field",
            "// and CLI prefix retain their pre-v1 name while the listener also hosts other",
            "// versioned M2M contracts such as the Matter binding feed.",
        ],
    ),
}


def base_text(base_ref: str, path: str) -> str:
    result = subprocess.run(
        ["git", "show", f"{base_ref}:{path}"], text=True, capture_output=True, check=False
    )
    if result.returncode:
        raise ValueError(f"cannot read base source for {path}")
    return result.stdout


def changed_lines(before: str, after: str) -> tuple[list[str], list[str]]:
    removed: list[str] = []
    added: list[str] = []
    for line in difflib.unified_diff(before.splitlines(), after.splitlines(), n=0):
        if line.startswith(("---", "+++", "@@")):
            continue
        if line.startswith("-"):
            removed.append(line[1:])
        elif line.startswith("+"):
            added.append(line[1:])
    return removed, added


def main() -> int:
    if len(sys.argv) != 3:
        return 2
    base_ref, path = sys.argv[1:]
    expected = EXPECTED.get(path)
    if expected is None:
        return 1
    try:
        before = base_text(base_ref, path)
        after = Path(path).read_text(encoding="utf-8")
    except (OSError, ValueError):
        return 1
    return int(changed_lines(before, after) != expected)


if __name__ == "__main__":
    raise SystemExit(main())
