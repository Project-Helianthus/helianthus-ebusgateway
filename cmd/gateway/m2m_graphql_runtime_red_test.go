package main

import (
	"testing"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
)

func TestM2MGraphQLRuntime_IsDisabledByDefaultAndSeparateFromGenericHTTP(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	runtime, err := newM2MGraphQLRuntime(cfg, nil)
	if err != nil {
		t.Fatalf("newM2MGraphQLRuntime: %v", err)
	}
	if runtime != nil {
		t.Fatalf("default config exposed M2M runtime: %#v", runtime)
	}
}

func TestM2MGraphQLRuntime_RequiresMutualTLSAndClosesListener(t *testing.T) {
	runtime, err := newM2MGraphQLRuntime(ebusgateway.Config{M2MGraphQL: ebusgateway.M2MGraphQLConfig{ListenAddr: "127.0.0.1:0", ClientCAFile: "testdata/client-ca.pem", ServerCertFile: "testdata/server.pem", ServerKeyFile: "testdata/server-key.pem", AllowedAssets: []string{"pv-asset-fixture"}}}, nil)
	if err != nil {
		t.Fatalf("newM2MGraphQLRuntime: %v", err)
	}
	if runtime == nil || !runtime.RequiresVerifiedClientCertificate() {
		t.Fatalf("runtime=%#v; want dedicated verified-mTLS listener", runtime)
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("close dedicated M2M listener: %v", err)
	}
}
