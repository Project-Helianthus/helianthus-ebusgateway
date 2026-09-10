package main

import (
	"flag"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
)

func TestSemanticPrometheusDomainsFollowEnabledConfiguration(t *testing.T) {
	at := time.Unix(100, 0)
	if domains := semanticPrometheusDomains(nil, nil, nil, false, false, false, "storage:private", at); len(domains) != 0 {
		t.Fatalf("disabled domains registered: %#v", domains)
	}
	domains := semanticPrometheusDomains(nil, nil, nil, true, true, true, "storage:private", at)
	if len(domains) != 3 || domains[0].Name != "pv" || domains[0].Available || domains[1].Name != "storage" || domains[1].Available || domains[2].Name != "evse" || domains[2].Available {
		t.Fatalf("configured startup outages were not represented as unavailable: %#v", domains)
	}
}

func TestBindFlagsPrometheusEVSEIsExplicitAndDisabledByDefault(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	if cfg.PrometheusEVSEEnabled {
		t.Fatal("EVSE Prometheus binding enabled by default")
	}
	fs := flag.NewFlagSet("gateway-test", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{"-semantic-prometheus-evse-enabled"}); err != nil || !cfg.PrometheusEVSEEnabled {
		t.Fatalf("explicit EVSE metrics configuration failed: enabled=%t err=%v", cfg.PrometheusEVSEEnabled, err)
	}
}
