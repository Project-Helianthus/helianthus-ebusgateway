package main

import (
	"testing"
	"time"
)

func TestSemanticPrometheusDomainsFollowEnabledConfiguration(t *testing.T) {
	at := time.Unix(100, 0)
	if domains := semanticPrometheusDomains(nil, nil, false, false, "storage:private", at); len(domains) != 0 {
		t.Fatalf("disabled domains registered: %#v", domains)
	}
	domains := semanticPrometheusDomains(nil, nil, true, true, "storage:private", at)
	if len(domains) != 2 || domains[0].Name != "pv" || domains[0].Available || domains[1].Name != "storage" || domains[1].Available {
		t.Fatalf("configured startup outages were not represented as unavailable: %#v", domains)
	}
}
