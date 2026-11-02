package main

import (
	"net/http/httptest"
	"strings"
	"testing"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

type baudPresenceTransport struct{ transport.RawTransport }

func TestBaudPresenceSurvivesDriverPreparation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cfg     ebusgateway.Config
		present bool
	}{
		{name: "empty"},
		{name: "modbus only", cfg: ebusgateway.Config{ModbusTCPConfig: ebusgateway.ModbusTCPConfig{Enabled: true}}},
		{name: "eebus only", cfg: ebusgateway.Config{EEBusConfig: ebusgateway.EEBusConfig{Enabled: true}}},
		{name: "protocol", cfg: ebusgateway.Config{TransportConfig: ebusgateway.TransportConfig{Protocol: ebusgateway.TransportENH}}, present: true},
		{name: "address", cfg: ebusgateway.Config{TransportConfig: ebusgateway.TransportConfig{Address: "synthetic"}}, present: true},
		{name: "active", cfg: ebusgateway.Config{Transport: &baudPresenceTransport{}}, present: true},
		{name: "passive", cfg: ebusgateway.Config{PassiveTransport: &baudPresenceTransport{}}, present: true},
		{name: "defaults", cfg: ebusgateway.DefaultConfig(), present: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg
			cfg.EvidenceRecorderConfig = ebusgateway.DefaultConfig().EvidenceRecorderConfig
			if _, err := prepareGatewayRunConfig(&cfg); err != nil {
				t.Fatal(err)
			}
			if _, err := prepareGatewayEBusDriver(&cfg); err != nil {
				t.Fatal(err)
			}
			if cfg.Transport == nil {
				t.Fatal("driver preparation did not inject the stable transport")
			}
			store, dedup, err := wireObserveFirstObservers(&cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			if dedup != nil {
				defer func() { _ = dedup.Close() }()
			}
			response := httptest.NewRecorder()
			store.MetricsHandler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
			body := response.Body.String()
			for _, part := range []string{
				"# HELP ebus_baud_observation_available ",
				"# TYPE ebus_baud_observation_available gauge\n",
				"\nebus_baud_observation_available 0\n",
			} {
				if strings.Contains(body, part) != tc.present {
					t.Errorf("presence of %q = %t, want %t", part, strings.Contains(body, part), tc.present)
				}
			}
		})
	}
}
