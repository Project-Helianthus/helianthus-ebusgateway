package ebusgateway

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTransportConfigFromSmokeEbusdTCPProfile(t *testing.T) {
	t.Parallel()

	cfg := smokeConfig{
		ENH: enhConfig{
			Type:       "tcp",
			Host:       "127.0.0.1",
			Port:       9999,
			TimeoutSec: 3,
		},
		Smoke: smokeBehavior{
			Profile: string(TransportEbusdTCP),
		},
	}

	transportCfg, err := transportConfigFromSmoke(cfg)
	if err != nil {
		t.Fatalf("transportConfigFromSmoke error = %v", err)
	}
	if transportCfg.Protocol != TransportEbusdTCP {
		t.Fatalf("protocol = %q; want %q", transportCfg.Protocol, TransportEbusdTCP)
	}
	if transportCfg.Network != "tcp" {
		t.Fatalf("network = %q; want tcp", transportCfg.Network)
	}
	if transportCfg.Address != "127.0.0.1:9999" {
		t.Fatalf("address = %q; want 127.0.0.1:9999", transportCfg.Address)
	}
}

func TestTransportConfigFromSmokeEbusdTCPRejectsNonTCP(t *testing.T) {
	t.Parallel()

	cfg := smokeConfig{
		ENH: enhConfig{
			Type:       "unix",
			Path:       "/tmp/ebusd.sock",
			TimeoutSec: 3,
		},
		Smoke: smokeBehavior{
			Profile: string(TransportEbusdTCP),
		},
	}

	_, err := transportConfigFromSmoke(cfg)
	if err == nil {
		t.Fatalf("expected error for ebusd-tcp profile with non-tcp enh.type")
	}
	if !strings.Contains(err.Error(), "requires enh.type tcp") {
		t.Fatalf("error = %v; want requires enh.type tcp", err)
	}
}

func TestSmokeReportPathResolutionAndWrite(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	reportPath, err := resolveSmokeReportPath("", root)
	if err != nil {
		t.Fatalf("resolveSmokeReportPath error = %v", err)
	}
	wantPath := filepath.Join(root, defaultSmokeReportPath)
	if reportPath != wantPath {
		t.Fatalf("report path = %q; want %q", reportPath, wantPath)
	}

	report := smokeReport{
		Version:  "1",
		Profile:  string(TransportEbusdTCP),
		ReadOnly: true,
		Startup:  smokeCheckSummary{OK: true, Details: "started"},
		Scan:     smokeScanSummary{OK: true, Devices: 1},
		GraphQL:  smokeCheckSummary{OK: true, Details: "query ok"},
		MCP:      smokeCheckSummary{OK: true, Details: "tools ok"},
	}
	if err := writeSmokeReport(reportPath, report); err != nil {
		t.Fatalf("writeSmokeReport error = %v", err)
	}

	raw, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("ReadFile error = %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("json.Unmarshal error = %v", err)
	}

	if _, ok := decoded["startup"].(map[string]any); !ok {
		t.Fatalf("report missing startup object: %v", decoded["startup"])
	}
	if _, ok := decoded["scan"].(map[string]any); !ok {
		t.Fatalf("report missing scan object: %v", decoded["scan"])
	}
	if _, ok := decoded["graphql"].(map[string]any); !ok {
		t.Fatalf("report missing graphql object: %v", decoded["graphql"])
	}
	if _, ok := decoded["mcp"].(map[string]any); !ok {
		t.Fatalf("report missing mcp object: %v", decoded["mcp"])
	}
}
