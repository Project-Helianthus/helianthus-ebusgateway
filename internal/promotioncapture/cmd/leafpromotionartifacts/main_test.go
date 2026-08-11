//go:build darwin || linux

package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/promotioncapture"
)

func TestReadPrivateJSONRequiresClosedOwnerOnlyRegularInput(t *testing.T) {
	type document struct {
		Value string `json:"value"`
	}
	directory := t.TempDir()
	valid := filepath.Join(directory, "valid.json")
	if err := os.WriteFile(valid, []byte(`{"value":"ok"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var result document
	if err := readPrivateJSON(valid, &result); err != nil || result.Value != "ok" {
		t.Fatalf("valid input = %#v, %v", result, err)
	}

	unknown := filepath.Join(directory, "unknown.json")
	if err := os.WriteFile(unknown, []byte(`{"value":"ok","extra":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := readPrivateJSON(unknown, &document{}); err == nil {
		t.Fatal("unknown field was accepted")
	}

	open := filepath.Join(directory, "open.json")
	if err := os.WriteFile(open, []byte(`{"value":"ok"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := readPrivateJSON(open, &document{}); err == nil {
		t.Fatal("group/world-readable input was accepted")
	}

	link := filepath.Join(directory, "link.json")
	if err := os.Symlink(valid, link); err != nil {
		t.Fatal(err)
	}
	if err := readPrivateJSON(link, &document{}); err == nil {
		t.Fatal("symlink input was accepted")
	}
}

func TestArtifactSummaryDoesNotWritePrivateCampaignFields(t *testing.T) {
	campaign := promotioncapture.Campaign{
		CampaignHash: "sha256:" + strings.Repeat("a", 64),
		Candidates: []promotioncapture.CampaignCandidate{
			{
				CandidateID: "m7-candidate-0009", Decision: promotioncapture.DecisionPromoted,
				EBusIdentity:  &promotioncapture.B524Identity{TargetPseudonym: "private-ebus-selector"},
				EEBusIdentity: &promotioncapture.EEBusIdentity{ServiceID: "private-eebus-service"},
			},
		},
	}
	var output bytes.Buffer
	if err := writeArtifactSummary(&output, campaign); err != nil {
		t.Fatal(err)
	}
	raw := output.String()
	for _, forbidden := range []string{"private-ebus-selector", "private-eebus-service", "ebus_identity", "eebus_identity"} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("summary leaked %q: %s", forbidden, raw)
		}
	}
	if !strings.Contains(raw, campaign.CampaignHash) || !strings.Contains(raw, campaign.Candidates[0].CandidateID) {
		t.Fatalf("summary omitted public receipt fields: %s", raw)
	}
}
