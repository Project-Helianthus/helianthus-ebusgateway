package main

import (
	"os"
	"strings"
	"testing"
)

func TestVaillantSemanticSplitKeepsThematicContractsDiscoverable(t *testing.T) {
	tests := []struct {
		file     string
		required []string
	}{
		{
			file: "semantic_vaillant_snapshots.go",
			required: []string{
				"type vaillantZoneSnapshot struct",
				"type semanticFieldFreshness struct",
				"func mergeZoneSnapshotFields(",
				"func mergeDhwSnapshotFields(",
			},
		},
		{
			file: "semantic_vaillant_startup.go",
			required: []string{
				"func (p *vaillantSemanticPoller) refreshStartupSemanticPlanes(",
				"func (p *vaillantSemanticPoller) refreshStartupCriticalSemanticPlanes(",
				"func (p *vaillantSemanticPoller) refreshFM5SemanticStartup(",
			},
		},
		{
			file: "semantic_vaillant_passive.go",
			required: []string{
				"func (p *vaillantSemanticPoller) AttachPassiveShadowProducer(",
				"func (p *vaillantSemanticPoller) handleAdjudicatedPassiveEvent(",
				"func passiveShadowLaneEnabled(",
			},
		},
	}
	for _, test := range tests {
		source, err := os.ReadFile(test.file)
		if err != nil {
			t.Fatalf("read %s: %v", test.file, err)
		}
		for _, required := range test.required {
			if !strings.Contains(string(source), required) {
				t.Errorf("%s does not retain %q", test.file, required)
			}
		}
	}
}
