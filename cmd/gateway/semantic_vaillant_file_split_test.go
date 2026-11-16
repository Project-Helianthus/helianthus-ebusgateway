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
			file: "semantic_vaillant.go",
			required: []string{
				"type vaillantSemanticPoller struct",
				"func startVaillantSemanticPolling(",
			},
		},
		{
			file: "semantic_vaillant_registers.go",
			required: []string{
				"type b524GroupDef struct",
				"var remoteDeviceGroups = []b524GroupDef{",
			},
		},
		{
			file: "semantic_vaillant_discovery.go",
			required: []string{
				"func (p *vaillantSemanticPoller) refreshDiscovery(",
				"func (p *vaillantSemanticPoller) discoverB524RootWithOptions(",
			},
		},
		{
			file: "semantic_vaillant_zones.go",
			required: []string{
				"func (p *vaillantSemanticPoller) refreshZoneSlowConfigFields(",
				"func zoneEquals(",
			},
		},
		{
			file: "semantic_vaillant_dhw.go",
			required: []string{
				"func (p *vaillantSemanticPoller) refreshDHW(",
				"func (p *vaillantSemanticPoller) readCylinderSnapshots(",
			},
		},
		{
			file: "semantic_vaillant_circuits.go",
			required: []string{
				"func (p *vaillantSemanticPoller) refreshCircuits(",
				"func mergeCircuitSnapshotNonDestructive(",
			},
		},
		{
			file: "semantic_vaillant_boiler.go",
			required: []string{
				"func (p *vaillantSemanticPoller) refreshBoilerStatusB509(",
				"func mergeBoilerSnapshotNonDestructive(",
			},
		},
		{
			file: "semantic_vaillant_boiler_config.go",
			required: []string{
				"func (p *vaillantSemanticPoller) SetBoilerConfig(",
				"var boilerConfigFieldSpecs = map[string]boilerConfigFieldSpec{",
			},
		},
		{
			file: "semantic_vaillant_system.go",
			required: []string{
				"func (p *vaillantSemanticPoller) refreshSystem(",
				"func (p *vaillantSemanticPoller) SetSystemConfig(",
			},
		},
		{
			file: "semantic_vaillant_radio_devices.go",
			required: []string{
				"func (p *vaillantSemanticPoller) refreshRadioDevices(",
				"type deviceSlotKey struct",
			},
		},
		{
			file: "semantic_vaillant_fm5.go",
			required: []string{
				"func (p *vaillantSemanticPoller) refreshFM5Semantic(",
				"type fm5EvidenceCapture struct",
			},
		},
		{
			file: "semantic_vaillant_energy.go",
			required: []string{
				"func (p *vaillantSemanticPoller) refreshEnergy(",
				"var b524EnergyQueries = []b524EnergyQuery{",
			},
		},
		{
			file: "semantic_vaillant_schedules.go",
			required: []string{
				"func (p *vaillantSemanticPoller) refreshSchedules(",
				"func (p *vaillantSemanticPoller) SetZoneTimeProgram(",
			},
		},
		{
			file: "semantic_vaillant_b524_access.go",
			required: []string{
				"func (p *vaillantSemanticPoller) readB524ValueWithMaxAge(",
				"func parseB524ReadPayload(",
			},
		},
		{
			file: "semantic_vaillant_b509_access.go",
			required: []string{
				"func (p *vaillantSemanticPoller) readB509ValueWithMaxAge(",
				"func (p *vaillantSemanticPoller) readB555Frame(",
			},
		},
		{
			file: "semantic_vaillant_codec.go",
			required: []string{
				"func decodeUint16Bool(",
				"func decodeB524Uint16(",
			},
		},
		{
			file: "vaillant_semantic_publication.go",
			required: []string{
				"func (p *vaillantSemanticPoller) publishZones(",
				"func (adapter mcpSemanticProviderAdapter) System(",
			},
		},
		{
			file: "semantic_vaillant_snapshots.go",
			required: []string{
				"type vaillantZoneSnapshot struct",
				"type semanticFieldFreshness struct",
			},
		},
		{
			file: "semantic_snapshot_merge.go",
			required: []string{
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
