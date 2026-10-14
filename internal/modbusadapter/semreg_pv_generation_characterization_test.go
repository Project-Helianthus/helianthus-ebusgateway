package modbusadapter

import (
	"path/filepath"
	"testing"
)

// TestPVCharacterizationGenerationAndEpochReplacement pins the canonical
// bytes of one PV source across a same-epoch driver-generation replacement
// and a later epoch replacement (issue #1024). The goldens were captured from
// the pre-hub publication path.
func TestPVCharacterizationGenerationAndEpochReplacement(t *testing.T) {
	core, err := newTestPVPublicationCore()
	if err != nil {
		t.Fatal(err)
	}
	words := observedFroniusFloatControlsWords()
	dir := filepath.Join("testdata", "characterization", "pv_generation_replacement")
	steps := []struct {
		name       string
		epoch      string
		generation uint64
		receipt    uint64
		started    uint64
	}{
		{"1_generation1_first", "source-epoch:pv:char-a", 1, 2_000, 0},
		{"2_generation1_refresh", "source-epoch:pv:char-a", 1, 3_000, 0},
		{"3_generation2_replacement", "source-epoch:pv:char-a", 2, 4_000, 0},
		{"4_generation2_refresh", "source-epoch:pv:char-a", 2, 5_000, 0},
		{"5_epoch_replacement", "source-epoch:pv:char-b", 1, 7_000, 6_500},
	}
	for _, step := range steps {
		draft := pvCoreDraft(t, words, step.epoch, step.generation, step.receipt)
		if step.started != 0 {
			draft.lifecycle.sourceStartedAt = pvCoreWall(step.started)
			draft.source.StartedAt = draft.lifecycle.sourceStartedAt
		}
		if _, err := core.ingest(draft); err != nil {
			t.Fatalf("%s: %v", step.name, err)
		}
		view, err := core.currentForValidation(draft.assetID)
		if err != nil {
			t.Fatal(err)
		}
		pvCharacterizationGolden(t, dir, step.name+".canonical.json", view.canonical)
		pvCharacterizationGolden(t, dir, step.name+".ids.json", pvCharacterizationIdentity(t, view.snapshot))
	}
}
