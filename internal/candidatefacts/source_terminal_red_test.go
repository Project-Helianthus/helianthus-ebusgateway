package candidatefacts

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/syncevidence"
)

const sourceTerminalOwnerCommit = "35d2eba256a77b6575a2b45c07e73f054ff74ced"

func TestMSP07SourceTerminalContractPin(t *testing.T) {
	got := PinnedContractV1()
	if got.OwnerCommit != sourceTerminalOwnerCommit {
		t.Fatalf("OwnerCommit = %s; want %s", got.OwnerCommit, sourceTerminalOwnerCommit)
	}
	if got.GraphSchemaSHA256 != "e7a2fd4d9a9494c8d25f6b688684e70d8bce72002df0bdcc76da766aec02706b" {
		t.Fatalf("GraphSchemaSHA256 = %s", got.GraphSchemaSHA256)
	}
	if got.ReplaySchemaSHA256 != "742a7e29f95ff17ad6b6b5669185f38b54fee07bc1af94b430cd7beb1db91a66" {
		t.Fatalf("ReplaySchemaSHA256 = %s", got.ReplaySchemaSHA256)
	}
}

func TestMSP07SourceTerminalBuildVerifyReplayAreCanonical(t *testing.T) {
	graphRaw := sourceTerminalFixture(t, "positive", "source-terminal-graph.json")
	replayRaw := sourceTerminalFixture(t, "positive", "source-terminal-replay-result.json")
	bundleRaw := sourceTerminalFixture(t, "source", "source-terminal-bundle.json")
	sourceReplayRaw := sourceTerminalFixture(t, "source", "source-terminal-replay-result.json")
	generatedSourceReplay, err := syncevidence.Replay(bundleRaw)
	if err != nil {
		t.Fatalf("syncevidence.Replay(source terminal): %v", err)
	}
	wantSourceReplay, err := syncevidence.CanonicalizeJSON(sourceReplayRaw)
	if err != nil || !bytes.Equal(generatedSourceReplay, wantSourceReplay) {
		t.Fatalf("source-terminal source replay differs from canonical bytes err=%v\ngot=%s\nwant=%s", err, generatedSourceReplay, wantSourceReplay)
	}
	sourceBundle, sourceReplay, err := verifySourceInputs(bundleRaw, sourceReplayRaw)
	if err != nil {
		t.Fatalf("verifySourceInputs(source terminal): %v", err)
	}
	registry, _, err := loadRegistryV1()
	if err != nil {
		t.Fatal(err)
	}
	value, err := parseJSON(graphRaw)
	if err != nil {
		t.Fatal(err)
	}
	graphValue, _ := objectValue(value)
	if err := checkProvenance(graphValue, registry, sourceBundle, sourceReplay); err != nil {
		t.Fatalf("checkProvenance(source terminal): %v", err)
	}
	if err := Verify(graphRaw, bundleRaw, sourceReplayRaw); err != nil {
		t.Fatalf("Verify(canonical source terminal): %v", err)
	}

	graph, err := decodeGraphV1(graphRaw)
	if err != nil {
		t.Fatalf("decodeGraphV1(source terminal): %v", err)
	}
	families := map[string]bool{}
	for index := range graph.Facts {
		terminal := graph.Facts[index].Provenance.SourceTerminal
		if terminal == nil {
			t.Fatalf("fact %d has no source-terminal provenance", index)
		}
		families[terminal.EBusIdentity.Family] = true
		graph.Facts[index].FactHash = ""
	}
	for _, family := range []string{"B509", "B524", "B555"} {
		if !families[family] {
			t.Fatalf("source-terminal graph does not cover %s", family)
		}
	}

	built, err := Build(BuildInputV1{
		SourceBundle:     bundleRaw,
		SourceReplay:     sourceReplayRaw,
		ComparatorDrafts: graph.ComparatorDrafts,
		Facts:            graph.Facts,
	})
	if err != nil {
		t.Fatalf("Build(source terminal): %v", err)
	}
	wantGraph, err := canonicalJSON(graphValue)
	if err != nil || !bytes.Equal(built, wantGraph) {
		t.Fatalf("source-terminal build differs from the canonical graph err=%v", err)
	}
	if err := Verify(built, bundleRaw, sourceReplayRaw); err != nil {
		t.Fatalf("Verify(source terminal): %v", err)
	}
	replayed, err := Replay(built, bundleRaw, sourceReplayRaw)
	if err != nil {
		t.Fatalf("Replay(source terminal): %v", err)
	}
	if !bytes.Equal(replayed, replayRaw) {
		t.Fatal("source-terminal replay differs from canonical bytes")
	}
}

func TestMSP07SourceTerminalCoverageCannotBeErased(t *testing.T) {
	graphRaw := sourceTerminalFixture(t, "positive", "source-terminal-graph.json")
	bundleRaw := sourceTerminalFixture(t, "source", "source-terminal-bundle.json")
	sourceReplayRaw := sourceTerminalFixture(t, "source", "source-terminal-replay-result.json")

	for _, test := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{
			name: "omitted",
			mutate: func(provenance map[string]any) {
				delete(provenance, "source_terminal")
			},
		},
		{
			name: "explicit null",
			mutate: func(provenance map[string]any) {
				provenance["source_terminal"] = nil
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			value, err := parseJSON(graphRaw)
			if err != nil {
				t.Fatal(err)
			}
			graph, _ := objectValue(value)
			facts, _ := arrayValue(graph["facts"])
			fact, _ := objectValue(facts[0])
			provenance, _ := objectValue(fact["provenance"])
			test.mutate(provenance)
			mutated, err := canonicalJSON(graph)
			if err != nil {
				t.Fatal(err)
			}
			assertCategory(t, Verify(mutated, bundleRaw, sourceReplayRaw), "provenance.binding")
		})
	}
}

func sourceTerminalFixture(t *testing.T, group, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "canonical", group, name))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
