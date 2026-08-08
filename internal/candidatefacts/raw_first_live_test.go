package candidatefacts

import (
	"bytes"
	"strings"
	"testing"
)

func TestMSP07RawFirstBuildConsumesM625PathsAndCloudObservation(t *testing.T) {
	bundle := rawFirstFixture(t, "m625-bundle.json")
	sourceReplay := rawFirstFixture(t, "m625-source-replay.json")

	firstGraph, firstReplay, err := BuildRawFirstV1(bundle, sourceReplay)
	if err != nil {
		t.Fatalf("BuildRawFirstV1(first): %v", err)
	}
	secondGraph, secondReplay, err := BuildRawFirstV1(bundle, sourceReplay)
	if err != nil {
		t.Fatalf("BuildRawFirstV1(second): %v", err)
	}
	if !bytes.Equal(firstGraph, secondGraph) || !bytes.Equal(firstReplay, secondReplay) {
		t.Fatal("raw-first build is not byte-identical")
	}
	if err := Verify(firstGraph, bundle, sourceReplay); err != nil {
		t.Fatalf("Verify(raw-first graph): %v", err)
	}

	graph, err := decodeGraphV1(firstGraph)
	if err != nil {
		t.Fatal(err)
	}
	if len(graph.Facts) != 2 {
		t.Fatalf("facts = %d; want one eeBUS path and one cloud observation", len(graph.Facts))
	}
	statuses := map[string]int{}
	for _, fact := range graph.Facts {
		statuses[fact.Status]++
		if fact.DraftValue != nil || fact.DraftUnit != nil || !fact.DebugOnly {
			t.Fatalf("fact %s escaped the raw/debug boundary", fact.CandidateID)
		}
		if fact.Status == "RAW_ONLY" {
			if fact.Provenance.EEBus == nil || len(fact.Provenance.EEBus.FeaturePath) != 4 {
				t.Fatalf("fact %s does not carry the complete M6.25 path", fact.CandidateID)
			}
		}
	}
	if statuses["RAW_ONLY"] != 1 || statuses["WITHHELD"] != 1 ||
		statuses["CANDIDATE"] != 0 || statuses["CONFLICTED"] != 0 {
		t.Fatalf("statuses = %#v", statuses)
	}
	if strings.Contains(string(firstGraph), "candidate_ref") || strings.Contains(string(firstReplay), "candidate_ref") {
		t.Fatal("candidate_ref leaked into raw-first output")
	}
}

func TestMSP07RawFirstBuildCoversEveryEligibleSourceTerminal(t *testing.T) {
	bundle := sourceTerminalFixture(t, "source", "source-terminal-bundle.json")
	sourceReplay := sourceTerminalFixture(t, "source", "source-terminal-replay-result.json")

	graphRaw, replayRaw, err := BuildRawFirstV1(bundle, sourceReplay)
	if err != nil {
		t.Fatalf("BuildRawFirstV1(source terminal): %v", err)
	}
	graph, err := decodeGraphV1(graphRaw)
	if err != nil {
		t.Fatal(err)
	}
	if len(graph.Facts) != 3 {
		t.Fatalf("facts = %d; want B509/B524/B555 terminal facts", len(graph.Facts))
	}
	families := map[string]bool{}
	for _, fact := range graph.Facts {
		terminal := fact.Provenance.SourceTerminal
		if terminal == nil || fact.Status != "WITHHELD" ||
			fact.TerminalNegativeState == nil || *fact.TerminalNegativeState != "NOT_TESTED" ||
			fact.DraftValue != nil || fact.DraftUnit != nil || !fact.DebugOnly {
			t.Fatalf("fact %s is not a bounded source-terminal result", fact.CandidateID)
		}
		families[terminal.EBusIdentity.Family] = true
	}
	for _, family := range []string{"B509", "B524", "B555"} {
		if !families[family] {
			t.Fatalf("missing %s source terminal", family)
		}
	}
	regenerated, err := Replay(graphRaw, bundle, sourceReplay)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(regenerated, replayRaw) {
		t.Fatal("BuildRawFirstV1 replay is not canonical")
	}
}

func rawFirstFixture(t *testing.T, name string) []byte {
	t.Helper()
	digests := map[string]string{
		"m625-bundle.json":        "f7296868773f81468413504992bd8dce5c74d5037e8135ca33b105da64ddc9a4",
		"m625-source-replay.json": "c6dcd896e307c7ab57b685f63f80e5f740d780eb416c34992d7a77d267264adb",
	}
	digest, ok := digests[name]
	if !ok {
		t.Fatalf("unregistered raw-first fixture %q", name)
	}
	return readPinned("testdata/canonical/source/"+name, digest)
}
