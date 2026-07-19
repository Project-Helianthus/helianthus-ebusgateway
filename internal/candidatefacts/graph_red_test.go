package candidatefacts

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

func TestMSP07BuildVerifyReplayAreDeterministicAndCanonical(t *testing.T) {
	artifacts := PinnedArtifactsV1()
	expected, err := DecodeGraphV1(artifacts.PositiveGraph)
	if err != nil {
		t.Fatalf("DecodeGraphV1(positive): %v", err)
	}
	drafts := append([]FactV1(nil), expected.Facts...)
	for index := range drafts {
		drafts[index].FactHash = ""
	}
	input := BuildInputV1{
		SourceBundle:     artifacts.SourceBundle,
		SourceReplay:     artifacts.SourceReplay,
		ComparatorDrafts: append([]ComparatorDraftV1(nil), expected.ComparatorDrafts...),
		Facts:            drafts,
	}

	first, err := Build(input)
	if err != nil {
		t.Fatalf("Build(first): %v", err)
	}
	t.Setenv("LANG", "invalid_LOCALE")
	t.Setenv("LC_ALL", "C")
	t.Setenv("TZ", "Pacific/Kiritimati")
	t.Setenv("HOME", t.TempDir())
	second, err := Build(input)
	if err != nil {
		t.Fatalf("Build(second): %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("Build is sensitive to host environment or mutable runtime state")
	}
	built, err := DecodeGraphV1(first)
	if err != nil {
		t.Fatalf("DecodeGraphV1(built): %v", err)
	}
	if !reflect.DeepEqual(built, expected) {
		t.Fatalf("built graph does not match the canonical positive graph\nbuilt: %#v\nwant:  %#v", built, expected)
	}
	if err := Verify(first, artifacts.SourceBundle, artifacts.SourceReplay); err != nil {
		t.Fatalf("Verify(built): %v", err)
	}

	replay1, err := Replay(first, artifacts.SourceBundle, artifacts.SourceReplay)
	if err != nil {
		t.Fatalf("Replay(first): %v", err)
	}
	replay2, err := Replay(first, artifacts.SourceBundle, artifacts.SourceReplay)
	if err != nil {
		t.Fatalf("Replay(second): %v", err)
	}
	if !bytes.Equal(replay1, replay2) || !bytes.Equal(replay1, artifacts.PositiveReplay) {
		t.Fatalf("replay bytes differ from canonical golden\ngot:  %s\nwant: %s", replay1, artifacts.PositiveReplay)
	}
}

func TestMSP07RequiresSeparatelyVerifiedMSP065BundleAndReplay(t *testing.T) {
	artifacts := PinnedArtifactsV1()
	graph, err := DecodeGraphV1(artifacts.PositiveGraph)
	if err != nil {
		t.Fatal(err)
	}
	drafts := append([]FactV1(nil), graph.Facts...)
	for index := range drafts {
		drafts[index].FactHash = ""
	}
	base := BuildInputV1{
		SourceBundle: artifacts.SourceBundle, SourceReplay: artifacts.SourceReplay,
		ComparatorDrafts: graph.ComparatorDrafts, Facts: drafts,
	}

	wrongBundle := replaceTopLevel(t, artifacts.SourceBundle, "bundle_hash", "sha256:"+repeatByte('f', 64))
	wrongReplay := replaceTopLevel(t, artifacts.SourceReplay, "bundle_id", "sebv1:sha256:"+repeatByte('f', 64))

	for _, test := range []struct {
		name   string
		bundle []byte
		replay []byte
	}{
		{name: "bundle", bundle: wrongBundle, replay: artifacts.SourceReplay},
		{name: "replay", bundle: artifacts.SourceBundle, replay: wrongReplay},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := base
			input.SourceBundle = test.bundle
			input.SourceReplay = test.replay
			_, buildErr := Build(input)
			assertCategory(t, buildErr, "provenance.binding")
			assertCategory(t, Verify(artifacts.PositiveGraph, test.bundle, test.replay), "provenance.binding")
			_, replayErr := Replay(artifacts.PositiveGraph, test.bundle, test.replay)
			assertCategory(t, replayErr, "provenance.binding")
		})
	}
}

func TestMSP07ClosedSchemaBoundsAndFirstErrorCategory(t *testing.T) {
	artifacts := PinnedArtifactsV1()
	for name, want := range negativeCategoriesV1 {
		name, want := name, want
		t.Run(name, func(t *testing.T) {
			assertCategory(t, Verify(artifacts.NegativeGraphs[name], artifacts.SourceBundle, artifacts.SourceReplay), want)
		})
	}
}

func TestMSP07RemainsV1AndRejectsPrematureV2(t *testing.T) {
	artifacts := PinnedArtifactsV1()
	v2 := replaceTopLevel(t, artifacts.PositiveGraph, "schema_version", json.Number("2"))
	assertCategory(t, Verify(v2, artifacts.SourceBundle, artifacts.SourceReplay), "schema.graph")
	if _, err := DecodeGraphV1(v2); err == nil || err.Error() != "schema.graph" {
		t.Fatalf("DecodeGraphV1(v2) error = %v; want schema.graph", err)
	}
}

func TestMSP07CanonicalGraphAndReplayHashes(t *testing.T) {
	artifacts := PinnedArtifactsV1()
	graph := decodeObject(t, artifacts.PositiveGraph)
	if graph["graph_id"] != "dcfgv1:sha256:e20087a0df5febae51a0edb5eecf1eda149419951554105416989130ceed30a3" ||
		graph["graph_hash"] != "sha256:e20087a0df5febae51a0edb5eecf1eda149419951554105416989130ceed30a3" {
		t.Fatalf("canonical graph hashes drifted: id=%v hash=%v", graph["graph_id"], graph["graph_hash"])
	}
	registry := objectAt(t, graph, "registry")
	if registry["digest"] != "sha256:"+canonicalArtifactDigestsV1["registry"] {
		t.Fatalf("graph registry digest = %v", registry["digest"])
	}
	source := objectAt(t, graph, "source_bundle")
	for key, want := range map[string]string{
		"bundle_id":   "sebv1:sha256:2df0e688d8c615c657698350eac545a5997af39d50502dc56fe2df5eade5da61",
		"bundle_hash": "sha256:2df0e688d8c615c657698350eac545a5997af39d50502dc56fe2df5eade5da61",
		"replay_hash": "sha256:d67fbf596558b994a42ed93e7436a340c22d34d93e4db6f2bf442a48a7f82a4f",
	} {
		if source[key] != want {
			t.Errorf("source_bundle.%s = %v; want %s", key, source[key], want)
		}
	}

	replay := decodeObject(t, artifacts.PositiveReplay)
	if replay["replay_id"] != "dcfrv1:sha256:e52a6c6db7eac4bf9bd1b6cb6f8e244b3c421b64cbab06267526185d01feab6c" ||
		replay["replay_hash"] != "sha256:e52a6c6db7eac4bf9bd1b6cb6f8e244b3c421b64cbab06267526185d01feab6c" {
		t.Fatalf("canonical replay hashes drifted: id=%v hash=%v", replay["replay_id"], replay["replay_hash"])
	}
}

func decodeObject(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func objectAt(t *testing.T, parent map[string]any, key string) map[string]any {
	t.Helper()
	value, ok := parent[key].(map[string]any)
	if !ok {
		t.Fatalf("%s has type %T; want object", key, parent[key])
	}
	return value
}

func arrayAt(t *testing.T, parent map[string]any, key string) []any {
	t.Helper()
	value, ok := parent[key].([]any)
	if !ok {
		t.Fatalf("%s has type %T; want array", key, parent[key])
	}
	return value
}

func replaceTopLevel(t *testing.T, raw []byte, key string, value any) []byte {
	t.Helper()
	object := decodeObject(t, raw)
	object[key] = value
	encoded, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func repeatByte(value byte, count int) string {
	return string(bytes.Repeat([]byte{value}, count))
}

func assertCategory(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil || err.Error() != want {
		t.Fatalf("error = %v; want %s", err, want)
	}
}
