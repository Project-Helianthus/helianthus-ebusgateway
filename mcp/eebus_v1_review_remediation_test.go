package mcp

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"testing"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

func TestIssue743RawListSelectorsComposeWithLiveAndEvidenceGet(t *testing.T) {
	server, provider := issue743Server(t)
	operator := issue743OperatorHandler(t, server)

	for _, subject := range []struct {
		name     string
		listTool string
		getTool  string
		listKey  string
		refKey   string
		wantKeys []string
	}{
		{
			name: "service", listTool: msp06ServicesListTool, getTool: msp06ServicesGetTool,
			listKey: "services", refKey: "services_get_ref",
			wantKeys: []string{"id_digest", "ski", "ship_id", "kind", "visible", "paired", "name", "identifier", "brand", "type", "model", "secondary_digest", "opaque"},
		},
		{
			name: "session", listTool: msp06SessionsListTool, getTool: msp06SessionsGetTool,
			listKey: "sessions", refKey: "sessions_get_ref",
			wantKeys: []string{"id_digest", "id", "remote_ski", "state", "since", "opaque"},
		},
	} {
		t.Run(subject.name, func(t *testing.T) {
			list := msp06Call(t, operator, subject.listTool, map[string]any{})
			listData := issue743AssertBoundarySuccess(t, list, subject.listTool, subject.listKey, "live", "raw", "eebus.raw.read")
			row := msp06Map(t, msp06Slice(t, listData[subject.listKey], subject.listKey)[0], subject.name)
			msp06AssertKeys(t, row, subject.name, subject.wantKeys...)
			selector := msp06AssertToken(t, row["id_digest"], subject.name+".id_digest")

			liveGet := msp06Call(t, operator, subject.getTool, map[string]any{"id_digest": selector})
			liveData := issue743AssertBoundarySuccess(t, liveGet, subject.getTool, subject.name, "live", "raw", "eebus.raw.read")
			if !reflect.DeepEqual(liveData, row) {
				t.Fatalf("live get does not return listed raw DTO:\nlist=%#v\nget=%#v", row, liveData)
			}

			capture := msp06Call(t, operator, msp06SnapshotCapture, map[string]any{})
			root := issue743AssertBoundarySuccess(t, capture, msp06SnapshotCapture, "whole-root", "live", "raw", "eebus.raw.read")
			refs := msp06RootRefs(t, root)
			evidenceListRefKey := strings.TrimSuffix(subject.refKey, "_get_ref") + "_list_ref"
			evidenceList := msp06Call(t, operator, subject.listTool, map[string]any{"evidence_ref": refs[evidenceListRefKey]})
			evidenceListData := issue743AssertBoundarySuccess(t, evidenceList, subject.listTool, subject.listKey, "evidence", "raw", "eebus.raw.read")
			evidenceRow := msp06Map(t, msp06Slice(t, evidenceListData[subject.listKey], subject.listKey)[0], subject.name)
			evidenceSelector := msp06AssertToken(t, evidenceRow["id_digest"], subject.name+".evidence.id_digest")
			evidenceGet := msp06Call(t, operator, subject.getTool, map[string]any{
				"evidence_ref": refs[subject.refKey], "id_digest": evidenceSelector,
			})
			evidenceData := issue743AssertBoundarySuccess(t, evidenceGet, subject.getTool, subject.name, "evidence", "raw", "eebus.raw.read")
			if !reflect.DeepEqual(evidenceData, evidenceRow) {
				t.Fatalf("evidence get does not return listed raw DTO:\nlist=%#v\nget=%#v", evidenceRow, evidenceData)
			}
		})
	}
	if providerCallCount(provider) != 6 {
		t.Fatalf("provider calls = %d, want two live list/get plus two captures", providerCallCount(provider))
	}
}

func TestIssue743EveryLiveToolUsesOneFreshSnapshotAndClosedDTOs(t *testing.T) {
	for boundaryName := range map[string]struct{}{"public": {}, "operator": {}} {
		t.Run(boundaryName, func(t *testing.T) {
			server, provider := issue743Server(t)
			handler := http.Handler(server.Handler())
			snapshot := issue743Snapshot(t, issue743FixtureBytes(t))
			serviceID, sessionID := issue743Selectors(t, snapshot, boundaryName)
			if boundaryName == "operator" {
				handler = issue743OperatorHandler(t, server)
			}
			calls := []struct {
				tool      string
				scope     string
				arguments map[string]any
				dataKeys  []string
			}{
				{msp06RuntimeStatusTool, "runtime-status", map[string]any{}, []string{"state"}},
				{msp06ServicesListTool, "services", map[string]any{}, []string{"services"}},
				{msp06ServicesGetTool, "service", map[string]any{"id_digest": serviceID}, nil},
				{msp06SessionsListTool, "sessions", map[string]any{}, []string{"sessions"}},
				{msp06SessionsGetTool, "session", map[string]any{"id_digest": sessionID}, nil},
				{msp06TopologyGetTool, "topology", map[string]any{}, nil},
				{msp06PairingStatusTool, "pairing-status", map[string]any{}, []string{"pairing"}},
				{msp06SnapshotCapture, "whole-root", map[string]any{}, []string{"snapshot_ref", "expires_at", "snapshot_content_hash", "evidence_refs", "snapshot"}},
			}
			var captured map[string]any
			for _, call := range calls {
				before := providerCallCount(provider)
				result := msp06Call(t, handler, call.tool, call.arguments)
				tier, auth := "redacted", "eebus.public.read"
				if boundaryName == "operator" {
					tier, auth = "raw", "eebus.raw.read"
				}
				data := issue743AssertBoundarySuccess(t, result, call.tool, call.scope, "live", tier, auth)
				if after := providerCallCount(provider); after != before+1 {
					t.Fatalf("%s provider calls %d -> %d, want exactly one", call.tool, before, after)
				}
				if call.dataKeys != nil {
					msp06AssertKeys(t, data, call.tool+" data", call.dataKeys...)
				}
				issue743AssertClosedBoundaryDTO(t, boundaryName, call.tool, data)
				if call.tool == msp06SnapshotCapture {
					captured = data
				}
			}
			beforeDrop := providerCallCount(provider)
			drop := msp06Call(t, handler, msp06SnapshotDrop, map[string]any{"snapshot_ref": captured["snapshot_ref"]})
			tier, auth := "redacted", "eebus.public.read"
			if boundaryName == "operator" {
				tier, auth = "raw", "eebus.raw.read"
			}
			dropData := issue743AssertBoundarySuccess(t, drop, msp06SnapshotDrop, "whole-root", "live", tier, auth)
			msp06AssertKeys(t, dropData, "drop", "status")
			if after := providerCallCount(provider); after != beforeDrop {
				t.Fatalf("drop called provider: %d -> %d", beforeDrop, after)
			}
		})
	}
}

func TestIssue743LiveFailuresNeverFallBackToStaleData(t *testing.T) {
	for boundaryName := range map[string]struct{}{"public": {}, "operator": {}} {
		t.Run(boundaryName, func(t *testing.T) {
			server, provider := issue743Server(t)
			handler := http.Handler(server.Handler())
			if boundaryName == "operator" {
				handler = issue743OperatorHandler(t, server)
			}
			success := msp06Call(t, handler, msp06TopologyGetTool, map[string]any{})
			if success.isError {
				t.Fatalf("initial live call failed: %s", success.raw)
			}
			provider.set(eebusruntime.SnapshotV1{}, errors.New("private stale runtime detail"))
			failed := msp06Call(t, handler, msp06TopologyGetTool, map[string]any{})
			tier, auth := "redacted", "eebus.public.read"
			if boundaryName == "operator" {
				tier, auth = "raw", "eebus.raw.read"
			}
			issue743AssertError(t, failed, tier, auth, "backend_unavailable")
			if failed.envelope["data"] != nil || strings.Contains(failed.raw, "VR940") {
				t.Fatalf("failed live call returned stale data: %s", failed.raw)
			}
		})
	}
}

func TestIssue743NormalizedErrorMappingsAreExhaustiveAndClosed(t *testing.T) {
	want := map[string]eebusV1ErrorV1{
		"invalid_argument":    {Code: "invalid_argument", Message: "invalid argument", SourceLayer: "mcp"},
		"not_found":           {Code: "not_found", Message: "not found", SourceLayer: "mcp"},
		"permission_denied":   {Code: "permission_denied", Message: "permission denied", SourceLayer: "policy"},
		"admin_required":      {Code: "admin_required", Message: "administrator authorization required", SourceLayer: "policy"},
		"backend_unavailable": {Code: "backend_unavailable", Message: "eeBUS runtime unavailable", Retriable: true, SourceLayer: "eebusruntime"},
		"timeout":             {Code: "timeout", Message: "eeBUS runtime request timed out", Retriable: true, SourceLayer: "eebusruntime"},
		"snapshot_gone":       {Code: "snapshot_gone", Message: "snapshot no longer available", SourceLayer: "snapshot-store"},
		"quota_exceeded":      {Code: "quota_exceeded", Message: "snapshot quota exceeded", Retriable: true, SourceLayer: "snapshot-store"},
		"contract_violation":  {Code: "contract_violation", Message: "eeBUS MCP contract violation", SourceLayer: "mcp"},
	}
	for code, expected := range want {
		actual, ok := eebusV1PublicErrorForCode(code)
		if !ok || actual != expected {
			t.Errorf("%s = %+v/%t, want %+v", code, actual, ok, expected)
		}
		encoded, err := json.Marshal(actual)
		if err != nil {
			t.Fatal(err)
		}
		var object map[string]any
		if err := json.Unmarshal(encoded, &object); err != nil {
			t.Fatal(err)
		}
		msp06AssertKeys(t, object, code, "code", "message", "retriable", "source_layer")
	}
	if _, ok := eebusV1PublicErrorForCode("private_backend_detail"); ok {
		t.Fatal("unknown private error code was shareable")
	}
}

func TestIssue743EveryCollectionPermutationIsByteAndHashDeterministic(t *testing.T) {
	canonical := issue743Snapshot(t, issue743FixtureBytes(t))
	permuted := issue743PermutedSnapshot(t, canonical)
	if canonical.Meta.DataHash != permuted.Meta.DataHash {
		t.Fatalf("raw snapshot hashes differ after collection permutation: %s != %s", canonical.Meta.DataHash, permuted.Meta.DataHash)
	}
	for boundaryName := range map[string]struct{}{"public": {}, "operator": {}} {
		t.Run(boundaryName, func(t *testing.T) {
			first := issue743DeterministicToolResults(t, canonical, boundaryName)
			second := issue743DeterministicToolResults(t, permuted, boundaryName)
			if !reflect.DeepEqual(first, second) {
				for tool := range first {
					if first[tool] != second[tool] {
						t.Errorf("%s differs after permutation:\nfirst=%s\nsecond=%s", tool, first[tool], second[tool])
					}
				}
			}
		})
	}
}

func TestIssue743SecretAndIdentityLeakCoverageSpansAllToolsModesAndBoundaries(t *testing.T) {
	var logs bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(previous) })

	for boundaryName := range map[string]struct{}{"public": {}, "operator": {}} {
		t.Run(boundaryName, func(t *testing.T) {
			server, provider := issue743Server(t)
			handler := http.Handler(server.Handler())
			tier, auth := "redacted", "eebus.public.read"
			if boundaryName == "operator" {
				handler = issue743OperatorHandler(t, server)
				tier, auth = "raw", "eebus.raw.read"
			}
			live, evidence, drop := issue743ExerciseAllTools(t, handler, boundaryName)
			for mode, results := range map[string]map[string]msp06CallResult{"live": live, "evidence": evidence} {
				for tool, result := range results {
					issue743AssertSecretCorpusAbsent(t, result.raw)
					if boundaryName == "public" {
						issue743AssertNoRawIdentity(t, result.raw)
					}
					if strings.Contains(result.raw, "candidate_ref") {
						t.Errorf("%s/%s leaked candidate_ref", mode, tool)
					}
				}
			}
			issue743AssertSecretCorpusAbsent(t, drop.raw)

			provider.set(eebusruntime.SnapshotV1{}, errors.New(strings.Join([]string{
				"-----BEGIN PRIVATE KEY-----",
				"-----BEGIN RSA PRIVATE KEY-----",
				"-----BEGIN EC PRIVATE KEY-----",
				"-----BEGIN ENCRYPTED PRIVATE KEY-----",
				"-----BEGIN OPENSSH PRIVATE KEY-----",
				"-----BEGIN DSA PRIVATE KEY-----",
				"-----BEGIN PGP PRIVATE KEY BLOCK-----",
				"credential_token bearer_token private_key 192.0.2.44",
			}, " ")))
			for _, tool := range []string{
				msp06RuntimeStatusTool, msp06ServicesListTool, msp06ServicesGetTool,
				msp06SessionsListTool, msp06SessionsGetTool, msp06TopologyGetTool,
				msp06PairingStatusTool, msp06SnapshotCapture,
			} {
				arguments := map[string]any{}
				if tool == msp06ServicesGetTool || tool == msp06SessionsGetTool {
					arguments["id_digest"] = strings.Repeat("A", 43)
				}
				result := msp06Call(t, handler, tool, arguments)
				issue743AssertError(t, result, tier, auth, "backend_unavailable")
				issue743AssertSecretCorpusAbsent(t, result.raw)
			}

			listResponse := doRPC(t, handler, rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/list"})
			manifest, err := json.Marshal(listResponse)
			if err != nil {
				t.Fatal(err)
			}
			issue743AssertSecretCorpusAbsent(t, string(manifest))
			if strings.Contains(string(manifest), "candidate_ref") {
				t.Fatal("tools/list manifest leaked candidate_ref")
			}
		})
	}
	issue743AssertSecretCorpusAbsent(t, logs.String())
}

func issue743Selectors(t *testing.T, snapshot eebusruntime.SnapshotV1, boundaryName string) (string, string) {
	t.Helper()
	if boundaryName == "operator" {
		return eebusV1RawServiceDigest(snapshot.Services[0]), eebusV1RawSessionDigest(snapshot.Sessions[0])
	}
	projection, err := eebusV1ProjectSnapshot(snapshot, bytes.Repeat([]byte{0x5a}, sha256.Size))
	if err != nil {
		t.Fatal(err)
	}
	return projection.Snapshot.Services[0].ID.Digest, projection.Snapshot.Sessions[0].ID.Digest
}

func issue743AssertBoundarySuccess(
	t *testing.T,
	result msp06CallResult,
	tool, scope, mode, tier, authScope string,
) map[string]any {
	t.Helper()
	if result.isError || result.envelope["error"] != nil || result.envelope["data"] == nil {
		t.Fatalf("tool %s failed: %s", tool, result.raw)
	}
	meta := msp06Map(t, result.envelope["meta"], "meta")
	if meta["tool"] != tool || meta["scope"] != scope || meta["mode"] != mode ||
		meta["mask_tier"] != tier || meta["auth_scope"] != authScope {
		t.Fatalf("meta binding = %#v", meta)
	}
	if hash, _ := meta["data_hash"].(string); !msp06HashPattern.MatchString(hash) {
		t.Fatalf("meta.data_hash = %#v", meta["data_hash"])
	}
	return msp06Map(t, result.envelope["data"], "data")
}

func issue743AssertClosedBoundaryDTO(t *testing.T, boundaryName, tool string, data map[string]any) {
	t.Helper()
	switch tool {
	case msp06ServicesListTool:
		row := msp06Map(t, msp06Slice(t, data["services"], "services")[0], "service")
		if boundaryName == "operator" {
			msp06AssertKeys(t, row, "raw service", "id_digest", "ski", "ship_id", "kind", "visible", "paired", "name", "identifier", "brand", "type", "model", "secondary_digest", "opaque")
		} else {
			msp06AssertKeys(t, row, "public service", "id", "kind", "visible", "paired")
		}
	case msp06ServicesGetTool:
		if boundaryName == "operator" {
			msp06AssertKeys(t, data, "raw service", "id_digest", "ski", "ship_id", "kind", "visible", "paired", "name", "identifier", "brand", "type", "model", "secondary_digest", "opaque")
		} else {
			msp06AssertKeys(t, data, "public service", "id", "kind", "visible", "paired")
		}
	case msp06SessionsListTool:
		row := msp06Map(t, msp06Slice(t, data["sessions"], "sessions")[0], "session")
		if boundaryName == "operator" {
			msp06AssertKeys(t, row, "raw session", "id_digest", "id", "remote_ski", "state", "since", "opaque")
		} else {
			msp06AssertKeys(t, row, "public session", "id", "remote", "state", "since")
		}
	case msp06SessionsGetTool:
		if boundaryName == "operator" {
			msp06AssertKeys(t, data, "raw session", "id_digest", "id", "remote_ski", "state", "since", "opaque")
		} else {
			msp06AssertKeys(t, data, "public session", "id", "remote", "state", "since")
		}
	case msp06TopologyGetTool:
		if boundaryName == "operator" {
			msp06AssertKeys(t, data, "raw topology", "devices", "entities", "features", "usecases", "opaque")
			msp06AssertKeys(t, msp06Map(t, msp06Slice(t, data["devices"], "devices")[0], "device"), "raw device",
				"ski", "ship_id", "address", "type", "description", "metadata", "secondary_digest", "opaque")
			msp06AssertKeys(t, msp06Map(t, msp06Slice(t, data["entities"], "entities")[0], "entity"), "raw entity",
				"device_address", "entity_address", "type", "description")
			msp06AssertKeys(t, msp06Map(t, msp06Slice(t, data["features"], "features")[0], "feature"), "raw feature",
				"device_address", "entity_address", "feature_address", "type", "role", "description")
			msp06AssertKeys(t, msp06Map(t, msp06Slice(t, data["usecases"], "usecases")[0], "usecase"), "raw usecase",
				"context_address", "name", "actor", "resolved_role", "scenarios", "version", "availability", "document_subrevision")
		} else {
			msp06AssertKeys(t, data, "public topology", "devices")
		}
	case msp06SnapshotCapture:
		snapshot := msp06Map(t, data["snapshot"], "snapshot")
		if boundaryName == "operator" {
			msp06AssertKeys(t, snapshot, "raw snapshot", "meta", "status", "pairing", "services", "sessions", "devices", "entities", "features", "usecases", "opaque")
		} else {
			msp06AssertKeys(t, snapshot, "public snapshot", "meta", "status", "pairing", "services", "sessions", "topology")
		}
	}
}

func issue743PermutedSnapshot(t *testing.T, source eebusruntime.SnapshotV1) eebusruntime.SnapshotV1 {
	t.Helper()
	draft := source.Clone()
	slices.Reverse(draft.Pairing)
	slices.Reverse(draft.Services)
	slices.Reverse(draft.Sessions)
	slices.Reverse(draft.Devices)
	slices.Reverse(draft.Entities)
	slices.Reverse(draft.Features)
	slices.Reverse(draft.UseCases)
	slices.Reverse(draft.Opaque)
	for index := range draft.Pairing {
		issue743ReverseOpaque(draft.Pairing[index].Opaque)
	}
	for index := range draft.Services {
		issue743ReverseOpaque(draft.Services[index].Opaque)
	}
	for index := range draft.Sessions {
		issue743ReverseOpaque(draft.Sessions[index].Opaque)
	}
	for index := range draft.Devices {
		issue743ReverseOpaque(draft.Devices[index].Opaque)
	}
	for index := range draft.Entities {
		issue743ReverseOpaque(draft.Entities[index].Opaque)
	}
	for index := range draft.Features {
		issue743ReverseOpaque(draft.Features[index].Opaque)
	}
	for index := range draft.UseCases {
		if draft.UseCases[index].Scenarios != nil {
			slices.Reverse(*draft.UseCases[index].Scenarios)
		}
		issue743ReverseOpaque(draft.UseCases[index].Opaque)
	}
	draft.Meta.DataHash = ""
	permuted, err := eebusruntime.NewSnapshotV1(draft)
	if err != nil {
		t.Fatal(err)
	}
	return permuted
}

func issue743ReverseOpaque(values *[]eebusruntime.OpaqueObservationV1) {
	if values != nil {
		slices.Reverse(*values)
	}
}

func issue743DeterministicToolResults(t *testing.T, snapshot eebusruntime.SnapshotV1, boundaryName string) map[string]string {
	t.Helper()
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: snapshot})
	handler := http.Handler(server.Handler())
	if boundaryName == "operator" {
		handler = issue743OperatorHandler(t, server)
	}
	serviceID, sessionID := issue743Selectors(t, snapshot, boundaryName)
	results := make(map[string]string)
	var captured map[string]any
	for _, call := range []struct {
		tool      string
		arguments map[string]any
	}{
		{msp06RuntimeStatusTool, map[string]any{}},
		{msp06ServicesListTool, map[string]any{}},
		{msp06ServicesGetTool, map[string]any{"id_digest": serviceID}},
		{msp06SessionsListTool, map[string]any{}},
		{msp06SessionsGetTool, map[string]any{"id_digest": sessionID}},
		{msp06TopologyGetTool, map[string]any{}},
		{msp06PairingStatusTool, map[string]any{}},
		{msp06SnapshotCapture, map[string]any{}},
	} {
		result := msp06Call(t, handler, call.tool, call.arguments)
		if result.isError {
			t.Fatalf("%s failed: %s", call.tool, result.raw)
		}
		results[call.tool] = result.raw
		if call.tool == msp06SnapshotCapture {
			captured = msp06Map(t, result.envelope["data"], "capture")
		}
	}
	drop := msp06Call(t, handler, msp06SnapshotDrop, map[string]any{"snapshot_ref": captured["snapshot_ref"]})
	results[msp06SnapshotDrop] = drop.raw
	return results
}

func issue743ExerciseAllTools(t *testing.T, handler http.Handler, boundaryName string) (map[string]msp06CallResult, map[string]msp06CallResult, msp06CallResult) {
	t.Helper()
	serviceList := msp06Call(t, handler, msp06ServicesListTool, map[string]any{})
	sessionList := msp06Call(t, handler, msp06SessionsListTool, map[string]any{})
	serviceRow := msp06Map(t, msp06Slice(t, msp06Map(t, serviceList.envelope["data"], "service list")["services"], "services")[0], "service")
	sessionRow := msp06Map(t, msp06Slice(t, msp06Map(t, sessionList.envelope["data"], "session list")["sessions"], "sessions")[0], "session")
	serviceID, sessionID := "", ""
	if boundaryName == "operator" {
		serviceID, _ = serviceRow["id_digest"].(string)
		sessionID, _ = sessionRow["id_digest"].(string)
	} else {
		serviceID, _ = msp06Map(t, serviceRow["id"], "service.id")["digest"].(string)
		sessionID, _ = msp06Map(t, sessionRow["id"], "session.id")["digest"].(string)
	}
	live := map[string]msp06CallResult{
		msp06ServicesListTool:  serviceList,
		msp06SessionsListTool:  sessionList,
		msp06RuntimeStatusTool: msp06Call(t, handler, msp06RuntimeStatusTool, map[string]any{}),
		msp06ServicesGetTool:   msp06Call(t, handler, msp06ServicesGetTool, map[string]any{"id_digest": serviceID}),
		msp06SessionsGetTool:   msp06Call(t, handler, msp06SessionsGetTool, map[string]any{"id_digest": sessionID}),
		msp06TopologyGetTool:   msp06Call(t, handler, msp06TopologyGetTool, map[string]any{}),
		msp06PairingStatusTool: msp06Call(t, handler, msp06PairingStatusTool, map[string]any{}),
	}
	capture := msp06Call(t, handler, msp06SnapshotCapture, map[string]any{})
	live[msp06SnapshotCapture] = capture
	root := msp06Map(t, capture.envelope["data"], "capture")
	refs := msp06RootRefs(t, root)
	evidence := map[string]msp06CallResult{
		msp06RuntimeStatusTool: msp06Call(t, handler, msp06RuntimeStatusTool, map[string]any{"evidence_ref": refs["runtime_status_ref"]}),
		msp06ServicesListTool:  msp06Call(t, handler, msp06ServicesListTool, map[string]any{"evidence_ref": refs["services_list_ref"]}),
		msp06ServicesGetTool:   msp06Call(t, handler, msp06ServicesGetTool, map[string]any{"evidence_ref": refs["services_get_ref"], "id_digest": serviceID}),
		msp06SessionsListTool:  msp06Call(t, handler, msp06SessionsListTool, map[string]any{"evidence_ref": refs["sessions_list_ref"]}),
		msp06SessionsGetTool:   msp06Call(t, handler, msp06SessionsGetTool, map[string]any{"evidence_ref": refs["sessions_get_ref"], "id_digest": sessionID}),
		msp06TopologyGetTool:   msp06Call(t, handler, msp06TopologyGetTool, map[string]any{"evidence_ref": refs["topology_ref"]}),
		msp06PairingStatusTool: msp06Call(t, handler, msp06PairingStatusTool, map[string]any{"evidence_ref": refs["pairing_status_ref"]}),
	}
	drop := msp06Call(t, handler, msp06SnapshotDrop, map[string]any{"snapshot_ref": root["snapshot_ref"]})
	return live, evidence, drop
}
