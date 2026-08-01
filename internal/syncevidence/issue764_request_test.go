package syncevidence

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func issue764RequestBytes(t *testing.T, digit byte) []byte {
	t.Helper()
	observed := time.Date(2026, 7, 30, 11, 12, 13, 456000000, time.UTC)
	normalized, err := CanonicalizeJSON(cloudPayload(
		observed,
		strings.Repeat(strings.ToUpper(string(digit)), 43),
		"21.5",
	))
	if err != nil {
		t.Fatalf("canonicalize cloud evidence: %v", err)
	}
	ref := EvidenceRefV1{
		Kind:            EvidenceKindContent,
		DigestAlgorithm: DigestAlgorithmContentBytes,
		Digest:          HashContentBytes(normalized),
	}
	raw, err := json.Marshal(map[string]any{
		"contract":            OneShotRequestContractV1,
		"schema_version":      1,
		"action_evidence_ref": ref,
		"cloud_app_action": map[string]any{
			"evidence_ref":        ref,
			"normalized_evidence": json.RawMessage(normalized),
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	return raw
}

func issue764MutateRequestCloudValue(t *testing.T, raw []byte) []byte {
	t.Helper()
	mutated := bytes.Replace(raw, []byte(`"value":"21.5"`), []byte(`"value":"22.5"`), 1)
	if bytes.Equal(mutated, raw) || len(mutated) != len(raw) {
		t.Fatalf("cloud mutation did not preserve request size")
	}
	return mutated
}

func issue764SecureTempDir(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("resolve temp directory: %v", err)
	}
	return root
}

func issue764WriteRequest(t *testing.T, root string, raw []byte, mode os.FileMode) string {
	t.Helper()
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("mkdir request root: %v", err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatalf("chmod request root: %v", err)
	}
	path := filepath.Join(root, OneShotRequestFileV1)
	if err := os.WriteFile(path, raw, mode); err != nil {
		t.Fatalf("write request: %v", err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("chmod request: %v", err)
	}
	return path
}

func TestIssue764RequestLoaderAcceptsOnlyFixedClosedRequest(t *testing.T) {
	root := filepath.Join(issue764SecureTempDir(t), "synchronized-evidence")
	raw := issue764RequestBytes(t, 'a')
	issue764WriteRequest(t, root, raw, 0o600)
	if _, err := parseOneShotRequest(raw); err != nil {
		t.Fatalf("parse request fixture: %v\n%s", err, raw)
	}
	request, err := loadOneShotRequestAt(root, nil)
	if err != nil {
		t.Fatalf("load request: %v", err)
	}
	if request.Contract != OneShotRequestContractV1 || request.SchemaVersion != 1 {
		t.Fatalf("request identity = %#v", request)
	}
	if !reflect.DeepEqual(request.ActionEvidenceRef, request.CloudAppAction.EvidenceRef) {
		t.Fatalf("request refs differ: %#v", request)
	}
	wantCloud, err := CanonicalizeJSON(cloudPayload(
		time.Date(2026, 7, 30, 11, 12, 13, 456000000, time.UTC),
		strings.Repeat("A", 43),
		"21.5",
	))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(request.CloudAppAction.NormalizedEvidence, wantCloud) {
		t.Fatalf("cloud evidence = %s, want canonical %s", request.CloudAppAction.NormalizedEvidence, wantCloud)
	}
}

func TestIssue764RequestRejectsCloudContentDigestMismatch(t *testing.T) {
	raw := issue764MutateRequestCloudValue(t, issue764RequestBytes(t, 'a'))
	if _, err := parseOneShotRequest(raw); err == nil {
		t.Fatal("accepted mutated canonical cloud evidence with retained content digest")
	}
}

func TestIssue764RequestLoaderRejectsModeSymlinkMutationAndSelectors(t *testing.T) {
	t.Run("mode", func(t *testing.T) {
		root := filepath.Join(issue764SecureTempDir(t), "synchronized-evidence")
		issue764WriteRequest(t, root, issue764RequestBytes(t, 'a'), 0o640)
		if _, err := loadOneShotRequestAt(root, nil); err == nil {
			t.Fatal("accepted request without exact mode 0600")
		}
	})

	t.Run("leaf symlink", func(t *testing.T) {
		base := issue764SecureTempDir(t)
		root := filepath.Join(base, "synchronized-evidence")
		target := filepath.Join(base, "request.json")
		if err := os.WriteFile(target, issue764RequestBytes(t, 'a'), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(root, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(root, OneShotRequestFileV1)); err != nil {
			t.Fatal(err)
		}
		if _, err := loadOneShotRequestAt(root, nil); err == nil {
			t.Fatal("followed request symlink")
		}
	})

	t.Run("parent symlink", func(t *testing.T) {
		base := issue764SecureTempDir(t)
		actual := filepath.Join(base, "actual")
		issue764WriteRequest(t, actual, issue764RequestBytes(t, 'a'), 0o600)
		link := filepath.Join(base, "synchronized-evidence")
		if err := os.Symlink(actual, link); err != nil {
			t.Fatal(err)
		}
		if _, err := loadOneShotRequestAt(link, nil); err == nil {
			t.Fatal("followed request parent symlink")
		}
	})

	t.Run("changed after metadata", func(t *testing.T) {
		root := filepath.Join(issue764SecureTempDir(t), "synchronized-evidence")
		path := issue764WriteRequest(t, root, issue764RequestBytes(t, 'a'), 0o600)
		if _, err := loadOneShotRequestAt(root, func() {
			if writeErr := os.WriteFile(path, issue764RequestBytes(t, 'b'), 0o600); writeErr != nil {
				t.Fatalf("mutate request: %v", writeErr)
			}
		}); err == nil {
			t.Fatal("accepted request changed between metadata verification and read")
		}
	})

	t.Run("same-size in-place changed after initial fstat", func(t *testing.T) {
		root := filepath.Join(issue764SecureTempDir(t), "synchronized-evidence")
		initial := issue764RequestBytes(t, 'a')
		mutated := issue764RequestBytes(t, 'b')
		if len(mutated) != len(initial) {
			t.Fatalf("mutation fixture sizes differ: %d != %d", len(mutated), len(initial))
		}
		path := issue764WriteRequest(t, root, initial, 0o600)
		if _, err := loadOneShotRequestAtWithHooks(root, func() {
			file, openErr := os.OpenFile(path, os.O_WRONLY, 0)
			if openErr != nil {
				t.Fatalf("open request for in-place mutation: %v", openErr)
			}
			if _, writeErr := file.WriteAt(mutated, 0); writeErr != nil {
				_ = file.Close()
				t.Fatalf("mutate request in place: %v", writeErr)
			}
			if syncErr := file.Sync(); syncErr != nil {
				_ = file.Close()
				t.Fatalf("sync in-place mutation: %v", syncErr)
			}
			if closeErr := file.Close(); closeErr != nil {
				t.Fatalf("close in-place mutation: %v", closeErr)
			}
			changedAt := time.Date(2030, 1, 2, 3, 4, 5, 6, time.UTC)
			if chtimesErr := os.Chtimes(path, changedAt, changedAt); chtimesErr != nil {
				t.Fatalf("set deterministic mutation time: %v", chtimesErr)
			}
		}, nil); err == nil {
			t.Fatal("accepted same-size in-place mutation after initial fstat")
		}
	})

	t.Run("caller selector", func(t *testing.T) {
		root := filepath.Join(issue764SecureTempDir(t), "synchronized-evidence")
		raw := bytes.TrimSuffix(issue764RequestBytes(t, 'a'), []byte("}"))
		raw = append(raw, []byte(`,"targets":[]}`)...)
		issue764WriteRequest(t, root, raw, 0o600)
		if _, err := loadOneShotRequestAt(root, nil); err == nil {
			t.Fatal("accepted caller-supplied targets")
		}
	})

	t.Run("different refs", func(t *testing.T) {
		root := filepath.Join(issue764SecureTempDir(t), "synchronized-evidence")
		var request map[string]any
		if err := json.Unmarshal(issue764RequestBytes(t, 'a'), &request); err != nil {
			t.Fatal(err)
		}
		request["action_evidence_ref"].(map[string]any)["digest"] = "sha256:" + strings.Repeat("f", 64)
		raw, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		issue764WriteRequest(t, root, raw, 0o600)
		if _, err := loadOneShotRequestAt(root, nil); err == nil {
			t.Fatal("accepted non-identical action and cloud evidence refs")
		}
	})

	t.Run("git blob evidence ref", func(t *testing.T) {
		root := filepath.Join(issue764SecureTempDir(t), "synchronized-evidence")
		var request map[string]any
		if err := json.Unmarshal(issue764RequestBytes(t, 'a'), &request); err != nil {
			t.Fatal(err)
		}
		ref := map[string]any{
			"kind":             EvidenceKindGitBlob,
			"digest_algorithm": DigestAlgorithmGitBlobV1,
			"digest":           "sha256:" + strings.Repeat("a", 64),
			"repository":       "Project-Helianthus/private-evidence",
			"commit":           strings.Repeat("b", 40),
			"path":             "evidence.json",
		}
		request["action_evidence_ref"] = ref
		request["cloud_app_action"].(map[string]any)["evidence_ref"] = ref
		raw, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		issue764WriteRequest(t, root, raw, 0o600)
		if _, err := loadOneShotRequestAt(root, nil); err == nil {
			t.Fatal("one-shot request accepted a Git-blob evidence ref")
		}
	})
}
