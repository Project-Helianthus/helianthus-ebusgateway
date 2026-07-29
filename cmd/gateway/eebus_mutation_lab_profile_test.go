package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
)

const (
	issue755ProfileDirectory = "eebusmutation"
	issue755ProfileBasename  = "mutation-lab-profile-v1.json"
)

func TestIssue755AbsentMutationLabProfileLeavesRuntimeConfigNil(t *testing.T) {
	tests := []struct {
		name      string
		stateRoot func(*testing.T) string
	}{
		{
			name: "root absent",
			stateRoot: func(t *testing.T) string {
				return filepath.Join(t.TempDir(), "absent")
			},
		},
		{
			name: "child absent",
			stateRoot: func(t *testing.T) string {
				return issue755SecureStateRoot(t)
			},
		},
		{
			name: "file absent",
			stateRoot: func(t *testing.T) string {
				root := issue755SecureStateRoot(t)
				issue755SecureProfileDirectory(t, root)
				return root
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := issue755EnabledConfig(test.stateRoot(t))
			runtime := &msp05bRuntime{}
			factoryCalls := 0

			adapter, err := startEEBusRuntime(
				context.Background(),
				cfg,
				issue755Resolver,
				func(got eebusruntime.Config) (eebusruntime.Runtime, error) {
					factoryCalls++
					if got.MutationLabProfiles != nil {
						t.Fatalf("MutationLabProfiles = %#v; want nil when profile is absent", got.MutationLabProfiles)
					}
					return runtime, nil
				},
			)
			if err != nil {
				t.Fatalf("start with absent profile: %v", err)
			}
			if adapter == nil {
				t.Fatal("adapter = nil; want started runtime")
			}
			t.Cleanup(func() {
				if err := adapter.Shutdown(); err != nil {
					t.Error(err)
				}
			})
			if factoryCalls != 1 || runtime.startCalls != 1 {
				t.Fatalf("calls factory=%d start=%d; want one each", factoryCalls, runtime.startCalls)
			}
		})
	}
}

func TestIssue755LoaderProductionSurfaceIsFixedAndNonLeaking(t *testing.T) {
	files, err := filepath.Glob("eebus_mutation_lab_profile*.go")
	if err != nil {
		t.Fatal(err)
	}
	var production []string
	var source strings.Builder
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		production = append(production, name)
		raw, readErr := os.ReadFile(name)
		if readErr != nil {
			t.Fatal(readErr)
		}
		source.Write(raw)
		source.WriteByte('\n')
	}
	if len(production) == 0 {
		t.Fatal("mutation lab profile loader production files are missing")
	}

	text := source.String()
	if count := strings.Count(text, issue755ProfileBasename); count != 1 {
		t.Fatalf("fixed profile basename occurrences = %d, want exactly one", count)
	}
	if count := strings.Count(text, issue755ProfileDirectory); count != 1 {
		t.Fatalf("fixed profile directory occurrences = %d, want exactly one", count)
	}
	for _, forbidden := range []string{
		"os.Getenv",
		"os.LookupEnv",
		"os.Args",
		"flag.",
		"log.",
		"slog.",
		"candidate" + "_ref",
		"mutation-lab-profile-v" + "2",
		"mutation_lab_profile_v" + "2",
		"legacy",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("loader production source contains forbidden surface %q", forbidden)
		}
	}
}

func TestIssue755RejectsRootLevelMutationLabProfileBeforeFactory(t *testing.T) {
	root := issue755SecureStateRoot(t)
	issue755WriteProfileAt(
		t,
		filepath.Join(root, issue755ProfileBasename),
		issue755ValidMutationLabProfileJSON(t),
	)
	issue755AssertGenericLoadFailure(t, root, "")
}

func issue755EnabledConfig(stateRoot string) ebusgateway.EEBusConfig {
	cfg := msp05bEnabledConfig()
	cfg.StateRoot = stateRoot
	cfg.RemoteSKIAllowlist = []string{issue755RemoteSKI}
	return cfg
}

func issue755Resolver(string) ([]netip.Addr, error) {
	return []netip.Addr{netip.MustParseAddr("192.0.2.42")}, nil
}

const issue755RemoteSKI = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func issue755ValidMutationLabProfile(t *testing.T) eebusraw.MutationLabProfileV1 {
	t.Helper()
	requested, err := eebusraw.NewTypedValueV1(int64(21))
	if err != nil {
		t.Fatal(err)
	}
	requestedHash, err := requested.ComputeHash()
	if err != nil {
		t.Fatal(err)
	}
	before, err := eebusraw.NewTypedValueV1(int64(20))
	if err != nil {
		t.Fatal(err)
	}
	beforeHash, err := before.ComputeHash()
	if err != nil {
		t.Fatal(err)
	}
	return eebusraw.MutationLabProfileV1{
		Contract:  eebusraw.MutationLabProfileContractV1,
		ProfileID: "issue755-lab-profile",
		Target: eebusraw.FeatureTargetV1{
			RemoteSKI:      issue755RemoteSKI,
			SHIPID:         "issue755-ship",
			DeviceAddress:  "issue755-device",
			EntityAddress:  []uint64{1},
			FeatureAddress: 7,
			FeatureType:    "measurement",
			FeatureRole:    eebusraw.FeatureRoleV1Server,
			Function:       "measurementListData",
			Operation:      eebusraw.OperationV1Write,
		},
		AllowedValueHashes:     []eebusraw.HashV1{requestedHash},
		RollbackValueHash:      beforeHash,
		MaximumProbeTTLSeconds: 60,
		SafetyPredicates: []string{
			"exact-target-capability-current",
			"rollback-representable",
		},
		EvidenceHashes: []eebusraw.HashV1{
			eebusraw.HashV1("sha256:" + strings.Repeat("3", 64)),
		},
		ExpiresAt: time.Unix(1_900_000_000, 0).UTC(),
	}
}

func issue755ValidMutationLabProfileJSON(t *testing.T) []byte {
	t.Helper()
	raw, err := json.Marshal(issue755ValidMutationLabProfile(t))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestIssue755RejectsCaseAliasedMutationLabProfileKeys(t *testing.T) {
	valid := string(issue755ValidMutationLabProfileJSON(t))
	tests := []struct {
		name        string
		oldFragment string
		newFragment string
	}{
		{
			name:        "root case alias",
			oldFragment: `"profile_id":"issue755-lab-profile"`,
			newFragment: `"PROFILE_ID":"issue755-lab-profile"`,
		},
		{
			name:        "root canonical and case alias collision",
			oldFragment: `"profile_id":"issue755-lab-profile"`,
			newFragment: `"profile_id":"issue755-lab-profile","PROFILE_ID":"issue755-lab-profile"`,
		},
		{
			name:        "target case alias",
			oldFragment: `"remote_ski":"` + issue755RemoteSKI + `"`,
			newFragment: `"REMOTE_SKI":"` + issue755RemoteSKI + `"`,
		},
		{
			name:        "target canonical and case alias collision",
			oldFragment: `"remote_ski":"` + issue755RemoteSKI + `"`,
			newFragment: `"remote_ski":"` + issue755RemoteSKI + `","REMOTE_SKI":"` + issue755RemoteSKI + `"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw := strings.Replace(valid, test.oldFragment, test.newFragment, 1)
			if raw == valid {
				t.Fatal("test fixture did not replace the expected canonical key")
			}
			if _, err := decodeEEBusMutationLabProfile([]byte(raw)); err == nil {
				t.Fatal("case-aliased profile key was accepted")
			}
		})
	}
}

func issue755WriteProfile(t *testing.T, stateRoot string, raw []byte) string {
	t.Helper()
	directory := issue755SecureProfileDirectory(t, stateRoot)
	return issue755WriteProfileAt(
		t,
		filepath.Join(directory, issue755ProfileBasename),
		raw,
	)
}

func issue755WriteProfileAt(t *testing.T, path string, raw []byte) string {
	t.Helper()
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func issue755SecureStateRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	return root
}

func issue755SecureProfileDirectory(t *testing.T, stateRoot string) string {
	t.Helper()
	directory := filepath.Join(stateRoot, issue755ProfileDirectory)
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return directory
}

func issue755AssertGenericLoadFailure(
	t *testing.T,
	stateRoot string,
	contentMarker string,
) {
	t.Helper()
	runtime := &msp05bRuntime{}
	factoryCalls := 0
	adapter, err := startEEBusRuntime(
		context.Background(),
		issue755EnabledConfig(stateRoot),
		issue755Resolver,
		func(eebusruntime.Config) (eebusruntime.Runtime, error) {
			factoryCalls++
			return runtime, nil
		},
	)
	if adapter != nil || err == nil {
		t.Fatalf("invalid profile startup = (%v, %v); want nil adapter and error", adapter, err)
	}
	if factoryCalls != 0 || runtime.startCalls != 0 {
		t.Fatalf("invalid profile calls factory=%d start=%d; want zero", factoryCalls, runtime.startCalls)
	}
	const generic = "eeBUS mutation lab profile load failed"
	if !strings.Contains(err.Error(), generic) {
		t.Fatalf("startup error = %q; want generic profile load failure", err)
	}
	for _, secret := range []string{stateRoot, contentMarker, issue755RemoteSKI} {
		if secret != "" && strings.Contains(err.Error(), secret) {
			t.Fatalf("startup error leaked protected profile detail %q: %v", secret, err)
		}
	}
	if errors.Unwrap(errors.Unwrap(err)) != nil {
		t.Fatalf("startup error exposes an implementation cause chain: %v", err)
	}
}
