//go:build linux

package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
	"golang.org/x/sys/unix"
)

func TestIssue755ValidProfileHandoffIsOneImmutableSnapshot(t *testing.T) {
	stateRoot := issue755SecureStateRoot(t)
	profile := issue755ValidMutationLabProfile(t)
	expected := profile.Clone()
	raw := issue755ValidMutationLabProfileJSON(t)
	path := issue755WriteProfile(t, stateRoot, raw)
	runtime := &msp05bRuntime{}
	factoryCalls := 0

	adapter, err := startEEBusRuntime(
		context.Background(),
		issue755EnabledConfig(stateRoot),
		issue755Resolver,
		func(got eebusruntime.Config) (eebusruntime.Runtime, error) {
			factoryCalls++
			if len(got.MutationLabProfiles) != 1 {
				t.Fatalf("MutationLabProfiles = %#v; want exactly one", got.MutationLabProfiles)
			}
			if !reflect.DeepEqual(got.MutationLabProfiles[0], expected) {
				t.Fatalf("loaded profile mismatch: got %#v", got.MutationLabProfiles[0])
			}

			replacement := profile.Clone()
			replacement.ProfileID = "replacement-after-load"
			replacementRaw, marshalErr := jsonMarshalIssue755(replacement)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			if writeErr := os.WriteFile(path, replacementRaw, 0o600); writeErr != nil {
				t.Fatal(writeErr)
			}
			if !reflect.DeepEqual(got.MutationLabProfiles[0], expected) {
				t.Fatal("runtime config profile changed after backing file replacement")
			}
			return runtime, nil
		},
	)
	if err != nil {
		t.Fatalf("start with valid profile: %v", err)
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
}

func TestIssue755RejectsUnsafeRootAndFileBoundariesBeforeFactory(t *testing.T) {
	valid := issue755ValidMutationLabProfileJSON(t)
	tests := []struct {
		name  string
		setup func(*testing.T) string
	}{
		{
			name: "root mode",
			setup: func(t *testing.T) string {
				root := issue755SecureStateRoot(t)
				if err := os.Chmod(root, 0o750); err != nil {
					t.Fatal(err)
				}
				issue755WriteProfile(t, root, valid)
				return root
			},
		},
		{
			name: "root symlink",
			setup: func(t *testing.T) string {
				parent := t.TempDir()
				actual := filepath.Join(parent, "actual")
				if err := os.Mkdir(actual, 0o700); err != nil {
					t.Fatal(err)
				}
				issue755WriteProfile(t, actual, valid)
				link := filepath.Join(parent, "linked")
				if err := os.Symlink(actual, link); err != nil {
					t.Fatal(err)
				}
				return link
			},
		},
		{
			name: "intermediate symlink",
			setup: func(t *testing.T) string {
				parent := t.TempDir()
				actual := filepath.Join(parent, "actual")
				if err := os.Mkdir(actual, 0o700); err != nil {
					t.Fatal(err)
				}
				issue755WriteProfile(t, actual, valid)
				link := filepath.Join(parent, "component")
				if err := os.Symlink(actual, link); err != nil {
					t.Fatal(err)
				}
				return link
			},
		},
		{
			name: "file symlink",
			setup: func(t *testing.T) string {
				root := issue755SecureStateRoot(t)
				target := filepath.Join(t.TempDir(), "profile.json")
				if err := os.WriteFile(target, valid, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, filepath.Join(root, issue755ProfileBasename)); err != nil {
					t.Fatal(err)
				}
				return root
			},
		},
		{
			name: "nonregular file",
			setup: func(t *testing.T) string {
				root := issue755SecureStateRoot(t)
				if err := os.Mkdir(filepath.Join(root, issue755ProfileBasename), 0o600); err != nil {
					t.Fatal(err)
				}
				return root
			},
		},
		{
			name: "file mode",
			setup: func(t *testing.T) string {
				root := issue755SecureStateRoot(t)
				path := issue755WriteProfile(t, root, valid)
				if err := os.Chmod(path, 0o640); err != nil {
					t.Fatal(err)
				}
				return root
			},
		},
		{
			name: "hardlink",
			setup: func(t *testing.T) string {
				root := issue755SecureStateRoot(t)
				original := filepath.Join(root, "original.json")
				if err := os.WriteFile(original, valid, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Link(original, filepath.Join(root, issue755ProfileBasename)); err != nil {
					t.Fatal(err)
				}
				return root
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			issue755AssertGenericLoadFailure(t, test.setup(t), "")
		})
	}
}

func TestIssue755MetadataValidationRejectsOwnerTypeModeLinksAndSize(t *testing.T) {
	euid := uint32(os.Geteuid())
	if !mutationLabRootMetadataValid(unix.S_IFDIR|0o700, euid, euid) {
		t.Fatal("valid root metadata rejected")
	}
	for _, test := range []struct {
		name string
		mode uint32
		uid  uint32
	}{
		{name: "owner", mode: unix.S_IFDIR | 0o700, uid: euid + 1},
		{name: "type", mode: unix.S_IFREG | 0o700, uid: euid},
		{name: "mode", mode: unix.S_IFDIR | 0o750, uid: euid},
	} {
		t.Run("root "+test.name, func(t *testing.T) {
			if mutationLabRootMetadataValid(test.mode, test.uid, euid) {
				t.Fatal("unsafe root metadata accepted")
			}
		})
	}

	if !mutationLabFileMetadataValid(unix.S_IFREG|0o600, euid, 1, 1, euid) ||
		!mutationLabFileMetadataValid(unix.S_IFREG|0o600, euid, 1, 65536, euid) {
		t.Fatal("valid boundary file metadata rejected")
	}
	for _, test := range []struct {
		name  string
		mode  uint32
		uid   uint32
		links uint64
		size  int64
	}{
		{name: "owner", mode: unix.S_IFREG | 0o600, uid: euid + 1, links: 1, size: 1},
		{name: "type", mode: unix.S_IFDIR | 0o600, uid: euid, links: 1, size: 1},
		{name: "mode", mode: unix.S_IFREG | 0o640, uid: euid, links: 1, size: 1},
		{name: "hardlink", mode: unix.S_IFREG | 0o600, uid: euid, links: 2, size: 1},
		{name: "empty", mode: unix.S_IFREG | 0o600, uid: euid, links: 1, size: 0},
		{name: "too large", mode: unix.S_IFREG | 0o600, uid: euid, links: 1, size: 65537},
	} {
		t.Run("file "+test.name, func(t *testing.T) {
			if mutationLabFileMetadataValid(test.mode, test.uid, test.links, test.size, euid) {
				t.Fatal("unsafe file metadata accepted")
			}
		})
	}
}

func TestIssue755ProfileSizeBoundary(t *testing.T) {
	valid := issue755ValidMutationLabProfileJSON(t)
	if len(valid) >= 65536 {
		t.Fatalf("valid fixture size = %d; want room for boundary padding", len(valid))
	}

	t.Run("65536 accepted", func(t *testing.T) {
		root := issue755SecureStateRoot(t)
		raw := append(append([]byte(nil), valid...), []byte(strings.Repeat(" ", 65536-len(valid)))...)
		issue755WriteProfile(t, root, raw)
		runtime := &msp05bRuntime{}
		adapter, err := startEEBusRuntime(
			context.Background(),
			issue755EnabledConfig(root),
			issue755Resolver,
			func(config eebusruntime.Config) (eebusruntime.Runtime, error) {
				if len(config.MutationLabProfiles) != 1 {
					t.Fatalf("profile count = %d, want 1", len(config.MutationLabProfiles))
				}
				return runtime, nil
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := adapter.Shutdown(); err != nil {
				t.Error(err)
			}
		})
	})

	t.Run("65537 rejected", func(t *testing.T) {
		root := issue755SecureStateRoot(t)
		raw := append(append([]byte(nil), valid...), []byte(strings.Repeat(" ", 65537-len(valid)))...)
		issue755WriteProfile(t, root, raw)
		issue755AssertGenericLoadFailure(t, root, "")
	})
}

func TestIssue755RejectsMalformedOrInvalidProfileBeforeFactory(t *testing.T) {
	valid := issue755ValidMutationLabProfileJSON(t)
	invalidTyped := issue755ValidMutationLabProfile(t)
	invalidTyped.MaximumProbeTTLSeconds = 0
	invalidTypedRaw, err := jsonMarshalIssue755(invalidTyped)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		raw  []byte
	}{
		{name: "empty", raw: nil},
		{name: "null", raw: []byte("null")},
		{name: "array", raw: []byte("[]")},
		{name: "multiple objects", raw: append(append([]byte(nil), valid...), []byte("{}")...)},
		{name: "nested duplicate", raw: []byte(strings.Replace(
			string(valid),
			`"entity_address":[1]`,
			`"entity_address":[1],"entity_address":[2]`,
			1,
		))},
		{name: "top-level duplicate", raw: []byte(strings.Replace(
			string(valid),
			`"profile_id":"issue755-lab-profile"`,
			`"profile_id":"duplicate-marker","profile_id":"issue755-lab-profile"`,
			1,
		))},
		{name: "unknown field", raw: []byte(strings.Replace(
			string(valid),
			`"profile_id":"issue755-lab-profile"`,
			`"profile_id":"issue755-lab-profile","unknown_marker":"secret-marker"`,
			1,
		))},
		{name: "root case alias", raw: []byte(strings.Replace(
			string(valid),
			`"profile_id":"issue755-lab-profile"`,
			`"PROFILE_ID":"issue755-lab-profile"`,
			1,
		))},
		{name: "root canonical and case alias collision", raw: []byte(strings.Replace(
			string(valid),
			`"profile_id":"issue755-lab-profile"`,
			`"profile_id":"issue755-lab-profile","PROFILE_ID":"issue755-lab-profile"`,
			1,
		))},
		{name: "target case alias", raw: []byte(strings.Replace(
			string(valid),
			`"remote_ski":"`+issue755RemoteSKI+`"`,
			`"REMOTE_SKI":"`+issue755RemoteSKI+`"`,
			1,
		))},
		{name: "target canonical and case alias collision", raw: []byte(strings.Replace(
			string(valid),
			`"remote_ski":"`+issue755RemoteSKI+`"`,
			`"remote_ski":"`+issue755RemoteSKI+`","REMOTE_SKI":"`+issue755RemoteSKI+`"`,
			1,
		))},
		{name: "trailing", raw: append(append([]byte(nil), valid...), []byte("secret-marker")...)},
		{name: "invalid typed profile", raw: invalidTypedRaw},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := issue755SecureStateRoot(t)
			issue755WriteProfile(t, root, test.raw)
			issue755AssertGenericLoadFailure(t, root, "secret-marker")
		})
	}
}

func jsonMarshalIssue755(value any) ([]byte, error) {
	return json.Marshal(value)
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
