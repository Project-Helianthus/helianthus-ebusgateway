package main

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
)

func TestIssue809EEBusAdminFlagsCarryOnlyProtectedFilePaths(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("issue809", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{
		"-eebus-admin-enabled",
		"-eebus-admin-owner-username", "operator",
		"-eebus-admin-owner-secret-file", "/run/helianthus/eebus-owner-secret",
		"-eebus-admin-ha-secret-file", "/run/helianthus/eebus-ha-secret",
		"-eebus-admin-origin", "https://gateway.example.test",
		"-eebus-admin-session-ttl", "20m",
	}); err != nil {
		t.Fatalf("parse admin flags: %v", err)
	}
	got := cfg.EEBusAdminConfig
	if !got.Enabled || got.OwnerUsername != "operator" || got.SessionTTL != 20*time.Minute ||
		got.OwnerSecretPath != "/run/helianthus/eebus-owner-secret" ||
		got.HASecretPath != "/run/helianthus/eebus-ha-secret" ||
		got.OwnerOrigin != "https://gateway.example.test" {
		t.Fatalf("admin config=%#v", got)
	}
}

func TestIssue809ProtectedAdminSecretFileValidation(t *testing.T) {
	root := t.TempDir()
	validPath := filepath.Join(root, "valid")
	value := strings.Repeat("a", eebusAdminSecretMinBytes)
	if err := os.WriteFile(validPath, []byte(value+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := readEEBusAdminSecret(validPath)
	if err != nil || string(got) != value {
		t.Fatalf("read valid secret=(%q,%v)", got, err)
	}

	for _, test := range []struct {
		name  string
		value string
		mode  os.FileMode
	}{
		{name: "short", value: "short", mode: 0o600},
		{name: "multiline", value: value + "\nsecond", mode: 0o600},
		{name: "non ASCII", value: strings.Repeat("a", eebusAdminSecretMinBytes-2) + "é", mode: 0o600},
		{name: "space", value: strings.Repeat("a", eebusAdminSecretMinBytes-1) + " ", mode: 0o600},
		{name: "wide permissions", value: value, mode: 0o640},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(root, test.name)
			if err := os.WriteFile(path, []byte(test.value), test.mode); err != nil {
				t.Fatal(err)
			}
			if _, err := readEEBusAdminSecret(path); err == nil {
				t.Fatal("invalid credential file accepted")
			}
		})
	}

	target := filepath.Join(root, "target")
	if err := os.WriteFile(target, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readEEBusAdminSecret(link); err == nil {
		t.Fatal("symlink credential accepted")
	}
}

func TestIssue809AdminUsernameIsHeaderSafe(t *testing.T) {
	for _, value := range []string{"", " owner", "owner name", "owner:name", "opérator"} {
		if validEEBusAdminUsername(value) {
			t.Fatalf("unsafe owner username %q accepted", value)
		}
	}
	if !validEEBusAdminUsername("operator-1") {
		t.Fatal("visible ASCII owner username rejected")
	}
}

func TestIssue809AdminRequiresEEBusRuntime(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.EEBusAdminConfig.Enabled = true
	if err := validateEEBusAdminRuntimeConfig(cfg); err == nil {
		t.Fatal("admin boundary accepted disabled eeBUS runtime")
	}
	cfg.EEBusConfig.Enabled = true
	if err := validateEEBusAdminRuntimeConfig(cfg); err != nil {
		t.Fatalf("enabled runtime rejected: %v", err)
	}
}
