package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
)

func writeProtectedEndpointFile(t *testing.T, content string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "modbus-endpoint")
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatalf("write endpoint file: %v", err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("chmod endpoint file: %v", err)
	}
	return path
}

func TestBindFlagsKeepsModbusEndpointFileOutOfRuntimeConfig(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("modbus-endpoint-file", flag.ContinueOnError)
	inputs := bindFlags(fs, &cfg)
	path := "/run/helianthus/modbus-endpoint"
	if err := fs.Parse([]string{
		"-modbus-tcp-enabled=true",
		"-modbus-tcp-endpoint-file", path,
		"-modbus-tcp-dial-timeout", "3s",
	}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	if inputs == nil || inputs.modbusEndpointFile != path {
		t.Fatalf("endpoint-file input = %+v", inputs)
	}
	if cfg.ModbusTCPConfig.Endpoint != "" {
		t.Fatalf("raw endpoint reached flag-populated runtime config: %q", cfg.ModbusTCPConfig.Endpoint)
	}
}

func TestResolveModbusEndpointFileLoadsProtectedBoundedValue(t *testing.T) {
	for _, mode := range []os.FileMode{0o400, 0o600} {
		t.Run(mode.String(), func(t *testing.T) {
			path := writeProtectedEndpointFile(t, "tcp://192.0.2.40:502", mode)
			cfg := ebusgateway.ModbusTCPConfig{Enabled: true, DialTimeout: 3 * time.Second}

			if err := resolveModbusEndpointFile(&cfg, path); err != nil {
				t.Fatalf("resolve endpoint file: %v", err)
			}
			if cfg.Endpoint != "tcp://192.0.2.40:502" {
				t.Fatalf("endpoint = %q", cfg.Endpoint)
			}
			if _, err := mapModbusRuntimeConfig(cfg); err != nil {
				t.Fatalf("map resolved config: %v", err)
			}
		})
	}
}

func TestResolveModbusEndpointFileRejectsInlineConflictWithoutEchoingEndpoint(t *testing.T) {
	secretEndpoint := "tcp://sensitive.internal:502"
	path := writeProtectedEndpointFile(t, secretEndpoint, 0o600)
	cfg := ebusgateway.ModbusTCPConfig{
		Enabled:     true,
		Endpoint:    "tcp://other.internal:502",
		DialTimeout: time.Second,
	}

	err := resolveModbusEndpointFile(&cfg, path)
	if err == nil {
		t.Fatal("inline/file conflict was accepted")
	}
	if strings.Contains(err.Error(), secretEndpoint) || strings.Contains(err.Error(), cfg.Endpoint) {
		t.Fatalf("error leaked endpoint material: %v", err)
	}
}

func TestResolveModbusEndpointFileDisabledIsInertForAnyRetainedInputs(t *testing.T) {
	cfg := ebusgateway.ModbusTCPConfig{
		Endpoint:    "tcp://retained.invalid:502",
		DialTimeout: -time.Second,
	}

	if err := resolveModbusEndpointFile(&cfg, "/missing/retained-endpoint"); err != nil {
		t.Fatalf("disabled config inspected retained inputs: %v", err)
	}
	if cfg != (ebusgateway.ModbusTCPConfig{}) {
		t.Fatalf("disabled config is not inert: %+v", cfg)
	}
}

func TestResolveModbusEndpointFileRejectsUnsafeFilesAndBounds(t *testing.T) {
	valid := writeProtectedEndpointFile(t, "tcp://192.0.2.40:502", 0o600)
	symlink := filepath.Join(t.TempDir(), "endpoint-link")
	if err := os.Symlink(valid, symlink); err != nil {
		t.Fatalf("create symlink: %v", err)
	}
	unsafeMode := writeProtectedEndpointFile(t, "tcp://192.0.2.40:502", 0o644)
	empty := writeProtectedEndpointFile(t, "", 0o600)
	oversized := writeProtectedEndpointFile(t, strings.Repeat("x", 513), 0o600)
	invalid := writeProtectedEndpointFile(t, "tcp://sensitive.internal:502\n", 0o600)
	directory := t.TempDir()

	for name, path := range map[string]string{
		"symlink":     symlink,
		"unsafe-mode": unsafeMode,
		"empty":       empty,
		"oversized":   oversized,
		"invalid":     invalid,
		"directory":   directory,
		"missing":     filepath.Join(t.TempDir(), "missing"),
	} {
		t.Run(name, func(t *testing.T) {
			cfg := ebusgateway.ModbusTCPConfig{Enabled: true, DialTimeout: time.Second}
			if err := resolveModbusEndpointFile(&cfg, path); err == nil {
				t.Fatalf("unsafe endpoint file %s was accepted", name)
			}
			if cfg.Endpoint != "" {
				t.Fatalf("unsafe endpoint file populated config: %q", cfg.Endpoint)
			}
		})
	}
}

func TestRedactFileSourcedModbusErrorRemovesEndpointVariants(t *testing.T) {
	endpoint := "tcp://sensitive.internal:502"
	err := errors.New(
		"dial tcp://sensitive.internal:502 via sensitive.internal:502: " +
			"lookup sensitive.internal: no such host",
	)

	redacted := redactFileSourcedModbusError(err, endpoint)
	if redacted == nil {
		t.Fatal("redacted error is nil")
	}
	for _, secret := range []string{endpoint, "sensitive.internal:502", "sensitive.internal"} {
		if strings.Contains(redacted.Error(), secret) {
			t.Fatalf("redacted startup error contains %q: %v", secret, redacted)
		}
	}
	if !strings.Contains(redacted.Error(), "[REDACTED_MODBUS_ENDPOINT]") {
		t.Fatalf("redacted startup error has no marker: %v", redacted)
	}
}

func TestRedactFileSourcedModbusErrorLeavesInlineErrorUnchanged(t *testing.T) {
	original := errors.New("dial tcp://inline.internal:502 failed")
	if got := redactFileSourcedModbusError(original, ""); got != original {
		t.Fatalf("inline error identity changed: got %v want %v", got, original)
	}
}

func TestRedactFileSourcedModbusErrorRemovesResolvedNetworkAddresses(t *testing.T) {
	endpoint := "tcp://modbus.internal:502"
	for name, address := range map[string]*net.TCPAddr{
		"ipv4": {IP: net.ParseIP("192.0.2.40"), Port: 502},
		"ipv6": {IP: net.ParseIP("2001:db8::40"), Port: 502},
	} {
		t.Run(name, func(t *testing.T) {
			dialError := &net.OpError{
				Op:   "dial",
				Net:  "tcp",
				Addr: address,
				Err:  errors.New("connection refused"),
			}
			wrapped := fmt.Errorf(
				"modbus startup: %w",
				errors.Join(errors.New("sidecar failed"), dialError),
			)

			redacted := redactFileSourcedModbusError(wrapped, endpoint)
			if strings.Contains(redacted.Error(), address.String()) ||
				strings.Contains(redacted.Error(), "modbus.internal") {
				t.Fatalf("redacted error leaks remote address: %v", redacted)
			}
			if !strings.Contains(redacted.Error(), "[REDACTED_MODBUS_ENDPOINT]") {
				t.Fatalf("redacted error has no marker: %v", redacted)
			}
		})
	}
}

func TestRedactFileSourcedModbusErrorRemovesWrappedDNSName(t *testing.T) {
	endpoint := "tcp://configured.invalid:502"
	dnsError := &net.DNSError{Name: "canonical.internal", Err: "no such host"}
	redacted := redactFileSourcedModbusError(fmt.Errorf("resolve: %w", dnsError), endpoint)
	if strings.Contains(redacted.Error(), "configured.invalid") ||
		strings.Contains(redacted.Error(), "canonical.internal") {
		t.Fatalf("redacted error leaks DNS name: %v", redacted)
	}
}
