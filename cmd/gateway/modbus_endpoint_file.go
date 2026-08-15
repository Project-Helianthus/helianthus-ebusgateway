package main

import (
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"sort"
	"strings"
	"syscall"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
)

const maxModbusEndpointFileBytes = 512

const (
	redactedModbusEndpoint        = "[REDACTED_MODBUS_ENDPOINT]"
	redactedAdapterDirectEndpoint = "[REDACTED_ADAPTER_DIRECT_ENDPOINT]"
	redactedNetworkEndpoint       = "[REDACTED_NETWORK_ENDPOINT]"
)

type endpointOwner uint8

const (
	endpointOwnerUnknown endpointOwner = iota
	endpointOwnerModbus
	endpointOwnerAdapterDirect
)

type endpointOwnedError struct {
	owner endpointOwner
	err   error
}

func (err *endpointOwnedError) Error() string { return err.err.Error() }
func (err *endpointOwnedError) Unwrap() error { return err.err }

func withEndpointOwner(owner endpointOwner, err error) error {
	if err == nil {
		return nil
	}
	return &endpointOwnedError{owner: owner, err: err}
}

func endpointOwnerOf(err error) endpointOwner {
	var owned *endpointOwnedError
	if errors.As(err, &owned) {
		return owned.owner
	}
	return endpointOwnerUnknown
}

type gatewayFlagInputs struct {
	modbusEndpointFile string
}

func resolveModbusEndpointFile(config *ebusgateway.ModbusTCPConfig, path string) error {
	if config == nil {
		return errors.New("invalid Modbus TCP endpoint file configuration")
	}
	if !config.Enabled {
		*config = ebusgateway.ModbusTCPConfig{}
		return nil
	}
	if path == "" {
		return nil
	}
	if config.Endpoint != "" {
		return errors.New("modbus TCP endpoint inputs are mutually exclusive")
	}

	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() {
		return errors.New("invalid Modbus TCP endpoint file")
	}
	file, err := os.Open(path)
	if err != nil {
		return errors.New("invalid Modbus TCP endpoint file")
	}
	defer func() { _ = file.Close() }()
	after, err := file.Stat()
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) {
		return errors.New("invalid Modbus TCP endpoint file")
	}
	if permissions := after.Mode().Perm(); permissions != 0o400 && permissions != 0o600 {
		return errors.New("invalid Modbus TCP endpoint file")
	}
	stat, ok := after.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return errors.New("invalid Modbus TCP endpoint file")
	}
	if after.Size() < 1 || after.Size() > maxModbusEndpointFileBytes {
		return errors.New("invalid Modbus TCP endpoint file")
	}
	content, err := io.ReadAll(io.LimitReader(file, maxModbusEndpointFileBytes+1))
	if err != nil || len(content) < 1 || len(content) > maxModbusEndpointFileBytes {
		return errors.New("invalid Modbus TCP endpoint file")
	}
	endpoint := string(content)
	if err := modbusadapter.ValidateTCPEndpoint(endpoint); err != nil {
		return errors.New("invalid Modbus TCP endpoint file")
	}
	config.Endpoint = endpoint
	return nil
}

func redactFileSourcedModbusError(err error, endpoint string) error {
	if err == nil || endpoint == "" {
		return err
	}
	values := make(map[string]string)
	addEndpointRedaction(values, endpoint, redactedModbusEndpoint)
	if parsed, parseErr := url.Parse(endpoint); parseErr == nil {
		addEndpointRedaction(values, parsed.Host, redactedModbusEndpoint)
		addEndpointRedaction(values, parsed.Hostname(), redactedModbusEndpoint)
	}
	collectEndpointRedactions(err, endpointOwnerUnknown, values, 0)
	ordered := make([]string, 0, len(values))
	for value := range values {
		ordered = append(ordered, value)
	}
	sort.Slice(ordered, func(i, j int) bool { return len(ordered[i]) > len(ordered[j]) })
	message := err.Error()
	for _, value := range ordered {
		message = strings.ReplaceAll(message, value, values[value])
	}
	return errors.New(message)
}

func addEndpointRedaction(values map[string]string, value, marker string) {
	if value == "" {
		return
	}
	if current, exists := values[value]; exists && current != marker {
		values[value] = redactedNetworkEndpoint
		return
	}
	values[value] = marker
}

func markerForEndpointOwner(owner endpointOwner) string {
	switch owner {
	case endpointOwnerModbus:
		return redactedModbusEndpoint
	case endpointOwnerAdapterDirect:
		return redactedAdapterDirectEndpoint
	default:
		return redactedNetworkEndpoint
	}
}

func collectEndpointRedactions(err error, owner endpointOwner, values map[string]string, depth int) {
	if err == nil || depth > 32 {
		return
	}
	if owned, ok := err.(*endpointOwnedError); ok {
		owner = owned.owner
	}
	if networkError, ok := err.(*net.OpError); ok && networkError.Addr != nil {
		addEndpointRedaction(values, networkError.Addr.String(), markerForEndpointOwner(owner))
	}
	if dnsError, ok := err.(*net.DNSError); ok && dnsError.Name != "" {
		addEndpointRedaction(values, dnsError.Name, markerForEndpointOwner(owner))
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			collectEndpointRedactions(child, owner, values, depth+1)
		}
		return
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		collectEndpointRedactions(wrapped.Unwrap(), owner, values, depth+1)
	}
}
