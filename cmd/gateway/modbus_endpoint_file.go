package main

import (
	"errors"
	"io"
	"net/url"
	"os"
	"sort"
	"strings"
	"syscall"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
)

const maxModbusEndpointFileBytes = 512

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
	values := []string{endpoint}
	if parsed, parseErr := url.Parse(endpoint); parseErr == nil {
		values = append(values, parsed.Host, parsed.Hostname())
	}
	sort.Slice(values, func(i, j int) bool { return len(values[i]) > len(values[j]) })
	message := err.Error()
	for _, value := range values {
		if value != "" {
			message = strings.ReplaceAll(message, value, "[REDACTED_MODBUS_ENDPOINT]")
		}
	}
	return errors.New(message)
}
