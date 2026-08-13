package main

import (
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"syscall"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
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
	if !validModbusTCPEndpoint(endpoint) {
		return errors.New("invalid Modbus TCP endpoint file")
	}
	config.Endpoint = endpoint
	return nil
}

func validModbusTCPEndpoint(endpoint string) bool {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "tcp" || parsed.Host == "" ||
		parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" ||
		parsed.Fragment != "" {
		return false
	}
	_, _, err = net.SplitHostPort(parsed.Host)
	return err == nil
}
