package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"expvar"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// Prometheus-style expvar metrics for adapter hardware telemetry.
var (
	adapterInfoSupported      = expvar.NewInt("ebus_adapter_info_supported")
	adapterInfoHealth         = expvar.NewInt("ebus_adapter_info_health")
	adapterTemperatureC       = expvar.NewFloat("ebus_adapter_temperature_celsius")
	adapterSupplyVoltageMV    = expvar.NewFloat("ebus_adapter_supply_voltage_millivolts")
	adapterBusVoltageMaxDV    = expvar.NewFloat("ebus_adapter_bus_voltage_max_decivolts")
	adapterBusVoltageMinDV    = expvar.NewFloat("ebus_adapter_bus_voltage_min_decivolts")
	adapterWiFiRSSIDBm        = expvar.NewFloat("ebus_adapter_wifi_rssi_dbm")
	adapterRestartCount       = expvar.NewFloat("ebus_adapter_restart_count")
	adapterInfoQueriesTotal   = expvar.NewMap("ebus_adapter_info_queries_total")
)

type vaillantAdapterInfoState struct {
	mu sync.Mutex

	bus      *protocol.Bus
	info     transport.InfoRequester
	provider *graphql.LiveSemanticProvider

	identity *transport.AdapterVersion
	hwID     string
	hwConfig string

	tempC        *float64
	supplyMV     *int
	busMaxDV     *int
	busMinDV     *int
	resetCause   *string
	resetCode    *byte
	restartCount *byte
	wifiRSSI     *int

	lastIdentity  time.Time
	lastTelemetry time.Time
}

func newVaillantAdapterInfoState(bus *protocol.Bus, rawTransport transport.RawTransport, provider *graphql.LiveSemanticProvider) *vaillantAdapterInfoState {
	info, ok := rawTransport.(transport.InfoRequester)
	if !ok {
		return nil
	}
	return &vaillantAdapterInfoState{
		bus:      bus,
		info:     info,
		provider: provider,
	}
}

func (s *vaillantAdapterInfoState) run(ctx context.Context) {
	if s == nil {
		return
	}

	// Initial identity query.
	s.refreshIdentity(ctx)
	s.refreshTelemetry(ctx)
	s.publish()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.refreshTelemetry(ctx)
			s.publish()
		}
	}
}

func (s *vaillantAdapterInfoState) refreshIdentity(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Query version (ID 0x00).
	data, err := s.queryInfo(ctx, transport.AdapterInfoVersion)
	if err != nil {
		log.Printf("adapter_info: version query failed: %v", err)
		adapterInfoHealth.Set(0)
		return
	}
	version, err := transport.ParseAdapterVersion(data)
	if err != nil {
		log.Printf("adapter_info: version parse failed: %v", err)
		return
	}
	s.identity = &version
	adapterInfoSupported.Set(boolToInt64(version.SupportsInfo))
	adapterInfoHealth.Set(1)

	if !version.SupportsInfo {
		return
	}

	// Query hardware ID (ID 0x01).
	if hwData, err := s.queryInfo(ctx, transport.AdapterInfoHardwareID); err == nil {
		s.hwID = hex.EncodeToString(hwData)
	} else {
		log.Printf("adapter_info: hw_id query failed: %v", err)
	}

	// Query hardware config (ID 0x02).
	if confData, err := s.queryInfo(ctx, transport.AdapterInfoHardwareConf); err == nil {
		s.hwConfig = hex.EncodeToString(confData)
	} else {
		log.Printf("adapter_info: hw_config query failed: %v", err)
	}

	s.lastIdentity = time.Now()
}

func (s *vaillantAdapterInfoState) refreshTelemetry(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.identity == nil || !s.identity.SupportsInfo {
		return
	}
	version := s.identity

	// Temperature (ID 0x03).
	if data, err := s.queryInfo(ctx, transport.AdapterInfoTemperature); err == nil && len(data) >= 2 {
		v := float64(binary.BigEndian.Uint16(data[:2]))
		s.tempC = &v
		adapterTemperatureC.Set(v)
	}

	// Supply voltage (ID 0x04).
	if data, err := s.queryInfo(ctx, transport.AdapterInfoSupplyVolt); err == nil && len(data) >= 2 {
		raw := int(binary.BigEndian.Uint16(data[:2]))
		if raw > 0 {
			s.supplyMV = &raw
			adapterSupplyVoltageMV.Set(float64(raw))
		}
	}

	// Bus voltage (ID 0x05).
	if data, err := s.queryInfo(ctx, transport.AdapterInfoBusVoltage); err == nil && len(data) >= 2 {
		if data[0] > 0 {
			v := int(data[0])
			s.busMaxDV = &v
			adapterBusVoltageMaxDV.Set(float64(v))
		}
		if data[1] > 0 {
			v := int(data[1])
			s.busMinDV = &v
			adapterBusVoltageMinDV.Set(float64(v))
		}
	}

	// Reset info (ID 0x06) — gated.
	if version.SupportsInfoID(transport.AdapterInfoResetInfo) {
		if data, err := s.queryInfo(ctx, transport.AdapterInfoResetInfo); err == nil {
			if info, err := transport.ParseAdapterResetInfo(data); err == nil {
				s.resetCause = &info.Cause
				s.resetCode = &info.CauseCode
				s.restartCount = &info.RestartCount
				adapterRestartCount.Set(float64(info.RestartCount))
			}
		}
	}

	// WiFi RSSI (ID 0x07) — gated.
	if version.SupportsInfoID(transport.AdapterInfoWiFiRSSI) {
		if data, err := s.queryInfo(ctx, transport.AdapterInfoWiFiRSSI); err == nil && len(data) >= 1 {
			v := int(int8(data[0]))
			if v != 0 {
				s.wifiRSSI = &v
				adapterWiFiRSSIDBm.Set(float64(v))
			}
		}
	}

	s.lastTelemetry = time.Now()
}

func (s *vaillantAdapterInfoState) queryInfo(ctx context.Context, id transport.AdapterInfoID) ([]byte, error) {
	var result []byte
	var queryErr error
	err := s.bus.RawTransportOp(ctx, func(rt transport.RawTransport) error {
		data, err := s.info.RequestInfo(id)
		if err != nil {
			adapterInfoQueriesTotal.Add(fmt.Sprintf("%s:error", id), 1)
			queryErr = err
			return err
		}
		adapterInfoQueriesTotal.Add(fmt.Sprintf("%s:ok", id), 1)
		result = data
		return nil
	})
	if err != nil && queryErr == nil {
		return nil, err
	}
	if queryErr != nil {
		return nil, queryErr
	}
	return result, nil
}

func (s *vaillantAdapterInfoState) publish() {
	s.mu.Lock()
	defer s.mu.Unlock()

	info := &graphql.AdapterHardwareInfo{}

	if s.identity != nil {
		v := s.identity
		info.FirmwareVersion = fmt.Sprintf("0x%02x", v.Version)
		info.Features = v.Features
		info.Jumpers = v.Jumpers
		info.IsWiFi = v.IsWiFi
		info.IsEthernet = v.IsEthernet
		info.VersionResponseLen = v.VersionResponseLen()
		info.InfoSupported = v.SupportsInfo

		if v.HasChecksum {
			info.FirmwareChecksum = fmt.Sprintf("0x%04x", v.Checksum)
		}
		if v.HasBootloader {
			info.BootloaderVersion = fmt.Sprintf("0x%02x", v.BootloaderVersion)
			info.BootloaderChecksum = fmt.Sprintf("0x%04x", v.BootloaderChecksum)
		}

		var flags []string
		if v.Jumpers&0x01 != 0 {
			flags = append(flags, "enhanced")
		}
		if v.IsHighSpeed {
			flags = append(flags, "high_speed")
		}
		if v.IsEthernet {
			flags = append(flags, "ethernet")
		}
		if v.IsWiFi {
			flags = append(flags, "wifi")
		}
		if v.IsV31 {
			flags = append(flags, "v3.1")
		}
		if v.Jumpers&0x20 != 0 {
			flags = append(flags, "soft_config")
		}
		info.JumperFlags = flags
	}

	info.HardwareID = s.hwID
	info.HardwareConfig = s.hwConfig
	info.TemperatureC = s.tempC
	info.SupplyVoltageMV = s.supplyMV
	info.BusVoltageMaxDV = s.busMaxDV
	info.BusVoltageMinDV = s.busMinDV
	info.ResetCause = s.resetCause
	info.ResetCauseCode = s.resetCode
	info.RestartCount = s.restartCount
	info.WiFiRSSIDBm = s.wifiRSSI

	if !s.lastIdentity.IsZero() {
		t := s.lastIdentity
		info.LastIdentityQuery = &t
	}
	if !s.lastTelemetry.IsZero() {
		t := s.lastTelemetry
		info.LastTelemetryQuery = &t
	}

	s.provider.SetAdapterHardwareInfo(info)
}

func boolToInt64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
