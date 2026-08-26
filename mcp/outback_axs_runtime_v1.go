package mcp

import (
	"context"
	"errors"
)

var (
	ErrOutBackAXSV1ProviderUnavailable = errors.New("outback AXS provider is unavailable")
	ErrOutBackAXSV1NotQualified        = errors.New("outback AXS snapshot is not qualified")
)

type OutBackAXSV1ProviderSnapshot struct {
	Profile                                                  string
	Qualified                                                bool
	FirmwareMajor, FirmwareMid, FirmwareMinor                uint16
	BatteryTemperature, AmbientTemperature, TemperatureScale int16
	Error, Status                                            uint16
	RawWords                                                 []uint16
	OutboundAllowed                                          bool
}
type OutBackAXSV1SnapshotProvider interface {
	OutBackAXSV1Snapshot(context.Context) (OutBackAXSV1ProviderSnapshot, error)
}
type OutBackAXSV1Runtime struct{ provider OutBackAXSV1SnapshotProvider }

func NewOutBackAXSV1Runtime(p OutBackAXSV1SnapshotProvider) (*OutBackAXSV1Runtime, error) {
	if p == nil {
		return nil, ErrOutBackAXSV1ProviderUnavailable
	}
	return &OutBackAXSV1Runtime{provider: p}, nil
}
func (r *OutBackAXSV1Runtime) OutBackAXSV1(ctx context.Context) (OutBackAXSV1Result, error) {
	if r == nil || r.provider == nil {
		return OutBackAXSV1Result{}, ErrOutBackAXSV1ProviderUnavailable
	}
	s, err := r.provider.OutBackAXSV1Snapshot(ctx)
	if err != nil {
		return OutBackAXSV1Result{}, err
	}
	if s.Profile != OutBackAXSV1Profile || !s.Qualified {
		return OutBackAXSV1Result{}, ErrOutBackAXSV1NotQualified
	}
	return OutBackAXSV1Result{Profile: s.Profile, Qualified: true, FirmwareMajor: s.FirmwareMajor, FirmwareMid: s.FirmwareMid, FirmwareMinor: s.FirmwareMinor, BatteryTemperature: s.BatteryTemperature, AmbientTemperature: s.AmbientTemperature, TemperatureScale: s.TemperatureScale, Error: s.Error, Status: s.Status, RawWords: append([]uint16(nil), s.RawWords...), OutboundAllowed: s.OutboundAllowed}, nil
}
