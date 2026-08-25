package mcp

import (
	"context"
	"errors"

	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

// GrowattBMSRS485V202Runtime composes an explicitly selected BMS RTU observer
// into the existing typed MCP provider surface. It owns no port, endpoint,
// discovery, retry, or control authority.
type GrowattBMSRS485V202Runtime struct {
	observer *modbusreg.GrowattBMSRS485RTUObserver
}

// NewGrowattBMSRS485V202Runtime binds one exact BMS tuple and unicast unit to
// an injected generic correlated read session. It does not open or configure
// a serial connection.
func NewGrowattBMSRS485V202Runtime(
	revision modbusreg.GrowattBMSRevisionTuple,
	unitID byte,
	session modbusreg.GrowattBMSRS485RTUObserverSession,
) (*GrowattBMSRS485V202Runtime, error) {
	observer, err := modbusreg.NewGrowattBMSRS485RTUObserver(revision, unitID, session)
	if err != nil {
		return nil, err
	}
	return &GrowattBMSRS485V202Runtime{observer: observer}, nil
}

// GrowattBMSRS485V202 returns only the bounded typed status produced after
// all four contract reads complete. It never returns a partial status.
func (runtime *GrowattBMSRS485V202Runtime) GrowattBMSRS485V202(ctx context.Context) (modbusreg.GrowattBMSTypedReadOnlyStatus, error) {
	if runtime == nil || runtime.observer == nil {
		return modbusreg.GrowattBMSTypedReadOnlyStatus{}, errors.New("growatt BMS RTU runtime is unavailable")
	}
	return runtime.observer.Observe(ctx)
}
