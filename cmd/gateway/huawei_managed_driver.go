package main

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

type huaweiRegistryProvider interface {
	Start(modbusreg.HuaweiProviderSession) error
	Refresh(context.Context) (modbusreg.HuaweiProviderCycle, error)
	NextRefreshAt() time.Time
	Close() error
	Reconnect(modbusreg.HuaweiProviderSession) error
}

func newHuaweiRegistryProvider(family modbusTCPFamily, config modbusreg.HuaweiProviderConfig) (huaweiRegistryProvider, error) {
	switch family {
	case modbusTCPFamilyHuaweiSmartLogger:
		return modbusreg.NewHuaweiSmartLoggerReadOnlyProvider(config)
	case modbusTCPFamilyHuaweiSDongle:
		return modbusreg.NewHuaweiSDongleReadOnlyProvider(config)
	default:
		return nil, modbusreg.ErrHuaweiProviderNotAdmitted
	}
}

func huaweiWait(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type huaweiManagedWork struct {
	ctx           context.Context
	cancel        context.CancelFunc
	adapter       *modbusadapter.Adapter
	instance      modbusTCPInstance
	provider      huaweiRegistryProvider
	publication   *huaweiPVPublication
	err           error
	done          chan struct{}
	started       bool
	rebindPending bool
}

func newHuaweiInstanceDriverAdapter(instance modbusTCPInstance, hub *semhub.Hub) runtimecontrol.LifecycleAdapter {
	return runtimecontrol.NewAdapter(func(ctx context.Context, c runtimecontrol.Construction) (*modbusManagedDriver, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var path modbusreg.HuaweiProviderPath
		switch instance.family {
		case modbusTCPFamilyHuaweiSmartLogger:
			path = modbusreg.HuaweiProviderPathSmartLogger
		case modbusTCPFamilyHuaweiSDongle:
			path = modbusreg.HuaweiProviderPathSDongle
		default:
			return nil, modbusreg.ErrHuaweiProviderNotAdmitted
		}
		disposition, ok := modbusreg.HuaweiRecordedProviderDisposition(path)
		if !ok || !modbusreg.EvaluateHuaweiProviderEnablement(disposition).Admitted {
			return nil, modbusreg.ErrHuaweiProviderNotAdmitted
		}
		if c.DriverID != instance.driverID || len(c.Sources) != 1 || c.Sources[0] == "" {
			return nil, errors.New("huawei managed identity incomplete")
		}
		c.Sources = append([]runtimecontrol.SourceID(nil), c.Sources...)
		var previous *semhub.Handle
		factory := func(ctx context.Context, adapter *modbusadapter.Adapter) modbusManagedWork {
			run, cancel := context.WithCancel(ctx)
			work := &huaweiManagedWork{ctx: run, cancel: cancel, adapter: adapter, instance: instance, done: make(chan struct{})}
			work.publication, work.err = newHuaweiPVPublication(hub, string(c.DriverID), string(c.Sources[0]), instance.label, previous, adapter.PublicationActive)
			if work.err == nil {
				previous = work.publication.handle
			}
			if work.err == nil {
				work.provider, work.err = newHuaweiRegistryProvider(instance.family, modbusreg.HuaweiProviderConfig{Source: instance.label, AllowedUnits: append([]byte(nil), instance.units...), Wait: huaweiWait})
			}
			if work.err == nil {
				var session *modbusadapter.ReadOnlySession
				session, work.err = modbusadapter.NewReadOnlySession(adapter)
				if work.err == nil {
					work.err = work.provider.Start(session)
				}
			}
			return work
		}
		// Tolerance is restricted to this native path. The native provider, rather
		// than the transport, decides whether each anomalous page is admissible.
		endpointFactory := func(config modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
			config.EventSink = huaweiTransportSink{family: instance.family, upstream: config.EventSink}
			config.Framing = modbus.TCPFramingTolerant
			config.MaxBufferedBytes = 7 + modbus.MaxTolerantPDUSize
			return newModbusEndpointFn(config)
		}
		return &modbusManagedDriver{config: instance.config, pacing: instance.pacing, hub: hub, dial: dialModbusEndpointFn, factory: endpointFactory, workFactory: factory, construction: c}, nil
	})
}

func (w *huaweiManagedWork) StartError() error { return w.err }
func (w *huaweiManagedWork) Start() {
	if w.err != nil {
		return
	}
	w.started = true
	go func() {
		defer close(w.done)
		for w.ctx.Err() == nil {
			started := time.Now()
			cycle, err := w.provider.Refresh(w.ctx)
			if err == nil && w.adapter.PublicationActive() {
				publishErr := w.publication.Publish(cycle)
				if publishErr != nil {
					modbusTCPLogf(w.instance)("Huawei PV publication refused")
				}
			}
			next := w.provider.NextRefreshAt()
			if configured := started.Add(w.instance.cycle); configured.After(next) {
				next = configured
			}
			if next.Before(time.Now()) {
				next = time.Now().Add(w.instance.cycle)
			}
			w.rebind()
			if huaweiWait(w.ctx, time.Until(next)) != nil {
				return
			}
		}
	}()
}

// rebind reconnects the endpoint when required and rebinds the provider to the
// new generation. A failed rebind stays pending and is retried each cycle,
// because the reopened endpoint no longer reports ReconnectRequired.
func (w *huaweiManagedWork) rebind() {
	if w.adapter.Snapshot().ReconnectRequired {
		if err := w.adapter.Reconnect(w.ctx); err != nil {
			return
		}
		w.rebindPending = true
	}
	if !w.rebindPending {
		return
	}
	session, err := modbusadapter.NewReadOnlySession(w.adapter)
	if err == nil {
		err = w.provider.Reconnect(session)
	}
	if err == nil {
		w.rebindPending = false
	}
}

// FenceSemantic is retained independently from native Close so a semantic
// cleanup failure can be retried without misclassifying a healthy endpoint.
func (w *huaweiManagedWork) FenceSemantic() error {
	if w.publication == nil {
		return nil
	}
	return w.publication.Fence()
}
func (w *huaweiManagedWork) Close() error {
	w.cancel()
	if w.started {
		<-w.done
	}
	if w.provider != nil {
		return w.provider.Close()
	}
	return nil
}

func (p *managedModbusMCPProvider) HuaweiInstanceStatus(ctx context.Context, family, label string) (out mcp.HuaweiInstanceStatus, err error) {
	out = mcp.HuaweiInstanceStatus{Instance: label, Family: family, Children: []mcp.HuaweiInstanceChildStatus{}, PublishedAssetIDs: []string{}, Loss: []string{}}
	id, ok := modbusTCPDriverID(modbusTCPFamily(family), label)
	if !ok {
		return out, errors.New("huawei instance unavailable")
	}
	entry := p.bindings.entry(id)
	if entry == nil || entry.modbusTCP == nil || string(entry.modbusTCP.family) != family {
		return out, errors.New("huawei instance unavailable")
	}
	if entry.withheld != nil {
		// Only a family-level withholding is a closed instance disposition.
		// Portal, descriptor or construction withholding is a lifecycle failure.
		if entry.modbusTCP.withheld == "" {
			return out, errors.New("huawei instance unavailable")
		}
		out.Disposition = string(entry.modbusTCP.withheld)
		return out, nil
	}
	err = entry.with(ctx, func(native drivermanager.Runtime) error {
		driver, ok := native.(*modbusManagedDriver)
		if !ok {
			return errors.New("huawei runtime unavailable")
		}
		attempt := driver.active.Load()
		if attempt == nil || !driver.current(attempt) {
			return errors.New("huawei generation unavailable")
		}
		work, ok := attempt.work.(*huaweiManagedWork)
		if !ok || work.publication == nil {
			return errors.New("huawei provider unavailable")
		}
		publication := work.publication
		publication.mu.Lock()
		defer publication.mu.Unlock()
		cycle := publication.latest
		if publication.fenced || !publication.active() {
			return errors.New("huawei generation withdrawn")
		}
		out.Disposition = string(cycle.Outcome)
		if cycle.Cycle == 0 {
			out.Disposition = string(modbusreg.HuaweiProviderCycleIncomplete)
		}
		out.Qualified = cycle.Gateway.State == modbusreg.HuaweiProviderGatewayIdentified
		out.Children = huaweiStatusChildren(publication, cycle)
		// Published assets are enumerated from the publication itself so an
		// incomplete cycle still reports the retained last-known assets.
		for _, id := range publication.published {
			out.PublishedAssetIDs = append(out.PublishedAssetIDs, string(id))
		}
		sort.Slice(out.Children, func(i, j int) bool { return out.Children[i].Unit < out.Children[j].Unit })
		sort.Strings(out.PublishedAssetIDs)
		for _, loss := range cycle.Loss {
			out.Loss = append(out.Loss, string(loss.Kind))
		}
		sort.Strings(out.Loss)
		out.RetainedWithdrawals = huaweiStatusRetainedWithdrawals(publication)
		return nil
	})
	return out, err
}

// huaweiStatusChildren reports the latest cycle's children with the asset each
// published child holds and the refusal, if any, the publication recorded for
// a child the provider admitted. The caller holds the publication lock.
func huaweiStatusChildren(p *huaweiPVPublication, cycle modbusreg.HuaweiProviderCycle) []mcp.HuaweiInstanceChildStatus {
	var out []mcp.HuaweiInstanceChildStatus
	for _, child := range cycle.Children {
		status := mcp.HuaweiInstanceChildStatus{Unit: child.UnitID, Family: child.Family, Profile: child.ProfileID, Evidence: string(child.Evidence), Reason: string(child.Reason)}
		if id, ok := p.published[child.UnitID]; ok {
			status.AssetID = string(id)
		} else if refusal, ok := p.refused[child.UnitID]; ok {
			status.Refusal = refusal
		}
		out = append(out, status)
	}
	return out
}

// huaweiStatusRetainedWithdrawals reports the capacity withdrawals this
// generation committed. The caller holds the publication lock.
func huaweiStatusRetainedWithdrawals(p *huaweiPVPublication) []mcp.HuaweiRetainedWithdrawalStatus {
	var out []mcp.HuaweiRetainedWithdrawalStatus
	for _, record := range p.capacity {
		out = append(out, mcp.HuaweiRetainedWithdrawalStatus{AssetID: string(record.AssetID), SourceEpochID: string(record.SourceEpochID), Generation: string(record.Generation), Count: record.Count, Reason: record.Reason})
	}
	return out
}
