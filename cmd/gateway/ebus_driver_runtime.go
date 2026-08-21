package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/v8classifier"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

const primaryEBusDriverID = "ebus.primary"

const (
	defaultEBusDriverDrainTimeout = 2 * time.Second
	defaultEBusRetryBudget        = 5
	defaultEBusRetryInitialDelay  = time.Second
	defaultEBusRetryMaxDelay      = 30 * time.Second
)

type ebusDriverController struct {
	manager    *drivermanager.Manager
	active     transport.RawTransport
	passive    transport.RawTransport
	classifier activeTxnClassifier
}

func newEBusDriverController(cfg ebusgateway.Config) (*ebusDriverController, error) {
	protocol := configuredEBusDriverProtocol(cfg)
	needsPassive := cfg.BroadcastListen && (protocol == ebusgateway.TransportAdapterDirect || shouldStartPassiveObserveFirst(cfg))
	runtimeSeam := transport.NewDriverRuntime(
		newEBusDriverFactory(cfg, protocol, needsPassive),
		transport.DriverRuntimeConfig{DrainTimeout: defaultEBusDriverDrainTimeout},
	)
	runtime := &ebusDriverRuntime{runtime: runtimeSeam}
	manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{
		ID:      primaryEBusDriverID,
		Enabled: true,
		Runtime: runtime,
		Capabilities: []drivermanager.Capability{
			drivermanager.CapabilityDiscovery,
			drivermanager.CapabilityRawEvidence,
			drivermanager.CapabilityRead,
			drivermanager.CapabilitySemanticProjection,
			drivermanager.CapabilityWrite,
		},
		ClassifyError: classifyEBusDriverError,
		Retry: drivermanager.RetryPolicy{
			Budget:       defaultEBusRetryBudget,
			InitialDelay: defaultEBusRetryInitialDelay,
			MaxDelay:     defaultEBusRetryMaxDelay,
		},
	}}})
	if err != nil {
		return nil, err
	}
	reporter := func(correlation drivermanager.Correlation, rawErr error) {
		manager.ReportFailure(primaryEBusDriverID, correlation, classifyEBusDriverError(rawErr))
	}
	controller := &ebusDriverController{
		manager: manager,
		active:  newManagedEBusStableTransport(manager, protocol, reporter),
	}
	if needsPassive {
		controller.passive = newEBusStablePassiveTransport(
			managedEBusSlotInvoker{manager: manager, id: primaryEBusDriverID},
			protocol,
		)
	}
	if protocol == ebusgateway.TransportAdapterDirect {
		controller.classifier = &ebusStableClassifier{invoker: managedEBusSlotInvoker{manager: manager, id: primaryEBusDriverID}}
	}
	return controller, nil
}

func configuredEBusDriverProtocol(cfg ebusgateway.Config) ebusgateway.TransportProtocol {
	if cfg.Transport != nil {
		if _, ok := cfg.Transport.(transport.EscapeAware); !ok {
			return ebusgateway.TransportTCPPlain
		}
		if _, ok := cfg.Transport.(transport.StructuralWriteSignaler); ok {
			return ebusgateway.TransportAdapterDirect
		}
		return ebusgateway.TransportENH
	}
	address := strings.ToLower(strings.TrimSpace(cfg.TransportConfig.Address))
	if strings.HasPrefix(address, "adapter-direct://") || strings.HasPrefix(address, "adapter-direct-ens://") {
		return ebusgateway.TransportAdapterDirect
	}
	return ebusgateway.CanonicalTransportProtocol(cfg.TransportConfig.Protocol)
}

func newEBusDriverFactory(cfg ebusgateway.Config, protocol ebusgateway.TransportProtocol, needsPassive bool) transport.DriverRuntimeFactory {
	return func(ctx context.Context) (*transport.ManagedRawTransport, error) {
		if protocol == ebusgateway.TransportAdapterDirect {
			return buildManagedAdapterDirectGeneration(ctx, cfg)
		}
		active, activeClose, err := ebusgateway.OpenEBusDriverTransport(ctx, cfg)
		if err != nil {
			return nil, err
		}
		if active == nil {
			return nil, errors.New("eBUS provider unavailable")
		}
		var passive transport.RawTransport
		if needsPassive {
			passive, err = ebusgateway.OpenEBusDriverPassiveTransport(ctx, cfg)
			if err != nil {
				// Return the partially constructed generation with its error so
				// DriverRuntime retires it through the same CloseRequest/Closed
				// proof path; the factory never closes beneath lifecycle code.
				return newManagedEBusGenerationParts(ctx, active, nil, nil, activeClose), err
			}
		}
		closeFn := func() error {
			var passiveErr error
			if passive != nil {
				passiveErr = passive.Close()
			}
			return errors.Join(activeClose(), passiveErr)
		}
		return newManagedEBusGenerationParts(ctx, active, passive, nil, closeFn), nil
	}
}

func buildManagedAdapterDirectGeneration(factoryCtx context.Context, cfg ebusgateway.Config) (*transport.ManagedRawTransport, error) {
	adapterCtx, adapterCancel := context.WithCancel(context.Background())
	stopForward := context.AfterFunc(factoryCtx, adapterCancel)
	generationCfg := cfg
	closer, classifier, err := wireAdapterDirect(adapterCtx, &generationCfg)
	if err != nil {
		stopForward()
		adapterCancel()
		return nil, err
	}
	if generationCfg.Transport == nil || closer == nil {
		stopForward()
		adapterCancel()
		return nil, errors.New("adapter-direct provider unavailable")
	}
	closeFn := func() error {
		adapterCancel()
		return closer()
	}
	managed := newManagedEBusGenerationParts(factoryCtx, generationCfg.Transport, generationCfg.PassiveTransport, classifier, closeFn)
	if !stopForward() || factoryCtx.Err() != nil {
		if err := factoryCtx.Err(); err != nil {
			return managed, err
		}
		return managed, context.Canceled
	}
	return managed, nil
}

func classifyEBusDriverError(err error) drivermanager.Failure {
	switch {
	case errors.Is(err, drivermanager.ErrUnavailable), errors.Is(err, transport.ErrDriverUnavailable):
		return drivermanager.Failure{Reason: drivermanager.Reason{Code: drivermanager.ReasonProviderUnavailable}}
	case errors.Is(err, ebuserrors.ErrInvalidPayload):
		return drivermanager.Failure{Reason: drivermanager.Reason{Code: drivermanager.ReasonConfigInvalid}}
	default:
		return drivermanager.Failure{Reason: drivermanager.Reason{Code: drivermanager.ReasonDependencyUnavailable, Retryable: true}}
	}
}

func (controller *ebusDriverController) Start(ctx context.Context) error {
	if controller == nil || controller.manager == nil {
		return drivermanager.ErrUnavailable
	}
	return controller.manager.Start(ctx, primaryEBusDriverID)
}

func (controller *ebusDriverController) Shutdown(ctx context.Context) error {
	if controller == nil || controller.manager == nil {
		return nil
	}
	return controller.manager.Shutdown(ctx)
}

func (controller *ebusDriverController) Snapshot() drivermanager.Snapshot {
	if controller == nil || controller.manager == nil {
		return drivermanager.Snapshot{DriverID: primaryEBusDriverID, ObservedState: drivermanager.ObservedFailed, Reason: drivermanager.Reason{Code: drivermanager.ReasonProviderUnavailable}}
	}
	snapshot, _ := controller.manager.Snapshot(primaryEBusDriverID)
	return snapshot
}

type ebusDriverRuntime struct {
	runtime *transport.DriverRuntime
}

func (runtime *ebusDriverRuntime) Start(ctx context.Context) (uint64, error) {
	if runtime == nil || runtime.runtime == nil {
		return 0, drivermanager.ErrUnavailable
	}
	generation, err := runtime.runtime.Start(ctx)
	return generation, mapEBusLifecycleError(err)
}

func (runtime *ebusDriverRuntime) Replace(ctx context.Context) (uint64, error) {
	if runtime == nil || runtime.runtime == nil {
		return 0, drivermanager.ErrUnavailable
	}
	generation, err := runtime.runtime.Replace(ctx)
	return generation, mapEBusLifecycleError(err)
}

func (runtime *ebusDriverRuntime) Stop(ctx context.Context) error {
	if runtime == nil || runtime.runtime == nil {
		return nil
	}
	return mapEBusLifecycleError(runtime.runtime.Stop(ctx))
}

func (runtime *ebusDriverRuntime) Generation() uint64 {
	if runtime == nil || runtime.runtime == nil {
		return 0
	}
	return runtime.runtime.Generation()
}

func (runtime *ebusDriverRuntime) Revision() uint64 {
	if runtime == nil || runtime.runtime == nil {
		return 0
	}
	return runtime.runtime.Revision()
}

func (runtime *ebusDriverRuntime) SafetyQuarantined() bool {
	return runtime != nil && runtime.runtime != nil && runtime.runtime.SafetyQuarantined()
}

func (runtime *ebusDriverRuntime) Admit(ctx context.Context) (*drivermanager.Admission, error) {
	if runtime == nil || runtime.runtime == nil {
		return nil, drivermanager.ErrUnavailable
	}
	lease, err := runtime.runtime.Admit(ctx)
	if err != nil {
		return nil, mapEBusLifecycleError(err)
	}
	return &drivermanager.Admission{
		Correlation: drivermanager.Correlation{Generation: lease.Generation()},
		Invoke: func(callback func(any) error) error {
			return mapEBusLifecycleError(lease.Invoke(func(raw transport.RawTransport) error {
				if callback == nil {
					return transport.ErrDriverUnavailable
				}
				return callback(raw)
			}))
		},
		Release: lease.Release,
	}, nil
}

func mapEBusLifecycleError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, transport.ErrDriverSafetyQuarantined):
		return errors.Join(drivermanager.ErrSafetyQuarantined, err)
	case errors.Is(err, transport.ErrDriverStopTimeout):
		return errors.Join(drivermanager.ErrStopTimeout, err)
	case errors.Is(err, transport.ErrDriverUnavailable):
		return errors.Join(drivermanager.ErrUnavailable, err)
	default:
		return err
	}
}

type ebusSlotInvoker interface {
	Invoke(context.Context, drivermanager.Capability, func(transport.RawTransport) error) (drivermanager.Correlation, error)
}

type directEBusSlotInvoker struct {
	runtime *transport.DriverRuntime
}

func (invoker directEBusSlotInvoker) Invoke(ctx context.Context, _ drivermanager.Capability, callback func(transport.RawTransport) error) (drivermanager.Correlation, error) {
	if invoker.runtime == nil {
		return drivermanager.Correlation{}, transport.ErrDriverUnavailable
	}
	lease, err := invoker.runtime.Admit(ctx)
	if err != nil {
		return drivermanager.Correlation{Generation: invoker.runtime.Generation()}, err
	}
	defer func() { _ = lease.Release() }()
	correlation := drivermanager.Correlation{Generation: lease.Generation()}
	return correlation, lease.Invoke(callback)
}

type managedEBusSlotInvoker struct {
	manager *drivermanager.Manager
	id      string
}

func (invoker managedEBusSlotInvoker) Invoke(ctx context.Context, capability drivermanager.Capability, callback func(transport.RawTransport) error) (drivermanager.Correlation, error) {
	if invoker.manager == nil {
		return drivermanager.Correlation{}, transport.ErrDriverUnavailable
	}
	return invoker.manager.Invoke(ctx, invoker.id, capability, func(provider any) error {
		raw, ok := provider.(transport.RawTransport)
		if !ok || raw == nil {
			return transport.ErrDriverUnavailable
		}
		return callback(raw)
	})
}

type ebusFailureReporter func(drivermanager.Correlation, error)

type ebusStableTransport struct {
	invoker  ebusSlotInvoker
	reporter ebusFailureReporter
}

func newEBusStableTransport(runtime *transport.DriverRuntime, protocol ebusgateway.TransportProtocol, onFailure func(uint64, error)) transport.RawTransport {
	var reporter ebusFailureReporter
	if onFailure != nil {
		reporter = func(correlation drivermanager.Correlation, err error) {
			onFailure(correlation.Generation, err)
		}
	}
	return newEBusStableTransportWithInvoker(directEBusSlotInvoker{runtime: runtime}, protocol, reporter)
}

func newManagedEBusStableTransport(manager *drivermanager.Manager, protocol ebusgateway.TransportProtocol, reporter ebusFailureReporter) transport.RawTransport {
	return newEBusStableTransportWithInvoker(managedEBusSlotInvoker{manager: manager, id: primaryEBusDriverID}, protocol, reporter)
}

func newEBusStableTransportWithInvoker(invoker ebusSlotInvoker, protocol ebusgateway.TransportProtocol, reporter ebusFailureReporter) transport.RawTransport {
	base := &ebusStableTransport{invoker: invoker, reporter: reporter}
	switch ebusgateway.CanonicalTransportProtocol(protocol) {
	case ebusgateway.TransportENH, ebusgateway.TransportENS:
		return &ebusStableEnhancedTransport{ebusStableTransport: base}
	case ebusgateway.TransportAdapterDirect:
		return &ebusStableAdapterDirectTransport{ebusStableTransport: base}
	default:
		return base
	}
}

func (slot *ebusStableTransport) ReadByte() (value byte, err error) {
	err = slot.invoke(drivermanager.CapabilityRead, func(raw transport.RawTransport) error {
		value, err = raw.ReadByte()
		return err
	})
	return value, err
}

func (slot *ebusStableTransport) Write(payload []byte) (written int, err error) {
	err = slot.invoke(drivermanager.CapabilityWrite, func(raw transport.RawTransport) error {
		written, err = raw.Write(payload)
		return err
	})
	return written, err
}

// Close is deliberately a no-op. DriverManager is the sole sender of the
// generation CloseRequest and therefore the sole lifecycle owner.
func (*ebusStableTransport) Close() error { return nil }

func (slot *ebusStableTransport) invoke(capability drivermanager.Capability, callback func(transport.RawTransport) error) error {
	if slot == nil || slot.invoker == nil {
		return transport.ErrDriverUnavailable
	}
	correlation, err := slot.invoker.Invoke(context.Background(), capability, callback)
	if errors.Is(err, drivermanager.ErrUnavailable) {
		err = errors.Join(transport.ErrDriverUnavailable, err)
	}
	if errors.Is(err, transport.ErrStaleDriverGeneration) || (correlation.Generation > 0 && errors.Is(err, transport.ErrDriverUnavailable)) {
		err = errors.Join(ebuserrors.ErrTransportClosed, err)
	}
	if err != nil && slot.reporter != nil && errors.Is(err, ebuserrors.ErrTransportClosed) {
		slot.reporter(correlation, err)
	}
	return err
}

type ebusStableEnhancedTransport struct {
	*ebusStableTransport
}

func (*ebusStableEnhancedTransport) BytesAreUnescaped() bool      { return true }
func (*ebusStableEnhancedTransport) ArbitrationSendsSource() bool { return true }

func (slot *ebusStableEnhancedTransport) ReadByteWithEscape() (value byte, escaped bool, err error) {
	err = slot.invoke(drivermanager.CapabilityRead, func(raw transport.RawTransport) error {
		reader, ok := raw.(transport.EscapeFlaggedReader)
		if !ok {
			return transport.ErrDriverUnavailable
		}
		value, escaped, err = reader.ReadByteWithEscape()
		return err
	})
	return value, escaped, err
}

func (slot *ebusStableEnhancedTransport) ReadEvent() (event transport.StreamEvent, err error) {
	err = slot.invoke(drivermanager.CapabilityRead, func(raw transport.RawTransport) error {
		reader, ok := raw.(transport.StreamEventReader)
		if !ok {
			return transport.ErrDriverUnavailable
		}
		event, err = reader.ReadEvent()
		return err
	})
	return event, err
}

func (slot *ebusStableEnhancedTransport) StartArbitration(initiator byte) error {
	return slot.invoke(drivermanager.CapabilityWrite, func(raw transport.RawTransport) error {
		starter, ok := raw.(interface{ StartArbitration(byte) error })
		if !ok {
			return transport.ErrDriverUnavailable
		}
		return starter.StartArbitration(initiator)
	})
}

func (slot *ebusStableEnhancedTransport) RequestInfo(id transport.AdapterInfoID) (data []byte, err error) {
	err = slot.invoke(drivermanager.CapabilityRead, func(raw transport.RawTransport) error {
		requester, ok := raw.(transport.InfoRequester)
		if !ok {
			return transport.ErrDriverUnavailable
		}
		data, err = requester.RequestInfo(id)
		return err
	})
	return data, err
}

func (slot *ebusStableEnhancedTransport) Reconnect() error {
	return slot.invoke(drivermanager.CapabilityRead, func(raw transport.RawTransport) error {
		reconnectable, ok := raw.(transport.Reconnectable)
		if !ok {
			return transport.ErrDriverUnavailable
		}
		return reconnectable.Reconnect()
	})
}

func (slot *ebusStableEnhancedTransport) SendResponderBytes(payload []byte) (written int, err error) {
	err = slot.invoke(drivermanager.CapabilityWrite, func(raw transport.RawTransport) error {
		responder, ok := raw.(transport.ResponderTransport)
		if !ok {
			return transport.ErrDriverUnavailable
		}
		written, err = responder.SendResponderBytes(payload)
		return err
	})
	return written, err
}

func (slot *ebusStableEnhancedTransport) PostGrantWindowExpiredCount() (count uint64) {
	_ = slot.invoke(drivermanager.CapabilityRead, func(raw transport.RawTransport) error {
		reporter, ok := raw.(transport.PostGrantWindowExpiredReporter)
		if !ok {
			return transport.ErrDriverUnavailable
		}
		count = reporter.PostGrantWindowExpiredCount()
		return nil
	})
	return count
}

type ebusStableAdapterDirectTransport struct {
	*ebusStableTransport
	pendingStructuralSyn atomic.Bool
}

func (*ebusStableAdapterDirectTransport) BytesAreUnescaped() bool      { return true }
func (*ebusStableAdapterDirectTransport) ArbitrationSendsSource() bool { return true }

func (slot *ebusStableAdapterDirectTransport) ReadByteWithEscape() (value byte, escaped bool, err error) {
	err = slot.invoke(drivermanager.CapabilityRead, func(raw transport.RawTransport) error {
		reader, ok := raw.(transport.EscapeFlaggedReader)
		if !ok {
			return transport.ErrDriverUnavailable
		}
		value, escaped, err = reader.ReadByteWithEscape()
		return err
	})
	return value, escaped, err
}

func (slot *ebusStableAdapterDirectTransport) ReadEvent() (event transport.StreamEvent, err error) {
	err = slot.invoke(drivermanager.CapabilityRead, func(raw transport.RawTransport) error {
		reader, ok := raw.(transport.StreamEventReader)
		if !ok {
			return transport.ErrDriverUnavailable
		}
		event, err = reader.ReadEvent()
		return err
	})
	return event, err
}

func (slot *ebusStableAdapterDirectTransport) StartArbitration(initiator byte) error {
	return slot.invoke(drivermanager.CapabilityWrite, func(raw transport.RawTransport) error {
		starter, ok := raw.(interface{ StartArbitration(byte) error })
		if !ok {
			return transport.ErrDriverUnavailable
		}
		return starter.StartArbitration(initiator)
	})
}

func (slot *ebusStableAdapterDirectTransport) RequestInfo(id transport.AdapterInfoID) (data []byte, err error) {
	err = slot.invoke(drivermanager.CapabilityRead, func(raw transport.RawTransport) error {
		requester, ok := raw.(transport.InfoRequester)
		if !ok {
			return transport.ErrDriverUnavailable
		}
		data, err = requester.RequestInfo(id)
		return err
	})
	return data, err
}

// SignalNextWriteIsStructuralSyn is paired with the next Write inside one
// generation admission, so replacement cannot split the signal from its write.
func (slot *ebusStableAdapterDirectTransport) SignalNextWriteIsStructuralSyn() {
	slot.pendingStructuralSyn.Store(true)
}

func (slot *ebusStableAdapterDirectTransport) Write(payload []byte) (written int, err error) {
	structural := slot.pendingStructuralSyn.Swap(false)
	err = slot.invoke(drivermanager.CapabilityWrite, func(raw transport.RawTransport) error {
		if structural {
			signaler, ok := raw.(transport.StructuralWriteSignaler)
			if !ok {
				return transport.ErrDriverUnavailable
			}
			signaler.SignalNextWriteIsStructuralSyn()
		}
		written, err = raw.Write(payload)
		return err
	})
	return written, err
}

type ebusStablePassiveTransport struct {
	invoker ebusSlotInvoker
}

type ebusStableEnhancedPassiveTransport struct {
	*ebusStablePassiveTransport
}

func newEBusStablePassiveTransport(invoker ebusSlotInvoker, protocol ebusgateway.TransportProtocol) transport.RawTransport {
	base := &ebusStablePassiveTransport{invoker: invoker}
	switch ebusgateway.CanonicalTransportProtocol(protocol) {
	case ebusgateway.TransportENH, ebusgateway.TransportENS, ebusgateway.TransportAdapterDirect:
		return &ebusStableEnhancedPassiveTransport{ebusStablePassiveTransport: base}
	default:
		return base
	}
}

func (slot *ebusStablePassiveTransport) withPassive(callback func(transport.RawTransport) error) error {
	if slot == nil || slot.invoker == nil {
		return transport.ErrDriverUnavailable
	}
	_, err := slot.invoker.Invoke(context.Background(), drivermanager.CapabilityRead, func(raw transport.RawTransport) error {
		provider, ok := raw.(interface{ PassiveTransport() transport.RawTransport })
		if !ok {
			return transport.ErrDriverUnavailable
		}
		passive := provider.PassiveTransport()
		if passive == nil {
			return transport.ErrDriverUnavailable
		}
		return callback(passive)
	})
	if errors.Is(err, drivermanager.ErrUnavailable) {
		return errors.Join(transport.ErrDriverUnavailable, err)
	}
	return err
}

func (slot *ebusStablePassiveTransport) ReadByte() (value byte, err error) {
	err = slot.withPassive(func(passive transport.RawTransport) error {
		value, err = passive.ReadByte()
		return err
	})
	return value, err
}

func (slot *ebusStableEnhancedPassiveTransport) ReadEvent() (event transport.StreamEvent, err error) {
	err = slot.withPassive(func(passive transport.RawTransport) error {
		reader, ok := passive.(transport.StreamEventReader)
		if !ok {
			return transport.ErrDriverUnavailable
		}
		event, err = reader.ReadEvent()
		return err
	})
	return event, err
}

func (*ebusStablePassiveTransport) Write([]byte) (int, error) {
	return 0, transport.ErrDriverUnavailable
}

func (*ebusStablePassiveTransport) Close() error { return nil }

func (*ebusStableEnhancedPassiveTransport) BytesAreUnescaped() bool { return true }

type ebusStableClassifier struct {
	invoker ebusSlotInvoker
}

func (classifier *ebusStableClassifier) withClassifier(callback func(activeTxnClassifier)) {
	if classifier == nil || classifier.invoker == nil {
		return
	}
	_, _ = classifier.invoker.Invoke(context.Background(), drivermanager.CapabilityRead, func(raw transport.RawTransport) error {
		provider, ok := raw.(interface{ Classifier() activeTxnClassifier })
		if !ok {
			return nil
		}
		current := provider.Classifier()
		if current != nil {
			callback(current)
		}
		return nil
	})
}

func (classifier *ebusStableClassifier) LastTxnClass() (class string) {
	classifier.withClassifier(func(current activeTxnClassifier) { class = current.LastTxnClass() })
	return class
}

func (classifier *ebusStableClassifier) ActiveTxnSnapshotForScan() (id uint64, writePrefix, readPrefix []byte, class string) {
	classifier.withClassifier(func(current activeTxnClassifier) {
		snapshotter, ok := current.(activeTxnSnapshotter)
		if ok {
			id, writePrefix, readPrefix, class = snapshotter.ActiveTxnSnapshotForScan()
		}
	})
	return id, writePrefix, readPrefix, class
}

func (classifier *ebusStableClassifier) ActiveTxnSnapshot() (snapshot adaptermux.ActiveTxnSnapshot) {
	classifier.withClassifier(func(current activeTxnClassifier) {
		snapshotter, ok := current.(adaptermuxDiagSnapshotter)
		if ok {
			snapshot = snapshotter.ActiveTxnSnapshot()
		}
	})
	return snapshot
}

func (classifier *ebusStableClassifier) V8Classifier() (result *v8classifier.Classifier) {
	classifier.withClassifier(func(current activeTxnClassifier) {
		mux, ok := current.(*adaptermux.Mux)
		if ok {
			result = mux.V8Classifier()
		}
	})
	v8AdminEventsCurrentClassifier.Store(result)
	return result
}

type managedEBusReadResult struct {
	event transport.StreamEvent
	err   error
}

type managedEBusEndpoint struct {
	ctx      context.Context
	raw      transport.RawTransport
	demand   chan struct{}
	results  chan managedEBusReadResult
	pumpDone chan struct{}
	closing  *atomic.Bool
}

// managedEBusGeneration keeps the blocking raw reader adapter-owned. Stable
// provider callbacks block only on results and return as soon as generation
// context is canceled, allowing DriverRuntime to drain before CloseRequest.
type managedEBusGeneration struct {
	active     *managedEBusEndpoint
	passive    *managedEBusEndpoint
	classifier activeTxnClassifier
	closeFn    func() error

	closeRequest chan struct{}
	closed       chan struct{}
	closing      atomic.Bool
	closeOnce    sync.Once
}

func newManagedEBusGeneration(ctx context.Context, raw transport.RawTransport, closeFn func() error) *transport.ManagedRawTransport {
	return newManagedEBusGenerationParts(ctx, raw, nil, nil, closeFn)
}

func newManagedEBusGenerationParts(ctx context.Context, active, passive transport.RawTransport, classifier activeTxnClassifier, closeFn func() error) *transport.ManagedRawTransport {
	if ctx == nil {
		ctx = context.Background()
	}
	generation := &managedEBusGeneration{
		classifier:   classifier,
		closeFn:      closeFn,
		closeRequest: make(chan struct{}),
		closed:       make(chan struct{}),
	}
	generation.active = newManagedEBusEndpoint(ctx, active, &generation.closing)
	if passive != nil {
		generation.passive = newManagedEBusEndpoint(ctx, passive, &generation.closing)
	}
	go generation.closeWorker()
	return &transport.ManagedRawTransport{
		Transport: generation,
		Lifecycle: transport.DriverLifecycleHandle{
			CloseRequest: generation.closeRequest,
			Closed:       generation.closed,
		},
	}
}

func newManagedEBusEndpoint(ctx context.Context, raw transport.RawTransport, closing *atomic.Bool) *managedEBusEndpoint {
	endpoint := &managedEBusEndpoint{
		ctx:      ctx,
		raw:      raw,
		demand:   make(chan struct{}),
		results:  make(chan managedEBusReadResult),
		pumpDone: make(chan struct{}),
		closing:  closing,
	}
	go endpoint.pump()
	return endpoint
}

func (endpoint *managedEBusEndpoint) pump() {
	defer close(endpoint.pumpDone)
	if endpoint.raw == nil {
		return
	}
	for {
		// Never prefetch from a live transport. ENH/ENS arbitration and INFO
		// exchanges consume control responses under their own read lock; an idle
		// pump must not race them and steal those bytes. The protocol-facing
		// ReadByte/ReadEvent call creates exactly one unit of demand instead.
		select {
		case <-endpoint.ctx.Done():
			return
		case <-endpoint.demand:
		}
		if endpoint.ctx.Err() != nil {
			return
		}
		event, err := readManagedEBusEvent(endpoint.raw)
		select {
		case endpoint.results <- managedEBusReadResult{event: event, err: err}:
		case <-endpoint.ctx.Done():
			return
		}
		if err != nil && endpoint.closing.Load() {
			return
		}
	}
}

func (generation *managedEBusGeneration) closeWorker() {
	<-generation.closeRequest
	generation.closing.Store(true)
	generation.closeOnce.Do(func() {
		if generation.closeFn != nil {
			_ = generation.closeFn()
		} else if generation.active != nil && generation.active.raw != nil {
			_ = generation.active.raw.Close()
		}
	})
	if generation.active != nil {
		<-generation.active.pumpDone
	}
	if generation.passive != nil {
		<-generation.passive.pumpDone
	}
	close(generation.closed)
}

func readManagedEBusEvent(raw transport.RawTransport) (transport.StreamEvent, error) {
	if reader, ok := raw.(transport.StreamEventReader); ok {
		return reader.ReadEvent()
	}
	value, err := raw.ReadByte()
	return transport.StreamEvent{Kind: transport.StreamEventByte, Byte: value}, err
}

func (endpoint *managedEBusEndpoint) nextByte() (byte, bool, error) {
	for {
		if err := endpoint.requestRead(); err != nil {
			return 0, false, err
		}
		select {
		case <-endpoint.ctx.Done():
			return 0, false, ebuserrors.ErrTransportClosed
		case result := <-endpoint.results:
			if result.err != nil {
				return 0, false, result.err
			}
			switch result.event.Kind {
			case transport.StreamEventByte:
				return result.event.Byte, result.event.WasEscaped, nil
			case transport.StreamEventReset:
				return 0, false, ebuserrors.ErrAdapterReset
			}
		}
	}
}

func (generation *managedEBusGeneration) ReadByte() (byte, error) {
	value, _, err := generation.active.nextByte()
	return value, err
}

func (generation *managedEBusGeneration) ReadByteWithEscape() (byte, bool, error) {
	return generation.active.nextByte()
}

func (generation *managedEBusGeneration) ReadEvent() (transport.StreamEvent, error) {
	return generation.active.readEvent()
}

func (endpoint *managedEBusEndpoint) readEvent() (transport.StreamEvent, error) {
	if err := endpoint.requestRead(); err != nil {
		return transport.StreamEvent{}, err
	}
	select {
	case <-endpoint.ctx.Done():
		return transport.StreamEvent{}, ebuserrors.ErrTransportClosed
	case result := <-endpoint.results:
		return result.event, result.err
	}
}

func (endpoint *managedEBusEndpoint) requestRead() error {
	if endpoint == nil || endpoint.raw == nil {
		return transport.ErrDriverUnavailable
	}
	select {
	case <-endpoint.ctx.Done():
		return ebuserrors.ErrTransportClosed
	case endpoint.demand <- struct{}{}:
		return nil
	}
}

func (generation *managedEBusGeneration) Write(payload []byte) (int, error) {
	if generation.active == nil || generation.active.raw == nil {
		return 0, transport.ErrDriverUnavailable
	}
	return generation.active.raw.Write(payload)
}

// Close never owns teardown; DriverRuntime's CloseRequest does.
func (*managedEBusGeneration) Close() error { return nil }

func (generation *managedEBusGeneration) BytesAreUnescaped() bool {
	escapeAware, ok := generation.active.raw.(transport.EscapeAware)
	return ok && escapeAware.BytesAreUnescaped()
}

func (generation *managedEBusGeneration) StartArbitration(initiator byte) error {
	starter, ok := generation.active.raw.(interface{ StartArbitration(byte) error })
	if !ok {
		return transport.ErrDriverUnavailable
	}
	return starter.StartArbitration(initiator)
}

func (generation *managedEBusGeneration) ArbitrationSendsSource() bool {
	behavior, ok := generation.active.raw.(interface{ ArbitrationSendsSource() bool })
	return ok && behavior.ArbitrationSendsSource()
}

func (generation *managedEBusGeneration) RequestInfo(id transport.AdapterInfoID) ([]byte, error) {
	requester, ok := generation.active.raw.(transport.InfoRequester)
	if !ok {
		return nil, transport.ErrDriverUnavailable
	}
	return requester.RequestInfo(id)
}

func (generation *managedEBusGeneration) Reconnect() error {
	reconnectable, ok := generation.active.raw.(transport.Reconnectable)
	if !ok {
		return transport.ErrDriverUnavailable
	}
	return reconnectable.Reconnect()
}

func (generation *managedEBusGeneration) SendResponderBytes(payload []byte) (int, error) {
	responder, ok := generation.active.raw.(transport.ResponderTransport)
	if !ok {
		return 0, transport.ErrDriverUnavailable
	}
	return responder.SendResponderBytes(payload)
}

func (generation *managedEBusGeneration) PostGrantWindowExpiredCount() uint64 {
	reporter, ok := generation.active.raw.(transport.PostGrantWindowExpiredReporter)
	if !ok {
		return 0
	}
	return reporter.PostGrantWindowExpiredCount()
}

func (generation *managedEBusGeneration) SignalNextWriteIsStructuralSyn() {
	if signaler, ok := generation.active.raw.(transport.StructuralWriteSignaler); ok {
		signaler.SignalNextWriteIsStructuralSyn()
	}
}

func (generation *managedEBusGeneration) PassiveTransport() transport.RawTransport {
	if generation == nil || generation.passive == nil {
		return nil
	}
	return &managedEBusPassiveTransport{endpoint: generation.passive}
}

func (generation *managedEBusGeneration) Classifier() activeTxnClassifier {
	if generation == nil {
		return nil
	}
	return generation.classifier
}

type managedEBusPassiveTransport struct {
	endpoint *managedEBusEndpoint
}

func (passive *managedEBusPassiveTransport) ReadByte() (byte, error) {
	value, _, err := passive.endpoint.nextByte()
	return value, err
}

func (passive *managedEBusPassiveTransport) ReadEvent() (transport.StreamEvent, error) {
	return passive.endpoint.readEvent()
}

func (*managedEBusPassiveTransport) Write([]byte) (int, error) {
	return 0, transport.ErrDriverUnavailable
}

func (*managedEBusPassiveTransport) Close() error            { return nil }
func (*managedEBusPassiveTransport) BytesAreUnescaped() bool { return true }
