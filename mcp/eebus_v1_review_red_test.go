package mcp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
	"github.com/Project-Helianthus/helianthus-eebusreg/eebusevidence"
	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
)

const (
	msp06ReviewMaxLiveWorkers      = 8
	msp06ReviewOversizedCollection = 1025
)

func TestMSP06EvidenceSizeOrderingUsesSerializedScalarBytes(t *testing.T) {
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	data := msp06AssertSuccess(
		t,
		msp06Call(t, server.Handler(), msp06ServicesListTool, map[string]any{}),
		msp06ServicesListTool,
		"services",
		"live",
	)

	var got []string
	for index, rawService := range msp06Slice(t, data["services"], "services") {
		service := msp06Map(t, rawService, fmt.Sprintf("services[%d]", index))
		rawEvidence, exists := service["evidence"]
		if !exists {
			continue
		}
		evidence := msp06Slice(t, rawEvidence, fmt.Sprintf("services[%d].evidence", index))
		if len(evidence) != 2 {
			continue
		}
		for evidenceIndex, raw := range evidence {
			item := msp06Map(t, raw, fmt.Sprintf("services[%d].evidence[%d]", index, evidenceIndex))
			size, ok := item["size"].(json.Number)
			if !ok {
				t.Fatalf("evidence size type = %T, want JSON number", item["size"])
			}
			got = append(got, size.String())
		}
		break
	}
	if want := []string{"10", "2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("evidence sizes = %v, want %v from bytewise serialized-scalar ordering", got, want)
	}
}

func msp06ReviewProjection(t *testing.T) eebusV1Projection {
	t.Helper()
	projection, err := eebusV1ProjectSnapshot(
		msp06Snapshot(t, "runtime-a"),
		bytes.Repeat([]byte{0x6b}, sha256.Size),
	)
	if err != nil {
		t.Fatalf("project review fixture: %v", err)
	}
	return projection
}

func msp06ReviewStoreWithRoot(t *testing.T, now time.Time) (*eebusV1SnapshotStore, eebusV1CapturedRootV1) {
	t.Helper()
	store := newEEBusV1SnapshotStore(func() time.Time { return now }, &msp06EntropyReader{})
	captured, code := store.capture(msp06ReviewProjection(t))
	if code != "" {
		t.Fatalf("capture review fixture code = %q", code)
	}
	return store, captured
}

func TestMSP06StoreSamplesOperationTimeOnlyWhileHoldingMutex(t *testing.T) {
	base := time.Now()
	for _, operation := range []string{"capture", "lookup", "drop"} {
		t.Run(operation, func(t *testing.T) {
			store, captured := msp06ReviewStoreWithRoot(t, base)
			projection := msp06ReviewProjection(t)
			entered := make(chan struct{})
			release := make(chan struct{})
			var enteredOnce sync.Once
			store.now = func() time.Time {
				enteredOnce.Do(func() { close(entered) })
				<-release
				return base.Add(time.Minute)
			}

			done := make(chan struct{})
			go func() {
				switch operation {
				case "capture":
					store.capture(projection)
				case "lookup":
					store.lookup(captured.EvidenceRefs.ServicesListRef, eebusV1ServicesListTool, "services")
				case "drop":
					store.drop(captured.SnapshotRef)
				}
				close(done)
			}()

			<-entered
			sampledBeforeLock := store.mu.TryLock()
			if sampledBeforeLock {
				store.mu.Unlock()
			}
			close(release)
			<-done
			if sampledBeforeLock {
				t.Fatalf("%s sampled time before acquiring the store mutex", operation)
			}
		})
	}
}

func TestMSP06StoreExpiryAndDropUseMutexLinearizationTime(t *testing.T) {
	base := time.Now()
	beforeExpiry := base.Add(eebusV1ActiveTTL - time.Nanosecond)
	atExpiry := base.Add(eebusV1ActiveTTL)

	t.Run("lookup", func(t *testing.T) {
		store, captured := msp06ReviewStoreWithRoot(t, base)
		store.now = func() time.Time {
			if store.mu.TryLock() {
				store.mu.Unlock()
				return beforeExpiry
			}
			return atExpiry
		}
		result := store.lookup(captured.EvidenceRefs.ServicesListRef, eebusV1ServicesListTool, "services")
		if result.ErrorCode != "snapshot_gone" {
			t.Fatalf("lookup at mutex linearization boundary code = %q, want snapshot_gone", result.ErrorCode)
		}
	})

	t.Run("drop", func(t *testing.T) {
		store, captured := msp06ReviewStoreWithRoot(t, base)
		store.now = func() time.Time {
			if store.mu.TryLock() {
				store.mu.Unlock()
				return beforeExpiry
			}
			return atExpiry
		}
		if result := store.drop(captured.SnapshotRef); result.Status != "already_gone" {
			t.Fatalf("drop at mutex linearization boundary status = %q, want already_gone", result.Status)
		}
	})
}

func TestMSP06StoreRetainsMonotonicTimeInternallyAndUsesUTCOnlyOnWire(t *testing.T) {
	base := time.Now()
	if base == base.Round(0) { //nolint:staticcheck // Struct equality intentionally detects monotonic clock metadata.
		t.Skip("host clock did not provide a monotonic reading")
	}
	store, captured := msp06ReviewStoreWithRoot(t, base)
	root := store.activeRoots[captured.SnapshotRef]
	if root == nil {
		t.Fatal("captured root missing from active store")
	}
	if root.ExpiresAt == root.ExpiresAt.Round(0) { //nolint:staticcheck // Struct equality intentionally detects monotonic clock metadata.
		t.Error("active expiry discarded the monotonic clock reading")
	}
	if want := eebusV1Timestamp(base.Add(eebusV1ActiveTTL)); captured.ExpiresAt != want {
		t.Errorf("wire expiry = %q, want UTC %q", captured.ExpiresAt, want)
	}

	terminalAt := base.Add(time.Minute)
	store.now = func() time.Time { return terminalAt }
	if result := store.drop(captured.SnapshotRef); result.Status != "dropped" {
		t.Fatalf("drop status = %q, want dropped", result.Status)
	}
	tombstone := store.tombstoneRoots[captured.SnapshotRef]
	if tombstone == nil {
		t.Fatal("dropped root missing tombstone")
	}
	if tombstone.TerminalAt == tombstone.TerminalAt.Round(0) { //nolint:staticcheck // Struct equality intentionally detects monotonic clock metadata.
		t.Error("terminal transition discarded the monotonic clock reading")
	}
}

func TestMSP06ProviderInterfaceIsCompileTimeTypedThroughRuntime(t *testing.T) {
	providerType := reflect.TypeOf((*EEBusV1Provider)(nil)).Elem()
	method, ok := providerType.MethodByName("Snapshot")
	if !ok {
		t.Fatal("EEBusV1Provider has no compile-time Snapshot method")
	}
	errorType := reflect.TypeOf((*error)(nil)).Elem()
	snapshotType := reflect.TypeOf(eebusruntime.SnapshotV1{})
	if method.Type.NumIn() != 0 || method.Type.NumOut() != 2 || method.Type.Out(0) != snapshotType || method.Type.Out(1) != errorType {
		t.Fatalf("EEBusV1Provider.Snapshot type = %s, want func() (eebusruntime.SnapshotV1, error)", method.Type)
	}
	runtimeField, ok := reflect.TypeOf(eebusV1Runtime{}).FieldByName("provider")
	if !ok || runtimeField.Type != providerType {
		t.Fatalf("eebusV1Runtime.provider type = %v, want EEBusV1Provider", runtimeField.Type)
	}
}

func TestMSP06ProviderInvocationDoesNotUseReflectiveMethodDispatch(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "eebus_v1.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if selector.Sel.Name == "MethodByName" {
			t.Errorf("provider method is discovered reflectively at %s", fset.Position(selector.Pos()))
		}
		if receiver, ok := selector.X.(*ast.Ident); ok && receiver.Name == "method" && selector.Sel.Name == "Call" {
			t.Errorf("provider method is invoked reflectively at %s", fset.Position(selector.Pos()))
		}
		return true
	})
}

type msp06ReviewBlockingProvider struct {
	snapshot eebusruntime.SnapshotV1
	release  <-chan struct{}
	exited   chan<- struct{}
	calls    atomic.Int64
}

var _ EEBusV1Provider = (*msp06ReviewBlockingProvider)(nil)

func (provider *msp06ReviewBlockingProvider) Snapshot() (eebusruntime.SnapshotV1, error) {
	provider.calls.Add(1)
	<-provider.release
	provider.exited <- struct{}{}
	return provider.snapshot, nil
}

func msp06ReviewCaptureWave(t *testing.T, server *Server, attempts int) {
	t.Helper()
	start := make(chan struct{})
	outcomes := make(chan struct {
		code string
		err  error
	}, attempts)
	for index := 0; index < attempts; index++ {
		go func() {
			<-start
			outcomes <- msp06ConcurrentCapture(server.Handler())
		}()
	}
	close(start)
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for index := 0; index < attempts; index++ {
		select {
		case outcome := <-outcomes:
			if outcome.err != nil {
				t.Fatalf("blocked capture %d: %v", index, outcome.err)
			}
			if outcome.code != "timeout" {
				t.Fatalf("blocked capture %d code = %q, want timeout", index, outcome.code)
			}
		case <-deadline.C:
			t.Fatalf("blocked capture wave did not finish after %d outcomes", index)
		}
	}
}

func TestMSP06BlockedProviderWorkersAreBoundedAndPermitsRemainUntilExit(t *testing.T) {
	const attempts = 32
	release := make(chan struct{})
	exited := make(chan struct{}, attempts*2+1)
	provider := &msp06ReviewBlockingProvider{
		snapshot: msp06Snapshot(t, "runtime-a"),
		release:  release,
		exited:   exited,
	}
	var releaseOnce sync.Once
	releaseProvider := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseProvider()

	server, _ := msp06TestServer(t, provider)
	server.eebusV1.liveTimeout = 250 * time.Millisecond
	msp06ReviewCaptureWave(t, server, attempts)
	firstWaveCalls := provider.calls.Load()
	msp06ReviewCaptureWave(t, server, attempts)
	secondWaveCalls := provider.calls.Load()

	releaseProvider()
	for index := int64(0); index < secondWaveCalls; index++ {
		select {
		case <-exited:
		case <-time.After(5 * time.Second):
			t.Fatalf("blocked provider worker %d/%d did not exit", index+1, secondWaveCalls)
		}
	}
	recovered := msp06ConcurrentCapture(server.Handler())
	if recovered.err != nil || recovered.code != "" {
		t.Fatalf("capture after worker exit = code %q error %v, want success", recovered.code, recovered.err)
	}

	if firstWaveCalls == 0 || firstWaveCalls > msp06ReviewMaxLiveWorkers {
		t.Errorf("first blocked wave started %d provider workers, want 1..%d", firstWaveCalls, msp06ReviewMaxLiveWorkers)
	}
	if secondWaveCalls != firstWaveCalls {
		t.Errorf("provider calls grew from %d to %d while timed-out workers were still blocked", firstWaveCalls, secondWaveCalls)
	}
}

type msp06ReviewBlockingNow struct {
	value       time.Time
	entered     chan struct{}
	release     <-chan struct{}
	enteredOnce sync.Once
}

func (clock *msp06ReviewBlockingNow) Now() time.Time {
	clock.enteredOnce.Do(func() { close(clock.entered) })
	<-clock.release
	return clock.value
}

func TestMSP06LiveDeadlineCoversPostProviderProjectionAndCaptureWork(t *testing.T) {
	server, _ := msp06TestServer(t, &msp06Provider{snapshot: msp06Snapshot(t, "runtime-a")})
	runtime := server.eebusV1
	runtime.liveTimeout = time.Hour
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWork := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseWork()
	clock := &msp06ReviewBlockingNow{
		value:   time.Now(),
		entered: make(chan struct{}),
		release: release,
	}
	runtime.store.now = clock.Now

	spec, ok := eebusV1Spec(eebusV1SnapshotCaptureTool)
	if !ok {
		t.Fatal("snapshot capture spec missing")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan eebusV1EnvelopeV1, 1)
	go func() {
		done <- runtime.call(ctx, spec, map[string]any{})
	}()

	select {
	case <-clock.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("live call never reached post-provider capture work")
	}
	cancel()

	var envelope eebusV1EnvelopeV1
	returnedBeforeRelease := false
	select {
	case envelope = <-done:
		returnedBeforeRelease = true
	case <-time.After(500 * time.Millisecond):
	}
	releaseWork()
	if !returnedBeforeRelease {
		envelope = <-done
		t.Fatalf("live call ignored cancellation after provider/projection completion; eventual error = %#v", envelope.Error)
	}
	if envelope.Error == nil || envelope.Error.Code != "timeout" {
		t.Fatalf("cancelled post-provider work error = %#v, want timeout", envelope.Error)
	}
}

func TestMSP06OversizedProviderCollectionsFailClosed(t *testing.T) {
	tests := []struct {
		name   string
		tool   string
		scope  string
		mutate func(*testing.T, *eebusruntime.SnapshotV1)
	}{
		{
			name:  "services",
			tool:  msp06ServicesListTool,
			scope: "services",
			mutate: func(t *testing.T, snapshot *eebusruntime.SnapshotV1) {
				snapshot.Services = make([]eebusruntime.ServiceV1, msp06ReviewOversizedCollection)
				for index := range snapshot.Services {
					snapshot.Services[index] = eebusruntime.ServiceV1{
						ID:      msp06SourceID(t, eebusraw.IDKindPeer, fmt.Sprintf("oversized-service-%04d", index)),
						Kind:    eebusruntime.ServiceKindV1Remote,
						Visible: true,
					}
				}
			},
		},
		{
			name:  "nested features",
			tool:  msp06TopologyGetTool,
			scope: "topology",
			mutate: func(t *testing.T, snapshot *eebusruntime.SnapshotV1) {
				features := make([]eebusruntime.FeatureV1, msp06ReviewOversizedCollection)
				for index := range features {
					features[index] = eebusruntime.FeatureV1{
						ID:   msp06SourceID(t, eebusraw.IDKindPeer, fmt.Sprintf("oversized-feature-%04d", index)),
						Role: eebusruntime.FeatureRoleV1Client,
					}
				}
				snapshot.Topology.Devices = []eebusruntime.DeviceV1{{
					ID: msp06SourceID(t, eebusraw.IDKindPeer, "oversized-device"),
					Entities: []eebusruntime.EntityV1{{
						ID:       msp06SourceID(t, eebusraw.IDKindPeer, "oversized-entity"),
						Features: features,
					}},
				}}
			},
		},
		{
			name:  "raw evidence",
			tool:  msp06ServicesListTool,
			scope: "services",
			mutate: func(t *testing.T, snapshot *eebusruntime.SnapshotV1) {
				evidence := make([]eebusevidence.ObjectV1, msp06ReviewOversizedCollection)
				for index := range evidence {
					evidence[index] = msp06Evidence(
						eebusevidence.ObjectKindService,
						fmt.Sprintf("oversized-evidence-%04d", index),
						index,
						snapshot.Meta.DataTimestamp,
					)
				}
				snapshot.Services[0].Raw = evidence
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := msp06Snapshot(t, "runtime-a")
			test.mutate(t, &snapshot)
			if err := snapshot.Validate(); err != nil {
				t.Fatalf("oversized provider fixture is otherwise invalid: %v", err)
			}
			server, _ := msp06TestServer(t, &msp06Provider{snapshot: snapshot})
			result := msp06Call(t, server.Handler(), test.tool, map[string]any{})
			rawError := result.envelope["error"]
			if rawError == nil {
				t.Fatalf("oversized %s provider collection succeeded, want contract_violation", test.name)
			}
			errorObject := msp06Map(t, rawError, "error")
			if errorObject["code"] != "contract_violation" {
				t.Fatalf("oversized %s provider collection code = %#v, want contract_violation", test.name, errorObject["code"])
			}
		})
	}
}
