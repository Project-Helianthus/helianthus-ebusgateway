package modbusadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

type withdrawalFailureJournal struct {
	*semhub.MemoryJournal
	failSource     semreg.SourceID
	failActivation semreg.SourceID
}

var errWithdrawalJournal = errors.New("synthetic withdrawal journal failure")

func (j *withdrawalFailureJournal) Append(records ...semhub.EpochRecord) error {
	for _, record := range records {
		if (record.Kind == semhub.EventGenerationFenced && record.Source == j.failSource) ||
			(record.Kind == semhub.EventGenerationActivated && record.Source == j.failActivation) {
			return errWithdrawalJournal
		}
	}
	return j.MemoryJournal.Append(records...)
}

func startManagedPublicationTestAdapter(t *testing.T, class *semhub.Class, scope *PublicationScope) *Adapter {
	t.Helper()
	config := validConfig()
	config.SemanticPV, config.Publication = class, scope
	client, peer := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = peer.Close() })
	adapter, err := Start(context.Background(), config,
		func(context.Context, string, string) (net.Conn, error) { return client, nil },
		func(modbus.TCPEndpointConfig) (Endpoint, error) { return &fakeEndpoint{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	// Semantic withdrawal is explicit at each tested replacement. Teardown
	// closes only fixture resources, without introducing another generation fence.
	t.Cleanup(func() {
		if err := adapter.endpoint.Close(); err != nil {
			t.Error(err)
		}
	})
	return adapter
}

func TestManagedWithdrawalRecoversAfterPartialSourceFencing(t *testing.T) {
	for _, failure := range []string{"journal", "control"} {
		t.Run(failure, func(t *testing.T) {
			journal := &withdrawalFailureJournal{MemoryJournal: semhub.NewMemoryJournal()}
			var failAsset semreg.AssetID
			hub, err := semhub.New(semhub.Options{Journal: journal, Faults: func(stage string, asset semreg.AssetID) {
				if stage == "before-swap" && asset != "" && asset == failAsset {
					panic("synthetic withdrawal interruption")
				}
			}})
			if err != nil {
				t.Fatal(err)
			}
			class, err := NewSemanticPVClass(hub)
			if err != nil {
				t.Fatal(err)
			}
			scope := &PublicationScope{Epoch: "source-epoch:pv:partial-withdrawal", Generation: 1}
			adapter := startManagedPublicationTestAdapter(t, class, scope)
			words := observedFroniusFloatWords()
			for i, nativeScope := range []string{"fixture:withdrawal-a", "fixture:withdrawal-b"} {
				rotated := append([]uint16(nil), words...)
				if i != 0 {
					putSunSpecString(rotated[52:68], "fixture-identity-b")
				}
				observation := pvCoreScopedObservation(t, rotated, uint64(i+1), uint64(i+2), nativeScope)
				if err := adapter.RecordSunSpecQualificationObservation(observation); err != nil {
					t.Fatal(err)
				}
			}
			sources := adapter.semanticPV.sortedHandleSources()
			if len(sources) != 2 {
				t.Fatalf("sources=%d", len(sources))
			}
			if failure == "journal" {
				journal.failSource = sources[1]
			} else {
				for id, asset := range adapter.semanticPV.assets {
					if asset.current.snapshot.Sources[0].SourceID == sources[1] {
						failAsset = id
					}
				}
				if failAsset == "" {
					t.Fatal("failure target unavailable")
				}
			}
			if err := adapter.Close(); err == nil {
				t.Fatal("partial withdrawal failure hidden")
			}
			if adapter.semanticPV.managedWithdrawn {
				t.Fatal("partial cleanup marked complete")
			}
			journal.failSource, failAsset = "", ""
			if err := adapter.Close(); err != nil {
				t.Fatalf("withdrawal retry after recovery: %v", err)
			}
			counts := make(map[semreg.SourceID]int)
			for _, record := range journal.Records() {
				if record.Kind == semhub.EventGenerationFenced {
					counts[record.Source]++
				}
			}
			for _, source := range sources {
				if counts[source] != 1 {
					t.Fatalf("source %s fenced %d times", source, counts[source])
				}
			}
			successor := startManagedPublicationTestAdapter(t, class, &PublicationScope{Previous: scope, Epoch: scope.Epoch, Generation: 2})
			if len(successor.semanticPV.handles) != 2 {
				t.Fatal("replacement did not activate both sources")
			}
		})
	}
}

func TestManagedReplacementRetainsRefreshEvidenceAcrossIdentities(t *testing.T) {
	class := testSemanticPVClass()
	scope := &PublicationScope{Epoch: "source-epoch:pv:retained-identities", Generation: 1}
	adapter := startManagedPublicationTestAdapter(t, class, scope)
	words := observedFroniusFloatWords()
	observations := make([]modbusreg.SunSpecQualificationObservation, 0, 2)
	assets := make([]semreg.AssetID, 0, 2)
	for i := 0; i < 2; i++ {
		if i != 0 {
			putSunSpecString(words[52:68], "fixture-identity-b")
		}
		observation := pvCoreObservation(t, words, uint64(i+1), uint64(i+2))
		if err := adapter.RecordSunSpecCurrentObservation(observation); err != nil {
			t.Fatal(err)
		}
		observations = append(observations, observation)
		assets = append(assets, adapter.semanticPV.activeAssetID)
	}
	for generation := uint64(2); generation <= 3; generation++ {
		if err := adapter.Close(); err != nil {
			t.Fatal(err)
		}
		scope = &PublicationScope{Previous: scope, Epoch: scope.Epoch, Generation: generation}
		adapter = startManagedPublicationTestAdapter(t, class, scope)
		if err := adapter.RecordSunSpecCurrentObservation(pvCoreObservation(t, words, generation+10, generation+20)); err != nil {
			t.Fatal(err)
		}
		for i, observation := range observations {
			inspected, err := class.Hub().Inspect(assets[i])
			if err != nil || !inspected.Present {
				t.Fatalf("retained hub snapshot missing: %v", err)
			}
			if _, _, ok := adapter.SunSpecQualificationObservation(observation.Capability().ProfileID(), observation.SampleID()); !ok {
				t.Fatalf("generation %d pruned evidence for retained identity %d", generation, i)
			}
		}
	}
}

func TestManagedPartialActivationRecoversSameEpoch(t *testing.T) {
	journal := &withdrawalFailureJournal{MemoryJournal: semhub.NewMemoryJournal()}
	hub, err := semhub.New(semhub.Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	class, err := NewSemanticPVClass(hub)
	if err != nil {
		t.Fatal(err)
	}
	scope := &PublicationScope{Epoch: "source-epoch:pv:partial-activation", Generation: 1}
	adapter := startManagedPublicationTestAdapter(t, class, scope)
	words := observedFroniusFloatWords()
	for i, nativeScope := range []string{"fixture:activation-a", "fixture:activation-b"} {
		rotated := append([]uint16(nil), words...)
		if i != 0 {
			putSunSpecString(rotated[52:68], "fixture-identity-b")
		}
		if err := adapter.RecordSunSpecQualificationObservation(pvCoreScopedObservation(t, rotated, uint64(i+1), uint64(i+2), nativeScope)); err != nil {
			t.Fatal(err)
		}
	}
	sources := adapter.semanticPV.sortedHandleSources()
	if len(sources) != 2 {
		t.Fatalf("sources=%d", len(sources))
	}
	if err := adapter.Close(); err != nil {
		t.Fatal(err)
	}
	journal.failActivation = sources[1]
	config := validConfig()
	config.SemanticPV = class
	config.Publication = &PublicationScope{Previous: scope, Epoch: scope.Epoch, Generation: 2}
	client, peer := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = peer.Close() }()
	failed, err := Start(context.Background(), config,
		func(context.Context, string, string) (net.Conn, error) { return client, nil },
		func(modbus.TCPEndpointConfig) (Endpoint, error) { return &fakeEndpoint{}, nil })
	if failed != nil || !errors.Is(err, errWithdrawalJournal) {
		t.Fatalf("partial activation result=%v err=%v", failed, err)
	}
	activated, fenced := false, false
	for _, record := range journal.Records() {
		if record.Source == sources[0] && record.Generation == 2 {
			activated = activated || record.Kind == semhub.EventGenerationActivated
			fenced = fenced || record.Kind == semhub.EventGenerationFenced
		}
	}
	if !activated || !fenced {
		t.Fatal("partial activation did not activate and clean up the first source")
	}
	journal.failActivation = ""
	successor := startManagedPublicationTestAdapter(t, class, &PublicationScope{Previous: scope, Epoch: scope.Epoch, Generation: 3})
	for _, source := range sources {
		handle := successor.semanticPV.handles[source]
		if handle == nil || handle.Generation() != 3 {
			t.Fatal("retry did not recover every source in the same epoch")
		}
	}
	if err := successor.RecordSunSpecQualificationObservation(pvCoreScopedObservation(t, words, 10, 11, "fixture:activation-a")); err != nil {
		t.Fatal(err)
	}
	if _, ok := successor.SemanticPVCurrentSingle(); !ok {
		t.Fatal("recovered generation did not publish")
	}
}

func TestManagedInheritedReplayCoverDoesNotRepublish(t *testing.T) {
	var (
		mu     sync.Mutex
		target semreg.AssetID
	)
	hub, err := semhub.New(semhub.Options{Faults: func(stage string, asset semreg.AssetID) {
		mu.Lock()
		hit := stage == "before-swap" && asset != "" && asset == target
		if hit {
			target = ""
		}
		mu.Unlock()
		if hit {
			panic("injected cover interruption")
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	class, err := NewSemanticPVClass(hub)
	if err != nil {
		t.Fatal(err)
	}
	core, err := newPVPublicationCore(class)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	current := started
	adapter := &Adapter{semanticPV: core, pvSourceEpoch: "source-epoch:pv:cover-a", startedWall: started.UTC(), startedMono: started,
		wallNow: func() time.Time { return current }, monotonicNow: func() time.Time { return current },
		qualifications: make(map[string]sunSpecQualificationRecord), profiles: make(map[string]ProfileObservationRecord), refreshEvidence: make(map[string]sunSpecQualificationRecord)}
	firstWords := observedFroniusFloatControlsWords()
	first := pvCoreObservation(t, firstWords, 1, 2)
	if err := adapter.RecordSunSpecQualificationObservation(first); err != nil {
		t.Fatal(err)
	}
	firstIdentity, _, err := resolvePVPublicationIdentity(first)
	if err != nil {
		t.Fatal(err)
	}
	firstAsset := semreg.AssetID("pv-asset-" + pvCoreRawHash(firstIdentity)[:32])
	current = current.Add(time.Second)
	rotatedWords := append([]uint16(nil), firstWords...)
	putSunSpecString(rotatedWords[52:68], "rotated-identity")
	if err := adapter.RecordSunSpecQualificationObservation(pvCoreObservation(t, rotatedWords, 3, 4)); err != nil {
		t.Fatal(err)
	}
	// A new epoch on the rotated asset retires the old epoch on the first
	// asset through a cover, whose submission is interrupted.
	current = current.Add(time.Second)
	adapter.startedWall = current.UTC()
	adapter.pvSourceEpoch = "source-epoch:pv:cover-b"
	mu.Lock()
	target = firstAsset
	mu.Unlock()
	observation := pvCoreObservation(t, rotatedWords, 5, 6)
	key := observation.Capability().ProfileID() + "\x00" + observation.SampleID()
	encoded, err := json.Marshal(observation)
	if err != nil {
		t.Fatal(err)
	}
	adapter.qualifications[key] = sunSpecQualificationRecord{observation: observation, encoded: encoded}
	adapter.replayQualifications = map[string]bool{key: true}
	err = adapter.RecordSunSpecQualificationObservation(observation)
	var cover *semhub.CoverError
	if !errors.As(err, &cover) {
		t.Fatalf("record with an interrupted cover=%v, want a CoverError", err)
	}
	adapter.profileMu.Lock()
	_, retained := adapter.qualifications[key]
	adapter.profileMu.Unlock()
	if !retained {
		t.Fatal("the accepted publication's staged evidence was deleted")
	}
	if hub.Readable(firstAsset) {
		t.Fatal("the asset behind the interrupted cover is readable")
	}
	before, ok := adapter.SemanticPVCurrentSingle()
	if !ok {
		t.Fatal("accepted replay snapshot missing")
	}
	if adapter.replayQualifications[key] {
		t.Fatal("accepted cover retained replay admission")
	}
	if err := adapter.RecordSunSpecQualificationObservation(observation); err != nil {
		t.Fatal(err)
	}
	after, ok := adapter.SemanticPVCurrentSingle()
	if !ok || !bytes.Equal(before.Canonical, after.Canonical) {
		t.Fatal("duplicate replay advanced accepted snapshot")
	}
	waitForPV(t, func() bool { return hub.Readable(firstAsset) })
	core.lifecycleMu.Lock()
	pending := len(core.covers)
	core.lifecycleMu.Unlock()
	if pending != 0 {
		t.Fatalf("covers still pending: %d", pending)
	}
}

type managedDrainingEndpoint struct {
	Endpoint
	entered chan struct{}
	release chan struct{}
}

func (e *managedDrainingEndpoint) Close() error {
	close(e.entered)
	<-e.release
	return nil
}
func TestManagedCloseRevokesAdmissionBeforeEndpointDrain(t *testing.T) {
	adapter := startManagedPublicationTestAdapter(t, testSemanticPVClass(), &PublicationScope{Epoch: "source-epoch:pv:draining", Generation: 1})
	endpoint := &managedDrainingEndpoint{Endpoint: adapter.endpoint, entered: make(chan struct{}), release: make(chan struct{})}
	adapter.endpoint = endpoint
	done := make(chan error, 1)
	go func() { done <- adapter.Close() }()
	<-endpoint.entered
	adapter.connectionMu.RLock()
	closed := adapter.closed
	adapter.connectionMu.RUnlock()
	observation := pvCoreObservation(t, observedFroniusFloatWords(), 1, 2)
	if err := adapter.RecordSunSpecQualificationObservation(observation); err == nil {
		t.Error("draining adapter admitted observation")
	}
	if !closed {
		t.Error("draining adapter still admits endpoint work")
	}
	close(endpoint.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// Fixture cleanup closes the original endpoint directly.
	adapter.endpoint = endpoint.Endpoint
}
