package modbusadapter

import (
	"context"
	"errors"
	"net"
	"testing"

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
