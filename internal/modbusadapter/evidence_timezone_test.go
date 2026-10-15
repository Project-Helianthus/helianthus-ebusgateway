package modbusadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	modbus "github.com/Project-Helianthus/helianthus-modbus"
)

func TestPVEvidenceTimezoneDigests(t *testing.T) {
	base := time.Unix(1_800_000_000, 123).UTC()
	publish := func(t *testing.T, loc *time.Location) SemanticPVCurrent {
		t.Helper()
		now := base.In(loc)
		config := validConfig()
		config.Clock = func() time.Time { return now }
		client, peer := net.Pipe()
		t.Cleanup(func() { _ = peer.Close() })
		adapter, err := Start(context.Background(), config,
			func(context.Context, string, string) (net.Conn, error) { return client, nil },
			func(modbus.TCPEndpointConfig) (Endpoint, error) {
				return pvCharacterizationEndpoint{fakeEndpoint: &fakeEndpoint{}, advance: func() {}}, nil
			})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = adapter.Close() })
		observation := pvCoreObservation(t, observedFroniusFloatControlsWords(), 1, 2)
		before, err := json.Marshal(observation)
		if err != nil {
			t.Fatal(err)
		}
		now = now.Add(5 * time.Second)
		if err := adapter.RecordSunSpecQualificationObservation(observation); err != nil {
			t.Fatal(err)
		}
		after, err := json.Marshal(observation)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(before, after) {
			t.Error("native evidence changed")
		}
		value, ok := adapter.SemanticPVCurrentSingleAt(now)
		if !ok {
			t.Fatal("PV current unavailable")
		}
		return value
	}
	want := publish(t, time.UTC)
	for _, loc := range []*time.Location{time.UTC, time.FixedZone("positive", 19800), time.FixedZone("negative", -25200)} {
		t.Run(loc.String(), func(t *testing.T) {
			got := publish(t, loc)
			if !bytes.Equal(got.Canonical, want.Canonical) {
				t.Error("same instant changed canonical snapshot, batch or retained evidence digests")
			}
			a, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			b, err := json.Marshal(want)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(a, b) {
				t.Error("same instant changed evaluated PV readback")
			}
		})
	}
}

func TestPVEvidenceTimezoneZeroAndMonotonic(t *testing.T) {
	zero := time.Time{}
	for _, loc := range []*time.Location{time.UTC, time.FixedZone("positive", 3600), time.FixedZone("negative", -3600)} {
		at := zero.In(loc)
		if pvPublicationWall(at) != pvPublicationWall(zero) {
			t.Error("zero wall point depends on offset")
		}
		adapter := &Adapter{startedWall: at, startedMono: at}
		if _, err := adapter.semanticPVReadContext(); err == nil {
			t.Error("zero clock admitted")
		}
	}
	anchor := time.Now()
	adapter := &Adapter{startedWall: anchor.UTC(), startedMono: anchor, wallNow: func() time.Time { return anchor.Add(time.Second) }, monotonicNow: func() time.Time { return anchor.Add(time.Second) }}
	a, err := adapter.semanticPVReadContext()
	if err != nil {
		t.Fatal(err)
	}
	adapter.wallNow = func() time.Time { return anchor.Add(time.Second).In(time.FixedZone("negative", -25200)) }
	b, err := adapter.semanticPVReadContext()
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Error("offset changed wall or monotonic lifecycle")
	}
	adapter.monotonicNow = func() time.Time { return anchor.Add(-time.Second) }
	if _, err := adapter.semanticPVReadContext(); err == nil {
		t.Error("negative monotonic elapsed admitted")
	}
}
