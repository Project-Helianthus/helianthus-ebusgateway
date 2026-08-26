package mcp

import (
	"context"
	"errors"
	"testing"
)

type outBackAXSV1RuntimeFixture struct{ snapshot OutBackAXSV1ProviderSnapshot }

func (f outBackAXSV1RuntimeFixture) OutBackAXSV1Snapshot(context.Context) (OutBackAXSV1ProviderSnapshot, error) {
	return f.snapshot, nil
}

func TestOutBackAXSV1RuntimePreservesNativeObservation(t *testing.T) {
	snapshot := OutBackAXSV1ProviderSnapshot{Profile: OutBackAXSV1Profile, Qualified: true, RawWords: []uint16{64110, 282}, OutboundAllowed: true, FirmwareMajor: 1, FirmwareMid: 2, FirmwareMinor: 3}
	runtime, err := NewOutBackAXSV1Runtime(outBackAXSV1RuntimeFixture{snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runtime.OutBackAXSV1(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Qualified || !result.OutboundAllowed || result.FirmwareMajor != 1 || len(result.RawWords) != 2 || result.RawWords[0] != 64110 {
		t.Fatalf("result=%#v", result)
	}
	snapshot.RawWords[0] = 0
	if result.RawWords[0] != 64110 {
		t.Fatalf("raw words aliased provider snapshot: %#v", result.RawWords)
	}
}

func TestOutBackAXSV1RuntimeRejectsUnqualifiedSnapshot(t *testing.T) {
	runtime, err := NewOutBackAXSV1Runtime(outBackAXSV1RuntimeFixture{snapshot: OutBackAXSV1ProviderSnapshot{Profile: OutBackAXSV1Profile}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.OutBackAXSV1(context.Background()); !errors.Is(err, ErrOutBackAXSV1NotQualified) {
		t.Fatalf("err=%v", err)
	}
}
