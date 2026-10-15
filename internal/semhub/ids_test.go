package semhub

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
	"strings"
	"sync"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

func TestNativeIDFormatIsDomainSeparatedAndDeterministic(t *testing.T) {
	id, err := NativeID(KindBinding, "heat_pump", "asset-1", "source-1", "profile", "epoch-1", "1")
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.New()
	digest.Write([]byte("helianthus.semhub.id.v1/binding/heat_pump"))
	for _, field := range []string{"asset-1", "source-1", "profile", "epoch-1", "1"} {
		digest.Write([]byte{0})
		digest.Write([]byte(field))
	}
	if want := "binding:heat_pump:" + hex.EncodeToString(digest.Sum(nil))[:32]; id != want {
		t.Fatalf("id=%q want %q", id, want)
	}
	if !regexp.MustCompile(`^binding:heat_pump:[0-9a-f]{32}$`).MatchString(id) || ValidateOpaque(id) != nil {
		t.Fatalf("format=%q", id)
	}
	again, _ := NativeID(KindBinding, "heat_pump", "asset-1", "source-1", "profile", "epoch-1", "1")
	swapped, _ := NativeID(KindBinding, "heat_pump", "source-1", "asset-1", "profile", "epoch-1", "1")
	service, _ := NativeID(KindService, "heat_pump", "asset-1", "source-1", "epoch-1", "1")
	joined, _ := NativeID(KindAsset, "heat_pump", "ab", "c")
	split, _ := NativeID(KindAsset, "heat_pump", "a", "bc")
	if again != id || swapped == id || strings.TrimPrefix(service, "service:") == strings.TrimPrefix(id, "binding:") || joined == split {
		t.Fatal("native identity is not a deterministic, ordered, domain-separated function of its material")
	}
}

func TestNativeIDRejectsInvalidMaterial(t *testing.T) {
	for name, mint := range map[string]func() (string, error){
		"nul field":       func() (string, error) { return NativeID(KindAsset, "pv", "serial\x00x") },
		"unknown kind":    func() (string, error) { return NativeID("epoch", "pv", "x") },
		"family case":     func() (string, error) { return NativeID(KindAsset, "PV", "x") },
		"family space":    func() (string, error) { return NativeID(KindAsset, "p v", "x") },
		"empty material":  func() (string, error) { return NativeID(KindSource, "pv") },
		"binding arity":   func() (string, error) { return NativeID(KindBinding, "pv", "a", "s", "p", "e") },
		"service arity":   func() (string, error) { return NativeID(KindService, "pv", "a", "d", "e") },
		"overlong family": func() (string, error) { return NativeID(KindAsset, strings.Repeat("a", 65), "x") },
	} {
		if _, err := mint(); !errors.Is(err, ErrInvalidIdentity) {
			t.Fatalf("%s accepted: %v", name, err)
		}
	}
	for _, value := range []string{"", " asset", "asset ", "!asset", "asset#x", strings.Repeat("a", 257)} {
		if ValidateOpaque(value) == nil {
			t.Fatalf("opaque value %q accepted", value)
		}
	}
	if ValidateOpaque(strings.Repeat("a", 256)) != nil {
		t.Fatal("256-byte opaque boundary rejected")
	}
}

// The expected values are copied from the pre-hub characterization goldens
// (commit 1 of issue #1024).
func TestLegacyMintersReproduceFirstAdmissionIdentifiers(t *testing.T) {
	tesla, _ := NewLegacyMinter(LegacyTeslaEVSE)
	teslaFirst := Incarnation{Asset: "asset:tesla-wc3-a", Source: "source:tesla-wc3-a", Epoch: "epoch:tesla-wc3-a", Generation: 7, First: true}
	binding := tesla.Binding(teslaFirst, "")
	if binding != "binding:tesla-wc3:04acb6030d7537c1cb7885976be38c95" {
		t.Fatalf("tesla binding=%q", binding)
	}
	if got := tesla.Service(teslaFirst, "evse.service.evse", binding); got != "service:tesla-wc3:6edd3058a1b32972e7b33fe9e4d0822f" {
		t.Fatalf("tesla evse service=%q", got)
	}
	if got := tesla.Service(teslaFirst, "evse.service.connector", binding); got != "service:tesla-wc3:03ae485218190fab9d8ce3446238532a" {
		t.Fatalf("tesla connector service=%q", got)
	}
	if got := tesla.Capability(teslaFirst, "evse.capability.read.evse", binding); got != "capability:tesla-wc3:272930c2222f87a9659d70da89c63223" {
		t.Fatalf("tesla evse capability=%q", got)
	}
	if got := tesla.Capability(teslaFirst, "evse.capability.read.connector", binding); got != "capability:tesla-wc3:d6814720e2e57f7af65acf5b9c411c38" {
		t.Fatalf("tesla connector capability=%q", got)
	}

	pv, _ := NewLegacyMinter(LegacyPVSunSpec)
	pvFirst := Incarnation{Asset: "pv-asset-5c57adba3ad8a529617c1f8627ae32f1", Source: "source:semantic-pv:af0b34ae1c1350f052fceb5289c655ad", Epoch: "source-epoch:semantic-pv:6565a9c3ac5e0f86603dd5c1715a2419", Generation: 1, First: true}
	pvBinding := pv.Binding(pvFirst, "")
	if pvBinding != "binding:semantic-pv:f7d3710e6c368e8130cf94c2620f2c48" {
		t.Fatalf("pv binding=%q", pvBinding)
	}
	if got := pv.Service(pvFirst, "pv.service.inverter", pvBinding); got != "service:semantic-pv:2e9dc71411098e8ebbf7aa249ece3e37" {
		t.Fatalf("pv service=%q", got)
	}
	if got := pv.Capability(pvFirst, "pv.capability.read.inverter", pvBinding); got != "capability:semantic-pv:6ba3099965b470ac85141e257435e284" {
		t.Fatalf("pv capability=%q", got)
	}
	if got := PVEpochID("tcp://127.0.0.1:1502", "2026-09-20T10:00:03Z"); got != pvFirst.Epoch {
		t.Fatalf("pv epoch=%q", got)
	}

	growatt, _ := NewLegacyMinter(LegacyGrowattStorage)
	growattFirst := Incarnation{Asset: "asset:growatt-bms-a", Source: "growatt-bms-a", Epoch: "source-epoch-1", Generation: 1, First: true}
	growattBinding := growatt.Binding(growattFirst, "growatt.bms.rs485.1xsxxp.v2_02.readonly.v1")
	sum := sha256.Sum256([]byte("asset:growatt-bms-a\x00growatt-bms-a\x00growatt.bms.rs485.1xsxxp.v2_02.readonly.v1"))
	if want := semreg.NativeBindingID("binding:semantic-storage:" + hex.EncodeToString(sum[:])[:32]); growattBinding != want {
		t.Fatalf("growatt binding=%q want %q", growattBinding, want)
	}
	service := growatt.Service(growattFirst, "pack", growattBinding)
	sum = sha256.Sum256([]byte("asset:growatt-bms-a\x00" + string(growattBinding) + "\x00pack"))
	if want := semreg.ServiceInstanceID("service:semantic-storage:" + hex.EncodeToString(sum[:])[:32]); service != want {
		t.Fatalf("growatt service=%q", service)
	}
}

func TestLegacyMintersScopeLaterAdmissions(t *testing.T) {
	for _, family := range []LegacyFamily{LegacyGrowattStorage, LegacyTeslaEVSE, LegacyPVSunSpec} {
		minter, err := NewLegacyMinter(family)
		if err != nil {
			t.Fatal(err)
		}
		first := Incarnation{Asset: "asset:x", Source: "source:x", Epoch: "epoch:a", Generation: 7, First: true}
		later := Incarnation{Asset: "asset:x", Source: "source:x", Epoch: "epoch:b", Generation: 8}
		laterAgain := Incarnation{Asset: "asset:x", Source: "source:x", Epoch: "epoch:c", Generation: 9}
		firstBinding, laterBinding := minter.Binding(first, "p"), minter.Binding(later, "p")
		if firstBinding == laterBinding || laterBinding == minter.Binding(laterAgain, "p") {
			t.Fatalf("%s binding not scoped by incarnation", family)
		}
		if minter.Service(first, "d", firstBinding) == minter.Service(later, "d", laterBinding) || minter.Capability(first, "c", firstBinding) == minter.Capability(later, "c", laterBinding) {
			t.Fatalf("%s service or capability not scoped by incarnation", family)
		}
		for _, id := range []string{string(laterBinding), string(minter.Service(later, "d", laterBinding)), string(minter.Capability(later, "c", laterBinding))} {
			if ValidateOpaque(id) != nil {
				t.Fatalf("%s minted invalid id %q", family, id)
			}
		}
	}
	if _, err := NewLegacyMinter("modbus_other"); !errors.Is(err, ErrInvalidIdentity) {
		t.Fatalf("open legacy set: %v", err)
	}
}

func TestPVIdentityMaterialIsHashedNotRetained(t *testing.T) {
	asset := PVAssetID([]byte("Maker\x00Model\x00SERIAL-0001"))
	src := PVSourceID([]byte(`{"endpoint":"tcp://198.51.100.7:502"}`))
	for _, id := range []string{string(asset), string(src)} {
		if strings.Contains(id, "SERIAL") || strings.Contains(id, "198.51") || ValidateOpaque(id) != nil {
			t.Fatalf("identity leaked material: %q", id)
		}
	}
}

func TestSyntheticFamiliesRegisterConcurrentlyWithDistinctIdentities(t *testing.T) {
	hub := newTestHub(t)
	families := []string{"heat_pump", "battery", "meter", "vehicle"}
	var group sync.WaitGroup
	errs := make(chan error, len(families))
	handles := make([]*Handle, len(families))
	for index, family := range families {
		group.Add(1)
		go func(index int, family string) {
			defer group.Done()
			asset, _ := NativeID(KindAsset, family, "material-"+family)
			src, _ := NativeID(KindSource, family, "resource-"+family)
			if _, err := hub.Declare(AssetSpec{AssetID: semreg.AssetID(asset), SourceID: semreg.SourceID(src), Driver: family, Validators: []semreg.PackValidator{newTestValidator()}}); err != nil {
				errs <- err
				return
			}
			handle, err := hub.AdmitSource(Admission{Source: semreg.SourceID(src), Driver: family})
			if err != nil {
				errs <- err
				return
			}
			handles[index] = handle
			for at := int64(1); at <= 5; at++ {
				if _, err := publish(t, hub, handle, semreg.AssetID(asset), family, at, at); err != nil {
					errs <- err
					return
				}
			}
		}(index, family)
	}
	group.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, info := range hub.Sources() {
		if seen[string(info.Epoch)] || info.Generation != 1 {
			t.Fatalf("epoch or generation not distinct: %+v", info)
		}
		seen[string(info.Epoch)] = true
	}
	for index, family := range families {
		asset, _ := NativeID(KindAsset, family, "material-"+family)
		view, _ := hub.Inspect(semreg.AssetID(asset))
		if view.Snapshot.Cursors[0].LastSequence != "5" || view.Snapshot.Sources[0].SourceEpochID != handles[index].Epoch() {
			t.Fatalf("%s view=%+v", family, view.Snapshot.Cursors)
		}
		for _, binding := range view.Snapshot.Bindings {
			if seen[string(binding.BindingID)] {
				t.Fatal("binding identity collided")
			}
			seen[string(binding.BindingID)] = true
		}
	}
}
