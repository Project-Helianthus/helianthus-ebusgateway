package runtimecontrol

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

func p11ScopedFailure(t *testing.T, r *Recorder, err error, bad AssetID, driver DriverID) {
	t.Helper()
	var re *RestoreError
	if !errors.As(err, &re) || !reflect.DeepEqual(re.Assets, []AssetID{bad}) || !reflect.DeepEqual(re.Drivers, []DriverID{driver}) || len(re.Sources) != 0 {
		t.Fatalf("expected asset-scoped restore failure: %v", err)
	}
	if !r.failedAssets[bad] || !reflect.DeepEqual(r.Report().Failed, []AssetID{bad}) {
		t.Fatalf("missing failed asset: %+v", r.Report())
	}
}

func TestP11RestoreClockHighCoversRetainedIntents(t *testing.T) {
	for _, bad := range []AssetID{synthAssets[0], synthAssets[1], secondAsset} {
		for _, mode := range []string{"below-first", "below-last", "equal-maximum"} {
			t.Run(string(bad)+"/"+mode, func(t *testing.T) {
				w := newWorld(t, worldOpts{second: true, compact: -1})
				w.mustRestore()
				w.admit(synthSource)
				w.admit(secondSource)
				src, driver, good, goodSrc := synthSource, synthDriver, secondAsset, secondSource
				if bad == secondAsset {
					src, driver, good, goodSrc = secondSource, secondDriver, synthAssets[0], synthSource
				}
				w.mustPublish(goodSrc, good)
				first := w.mustPublish(src, bad).Accepted.Point
				last := w.mustPublish(src, bad).Accepted.Point
				if last <= first {
					t.Fatal("fixture must retain distinct points")
				}
				pre := w.states(good)
				w.loseProcess(time.Second)
				w.rewrite(func(s *walStream) {
					mark := s.Clocks[bad]
					switch mode {
					case "below-first":
						mark.High = first - 1
					case "below-last":
						mark.High = last - 1
					default:
						mark.High = last
					}
					s.Clocks[bad] = mark
				})
				err := w.restore()
				if mode == "equal-maximum" {
					if err != nil {
						t.Fatal(err)
					}
					if !w.bridge.hub.Readable(semreg.AssetID(bad)) {
						t.Fatal("equal maximum did not reopen reads")
					}
					w.admit(src)
					if point := w.mustPublish(src, bad).Accepted.Point; point <= last {
						t.Fatalf("allocator went backward: %d <= %d", point, last)
					}
				} else {
					p11ScopedFailure(t, w.rec, err, bad, driver)
					if w.bridge.hub.ReadAdmitted(semreg.AssetID(bad)) || w.bridge.hub.Readable(semreg.AssetID(bad)) {
						t.Fatal("damaged asset reads admitted")
					}
					if _, err := w.publish(src, bad); refusalCode(err) != RefusalFailedPrecondition {
						t.Fatalf("damaged asset writer: %v", err)
					}
				}
				assertStates(t, "unrelated retained asset", w.states(good), pre)
				if !w.bridge.hub.Readable(semreg.AssetID(good)) || w.rec.admit(goodSrc, []AssetID{good}) != nil {
					t.Fatal("healthy reads or writer admission closed")
				}
				w.admit(goodSrc)
				w.mustPublish(goodSrc, good)
			})
		}
	}
}

// Exercise the clock helper directly so unrelated replay validation cannot mask
// the high-water guard. Each alternative changes exactly one claimed field.
func TestP11ClockCoverageIndependentFields(t *testing.T) {
	for _, field := range []string{"baseline", "asset", "source", "driver", "clock-epoch", "intent-point"} {
		t.Run(field, func(t *testing.T) {
			asset, source, driver, epoch, point := AssetID("asset:p11-a"), SourceID("source:p11-a"), DriverID("fam.p11-a"), "boot-p11-a", uint64(17)
			switch field {
			case "asset":
				asset = "asset:p11-b"
			case "source":
				source = "source:p11-b"
			case "driver":
				driver = "fam.p11-b"
			case "clock-epoch":
				epoch = "boot-p11-b"
			case "intent-point":
				point = 29
			}
			host := &synthHost{boot: epoch, now: 100}
			clock := newSynthClock(host)
			anchor, err := clock.Anchor(0)
			if err != nil {
				t.Fatal(err)
			}
			good := AssetID("asset:p11-healthy")
			r, err := NewRecorder(RecorderOptions{Store: &faultStore{}, Hub: &hubBridge{}, Clock: clock, Sources: []ManagedSource{
				{Driver: driver, Source: source, Assets: []AssetID{asset}}, {Driver: "fam.p11-healthy", Source: "source:p11-healthy", Assets: []AssetID{good}},
			}})
			if err != nil {
				t.Fatal(err)
			}
			s := newStream()
			// A later array element has a lower point: coverage must take the maximum.
			s.Entries = []walEntry{{Source: source, Point: point, Assets: []walAsset{{Asset: asset}}}, {Source: source, Point: point - 2, Assets: []walAsset{{Asset: asset}}}}
			s.Clocks[asset] = clockMark{High: point - 1, Anchor: anchor, Unit: clockUnit}
			failed := map[AssetID]bool{}
			clocks := r.restoreClocks(s, 0, failed)
			if !failed[asset] || !clocks[asset].failed || failed[good] || clocks[good].failed {
				t.Fatalf("clock failure escaped its asset: %v", failed)
			}
			mark := s.Clocks[asset]
			mark.High = point
			s.Clocks[asset] = mark
			failed = map[AssetID]bool{}
			clocks = r.restoreClocks(s, 0, failed)
			if len(failed) != 0 || clocks[asset].failed || clocks[asset].last != point {
				t.Fatalf("equal maximum rejected: %v %+v", failed, clocks[asset])
			}
		})
	}
}

type p11ReprojectHub struct {
	ManagedHub
	bad AssetID
}

func (h p11ReprojectHub) Reproject(asset AssetID) error {
	if asset == h.bad {
		return errors.New("synthetic reprojection refusal")
	}
	return h.ManagedHub.Reproject(asset)
}

func TestP11ReprojectionFailureWithholdsOnlyOwningDriver(t *testing.T) {
	for _, bad := range []AssetID{synthAssets[0], synthAssets[1], secondAsset} {
		t.Run(string(bad), func(t *testing.T) {
			w := newWorld(t, worldOpts{second: true, compact: -1})
			w.mustRestore()
			w.admit(synthSource)
			w.admit(secondSource)
			w.mustPublish(synthSource, synthAssets[0])
			w.mustPublish(synthSource, synthAssets[1])
			w.mustPublish(secondSource, secondAsset)
			badSrc, badDriver, good, goodSrc, goodDriver := synthSource, synthDriver, secondAsset, secondSource, secondDriver
			if bad == secondAsset {
				badSrc, badDriver, good, goodSrc, goodDriver = secondSource, secondDriver, synthAssets[0], synthSource, synthDriver
			}
			pre := w.states(good)
			w.loseProcess(time.Second)
			w.rec.hub = p11ReprojectHub{ManagedHub: w.bridge, bad: bad}
			execs := map[DriverID]*synthExec{
				synthDriver:  {w: w, src: synthSource, start: func(context.Context, LifecycleCorrelation) error { return nil }},
				secondDriver: {w: w, src: secondSource, start: func(context.Context, LifecycleCorrelation) error { return nil }},
			}
			// Join the service before its store manager cleanup, including on failure.
			defer func() {
				if w.svc != nil {
					w.svc.Close()
				}
			}()
			p11ScopedFailure(t, w.rec, w.startService("inc-p11", execs), bad, badDriver)
			waitFor(t, "healthy driver running", func() bool { return w.status(goodDriver).ObservedState == drivermanager.ObservedRunning })
			if len(execs[badDriver].snapshot()) != 0 || w.status(badDriver).ObservedState != drivermanager.ObservedFailed {
				t.Fatal("failed driver was started")
			}
			if w.bridge.hub.ReadAdmitted(semreg.AssetID(bad)) || w.bridge.hub.Readable(semreg.AssetID(bad)) {
				t.Fatal("failed reprojection opened reads")
			}
			if _, err := w.publish(badSrc, bad); refusalCode(err) != RefusalFailedPrecondition {
				t.Fatalf("failed reprojection admitted writer: %v", err)
			}
			assertStates(t, "healthy asset", w.states(good), pre)
			if !w.bridge.hub.Readable(semreg.AssetID(good)) || w.rec.admit(goodSrc, []AssetID{good}) != nil {
				t.Fatal("healthy path closed")
			}
			w.admit(goodSrc)
			w.mustPublish(goodSrc, good)
		})
	}
}

func TestP11EmptyRestoreVerifiesLiveHub(t *testing.T) {
	for _, mode := range []string{"fresh", "admitted", "populated"} {
		t.Run(mode, func(t *testing.T) {
			w := newWorld(t, worldOpts{second: true, compact: -1})
			if mode != "fresh" {
				w.admit(synthSource)
			}
			if mode == "populated" {
				// Bypass the recorder to model live state without any durable material.
				batch, err := w.stage(synthSource, synthAssets[0], nil)(7)
				if err != nil {
					t.Fatal(err)
				}
				if out := w.bridge.Publish(context.Background(), synthSource, batch, 7); out.Accepted == nil {
					t.Fatalf("fixture publish: %+v", out)
				}
			}
			if w.material() != nil {
				t.Fatal("fixture unexpectedly durable")
			}
			before := w.bridge.hub.ExportRecovery()
			readAdmission := map[AssetID]bool{}
			for asset := range w.rec.assets {
				readAdmission[asset] = w.bridge.hub.ReadAdmitted(semreg.AssetID(asset))
			}
			err := w.restore()
			if mode == "fresh" {
				if err != nil {
					t.Fatal(err)
				}
				if len(w.rec.Report().Failed) != 0 {
					t.Fatal("fresh restore failed")
				}
				for src, assets := range w.sources() {
					w.admit(src)
					for _, asset := range assets {
						w.mustPublish(src, asset)
						if !w.bridge.hub.Readable(semreg.AssetID(asset)) {
							t.Fatal("fresh publication not readable")
						}
					}
				}
			} else {
				var re *RestoreError
				if !errors.As(err, &re) || len(re.Assets) == 0 {
					t.Fatalf("preexisting live state admitted: %v", err)
				}
				want := append([]AssetID{secondAsset}, synthAssets...)
				if !reflect.DeepEqual(re.Assets, want) || !reflect.DeepEqual(re.Drivers, []DriverID{secondDriver, synthDriver}) || len(re.Sources) != 0 {
					t.Fatalf("unexpected refusal scope: %+v", re)
				}
				// Hub Restore refuses prior admission without changing live state.
				// Recorder refusal must block every writer without replacing that
				// state or claiming authority to revoke the existing hub handles.
				if after := w.bridge.hub.ExportRecovery(); !reflect.DeepEqual(after, before) {
					t.Fatal("refused restore changed preexisting live hub state")
				}
				for _, asset := range re.Assets {
					if !w.rec.failedAssets[asset] || w.rec.admit(w.rec.assets[asset].source, []AssetID{asset}) == nil {
						t.Fatalf("refused asset %s admitted a writer", asset)
					}
					if w.bridge.hub.ReadAdmitted(semreg.AssetID(asset)) != readAdmission[asset] {
						t.Fatalf("refused restore changed live read admission for %s", asset)
					}
				}
			}
		})
	}
}

type p11InstallHub struct {
	ManagedHub
	bad       AssetID
	installed bool
}

func (h *p11InstallHub) Replay(base, frame []byte, scratch bool) (Replay, error) {
	replay, err := h.ManagedHub.Replay(base, frame, scratch)
	if err != nil {
		return nil, err
	}
	return &p11InstallReplay{Replay: replay, hub: h}, nil
}

type p11InstallReplay struct {
	Replay
	hub *p11InstallHub
}

func (r *p11InstallReplay) Install(failed map[AssetID]bool) (map[AssetID][]byte, map[AssetID]error) {
	r.hub.installed = true
	closed := make(map[AssetID]bool, len(failed)+1)
	for asset, value := range failed {
		closed[asset] = value
	}
	closed[r.hub.bad] = true
	installed, refused := r.Replay.Install(closed)
	if refused == nil {
		refused = map[AssetID]error{}
	}
	refused[r.hub.bad] = errors.New("synthetic asset-scoped install refusal")
	return installed, refused
}

func TestP11EmptyRestoreHonorsScopedInstallRefusal(t *testing.T) {
	for _, bad := range []AssetID{synthAssets[0], synthAssets[1], secondAsset} {
		t.Run(string(bad), func(t *testing.T) {
			w := newWorld(t, worldOpts{second: true, compact: -1})
			badSrc, badDriver, good, goodSrc := synthSource, synthDriver, secondAsset, secondSource
			if bad == secondAsset {
				badSrc, badDriver, good, goodSrc = secondSource, secondDriver, synthAssets[0], synthSource
			}
			hub := &p11InstallHub{ManagedHub: w.bridge, bad: bad}
			w.rec.hub = hub
			p11ScopedFailure(t, w.rec, w.restore(), bad, badDriver)
			if !hub.installed {
				t.Fatal("empty replay installation bypassed")
			}
			if w.bridge.hub.ReadAdmitted(semreg.AssetID(bad)) || w.bridge.hub.Readable(semreg.AssetID(bad)) {
				t.Fatal("refused asset reads opened")
			}
			if _, err := w.publish(badSrc, bad); refusalCode(err) != RefusalFailedPrecondition {
				t.Fatalf("refused asset writer: %v", err)
			}
			if w.rec.admit(goodSrc, []AssetID{good}) != nil || !w.bridge.hub.ReadAdmitted(semreg.AssetID(good)) {
				t.Fatal("healthy empty asset admission closed")
			}
			w.admit(goodSrc)
			w.mustPublish(goodSrc, good)
			if !w.bridge.hub.Readable(semreg.AssetID(good)) {
				t.Fatal("healthy empty asset did not become readable")
			}
		})
	}
}
