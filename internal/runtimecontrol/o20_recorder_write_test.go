package runtimecontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type o20Store struct {
	Store
	mu       sync.Mutex
	material *RecoveryMaterial
	writes   int
	before   func(*RecoveryMaterial) error
}

func (s *o20Store) AdmissionOpen() bool { return true }
func (s *o20Store) Recovery() *RecoveryMaterial {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.material
}
func (s *o20Store) CommitRecovery(_ context.Context, m *RecoveryMaterial) error {
	s.mu.Lock()
	s.writes++
	before := s.before
	s.mu.Unlock()
	if before != nil {
		if err := before(m); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.material = &RecoveryMaterial{DriverIDs: append([]DriverID(nil), m.DriverIDs...), Payload: append([]byte(nil), m.Payload...)}
	s.mu.Unlock()
	return nil
}

type o20Hub struct {
	ManagedHub
	control func(SourceID, uint64, func(StagedBatch) error) []AssetOutcome
}

func (*o20Hub) Frame() ([]byte, error) { return nil, nil }
func (h *o20Hub) Control(_ context.Context, source SourceID, _ []byte, point uint64, before func(StagedBatch) error) []AssetOutcome {
	return h.control(source, point, before)
}

func o20Recorder(t *testing.T, source SourceID, assets ...AssetID) (*Recorder, *Writer, *o20Store, *o20Hub) {
	t.Helper()
	store, hub := &o20Store{}, &o20Hub{}
	rec, err := NewRecorder(RecorderOptions{Store: store, Hub: hub, Clock: newSynthClock(&synthHost{boot: "o20-boot", now: 100}), Sources: []ManagedSource{{Driver: "fam.o20", Source: source, Assets: assets}}, CompactAfter: -1})
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.RestoreEmpty(context.Background()); err != nil {
		t.Fatal(err)
	}
	writer, err := rec.Writer(source)
	if err != nil {
		t.Fatal(err)
	}
	return rec, writer, store, hub
}

func o20Batch(asset AssetID) StagedBatch {
	digest := sha256.Sum256([]byte("o20-digest"))
	return StagedBatch{Asset: asset, Key: "o20-source/epoch-1/generation-1", Sequence: 1, Digest: hex.EncodeToString(digest[:]), Bytes: []byte("o20-staged"), Meta: []byte("o20-meta")}
}
func o20Accepted(batch StagedBatch, point uint64) AssetOutcome {
	return AssetOutcome{Asset: batch.Asset, Accepted: &AcceptedStep{Batch: cloneStaged(batch), Point: point, Result: []byte("o20-result"), Incarnation: "o20-incarnation", HubSeq: 1}}
}
func o20Stream(t *testing.T, store *o20Store) walStream {
	t.Helper()
	stream, err := decodeStream(store.Recovery().Payload)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range stream.Entries {
		for _, asset := range entry.Assets {
			if !wellFormedAsset(entry, asset) {
				t.Fatalf("invalid durable asset: %+v", asset)
			}
		}
	}
	return stream
}

func TestO20AcceptedBytes(t *testing.T) {
	for _, field := range []string{"baseline", "source", "asset", "epoch", "generation", "sequence", "digest", "bytes"} {
		for _, mismatch := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/mismatch=%t", field, mismatch), func(t *testing.T) {
				source := SourceID("o20-source")
				batch := o20Batch("o20-asset")
				switch field {
				case "source":
					source = "o20-other-source"
					batch.Key = "o20-other-source/epoch-1/generation-1"
				case "asset":
					batch.Asset = "o20-other-asset"
				case "epoch":
					batch.Key = "o20-source/epoch-2/generation-1"
				case "generation":
					batch.Key = "o20-source/epoch-1/generation-2"
				case "sequence":
					batch.Sequence = 2
				case "digest":
					digest := sha256.Sum256([]byte("o20-other-digest"))
					batch.Digest = hex.EncodeToString(digest[:])
				case "bytes":
					batch.Bytes = []byte("o20-other-staged")
				}
				rec, writer, store, hub := o20Recorder(t, source, batch.Asset, "o20-sibling")
				sibling := o20Batch("o20-sibling")
				hub.control = func(got SourceID, point uint64, before func(StagedBatch) error) []AssetOutcome {
					if got != source {
						t.Fatalf("source = %s, want %s", got, source)
					}
					if err := before(batch); err != nil {
						t.Fatal(err)
					}
					if err := before(sibling); err != nil {
						t.Fatal(err)
					}
					accepted := cloneStaged(batch)
					if mismatch {
						accepted.Bytes = []byte("o20-different-accepted")
					}
					return []AssetOutcome{o20Accepted(accepted, point), o20Accepted(sibling, point)}
				}
				outs, err := writer.Control(context.Background(), []byte("o20-control"))
				if len(outs) != 2 {
					t.Fatalf("outcomes: %+v", outs)
				}
				var bad, good AssetOutcome
				for _, out := range outs {
					if out.Asset == batch.Asset {
						bad = out
					} else {
						good = out
					}
				}
				if good.Accepted == nil || good.Uncertain {
					t.Fatalf("sibling outcome lost: %+v", good)
				}
				if mismatch {
					if !errors.Is(err, ErrOutcomeUncertain) || !bad.Uncertain || bad.Accepted != nil {
						t.Fatalf("mismatch: %+v %v", bad, err)
					}
					if err := rec.admit(source, []AssetID{batch.Asset}); err == nil {
						t.Fatal("uncertain writer remained admitted")
					}
				} else if err != nil || bad.Accepted == nil || bad.Uncertain {
					t.Fatalf("valid acceptance: %+v %v", bad, err)
				}
				stream := o20Stream(t, store)
				if len(stream.Entries) != 1 || stream.Entries[0].Source != source {
					t.Fatalf("wrong intent: %+v", stream.Entries)
				}
				for _, asset := range stream.Entries[0].Assets {
					want := sibling
					if asset.Asset == batch.Asset {
						want = batch
					}
					if asset.Staged == nil || asset.Staged.Asset != want.Asset || asset.Staged.Key != want.Key || asset.Staged.Sequence != want.Sequence || asset.Staged.Digest != want.Digest || !bytes.Equal(asset.Staged.Bytes, want.Bytes) || !bytes.Equal(asset.Staged.Meta, want.Meta) {
						t.Fatalf("staged identity changed: %+v", asset)
					}
					if mismatch && asset.Outcome != nil {
						t.Fatal("uncertain call wrote a durable outcome")
					}
					if !mismatch && (asset.Outcome == nil || asset.Outcome.Accepted == nil) {
						t.Fatal("valid call did not record acceptance")
					}
				}
			})
		}
	}
}

func TestO20ConcurrentReservation(t *testing.T) {
	batch, sibling := o20Batch("o20-asset"), o20Batch("o20-sibling")
	_, writer, store, hub := o20Recorder(t, "o20-source", batch.Asset, sibling.Asset)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	store.before = func(m *RecoveryMaterial) error {
		stream, err := decodeStream(m.Payload)
		if err != nil {
			return err
		}
		for _, asset := range stream.Entries[0].Assets {
			if asset.Asset == batch.Asset && asset.Staged != nil {
				select {
				case <-entered:
				default:
					close(entered)
					<-release
				}
				break
			}
		}
		return nil
	}
	hub.control = func(_ SourceID, point uint64, before func(StagedBatch) error) []AssetOutcome {
		first := make(chan error, 1)
		go func() { first <- before(batch) }()
		defer func() {
			unblock()
			if err := <-first; err != nil {
				t.Error(err)
			}
		}()
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("first staging did not enter durable write")
		}
		duplicate := make(chan error, 1)
		go func() { duplicate <- before(batch) }()
		var duplicateErr error
		select {
		case duplicateErr = <-duplicate:
		case <-time.After(3 * time.Second):
			unblock()
			duplicateErr = <-duplicate
			t.Error("duplicate waited for durable commit instead of refusing the reservation")
		}
		if !errors.Is(duplicateErr, ErrInvalidStaging) {
			t.Errorf("duplicate staging = %v", duplicateErr)
		}
		unblock()
		if err := before(sibling); err != nil {
			t.Error(err)
		}
		return []AssetOutcome{o20Accepted(batch, point), o20Accepted(sibling, point)}
	}
	if _, err := writer.Control(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	stream := o20Stream(t, store)
	if len(stream.Entries) != 1 {
		t.Fatal("unexpected durable attempts")
	}
	if store.writes != 4 {
		t.Fatalf("durable writes = %d, want intent + two stages + outcome", store.writes)
	}
}

func TestO20FailedReservationRollback(t *testing.T) {
	batch, sibling := o20Batch("o20-asset"), o20Batch("o20-sibling")
	batch.Bytes = bytes.Repeat([]byte("x"), MaxStagedBytes/2)
	sibling.Bytes = bytes.Repeat([]byte("y"), MaxStagedBytes/2-128)
	_, writer, store, hub := o20Recorder(t, "o20-source", batch.Asset, sibling.Asset)
	failure := errors.New("o20 synthetic precommit refusal")
	failed := false
	store.before = func(m *RecoveryMaterial) error {
		stream, err := decodeStream(m.Payload)
		if err != nil {
			return err
		}
		for _, asset := range stream.Entries[0].Assets {
			if asset.Asset == batch.Asset && asset.Staged != nil && !failed {
				failed = true
				return failure
			}
		}
		return nil
	}
	hub.control = func(_ SourceID, point uint64, before func(StagedBatch) error) []AssetOutcome {
		if err := before(sibling); err != nil {
			t.Fatal(err)
		}
		if err := before(batch); !errors.Is(err, failure) {
			t.Fatalf("first staging = %v", err)
		}
		for _, asset := range o20Stream(t, store).Entries[0].Assets {
			if asset.Asset == batch.Asset && asset.Staged != nil {
				t.Fatal("failed staging persisted")
			}
		}
		if err := before(sibling); !errors.Is(err, ErrInvalidStaging) {
			t.Fatalf("failure discarded sibling reservation: %v", err)
		}
		oversized := cloneStaged(batch)
		oversized.Bytes = append(oversized.Bytes, bytes.Repeat([]byte("z"), 256)...)
		if err := before(oversized); refusalCode(err) != RefusalResourceExhausted {
			t.Fatalf("failure discarded sibling byte budget: %v", err)
		}
		if err := before(batch); err != nil {
			t.Fatalf("retry after failure: %v", err)
		}
		if err := before(batch); !errors.Is(err, ErrInvalidStaging) {
			t.Fatalf("successful retry permitted another attempt: %v", err)
		}
		return []AssetOutcome{o20Accepted(batch, point), o20Accepted(sibling, point)}
	}
	if _, err := writer.Control(context.Background(), []byte("o20-control")); err != nil {
		t.Fatal(err)
	}
	o20Stream(t, store)
	if !failed || store.writes != 5 {
		t.Fatalf("failure=%t writes=%d, want one failure and four durable writes", failed, store.writes)
	}
}
