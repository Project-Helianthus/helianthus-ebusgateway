package b503session

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestNonzeroEpochTimerGeneration(t *testing.T) {
	for _, tc := range []struct {
		name        string
		replacement string
	}{
		{name: "current callback"},
		{name: "callback before epoch refresh", replacement: "refresh"},
		{name: "callback and transport key before lifecycle replacement", replacement: "lifecycle"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			initial := TransportKey{AdapterInstanceID: "synthetic-adapter", TransportEpoch: 17}
			next := initial
			next.TransportEpoch++
			m := New(initial, time.Hour, func(context.Context) (TransportKey, error) { return next, nil })
			t.Cleanup(func() { m.ResetForRestart() })
			generation := func() uint64 {
				m.stateMu.Lock()
				defer m.stateMu.Unlock()
				return m.idleTimerGen
			}
			ack := func(context.Context, byte) DispatchOutcome {
				return DispatchOutcome{Emitted: true, Native: NativeACK, Transport: m.TransportKey()}
			}
			var cleanupTargets []byte
			var cleanupTransports []TransportKey
			m.SetCleanupDispatcher(func(_ context.Context, target byte) DispatchOutcome {
				cleanupTargets = append(cleanupTargets, target)
				cleanupTransports = append(cleanupTransports, m.TransportKey())
				return ack(ctx, target)
			})
			target := byte(0x15)
			var owner SessionKey
			var err error
			if tc.replacement == "lifecycle" {
				owner, err = m.Enable(ctx)
			} else {
				owner, err = m.EnableOperation(ctx, target, ack)
			}
			if err != nil {
				t.Fatal(err)
			}
			oldGeneration := generation()
			wantTransport := initial
			if tc.replacement != "" {
				if tc.replacement == "lifecycle" {
					if err := m.Disable(owner); err != nil {
						t.Fatal(err)
					}
				}
				m.OnEpochAdvance(ctx, next.TransportEpoch)
				if tc.replacement == "refresh" {
					if _, err := m.ReadOperation(ctx, target, ack); err != nil {
						t.Fatal(err)
					}
				} else {
					target = 0x25
					if _, err := m.EnableOperation(ctx, target, ack); err != nil {
						t.Fatal(err)
					}
				}
				wantTransport = next
				beforeCleanup, hadCleanup := m.CleanupObligation()
				currentGeneration := generation()
				if currentGeneration <= oldGeneration {
					t.Fatal("replacement did not advance timer generation")
				}
				// Delivery is synchronous: all callback effects complete before assertions.
				m.idleTimerFired(oldGeneration)
				if err := m.Read(initial); !errors.Is(err, ErrNotActive) {
					t.Fatalf("old transport read = %v, want ErrNotActive", err)
				}
				if tc.replacement == "lifecycle" {
					if err := m.Disable(owner); !errors.Is(err, ErrWrongToken) {
						t.Fatalf("old owner disable = %v, want ErrWrongToken", err)
					}
				}
				if got := m.StatusSnapshot(); got.State != Active || !got.Owned {
					t.Fatalf("stale delivery changed replacement owner: %+v", got)
				}
				if generation() != currentGeneration {
					t.Fatal("stale delivery changed current timer")
				}
				if after, ok := m.CleanupObligation(); ok != hadCleanup || !reflect.DeepEqual(after, beforeCleanup) {
					t.Fatalf("stale delivery changed cleanup: before=%+v after=%+v", beforeCleanup, after)
				}
				if len(cleanupTargets) != 0 {
					t.Fatalf("stale delivery emitted cleanup: %v", cleanupTargets)
				}
			}
			if got := m.TransportKey(); got != wantTransport {
				t.Fatalf("transport = %+v, want %+v", got, wantTransport)
			}
			m.idleTimerFired(generation())
			if got := m.StatusSnapshot(); got.State != Idle || got.Owned {
				t.Fatalf("expiry owner = %+v", got)
			}
			cleanup, ok := m.CleanupObligation()
			if !ok || cleanup.Target != target || cleanup.TransportEpoch != wantTransport.TransportEpoch || !cleanup.LastAttempted || cleanup.LastAttemptEpoch != wantTransport.TransportEpoch || !cleanup.LastEmitted || cleanup.LastNative != NativeACK {
				t.Fatalf("expiry cleanup = %+v, present=%v", cleanup, ok)
			}
			if len(cleanupTargets) != 1 || cleanupTargets[0] != target || cleanupTransports[0] != wantTransport {
				t.Fatalf("cleanup dispatch targets=%v transports=%v", cleanupTargets, cleanupTransports)
			}
			m.idleTimerFired(oldGeneration)
			if len(cleanupTargets) != 1 {
				t.Fatal("late callback emitted additional cleanup")
			}
		})
	}
}
