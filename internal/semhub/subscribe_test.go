package semhub

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

type publisher struct {
	asset  semreg.AssetID
	handle *Handle
	family string
}

func startPublishers(t *testing.T, hub *Hub, families, perFamily int) []publisher {
	t.Helper()
	out := []publisher{}
	for f := 0; f < families; f++ {
		family := fmt.Sprintf("family_%d", f)
		src := semreg.SourceID("source:" + family)
		for a := 0; a < perFamily; a++ {
			id := semreg.AssetID(fmt.Sprintf("asset:%s:%d", family, a))
			mustDeclare(t, hub, id, src, family, nil)
		}
		handle, err := hub.AdmitSource(Admission{Source: src, Driver: family, Legacy: LegacyGrowattStorage, Epoch: semreg.SourceEpochID("epoch:" + family), Generation: 1})
		if err != nil {
			t.Fatal(err)
		}
		for a := 0; a < perFamily; a++ {
			out = append(out, publisher{asset: semreg.AssetID(fmt.Sprintf("asset:%s:%d", family, a)), handle: handle, family: family})
		}
	}
	return out
}

func TestFanOutIsLosslessAndOrderedForEverySubscriber(t *testing.T) {
	hub, err := newHub(Options{}, 4096)
	if err != nil {
		t.Fatal(err)
	}
	publishers := startPublishers(t, hub, 3, 2)
	subscriptions := make([]*Subscription, 4)
	for i := range subscriptions {
		subscriptions[i] = hub.Subscribe()
		defer subscriptions[i].Close()
	}
	start := hub.Cursor().Seq
	var group sync.WaitGroup
	for _, p := range publishers {
		group.Add(1)
		go func(p publisher) {
			defer group.Done()
			for at := int64(1); at <= 20; at++ {
				if _, err := publish(t, hub, p.handle, p.asset, p.family, at, at); err != nil {
					t.Error(err)
					return
				}
			}
		}(p)
	}
	group.Wait()
	end := hub.Cursor().Seq
	for index, subscription := range subscriptions {
		last := uint64(0)
		sequences := map[semreg.AssetID]uint64{}
		count := 0
		for count < int(end-start) {
			event, err := subscription.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if event.Kind == EventOverflow || event.HubSeq <= last {
				t.Fatalf("subscriber %d: event %+v after %d", index, event, last)
			}
			last = event.HubSeq
			if event.Kind != EventPublished {
				continue
			}
			count++
			sequence, _ := strconv.ParseUint(string(event.Sequence), 10, 64)
			if sequence <= sequences[event.Asset] {
				t.Fatalf("subscriber %d: sequence of %s went %d -> %d", index, event.Asset, sequences[event.Asset], sequence)
			}
			sequences[event.Asset] = sequence
		}
		if count != len(publishers)*20 || last != end {
			t.Fatalf("subscriber %d saw %d events, last %d of %d", index, count, last, end)
		}
	}
}

func TestBlockedSubscriberReceivesOneCoalescedOverflow(t *testing.T) {
	hub, err := newHub(Options{}, 4)
	if err != nil {
		t.Fatal(err)
	}
	publishers := startPublishers(t, hub, 1, 1)
	subscription := hub.Subscribe()
	defer subscription.Close()
	for at := int64(1); at <= 10; at++ {
		mustPublish(t, hub, publishers[0].handle, publishers[0].asset, "family_0", at, at)
	}
	latest := hub.Cursor().Seq
	overflow, err := subscription.Next(context.Background())
	if err != nil || overflow.Kind != EventOverflow || overflow.HubSeq != 0 || overflow.Dropped != 6 || overflow.ResumeSeq != latest-3 {
		t.Fatalf("overflow=%+v err=%v", overflow, err)
	}
	for want := latest - 3; want <= latest; want++ {
		event, err := subscription.Next(context.Background())
		if err != nil || event.HubSeq != want || event.Kind == EventOverflow {
			t.Fatalf("event=%+v want HubSeq %d", event, want)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := subscription.Next(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drained subscription returned %v", err)
	}
	if hub.Cursor().Seq != latest {
		t.Fatal("overflow consumed a HubSeq")
	}
}

func TestCommitLatencyIsBoundedWithAPermanentlyBlockedSubscriber(t *testing.T) {
	const queueLimit = 16
	hub, err := newHub(Options{}, queueLimit)
	if err != nil {
		t.Fatal(err)
	}
	publishers := startPublishers(t, hub, 3, 1)
	control, err := newHub(Options{}, queueLimit)
	if err != nil {
		t.Fatal(err)
	}
	controlPublishers := startPublishers(t, control, 3, 1)
	blocked := hub.Subscribe()

	// Each worker owns its samples until it signals completion. No consumer
	// runs before all workers finish or the independent wall-clock watchdog
	// fails the test. The control performs the same commits without fan-out.
	type samples struct {
		blocked []time.Duration
		control []time.Duration
	}
	results := make([]samples, len(publishers))
	ready := make(chan struct{}, len(publishers))
	start := make(chan struct{})
	stop := make(chan struct{})
	done := make(chan struct{}, len(publishers))
	for index, p := range publishers {
		go func() {
			defer func() { done <- struct{}{} }()
			ready <- struct{}{}
			<-start
			commit := func(h *Hub, p publisher, at int64, durations *[]time.Duration) bool {
				tx, err := h.Begin(p.handle, p.asset)
				if err != nil {
					t.Error(err)
					return false
				}
				batch := testBatch(t, tx, p.family, at, at)
				if _, _, err := tx.Apply(batch, testMono(at)); err != nil {
					t.Error(err)
					tx.Abort()
					return false
				}
				started := time.Now()
				_, err = tx.Commit(nil)
				elapsed := time.Since(started)
				if err != nil {
					t.Error(err)
					return false
				}
				*durations = append(*durations, elapsed)
				return true
			}
			for at := int64(1); at <= 100; at++ {
				select {
				case <-stop:
					return
				default:
				}
				// Alternate order so neither hub always gets a warm worker or
				// systematically samples a different part of a host load burst.
				if at%2 == 0 {
					if !commit(control, controlPublishers[index], at, &results[index].control) {
						return
					}
				}
				if !commit(hub, p, at, &results[index].blocked) {
					return
				}
				if at%2 != 0 {
					if !commit(control, controlPublishers[index], at, &results[index].control) {
						return
					}
				}
			}
		}()
	}
	for range publishers {
		<-ready
	}
	// This is a liveness deadline for the entire workload, independent of
	// the measured control allowance. It cannot expand with a blocked call.
	const watchdog = 30 * time.Second
	timer := time.NewTimer(watchdog)
	defer timer.Stop()
	close(start)
	completed := 0
	for completed < len(publishers) {
		select {
		case <-done:
			completed++
		case <-timer.C:
			// A fan-out regression may hold the bus lock while waiting for
			// queue space, so Close alone could deadlock. Release by draining
			// first, stop further work, then join both producers and consumer.
			close(stop)
			ctx, cancel := context.WithCancel(context.Background())
			drained := make(chan struct{})
			go func() {
				defer close(drained)
				for {
					if _, err := blocked.Next(ctx); err != nil {
						return
					}
				}
			}()
			cleanup := time.NewTimer(watchdog)
			for completed < len(publishers) {
				select {
				case <-done:
					completed++
				case <-cleanup.C:
					cancel()
					// Cancellation cannot interrupt Next waiting on blocked.mu.
					select {
					case <-drained:
					case <-time.After(watchdog):
						t.Fatal("commit watchdog expired; publishers and subscriber consumer did not join after cancellation")
					}
					t.Fatal("commit watchdog expired; publishers did not join after subscriber release")
				}
			}
			cleanup.Stop()
			cancel()
			select {
			case <-drained:
			case <-time.After(watchdog):
				t.Fatal("commit watchdog expired; subscriber consumer did not join after cancellation")
			}
			blocked.Close()
			t.Fatalf("commits did not complete within %s while subscriber was undrained; released and joined all workers", watchdog)
		}
	}
	defer blocked.Close()

	var durations, controlDurations []time.Duration
	for _, result := range results {
		durations = append(durations, result.blocked...)
		controlDurations = append(controlDurations, result.control...)
	}
	// Check counts before indexing and prove saturation before any Next/Close.
	if len(durations) != 300 || len(controlDurations) != 300 {
		t.Fatalf("commits=%d control commits=%d, want 300 each before subscriber release", len(durations), len(controlDurations))
	}
	blocked.mu.Lock()
	queued, dropped := len(blocked.queue), blocked.dropped
	blocked.mu.Unlock()
	if queued != queueLimit || dropped != uint64(300-queueLimit) {
		t.Fatalf("undrained queue=%d dropped=%d, want %d and %d", queued, dropped, queueLimit, 300-queueLimit)
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	sort.Slice(controlDurations, func(i, j int) bool { return controlDurations[i] < controlDurations[j] })
	p99 := durations[len(durations)*99/100]
	controlP99 := controlDurations[len(controlDurations)*99/100]
	bound := 5 * time.Millisecond
	if raceEnabled {
		bound *= 10
	}
	// The contract promises nonblocking fan-out, not a numeric wall-time SLA.
	// Keep the original unloaded budget; allow twice the matched control p99
	// for host scheduling/GC and hub-lock contention, consistent with the
	// parent issue's 2x slowdown requirement. This is a measured allowance,
	// not time subtracted from Commit: both p99s use real elapsed time, and
	// the independent watchdog still rejects dependence on consumer progress.
	allowance := 2 * controlP99
	t.Logf("commits=%d queue=%d dropped=%d p99=%s control_p99=%s baseline=%s scheduling_allowance=%s", len(durations), queued, dropped, p99, controlP99, bound, allowance)
	if p99 >= bound+allowance {
		t.Fatalf("p99=%s exceeds baseline=%s plus scheduling allowance=%s (control p99=%s)", p99, bound, allowance, controlP99)
	}
}

func TestReentrantSubscriberPublishingDoesNotDeadlockOrLeak(t *testing.T) {
	baseline := runtime.NumGoroutine()
	hub := newTestHub(t)
	publishers := startPublishers(t, hub, 2, 1)
	subscription := hub.Subscribe()
	done := make(chan error, 1)
	go func() {
		var at int64 = 100
		for i := 0; i < 5; i++ {
			event, err := subscription.Next(context.Background())
			if err != nil {
				done <- err
				return
			}
			if event.Kind == EventPublished && event.Asset == publishers[0].asset {
				at++
				if _, err := publish(t, hub, publishers[1].handle, publishers[1].asset, "family_1", at, at); err != nil {
					done <- err
					return
				}
			}
		}
		done <- nil
	}()
	for at := int64(1); at <= 3; at++ {
		mustPublish(t, hub, publishers[0].handle, publishers[0].asset, "family_0", at, at)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("re-entrant subscriber deadlocked")
	}
	subscription.Close()
	if _, err := subscription.Next(context.Background()); !errors.Is(err, ErrSubscriptionClosed) {
		t.Fatalf("closed subscription=%v", err)
	}
	waitFor(t, func() bool { return runtime.NumGoroutine() <= baseline })
}

func TestSinceResumesOrRequiresResync(t *testing.T) {
	hub, err := newHub(Options{}, SubscriberQueueLimit)
	if err != nil {
		t.Fatal(err)
	}
	publishers := startPublishers(t, hub, 1, 1)
	mustPublish(t, hub, publishers[0].handle, publishers[0].asset, "family_0", 1, 1)
	cursor := hub.Cursor()
	mustPublish(t, hub, publishers[0].handle, publishers[0].asset, "family_0", 2, 2)
	events, err := hub.Since(cursor)
	if err != nil || len(events) != 1 || events[0].HubSeq != cursor.Seq+1 {
		t.Fatalf("since=%+v err=%v", events, err)
	}
	for name, bad := range map[string]Cursor{
		"other incarnation": {Incarnation: "other", Seq: cursor.Seq},
		"beyond latest":     {Incarnation: hub.Incarnation(), Seq: cursor.Seq + 10},
	} {
		if _, err := hub.Since(bad); !errors.Is(err, ErrResyncRequired) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	for at := int64(3); at <= replayRingLimit+5; at++ {
		mustPublish(t, hub, publishers[0].handle, publishers[0].asset, "family_0", at, at)
	}
	if _, err := hub.Since(cursor); !errors.Is(err, ErrResyncRequired) {
		t.Fatalf("cursor older than the ring: %v", err)
	}
}

// TestScriptedEventStreamGolden pins the byte-stable event stream of one
// single-publisher scenario with declared epochs and fixed clocks.
func TestScriptedEventStreamGolden(t *testing.T) {
	hub := newTestHub(t)
	mustDeclare(t, hub, "asset:golden", "source:golden", "golden", func(ControlEvent) error { return nil })
	mustDeclare(t, hub, "asset:golden-idle", "source:golden", "golden", nil)
	handle, err := hub.AdmitSource(Admission{Source: "source:golden", Driver: "golden", Legacy: LegacyGrowattStorage, Epoch: "epoch:golden-1", Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, hub, handle, "asset:golden", "golden", 10, 1)
	mustPublish(t, hub, handle, "asset:golden", "golden", 11, 2)
	if _, err := hub.Fence(testFence("source:golden", "epoch:golden-1", 1, 3)); err != nil {
		t.Fatal(err)
	}
	next, err := hub.Activate(handle, 2)
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, hub, next, "asset:golden", "golden", 12, 4)
	events, err := hub.Since(Cursor{Incarnation: hub.Incarnation()})
	if err != nil {
		t.Fatal(err)
	}
	for index := range events {
		if events[index].Control != nil {
			events[index].Control.Incarnation = "<incarnation>"
		}
	}
	got, err := json.MarshalIndent(events, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')
	path := filepath.Join("testdata", "scripted_event_stream.golden.json")
	if os.Getenv("HELIANTHUS_UPDATE_CHARACTERIZATION") == "1" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("event stream changed:\n%s", got)
	}
}
