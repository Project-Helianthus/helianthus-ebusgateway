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

	// The consumer cannot call Next until all commits and their observable
	// effects have been proven. Time is only a diagnostic for a hung test.
	const watchdog = 5 * time.Minute
	watchdogCtx, stopWatchdog := context.WithTimeout(context.Background(), watchdog)
	defer stopWatchdog()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type commits struct {
		blocked int
		control int
	}
	results := make([]commits, len(publishers))
	commitsProven := make(chan struct{})
	subscriberReleased := make(chan struct{})
	cleanupRequested := make(chan struct{})
	var releaseOnce sync.Once
	releaseSubscriber := func() {
		select {
		case <-commitsProven:
		default:
			t.Error("subscriber released before commit completion was proven")
		}
		releaseOnce.Do(func() { close(subscriberReleased) })
	}
	consumerDone := make(chan struct{})
	events := make(chan Event, queueLimit+1)
	go func() {
		defer close(consumerDone)
		<-subscriberReleased
		select {
		case <-commitsProven:
		case <-cleanupRequested:
		default:
			t.Error("consumer admitted before commit completion was proven")
		}
		for {
			event, err := blocked.Next(ctx)
			if err != nil {
				return
			}
			// Retain the overflow and surviving queue for assertions; keep
			// draining during failure cleanup to unblock regressed producers.
			select {
			case events <- event:
			default:
			}
		}
	}()
	ready := make(chan struct{}, len(publishers))
	start := make(chan struct{})
	stop := make(chan struct{})
	done := make(chan struct{}, len(publishers))
	for index, p := range publishers {
		go func() {
			defer func() { done <- struct{}{} }()
			ready <- struct{}{}
			<-start
			commit := func(h *Hub, p publisher, at int64, count *int) bool {
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
				_, err = tx.Commit(nil)
				if err != nil {
					t.Error(err)
					return false
				}
				(*count)++
				return true
			}
			for at := int64(1); at <= 100; at++ {
				select {
				case <-stop:
					return
				default:
				}
				// Exercise the same bounded workload with and without fan-out.
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
	completed := 0
	defer func() {
		close(stop)
		close(cleanupRequested)
		releaseOnce.Do(func() { close(subscriberReleased) })
		// A queue-space regression may hold the bus lock, so drain before
		// Close, and join producers before canceling the consumer.
		cleanup := time.NewTimer(30 * time.Second)
		defer cleanup.Stop()
	joinProducers:
		for completed < len(publishers) {
			select {
			case <-done:
				completed++
			case <-cleanup.C:
				t.Error("cleanup watchdog expired; producers did not join after subscriber release")
				break joinProducers
			}
		}
		cleanup.Stop()
		cancel()
		consumerCleanup := time.NewTimer(30 * time.Second)
		defer consumerCleanup.Stop()
		select {
		case <-consumerDone:
			if completed == len(publishers) {
				blocked.Close()
			}
		case <-consumerCleanup.C:
			t.Error("cleanup watchdog expired; consumer did not join after cancellation")
		}
	}()
	// Always open the producer barrier, including an early readiness failure.
	defer func() {
		select {
		case <-start:
		default:
			close(start)
		}
	}()
	for range publishers {
		select {
		case <-ready:
		case <-watchdogCtx.Done():
			t.Fatal("hang watchdog expired waiting for producer readiness")
		}
	}
	close(start)
	for completed < len(publishers) {
		select {
		case <-done:
			completed++
		case <-watchdogCtx.Done():
			t.Fatal("hang watchdog expired waiting for commits with subscriber held")
		}
	}

	select {
	case <-subscriberReleased:
		t.Fatal("subscriber was released before all commit barriers completed")
	default:
	}
	committed, controlCommitted := 0, 0
	for _, result := range results {
		if result.blocked != 100 || result.control != 100 {
			t.Fatalf("worker commits=%d control commits=%d, want 100 each", result.blocked, result.control)
		}
		committed += result.blocked
		controlCommitted += result.control
	}
	if committed != 300 || controlCommitted != 300 {
		t.Fatalf("commits=%d control commits=%d, want 300 each before subscriber release", committed, controlCommitted)
	}
	blocked.mu.Lock()
	queued, dropped := len(blocked.queue), blocked.dropped
	closed := blocked.closed
	blocked.mu.Unlock()
	if closed || queued != queueLimit || dropped != uint64(300-queueLimit) {
		t.Fatalf("undrained closed=%t queue=%d dropped=%d, want false, %d and %d", closed, queued, dropped, queueLimit, 300-queueLimit)
	}
	for index, p := range publishers {
		for _, target := range []struct {
			hub       *Hub
			publisher publisher
		}{{hub, p}, {control, controlPublishers[index]}} {
			next, err := target.hub.NextSequence(target.publisher.asset, semreg.SourceID("source:"+p.family), semreg.SourceEpochID("epoch:"+p.family), 1)
			if err != nil || next != "101" {
				t.Fatalf("next sequence=%s err=%v, want 101 before subscriber release", next, err)
			}
		}
	}
	close(commitsProven)
	releaseSubscriber()
	latest := hub.Cursor().Seq
	nextEvent := func() Event {
		t.Helper()
		select {
		case event := <-events:
			return event
		case <-watchdogCtx.Done():
			t.Fatal("hang watchdog expired waiting for released subscriber")
			return Event{}
		}
	}
	overflow := nextEvent()
	if overflow.Kind != EventOverflow || overflow.Dropped != 284 || overflow.ResumeSeq != latest-15 {
		t.Fatalf("overflow=%+v", overflow)
	}
	for want := latest - 15; want <= latest; want++ {
		event := nextEvent()
		if event.Kind != EventPublished || event.HubSeq != want {
			t.Fatalf("event=%+v, want published HubSeq %d", event, want)
		}
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
