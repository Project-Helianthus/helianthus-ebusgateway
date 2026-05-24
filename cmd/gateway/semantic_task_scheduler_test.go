package main

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

func TestSemanticTaskScheduler_QueueDepthLimit(t *testing.T) {
	scheduler := newSemanticTaskScheduler()
	scheduler.maxDepth = 1

	if err := scheduler.submit(semanticTaskPriorityLow, func(context.Context) {}); err != nil {
		t.Fatalf("first submit error = %v", err)
	}
	err := scheduler.submit(semanticTaskPriorityLow, func(context.Context) {})
	if !errors.Is(err, errSemanticTaskQueueOverloaded) {
		t.Fatalf("second submit error = %v; want %v", err, errSemanticTaskQueueOverloaded)
	}
}

func TestSemanticTaskScheduler_EmergencyPromotionPreventsStarvation(t *testing.T) {
	now := time.Unix(0, 0)
	scheduler := newSemanticTaskScheduler()
	scheduler.now = func() time.Time { return now }
	scheduler.promoteAfter = 15 * time.Second
	scheduler.emergencyAt = 30 * time.Second

	scheduler.queue = []*semanticTask{
		{
			priority:   semanticTaskPriorityLow,
			enqueuedAt: now.Add(-31 * time.Second),
			seq:        1,
		},
		{
			priority:   semanticTaskPriorityHigh,
			enqueuedAt: now.Add(-1 * time.Second),
			seq:        2,
		},
	}

	got := scheduler.nextTaskIndexLocked()
	if got != 0 {
		t.Fatalf("nextTaskIndexLocked() = %d; want 0 (emergency promoted low-priority task)", got)
	}
}

func TestSemanticTaskScheduler_FIFOWithinBand(t *testing.T) {
	now := time.Unix(0, 0)
	scheduler := newSemanticTaskScheduler()
	scheduler.now = func() time.Time { return now }
	scheduler.promoteAfter = 0
	scheduler.emergencyAt = 0

	scheduler.queue = []*semanticTask{
		{priority: semanticTaskPriorityMedium, enqueuedAt: now, seq: 2},
		{priority: semanticTaskPriorityMedium, enqueuedAt: now, seq: 1},
	}

	got := scheduler.nextTaskIndexLocked()
	if got != 1 {
		t.Fatalf("nextTaskIndexLocked() = %d; want 1 (lower sequence first)", got)
	}
}

func TestSemanticTaskScheduler_RunPriorityOrder(t *testing.T) {
	scheduler := newSemanticTaskScheduler()
	scheduler.promoteAfter = 0
	scheduler.emergencyAt = 0

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := make(chan string, 3)

	if err := scheduler.submit(semanticTaskPriorityLow, func(context.Context) { out <- "low" }); err != nil {
		t.Fatalf("submit low error = %v", err)
	}
	if err := scheduler.submit(semanticTaskPriorityHigh, func(context.Context) { out <- "high" }); err != nil {
		t.Fatalf("submit high error = %v", err)
	}
	if err := scheduler.submit(semanticTaskPriorityMedium, func(context.Context) { out <- "medium" }); err != nil {
		t.Fatalf("submit medium error = %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		scheduler.run(ctx)
	}()

	got := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		select {
		case item := <-out:
			got = append(got, item)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for scheduled task output")
		}
	}
	want := []string{"high", "medium", "low"}
	if !slices.Equal(got, want) {
		t.Fatalf("run order = %v; want %v", got, want)
	}

	cancel()
	<-done
}

func TestSemanticTaskScheduler_CoalescesPendingTasksByKey(t *testing.T) {
	scheduler := newSemanticTaskScheduler()

	if err := scheduler.submitCoalesced("state", semanticTaskPriorityLow, func(context.Context) {}); err != nil {
		t.Fatalf("submit low error = %v", err)
	}
	if err := scheduler.submitCoalesced("state", semanticTaskPriorityHigh, func(context.Context) {}); err != nil {
		t.Fatalf("submit duplicate high error = %v", err)
	}

	if got := len(scheduler.queue); got != 1 {
		t.Fatalf("queue length = %d; want 1", got)
	}
	if got := scheduler.queue[0].priority; got != semanticTaskPriorityHigh {
		t.Fatalf("coalesced priority = %d; want %d", got, semanticTaskPriorityHigh)
	}
}

func TestSemanticTaskScheduler_DefersOneRerunForRunningTaskKey(t *testing.T) {
	scheduler := newSemanticTaskScheduler()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	release := make(chan struct{})
	ran := make(chan string, 3)

	if err := scheduler.submitCoalesced("state", semanticTaskPriorityHigh, func(context.Context) {
		close(started)
		<-release
		ran <- "initial"
	}); err != nil {
		t.Fatalf("submit initial error = %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		scheduler.run(ctx)
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for task start")
	}

	if err := scheduler.submitCoalesced("state", semanticTaskPriorityHigh, func(context.Context) {
		ran <- "deferred"
	}); err != nil {
		t.Fatalf("submit duplicate while running error = %v", err)
	}
	if err := scheduler.submitCoalesced("state", semanticTaskPriorityLow, func(context.Context) {
		ran <- "extra"
	}); err != nil {
		t.Fatalf("submit second duplicate while running error = %v", err)
	}

	close(release)
	select {
	case got := <-ran:
		if got != "initial" {
			t.Fatalf("first task = %q; want initial", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial task completion")
	}
	select {
	case got := <-ran:
		if got != "deferred" {
			t.Fatalf("deferred task = %q; want deferred", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deferred task completion")
	}
	select {
	case got := <-ran:
		t.Fatalf("extra duplicate running task executed: %q", got)
	case <-time.After(25 * time.Millisecond):
	}

	cancel()
	<-done
}
