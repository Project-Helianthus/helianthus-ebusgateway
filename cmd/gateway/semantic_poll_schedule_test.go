package main

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"
)

func newScheduleUnderTest(s *semanticPollSchedule) *semanticPollSchedule { return s }

func scheduleWait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("schedule did not reach expected state")
	}
}

func TestSemanticPollScheduleBarrier(t *testing.T) {
	for _, cancelBefore := range []bool{false, true} {
		t.Run(map[bool]string{false: "release_then_cancel", true: "cancel_before_release"}[cancelBefore], func(t *testing.T) {
			barrier := make(chan struct{})
			adapter := make(chan struct{})
			called := make(chan struct{}, 2)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := newScheduleUnderTest(&semanticPollSchedule{barrier: barrier, pollMu: &sync.Mutex{},
				initial: []semanticPollTask{{key: "discovery", refresh: func(context.Context) { called <- struct{}{} }}},
				adapter: func(ctx context.Context) { close(adapter); <-ctx.Done() },
			})
			s.Start(ctx)
			select {
			case <-called:
				t.Fatal("refresh before barrier")
			case <-adapter:
				t.Fatal("adapter before barrier")
			default:
			}
			if cancelBefore {
				cancel()
				return
			}
			close(barrier)
			scheduleWait(t, adapter)
			scheduleWait(t, called)
			cancel()
		})
	}
}

func TestSemanticPollScheduleInitialOrder(t *testing.T) {
	var got []string
	s := newScheduleUnderTest(&semanticPollSchedule{pollMu: &sync.Mutex{}, adapter: func(context.Context) {},
		initial: []semanticPollTask{
			{key: "discovery", priority: semanticTaskPriorityHigh, refresh: func(context.Context) { got = append(got, "discovery") }},
			{key: "schedules", priority: semanticTaskPriorityLow, refresh: func(context.Context) { got = append(got, "schedules") }},
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.startPollingLoops(ctx)
	if !reflect.DeepEqual(got, []string{"discovery", "schedules"}) {
		t.Fatalf("order %v", got)
	}
}

func TestSemanticPollScheduleControlledTicks(t *testing.T) {
	for _, interval := range []time.Duration{-time.Second, 0, 7 * time.Second} {
		t.Run(interval.String(), func(t *testing.T) {
			ticks := make(chan time.Time)
			ready := make(chan struct{})
			stopped := make(chan struct{})
			done := make(chan struct{})
			called := make(chan struct{}, 1)
			s := newScheduleUnderTest(&semanticPollSchedule{pollMu: &sync.Mutex{}, newTicker: func(d time.Duration) *semanticPollTicker {
				if d != interval {
					t.Errorf("interval %v want %v", d, interval)
				}
				close(ready)
				return &semanticPollTicker{C: ticks, stop: func() { close(stopped) }}
			}})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() {
				defer close(done)
				s.runLoop(ctx, interval, "tick", semanticTaskPriorityMedium, func(context.Context) { called <- struct{}{} })
			}()
			if interval <= 0 {
				scheduleWait(t, done)
				select {
				case <-ready:
					t.Fatal("disabled ticker created")
				default:
				}
				return
			}
			scheduleWait(t, ready)
			select {
			case <-called:
				t.Fatal("refresh before first tick")
			default:
			}
			ticks <- time.Time{}
			scheduleWait(t, called)
			cancel()
			scheduleWait(t, done)
			scheduleWait(t, stopped)
			select {
			case <-called:
				t.Fatal("extra refresh after cancellation")
			default:
			}
		})
	}
}

func TestSemanticPollScheduleTaskIdentityCoalescingOverload(t *testing.T) {
	q := newSemanticTaskScheduler()
	q.maxDepth = 2
	s := newScheduleUnderTest(&semanticPollSchedule{tasks: q, pollMu: &sync.Mutex{}})
	var got []string
	s.enqueueTask("first", semanticTaskPriorityLow, func(context.Context) { got = append(got, "first") })
	s.enqueueTask("first", semanticTaskPriorityHigh, func(context.Context) { got = append(got, "replacement") })
	s.enqueueTask("second", semanticTaskPriorityMedium, func(context.Context) { got = append(got, "second") })
	s.enqueueTask("overload", semanticTaskPriorityHigh, func(context.Context) { got = append(got, "overload") })
	if len(q.queue) != 2 {
		t.Fatalf("queue depth %d", len(q.queue))
	}
	if q.queue[0].key != "first" || q.queue[0].priority != semanticTaskPriorityHigh || q.queue[1].key != "second" || q.queue[1].priority != semanticTaskPriorityMedium {
		t.Fatal("identity or priority changed")
	}
	for range 2 {
		task := q.nextTask(context.Background())
		task.run(context.Background())
		q.taskDone(task)
	}
	if !reflect.DeepEqual(got, []string{"first", "second"}) {
		t.Fatalf("callbacks %v", got)
	}
}

func TestSemanticPollScheduleRunningKey(t *testing.T) {
	q := newSemanticTaskScheduler()
	s := newScheduleUnderTest(&semanticPollSchedule{tasks: q, pollMu: &sync.Mutex{}})
	calls := 0
	fn := func(context.Context) { calls++ }
	s.enqueueTask("same", semanticTaskPriorityLow, fn)
	running := q.nextTask(context.Background())
	s.enqueueTask("same", semanticTaskPriorityLow, fn)
	s.enqueueTask("same", semanticTaskPriorityHigh, fn)
	if len(q.queue) != 1 || q.queue[0].priority != semanticTaskPriorityHigh {
		t.Fatal("running key must retain one pending refresh")
	}
	running.run(context.Background())
	q.taskDone(running)
	task := q.nextTask(context.Background())
	task.run(context.Background())
	q.taskDone(task)
	if calls != 2 {
		t.Fatalf("calls %d want 2", calls)
	}
}

func TestSemanticPollScheduleFallback(t *testing.T) {
	s := newScheduleUnderTest(&semanticPollSchedule{pollMu: &sync.Mutex{}})
	s.pollMu.Lock()
	defer s.pollMu.Unlock()
	called := false
	s.enqueueTask("inline", semanticTaskPriorityLow, func(ctx context.Context) {
		called = true
		if ctx.Done() != nil {
			t.Error("fallback context must be background")
		}
	})
	if !called {
		t.Fatal("fallback was not synchronous")
	}
	s.enqueueTask("nil", semanticTaskPriorityLow, nil)
}

func TestSemanticPollScheduleSerialAcquisition(t *testing.T) {
	q := newSemanticTaskScheduler()
	s := newScheduleUnderTest(&semanticPollSchedule{tasks: q, pollMu: &sync.Mutex{}})
	entered := make(chan struct{})
	release := make(chan struct{})
	second := make(chan struct{})
	attempt := make(chan struct{})
	s.enqueueTask("queued", semanticTaskPriorityLow, func(context.Context) { close(entered); <-release })
	task := q.nextTask(context.Background())
	done := make(chan struct{})
	go func() { task.run(context.Background()); close(done) }()
	scheduleWait(t, entered)
	if s.pollMu.TryLock() {
		s.pollMu.Unlock()
		close(release)
		scheduleWait(t, done)
		t.Fatal("queued acquisition did not hold poll mutex")
	}
	directDone := make(chan struct{})
	go func() {
		close(attempt)
		s.withPollLock(context.Background(), func(context.Context) { close(second) })
		close(directDone)
	}()
	scheduleWait(t, attempt)
	select {
	case <-second:
		t.Fatal("overlapping acquisition")
	default:
	}
	close(release)
	scheduleWait(t, done)
	scheduleWait(t, second)
	scheduleWait(t, directDone)
}

func TestSemanticPollScheduleSchedulerCancellation(t *testing.T) {
	q := newSemanticTaskScheduler()
	s := newScheduleUnderTest(&semanticPollSchedule{tasks: q, pollMu: &sync.Mutex{}, barrier: make(chan struct{})})
	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	cancel()
	deadline := time.After(time.Second)
	for {
		q.mu.Lock()
		stopped := q.stopped
		q.mu.Unlock()
		if stopped {
			break
		}
		select {
		case <-deadline:
			t.Fatal("queue did not stop")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	called := false
	s.enqueueTask("late", semanticTaskPriorityHigh, func(context.Context) { called = true })
	if called || len(q.queue) != 0 {
		t.Fatal("stopped scheduler admitted work")
	}
}

func scheduleExpectedTasks() []semanticPollTask {
	keys := []semanticTaskKey{"refresh_regulator_capability", "refresh_discovery", "refresh_config", "refresh_state", "refresh_circuits", "refresh_system", "refresh_radio_devices", "refresh_energy", "refresh_schedules", "refresh_boiler_fast", "refresh_boiler_medium", "refresh_boiler_slow"}
	priorities := []semanticTaskPriority{0, 0, 1, 2, 0, 0, 0, 1, 0, 2, 1, 0}
	intervals := []time.Duration{11, 13, 17, 19, 17, 17, 17, 23, 29, 31, 37, 41}
	tasks := make([]semanticPollTask, len(keys))
	for i, key := range keys {
		tasks[i] = semanticPollTask{key: key, priority: priorities[i], interval: intervals[i] * time.Second}
	}
	return tasks
}

func TestSemanticPollScheduleRecurringTasks(t *testing.T) {
	type tick struct {
		interval time.Duration
		ch       chan time.Time
	}
	ticks := make(chan tick, 12)
	calls := make(map[semanticTaskKey]int)
	q := newSemanticTaskScheduler()
	tasks := scheduleExpectedTasks()
	for i := range tasks {
		key := tasks[i].key
		tasks[i].refresh = func(context.Context) { calls[key]++ }
	}
	adapter := make(chan struct{})
	stops := make(chan struct{}, 12)
	s := newScheduleUnderTest(&semanticPollSchedule{tasks: q, pollMu: &sync.Mutex{}, recurring: tasks,
		initial: []semanticPollTask{{key: "refresh_discovery", priority: 2, refresh: tasks[1].refresh}, {key: "refresh_schedules", priority: 0, refresh: tasks[8].refresh}},
		adapter: func(ctx context.Context) { close(adapter); <-ctx.Done() },
		newTicker: func(d time.Duration) *semanticPollTicker {
			ch := make(chan time.Time)
			ticks <- tick{d, ch}
			return &semanticPollTicker{C: ch, stop: func() { stops <- struct{}{} }}
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.startPollingLoops(ctx)
	scheduleWait(t, adapter)
	seenIntervals := map[time.Duration]int{}
	for range 12 {
		select {
		case tk := <-ticks:
			seenIntervals[tk.interval]++
			tk.ch <- time.Time{}
		case <-time.After(time.Second):
			t.Fatal("missing recurring loop")
		}
	}
	expectedIntervals := map[time.Duration]int{}
	for _, task := range tasks {
		expectedIntervals[task.interval]++
	}
	if !reflect.DeepEqual(seenIntervals, expectedIntervals) {
		t.Fatalf("cadences %v want %v", seenIntervals, expectedIntervals)
	}
	// Each tick send unblocks before enqueue completes; wait for the queue's twelve identities.
	deadline := time.After(time.Second)
	for {
		q.mu.Lock()
		n := len(q.queue)
		q.mu.Unlock()
		if n == 12 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("missing recurring identities")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	for range 12 {
		scheduleWait(t, stops)
	}
	if q.queue[0].key != "refresh_discovery" || q.queue[0].seq != 1 || q.queue[1].key != "refresh_schedules" || q.queue[1].seq != 2 {
		t.Fatal("initial submission order changed")
	}
	for _, want := range tasks {
		got := q.pendingByKey[want.key]
		priority := want.priority
		if want.key == "refresh_discovery" {
			priority = 2
		}
		if got == nil || got.priority != priority {
			t.Fatalf("descriptor %s priority want %d", want.key, priority)
		}
	}
	for range 12 {
		task := q.nextTask(context.Background())
		task.run(context.Background())
		q.taskDone(task)
	}
	for _, task := range tasks {
		if calls[task.key] != 1 {
			t.Fatalf("callback %s called %d times", task.key, calls[task.key])
		}
	}
}

func TestSemanticPollScheduleBinding(t *testing.T) {
	p := &vaillantSemanticPoller{
		tasks: newSemanticTaskScheduler(), startupBarrier: make(chan struct{}),
		regulatorRecheckInterval: 11 * time.Second, discoveryInterval: 13 * time.Second,
		configInterval: 17 * time.Second, stateInterval: 19 * time.Second, energyInterval: 23 * time.Second,
		scheduleInterval: 29 * time.Second, boilerFastInterval: 31 * time.Second,
		boilerMediumInterval: 37 * time.Second, boilerSlowInterval: 41 * time.Second,
	}
	s := p.pollSchedule()
	if s.tasks != p.tasks || s.pollMu != &p.pollMu || s.barrier != p.startupBarrier || s.adapter == nil {
		t.Fatal("runtime ownership wiring changed")
	}
	if len(s.initial) != 2 || s.initial[0].key != "refresh_discovery" || s.initial[0].priority != 2 || s.initial[1].key != "refresh_schedules" || s.initial[1].priority != 0 {
		t.Fatal("initial tasks changed")
	}
	expected := scheduleExpectedTasks()
	if len(s.recurring) != len(expected) {
		t.Fatalf("recurring tasks %d", len(s.recurring))
	}
	callbacks := []func(context.Context){p.refreshRegulatorCapability, p.refreshDiscovery, p.refreshConfig, p.refreshState, p.refreshCircuits, p.refreshSystem, p.refreshRadioDevices, p.refreshEnergy, p.refreshSchedules}
	for i, want := range expected {
		got := s.recurring[i]
		if got.key != want.key || got.priority != want.priority || got.interval != want.interval || got.refresh == nil {
			t.Fatalf("task %d: key=%s priority=%d interval=%v", i, got.key, got.priority, got.interval)
		}
		if i < len(callbacks) && reflect.ValueOf(got.refresh).Pointer() != reflect.ValueOf(callbacks[i]).Pointer() {
			t.Fatalf("task %s callback changed", got.key)
		}
	}
	for i, fn := range []func(context.Context){p.refreshDiscovery, p.refreshSchedules} {
		if reflect.ValueOf(s.initial[i].refresh).Pointer() != reflect.ValueOf(fn).Pointer() {
			t.Fatal("initial callback changed")
		}
	}
}

func TestSemanticPollScheduleNilDelegation(t *testing.T) {
	var nilCtx context.Context
	var p *vaillantSemanticPoller
	p.Start(nilCtx)
	p.enqueueTask("nil", semanticTaskPriorityLow, func(context.Context) { t.Fatal("nil poller refreshed") })
	p.withPollLock(nilCtx, func(context.Context) { t.Fatal("nil poller locked") })
	p = &vaillantSemanticPoller{}
	p.enqueueTask("nil", semanticTaskPriorityLow, nil)
	p.withPollLock(nilCtx, nil)
}
