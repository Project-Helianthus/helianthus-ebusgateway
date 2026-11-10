package main

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

// semanticPollTask describes submission only; refresh owns acquisition and publication.
type semanticPollTask struct {
	key      semanticTaskKey
	priority semanticTaskPriority
	interval time.Duration
	refresh  func(context.Context)
}

type semanticPollTicker struct {
	C    <-chan time.Time
	stop func()
}

func (t *semanticPollTicker) Stop() { t.stop() }

type semanticPollSchedule struct {
	tasks     *semanticTaskScheduler
	pollMu    *sync.Mutex
	barrier   <-chan struct{}
	initial   []semanticPollTask
	recurring []semanticPollTask
	adapter   func(context.Context)
	newTicker func(time.Duration) *semanticPollTicker
}

func (s *semanticPollSchedule) Start(ctx context.Context) {
	if s == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	go s.tasks.run(ctx)
	if s.barrier != nil {
		go func() {
			select {
			case <-ctx.Done():
				return
			case <-s.barrier:
			}
			log.Printf("semantic poller: startup barrier cleared, starting polling loops")
			s.startPollingLoops(ctx)
		}()
		return
	}
	s.startPollingLoops(ctx)
}

func (s *semanticPollSchedule) startPollingLoops(ctx context.Context) {
	for _, task := range s.initial {
		s.enqueueTask(task.key, task.priority, task.refresh)
	}
	for _, task := range s.recurring {
		go s.runLoop(ctx, task.interval, task.key, task.priority, task.refresh)
	}
	if s.adapter != nil {
		go s.adapter(ctx)
	}
}

func (s *semanticPollSchedule) runLoop(ctx context.Context, interval time.Duration, key semanticTaskKey, priority semanticTaskPriority, fn func(context.Context)) {
	if interval <= 0 {
		return
	}
	ticker := s.ticker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.enqueueTask(key, priority, fn)
		}
	}
}

func (s *semanticPollSchedule) ticker(interval time.Duration) *semanticPollTicker {
	if s != nil && s.newTicker != nil {
		return s.newTicker(interval)
	}
	ticker := time.NewTicker(interval)
	return &semanticPollTicker{C: ticker.C, stop: ticker.Stop}
}

func (s *semanticPollSchedule) enqueueTask(key semanticTaskKey, priority semanticTaskPriority, fn func(context.Context)) {
	if s == nil || fn == nil {
		return
	}
	if s.tasks == nil {
		fn(context.Background())
		return
	}
	err := s.tasks.submitCoalesced(key, priority, func(ctx context.Context) { s.withPollLock(ctx, fn) })
	if errors.Is(err, errSemanticTaskQueueOverloaded) {
		log.Printf("semantic poll scheduler overloaded: skipping task key=%s priority=%d", key, priority)
		return
	}
	if err != nil {
		log.Printf("semantic poll scheduler submit failed key=%s err=%v", key, err)
	}
}

// pollMu is shared with direct acquisition callers; the nil-queue fallback is inline.
func (s *semanticPollSchedule) withPollLock(ctx context.Context, fn func(context.Context)) {
	if s == nil || fn == nil {
		return
	}
	s.pollMu.Lock()
	defer s.pollMu.Unlock()
	fn(ctx)
}

func (p *vaillantSemanticPoller) pollSchedule() *semanticPollSchedule {
	if p == nil {
		return nil
	}
	schedule := &semanticPollSchedule{
		tasks: p.tasks, pollMu: &p.pollMu, barrier: p.startupBarrier, adapter: p.adapterInfo.run,
		initial: []semanticPollTask{
			{key: semanticTaskRefreshDiscovery, priority: semanticTaskPriorityHigh, refresh: p.refreshDiscovery},
			{key: semanticTaskRefreshSchedules, priority: semanticTaskPriorityLow, refresh: p.refreshSchedules},
		},
		recurring: []semanticPollTask{
			{key: semanticTaskRefreshRegulatorCapability, interval: p.regulatorRecheckInterval, priority: semanticTaskPriorityLow, refresh: p.refreshRegulatorCapability},
			{key: semanticTaskRefreshDiscovery, interval: p.discoveryInterval, priority: semanticTaskPriorityLow, refresh: p.refreshDiscovery},
			{key: semanticTaskRefreshConfig, interval: p.configInterval, priority: semanticTaskPriorityMedium, refresh: p.refreshConfig},
			{key: semanticTaskRefreshState, interval: p.stateInterval, priority: semanticTaskPriorityHigh, refresh: p.refreshState},
			{key: semanticTaskRefreshCircuits, interval: p.configInterval, priority: semanticTaskPriorityLow, refresh: p.refreshCircuits},
			{key: semanticTaskRefreshSystem, interval: p.configInterval, priority: semanticTaskPriorityLow, refresh: p.refreshSystem},
			{key: semanticTaskRefreshRadioDevices, interval: p.configInterval, priority: semanticTaskPriorityLow, refresh: p.refreshRadioDevices},
			{key: semanticTaskRefreshEnergy, interval: p.energyInterval, priority: semanticTaskPriorityMedium, refresh: p.refreshEnergy},
			{key: semanticTaskRefreshSchedules, interval: p.scheduleInterval, priority: semanticTaskPriorityLow, refresh: p.refreshSchedules},
		},
	}
	for _, tier := range p.boilerStatusTierSchedules() {
		schedule.recurring = append(schedule.recurring, semanticPollTask{
			key: boilerStatusTierTaskKey(tier.tier), interval: tier.interval,
			priority: tier.priority, refresh: p.boilerStatusTierTask(tier.tier),
		})
	}
	return schedule
}
