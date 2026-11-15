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
		// The inline fallback has no worker to wait in; a suspended
		// acquisition is skipped instead.
		if _, suspended := s.suspension(); suspended {
			return
		}
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
// A suspended acquisition waits here, at the task boundary, until it is
// resumed; it is dropped when ctx ends first.
func (s *semanticPollSchedule) withPollLock(ctx context.Context, fn func(context.Context)) {
	if s == nil || fn == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !s.lockResumed(ctx) {
		return
	}
	defer s.pollMu.Unlock()
	fn(ctx)
}

// semanticPollSuspensions holds one entry per suspended poller, keyed by its
// acquisition lock so that every schedule view of one poller shares it. The
// entry exists only while acquisition is suspended; its channel closes on
// resume.
var semanticPollSuspensions sync.Map // *sync.Mutex -> chan struct{}

// suspension returns the resume signal of a suspended acquisition.
func (s *semanticPollSchedule) suspension() (<-chan struct{}, bool) {
	if s == nil || s.pollMu == nil {
		return nil, false
	}
	resumed, ok := semanticPollSuspensions.Load(s.pollMu)
	if !ok {
		return nil, false
	}
	return resumed.(chan struct{}), true
}

// suspendAcquisition stops acquisition at the next task boundary. An
// acquisition that already holds the lock ends as before. Repeated calls keep
// one suspension.
func (s *semanticPollSchedule) suspendAcquisition() {
	if s == nil || s.pollMu == nil {
		return
	}
	semanticPollSuspensions.LoadOrStore(s.pollMu, make(chan struct{}))
}

// resumeAcquisition releases every acquisition waiting on the suspension.
func (s *semanticPollSchedule) resumeAcquisition() {
	if s == nil || s.pollMu == nil {
		return
	}
	if resumed, ok := semanticPollSuspensions.LoadAndDelete(s.pollMu); ok {
		close(resumed.(chan struct{}))
	}
}

// lockResumed takes pollMu once acquisition is not suspended and reports
// false when ctx ends first. The suspension is checked again under the lock,
// so a suspension that precedes the lock is never overtaken.
func (s *semanticPollSchedule) lockResumed(ctx context.Context) bool {
	for {
		if resumed, suspended := s.suspension(); suspended {
			select {
			case <-ctx.Done():
				return false
			case <-resumed:
			}
			continue
		}
		s.pollMu.Lock()
		if _, suspended := s.suspension(); !suspended {
			return true
		}
		s.pollMu.Unlock()
	}
}

// suspendAcquisition and resumeAcquisition gate the poller's acquisition
// between eBUS generations; see the schedule methods.
func (p *vaillantSemanticPoller) suspendAcquisition() {
	if p != nil {
		(&semanticPollSchedule{pollMu: &p.pollMu}).suspendAcquisition()
	}
}

func (p *vaillantSemanticPoller) resumeAcquisition() {
	if p != nil {
		(&semanticPollSchedule{pollMu: &p.pollMu}).resumeAcquisition()
	}
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
