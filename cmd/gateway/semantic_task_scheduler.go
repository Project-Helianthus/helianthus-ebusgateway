package main

import (
	"context"
	"errors"
	"sync"
	"time"
)

var errSemanticTaskQueueOverloaded = errors.New("semantic task queue overloaded")

type semanticTaskPriority int

const (
	semanticTaskPriorityLow semanticTaskPriority = iota
	semanticTaskPriorityMedium
	semanticTaskPriorityHigh
	semanticTaskPriorityEmergency semanticTaskPriority = 100

	defaultSemanticTaskQueueDepth   = 100
	defaultSemanticTaskPromoteAfter = 15 * time.Second
	defaultSemanticTaskEmergencyAt  = 30 * time.Second
)

type semanticTaskScheduler struct {
	mu sync.Mutex

	cond *sync.Cond

	queue []*semanticTask
	seq   uint64

	maxDepth     int
	promoteAfter time.Duration
	emergencyAt  time.Duration
	now          func() time.Time

	stopped bool
}

type semanticTask struct {
	priority   semanticTaskPriority
	enqueuedAt time.Time
	seq        uint64
	run        func(context.Context)
}

func newSemanticTaskScheduler() *semanticTaskScheduler {
	s := &semanticTaskScheduler{
		maxDepth:     defaultSemanticTaskQueueDepth,
		promoteAfter: defaultSemanticTaskPromoteAfter,
		emergencyAt:  defaultSemanticTaskEmergencyAt,
		now:          time.Now,
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *semanticTaskScheduler) submit(priority semanticTaskPriority, run func(context.Context)) error {
	if s == nil || run == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return context.Canceled
	}
	if s.maxDepth > 0 && len(s.queue) >= s.maxDepth {
		return errSemanticTaskQueueOverloaded
	}

	s.seq++
	s.queue = append(s.queue, &semanticTask{
		priority:   priority,
		enqueuedAt: s.now(),
		seq:        s.seq,
		run:        run,
	})
	s.cond.Signal()
	return nil
}

func (s *semanticTaskScheduler) run(ctx context.Context) {
	if s == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			s.mu.Lock()
			s.stopped = true
			s.mu.Unlock()
			s.cond.Broadcast()
		case <-stop:
		}
	}()
	defer close(stop)

	for {
		task := s.nextTask(ctx)
		if task == nil {
			return
		}
		task.run(ctx)
	}
}

func (s *semanticTaskScheduler) nextTask(ctx context.Context) *semanticTask {
	s.mu.Lock()
	defer s.mu.Unlock()

	for len(s.queue) == 0 && !s.stopped {
		if ctx.Err() != nil {
			s.stopped = true
			return nil
		}
		s.cond.Wait()
	}
	if s.stopped || len(s.queue) == 0 {
		return nil
	}

	idx := s.nextTaskIndexLocked()
	if idx < 0 || idx >= len(s.queue) {
		return nil
	}
	task := s.queue[idx]
	s.queue = append(s.queue[:idx], s.queue[idx+1:]...)
	return task
}

func (s *semanticTaskScheduler) nextTaskIndexLocked() int {
	if len(s.queue) == 0 {
		return -1
	}

	now := s.now()
	bestIdx := 0
	bestPriority := s.effectivePriorityLocked(s.queue[0], now)
	bestSeq := s.queue[0].seq

	for i := 1; i < len(s.queue); i++ {
		currentPriority := s.effectivePriorityLocked(s.queue[i], now)
		if currentPriority > bestPriority {
			bestIdx = i
			bestPriority = currentPriority
			bestSeq = s.queue[i].seq
			continue
		}
		if currentPriority == bestPriority && s.queue[i].seq < bestSeq {
			bestIdx = i
			bestSeq = s.queue[i].seq
		}
	}

	return bestIdx
}

func (s *semanticTaskScheduler) effectivePriorityLocked(task *semanticTask, now time.Time) semanticTaskPriority {
	if task == nil {
		return semanticTaskPriorityLow
	}

	wait := now.Sub(task.enqueuedAt)
	if s.emergencyAt > 0 && wait >= s.emergencyAt {
		return semanticTaskPriorityEmergency
	}

	priority := task.priority
	if s.promoteAfter > 0 && wait >= s.promoteAfter {
		steps := int(wait / s.promoteAfter)
		priority += semanticTaskPriority(steps)
	}
	if priority > semanticTaskPriorityHigh {
		priority = semanticTaskPriorityHigh
	}
	return priority
}
