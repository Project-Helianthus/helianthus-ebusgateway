package main

import (
	"context"
	"errors"
	"sync"
	"time"

	canbusreg "github.com/Project-Helianthus/helianthus-canbusreg"
	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
)

// Native contract: helianthus-docs-canbus/protocols/growatt/
// growatt-low-voltage-bms-can-pcs-protocol-v1.md, Profile selection,
// Measurement conversions, and Data validity and consumer obligations.
// This factory delegates native interpretation, without V1.04 fallback.
func newGrowattPCSManagedFactory(config ebusgateway.GrowattPCSConfig, open growattProjectOpen, associate growattProjectAssociation) runtimecontrol.LifecycleAdapter {
	if open == nil {
		open = openGrowattProjectSocket
	}
	return runtimecontrol.NewAdapter(func(ctx context.Context, _ runtimecontrol.Construction) (*growattPCSManagedDriver, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		d := &growattPCSManagedDriver{config: config, open: open}
		p, err := newGrowattPCSProjectProvider(config, d.openReceiver, associate)
		if err != nil {
			return nil, err
		}
		d.provider = p
		return d, nil
	})
}

type growattPCSSession struct {
	ctx    context.Context
	cancel context.CancelFunc
	failed chan struct{}
	once   sync.Once
	bound  bool
}

type growattPCSReceiver struct {
	growattProjectReceiver
	session *growattPCSSession
}

func (r *growattPCSReceiver) Receive(ctx context.Context) (growattProjectRecord, error) {
	stop := context.AfterFunc(ctx, r.session.cancel)
	defer stop()
	record, err := r.growattProjectReceiver.Receive(r.session.ctx)
	if err != nil && ctx.Err() == nil && r.session.ctx.Err() == nil {
		r.session.once.Do(func() { close(r.session.failed) })
	}
	return record, err
}

type growattPCSManagedDriver struct {
	mu                   sync.Mutex
	config               ebusgateway.GrowattPCSConfig
	open                 growattProjectOpen
	provider             *growattProjectProvider
	session              *growattPCSSession
	generation, revision uint64
	active               bool
	starting, draining   chan struct{}
	drainErr             error
	cached               growattProjectSnapshot
	lifecycle            runtimecontrol.LifecycleCorrelation
	reportFailure        func(drivermanager.Correlation, runtimecontrol.LifecycleCorrelation)
}

var _ drivermanager.Runtime = (*growattPCSManagedDriver)(nil)

func (d *growattPCSManagedDriver) openReceiver(name string) (growattProjectReceiver, error) {
	d.mu.Lock()
	s := d.session
	d.mu.Unlock()
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	r, err := d.open(name)
	if r == nil {
		return nil, err
	}
	return &growattPCSReceiver{growattProjectReceiver: r, session: s}, err
}

func (d *growattPCSManagedDriver) Start(ctx context.Context) (uint64, error) {
	d.mu.Lock()
	if err := ctx.Err(); err != nil {
		d.mu.Unlock()
		return 0, err
	}
	if d.drainErr != nil {
		err := d.drainErr
		d.mu.Unlock()
		return 0, err
	}
	if d.starting != nil || d.draining != nil {
		d.mu.Unlock()
		return 0, drivermanager.ErrUnavailable
	}
	if d.active {
		if d.session.ctx.Err() != nil {
			d.mu.Unlock()
			return 0, drivermanager.ErrUnavailable
		}
		g := d.generation
		d.mu.Unlock()
		return g, nil
	}
	if d.provider == nil || d.generation == ^uint64(0) {
		d.mu.Unlock()
		return 0, drivermanager.ErrUnavailable
	}
	lifetime, cancel := context.WithCancel(context.Background())
	s := &growattPCSSession{ctx: lifetime, cancel: cancel, failed: make(chan struct{})}
	done := make(chan struct{})
	d.starting, d.session = done, s
	d.generation++
	g := d.generation
	d.mu.Unlock()
	stop := context.AfterFunc(ctx, cancel)
	err := d.provider.Start(g)
	stop()
	d.mu.Lock()
	if err == nil {
		err = errors.Join(ctx.Err(), lifetime.Err())
	}
	if err != nil {
		cancel()
		d.mu.Unlock()
		closeErr := d.provider.Stop()
		d.mu.Lock()
		if closeErr != nil {
			d.drainErr = errors.Join(drivermanager.ErrSafetyQuarantined, closeErr)
		}
		err = errors.Join(err, d.drainErr)
	} else {
		d.active = true
	}
	d.revision++
	d.starting = nil
	close(done)
	d.mu.Unlock()
	if err != nil {
		return 0, err
	}
	return g, nil
}

func (d *growattPCSManagedDriver) FenceWithdrawal() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.session != nil {
		d.session.cancel()
	}
}

func (d *growattPCSManagedDriver) Stop(ctx context.Context) error {
	d.mu.Lock()
	if d.starting != nil {
		d.session.cancel()
		done := d.starting
		d.mu.Unlock()
		select {
		case <-done:
			return d.Stop(ctx)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if d.draining == nil && d.active {
		d.cached = d.provider.Snapshot(0)
		d.active = false
		d.session.cancel()
		done := make(chan struct{})
		d.draining = done
		go func() {
			err := d.provider.Stop()
			d.mu.Lock()
			if err != nil {
				d.drainErr = errors.Join(drivermanager.ErrSafetyQuarantined, err)
			}
			d.draining = nil
			d.revision++
			close(done)
			d.mu.Unlock()
		}()
	}
	done, err := d.draining, d.drainErr
	d.mu.Unlock()
	if done == nil {
		return err
	}
	select {
	case <-done:
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.drainErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *growattPCSManagedDriver) Replace(ctx context.Context) (uint64, error) {
	if err := d.Stop(ctx); err != nil {
		return 0, err
	}
	return d.Start(ctx)
}
func (d *growattPCSManagedDriver) Generation() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.generation
}
func (d *growattPCSManagedDriver) Revision() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.revision
}
func (d *growattPCSManagedDriver) SafetyQuarantined() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.drainErr != nil
}

func (d *growattPCSManagedDriver) BindLifecycle(c runtimecontrol.LifecycleCorrelation) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lifecycle = c
	return nil
}
func (d *growattPCSManagedDriver) BindCorrelation(c drivermanager.Correlation) {
	d.mu.Lock()
	s, lifecycle, report := d.session, d.lifecycle, d.reportFailure
	if !d.active || s == nil || s.bound || c.Generation != d.generation || report == nil {
		d.mu.Unlock()
		return
	}
	s.bound = true
	d.mu.Unlock()
	go func() {
		select {
		case <-s.failed:
		case <-s.ctx.Done():
			return
		}
		d.mu.Lock()
		current := d.active && d.session == s && s.ctx.Err() == nil
		d.mu.Unlock()
		if current {
			report(c, lifecycle)
		}
	}()
}

// Snapshot only reads detached native caches. It preserves native units, grades,
// raw status, field age and last-good history, including unknown member age.
func (d *growattPCSManagedDriver) Snapshot(now time.Duration) growattProjectSnapshot {
	d.mu.Lock()
	defer d.mu.Unlock()
	var s growattProjectSnapshot
	if d.provider != nil && d.starting == nil && d.draining == nil {
		s = d.provider.Snapshot(now)
	} else {
		// Native caches have their own locks and remain safe during a join.
		s = growattProjectSnapshot{ReceiveError: d.cached.ReceiveError, Quarantined: d.drainErr != nil, Evicted: d.cached.Evicted}
		for _, raw := range d.cached.Raw {
			raw.Record.Raw = append([]byte(nil), raw.Record.Raw...)
			s.Raw = append(s.Raw, raw)
		}
		if d.provider != nil {
			for _, native := range d.provider.providers {
				s.Providers = append(s.Providers, native.CX17Snapshot(now, d.config.StaleAfter))
			}
		}
	}
	if !d.active || d.session == nil || d.session.ctx.Err() != nil {
		for n := range s.Providers {
			s.Providers[n].Current = map[string]canbusreg.CXNativeField{}
			s.Providers[n].Promotable = false
		}
	}
	return s
}
