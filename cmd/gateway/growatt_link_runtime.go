package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	canbusreg "github.com/Project-Helianthus/helianthus-canbusreg"
	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
)

// Native contract: helianthus-docs-canbus, growatt-low-voltage-bms-can-link-protocol-v1.md:
// Transport, addressing and data validity; Cell and sensor counts;
// Identify the peer before interpreting version-dependent fields.
// This factory is deliberately unregistered. Association is explicit caller
// context; neither arrival order nor a PCS stream can supply it.
func newGrowattLINKManagedDriverFactory(config ebusgateway.GrowattLINKConfig, open growattProjectOpen, associate growattProjectAssociation) runtimecontrol.LifecycleAdapter {
	if open == nil {
		open = openGrowattProjectSocket
	}
	return runtimecontrol.NewAdapter(func(ctx context.Context, _ runtimecontrol.Construction) (*growattLINKManagedDriver, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		d := &growattLINKManagedDriver{}
		p, err := newGrowattLINKProjectProvider(config, func(name string) (growattProjectReceiver, error) {
			r, err := open(name)
			if r == nil {
				return nil, err
			}
			return &growattLINKReceiver{growattProjectReceiver: r, lifetime: d.lifetime}, err
		}, func(s canbusreg.CXSelection, e canbusreg.Evidence) canbusreg.CX16ReplayInput {
			in := associate(s, e)
			// The unbuffered native source applies observations before receiving
			// their successors. Capture confirmed counts before bounded history
			// can evict them, even when no managed snapshot has been requested.
			for n, selection := range d.provider.selections {
				if selection == s {
					native := d.provider.providers[n].CX17Snapshot(e.Monotonic, d.provider.staleAfter)
					d.metadataMu.Lock()
					d.retainCounts(native)
					d.metadataMu.Unlock()
					break
				}
			}
			d.metadataMu.Lock()
			history := d.counts[s]
			for _, c := range history {
				if c.evidence == e {
					// The first association for an exact observation is the one
					// native admission saw; a replay of it cannot rebind the count.
					d.metadataMu.Unlock()
					return in
				}
			}
			if len(history) == 256 {
				copy(history, history[1:])
				history = history[:255]
			}
			d.counts[s] = append(history, growattLINKCount{s, e, in.CellCount, in.CellCountValid})
			d.metadataMu.Unlock()
			return in
		})
		if config.Enabled && associate == nil {
			return nil, errors.New("explicit LINK association required")
		}
		if err != nil {
			return nil, err
		}
		d.provider = p
		return d, nil
	})
}

type growattLINKReceiver struct {
	growattProjectReceiver
	lifetime context.Context
}

func (r *growattLINKReceiver) Receive(ctx context.Context) (growattProjectRecord, error) {
	merged, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.lifetime, cancel)
	defer stop()
	defer cancel()
	record, err := r.growattProjectReceiver.Receive(merged)
	if r.lifetime.Err() != nil {
		return growattProjectRecord{}, r.lifetime.Err()
	}
	return record, err
}

type growattLINKCount struct {
	selection canbusreg.CXSelection
	evidence  canbusreg.Evidence
	count     int
	valid     bool
}
type growattLINKFence struct {
	cancel context.CancelFunc
	fenced atomic.Bool
}
type growattLINKManagedDriver struct {
	mu                   sync.Mutex
	provider             *growattProjectProvider
	generation, revision uint64
	active               bool
	operation            chan struct{}
	cleanupErr           error
	quarantine           bool
	lifetime             context.Context
	fence                atomic.Pointer[growattLINKFence]
	metadataMu           sync.Mutex
	counts               map[canbusreg.CXSelection][]growattLINKCount
	admitted             map[canbusreg.CXSelection]growattLINKAdmitted
}

// growattLINKAdmitted is the latest count seen on an admitted descriptive
// record. It outlives eviction from the bounded diagnostic ring so duplicate
// observations cannot invalidate coverage of unchanged accepted cells.
type growattLINKAdmitted struct {
	sequence  uint64
	monotonic time.Duration
	count     int
	valid     bool
}

var _ drivermanager.Runtime = (*growattLINKManagedDriver)(nil)

func (d *growattLINKManagedDriver) Start(ctx context.Context) (uint64, error) {
	d.mu.Lock()
	if err := ctx.Err(); err != nil {
		d.mu.Unlock()
		return 0, err
	}
	if d.quarantine || d.operation != nil || d.provider == nil {
		d.mu.Unlock()
		return 0, drivermanager.ErrUnavailable
	}
	if d.active {
		if f := d.fence.Load(); f == nil || f.fenced.Load() {
			d.mu.Unlock()
			return 0, drivermanager.ErrUnavailable
		}
		g := d.generation
		d.mu.Unlock()
		return g, nil
	}
	if d.generation == ^uint64(0) {
		d.mu.Unlock()
		return 0, errors.New("LINK generation exhausted")
	}
	d.metadataMu.Lock()
	d.counts = make(map[canbusreg.CXSelection][]growattLINKCount)
	d.admitted = make(map[canbusreg.CXSelection]growattLINKAdmitted)
	d.metadataMu.Unlock()
	d.generation++
	generation := d.generation
	lifetime, cancel := context.WithCancel(context.Background())
	d.lifetime = lifetime
	fence := &growattLINKFence{cancel: cancel}
	d.fence.Store(fence)
	done := make(chan struct{})
	var result error
	d.operation = done
	d.mu.Unlock()
	go func() {
		err := d.provider.Start(generation)
		d.mu.Lock()
		if err == nil {
			err = ctx.Err()
		}
		if err == nil && fence.fenced.Load() {
			err = context.Canceled
		}
		if err != nil {
			fence.fenced.Store(true)
			cancel()
			d.mu.Unlock()
			closeErr := d.provider.Stop()
			d.mu.Lock()
			d.cleanupErr = closeErr
			d.quarantine = closeErr != nil
			err = errors.Join(err, closeErr)
		} else {
			d.active = true
		}
		result = err
		d.operation = nil
		d.revision++
		close(done)
		d.mu.Unlock()
	}()
	select {
	case <-done:
		// result is written before done closes; later operations cannot alter it.
		return generation, result
	case <-ctx.Done():
		d.mu.Lock()
		fence.fenced.Store(true)
		cancel()
		if d.fence.Load() == fence && d.active && d.operation == nil {
			d.beginStopLocked()
		}
		d.mu.Unlock()
		return 0, ctx.Err()
	}
}
func (d *growattLINKManagedDriver) FenceWithdrawal() {
	if f := d.fence.Load(); f != nil {
		f.fenced.Store(true)
		f.cancel()
	}
}
func (d *growattLINKManagedDriver) Stop(ctx context.Context) error {
	d.FenceWithdrawal()
	d.mu.Lock()
	for d.operation != nil {
		done := d.operation
		waited := d.generation
		d.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
		d.mu.Lock()
		if d.generation != waited {
			// A restart began after the awaited operation; the successor
			// generation is not this caller's to stop.
			d.mu.Unlock()
			return nil
		}
	}
	if !d.active {
		err := d.cleanupErr
		d.mu.Unlock()
		return err
	}
	done, result := d.beginStopLocked()
	d.mu.Unlock()
	select {
	case <-done:
		return *result
	case <-ctx.Done():
		return ctx.Err()
	}
}

// beginStopLocked assigns cleanup to this generation before releasing mu.
// A delayed cancellation can therefore never stop a successor generation.
func (d *growattLINKManagedDriver) beginStopLocked() (chan struct{}, *error) {
	d.active = false
	done := make(chan struct{})
	d.operation = done
	result := new(error)
	go func() {
		err := d.provider.Stop()
		d.mu.Lock()
		defer d.mu.Unlock()
		*result = err
		d.cleanupErr = err
		d.quarantine = err != nil
		d.operation = nil
		d.revision++
		close(done)
	}()
	return done, result
}
func (d *growattLINKManagedDriver) Replace(ctx context.Context) (uint64, error) {
	if err := d.Stop(ctx); err != nil {
		return 0, err
	}
	return d.Start(ctx)
}
func (d *growattLINKManagedDriver) Generation() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.generation
}
func (d *growattLINKManagedDriver) Revision() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.revision
}
func (d *growattLINKManagedDriver) SafetyQuarantined() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.quarantine
}

type growattLINKSnapshot struct {
	Native     growattProjectSnapshot
	Cells      []canbusreg.CX12VectorSnapshot
	State      string
	Generation uint64
}

// Snapshot only reads bounded native caches. During transition it withholds
// interpretation without waiting for a potentially blocked open or close.
func (d *growattLINKManagedDriver) Snapshot(now time.Duration) growattLINKSnapshot {
	d.mu.Lock()
	defer d.mu.Unlock()
	s := growattLINKSnapshot{State: "stopped", Generation: d.generation}
	if d.provider == nil {
		s.State = "disabled"
		return s
	}
	if d.operation != nil {
		s.State = "transitioning"
		return s
	}
	// Freeze retained counts before capturing native fields so callbacks cannot
	// combine a later admitted count with an earlier native observation.
	d.metadataMu.Lock()
	defer d.metadataMu.Unlock()
	s.Native = d.provider.Snapshot(now)
	f := d.fence.Load()
	usable := d.active && f != nil && !f.fenced.Load() && s.Native.ReceiveError == nil
	if usable {
		s.State = "receiving"
	} else if d.active && s.Native.ReceiveError != nil {
		s.State = "degraded"
	}
	if d.quarantine {
		s.State = "quarantined"
	}
	for n := range s.Native.Providers {
		p := &s.Native.Providers[n]
		if !usable {
			p.Current = map[string]canbusreg.CXNativeField{}
			p.Promotable = false
		}
		v := canbusreg.CX12VectorSnapshot{Selection: p.Selection, Kind: "cells", AddressableSlots: 16, AsOf: now, MaxAge: d.provider.staleAfter, Fields: map[int]canbusreg.CXNativeField{}, LastGood: map[int]canbusreg.CXNativeField{}}
		d.retainCounts(*p)
		if a, ok := d.admitted[p.Selection]; ok {
			v.ConfiguredCount, v.CountValid = a.count, a.valid
		}
		var minAge, maxAge, first, last time.Duration
		for i := 1; i <= 16; i++ {
			key := fmt.Sprintf("cells/cell_%d", i)
			if g, ok := p.LastGood[key]; ok {
				v.LastGood[i] = g
			}
			field, ok := p.Current[key]
			if !ok {
				continue
			}
			v.Fields[i] = field
			v.ReceivedIndices = append(v.ReceivedIndices, i)
			mono := field.Evidence.Monotonic
			if len(v.ReceivedIndices) == 1 || mono < first {
				first = mono
			}
			if mono > last {
				last = mono
			}
			if field.Status == "accepted" {
				v.ValidIndices = append(v.ValidIndices, i)
				if len(v.ValidIndices) == 1 || field.Age < minAge {
					minAge = field.Age
				}
				if field.Age > maxAge {
					maxAge = field.Age
				}
			}
		}
		v.Span = maxAge - minAge
		v.ReceivedSpan = last - first
		v.CompleteCoverage = v.CountValid
		for i := 1; i <= v.ConfiguredCount && i <= 16; i++ {
			if field, ok := v.Fields[i]; !ok || field.Status != "accepted" {
				v.CompleteCoverage = false
			}
		}
		s.Cells = append(s.Cells, v)
	}
	return s
}

// retainCounts requires metadataMu. Only native-confirmed descriptive records
// can update the retained count; rejected or unrelated observations cannot.
func (d *growattLINKManagedDriver) retainCounts(p canbusreg.CX17Snapshot) {
	for j := len(p.Raw) - 1; j >= 0; j-- {
		r := p.Raw[j]
		if r.Generation != p.Generation || r.Status != "descriptive" {
			continue
		}
		for k := len(d.counts[p.Selection]) - 1; k >= 0; k-- {
			c := d.counts[p.Selection][k]
			if c.evidence != r.Input.Evidence {
				continue
			}
			a, ok := d.admitted[p.Selection]
			if !ok || (c.evidence.Sequence > a.sequence && c.evidence.Monotonic >= a.monotonic) {
				d.admitted[p.Selection] = growattLINKAdmitted{c.evidence.Sequence, c.evidence.Monotonic, c.count, c.valid && c.count >= 1 && c.count <= 16}
			}
			return
		}
	}
}
