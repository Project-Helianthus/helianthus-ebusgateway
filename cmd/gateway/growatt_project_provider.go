package main

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	canbus "github.com/Project-Helianthus/helianthus-canbus"
	canbusreg "github.com/Project-Helianthus/helianthus-canbusreg"
	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
)

// The selected adapters preserve native grades and evidence. They do not infer
// family, release, revision labels or physical qualification from a frame ID.
// Native contracts: helianthus-docs-canbus PCS Profile selection / Data validity
// and consumer obligations; LINK Transport, addressing and data validity /
// Identify the peer before interpreting version-dependent fields.
type growattProjectRecord struct {
	Interface canbus.InterfaceIdentity
	Raw       []byte
	Sequence  uint64
	Monotonic time.Duration
}

type growattProjectReceiver interface {
	Identity() canbus.InterfaceIdentity
	Receive(context.Context) (growattProjectRecord, error)
	Close() error
}

type growattProjectOpen func(string) (growattProjectReceiver, error)

// Association supplies explicit native context, including independent validity
// gates and ages. It must return detached input; the adapter supplies only the
// unmodified transport evidence. No native context is inferred from payloads.
type growattProjectAssociation func(canbusreg.CXSelection, canbusreg.Evidence) canbusreg.CX16ReplayInput

type growattProjectSource chan canbusreg.CX17Observation

func (s growattProjectSource) CX17Receive(ctx context.Context) (canbusreg.CX17Observation, error) {
	select {
	case o, ok := <-s:
		if !ok {
			return o, io.EOF
		}
		return o, nil
	case <-ctx.Done():
		return canbusreg.CX17Observation{}, ctx.Err()
	}
}

type growattProjectDiagnostic struct {
	Record     growattProjectRecord
	Generation uint64
	Status     string
}

type growattProjectSnapshot struct {
	Providers    []canbusreg.CX17Snapshot
	Raw          []growattProjectDiagnostic
	Evicted      uint64
	ReceiveError error
	Quarantined  bool
}

type growattProjectProvider struct {
	lifecycle            sync.Mutex
	mu                   sync.Mutex
	selections           []canbusreg.CXSelection
	providers            []*canbusreg.CX17Provider
	sources              []growattProjectSource
	staleAfter           time.Duration
	open                 growattProjectOpen
	associate            growattProjectAssociation
	receiver             growattProjectReceiver
	generation           uint64
	cancel               context.CancelFunc
	done                 chan struct{}
	raw                  []growattProjectDiagnostic
	evicted              uint64
	receiveErr, closeErr error
}

func newGrowattProjectProvider(selections []canbusreg.CXSelection, staleAfter time.Duration, listenOnlyConfirmed bool, open growattProjectOpen, associate growattProjectAssociation) (*growattProjectProvider, error) {
	if !listenOnlyConfirmed || len(selections) == 0 || len(selections) > 32 || staleAfter <= 0 || open == nil || associate == nil {
		return nil, errors.New("selected provider preconditions required")
	}
	p := &growattProjectProvider{selections: append([]canbusreg.CXSelection(nil), selections...), staleAfter: staleAfter, open: open, associate: associate}
	seen := map[uint8]bool{}
	for _, s := range selections {
		c := s.CX01Spec()
		if c.Interface != selections[0].CX01Spec().Interface || (len(selections) > 1 && c.Flavor != "link") || seen[c.Source] {
			return nil, errors.New("one interface and unique selected sources required")
		}
		seen[c.Source] = true
		var native *canbusreg.CX17Provider
		var err error
		if c.Flavor == "link" {
			native, err = canbusreg.CX17NewLINKProvider(s)
		} else {
			native, err = canbusreg.CX17NewPCSProvider(s)
		}
		if err != nil {
			return nil, err
		}
		p.providers = append(p.providers, native)
	}
	return p, nil
}

// Start opens exactly one selected interface. The caller must have validated
// listen-only confirmation before construction. It is never configured here.
func (p *growattProjectProvider) Start(generation uint64) error {
	p.lifecycle.Lock()
	defer p.lifecycle.Unlock()
	if generation == 0 || generation <= p.generation {
		return errors.New("strictly increasing positive generation required")
	}
	if err := p.stop(); err != nil {
		return err
	}
	c := p.selections[0].CX01Spec()
	r, err := p.open(c.Interface.Name())
	if err != nil {
		if r != nil {
			p.receiver = r
			return errors.Join(err, p.stop())
		}
		return err
	}
	if r == nil {
		return errors.New("nil receiver")
	}
	p.receiver = r
	if r.Identity() != c.Interface {
		return errors.Join(errors.New("receiver identity mismatch"), p.stop())
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.sources = make([]growattProjectSource, len(p.providers))
	for n, native := range p.providers {
		p.sources[n] = make(growattProjectSource)
		if err := native.Start(p.sources[n], generation); err != nil {
			cancel()
			for _, started := range p.providers {
				started.Stop()
			}
			return errors.Join(err, p.stop())
		}
	}
	p.generation = generation
	p.cancel, p.done = cancel, make(chan struct{})
	p.mu.Lock()
	p.receiveErr = nil
	p.mu.Unlock()
	go p.receive(ctx, r, generation, p.done)
	return nil
}

func (p *growattProjectProvider) receive(ctx context.Context, r growattProjectReceiver, generation uint64, done chan struct{}) {
	defer close(done)
	defer func() {
		for _, s := range p.sources {
			close(s)
		}
	}()
	for {
		record, err := r.Receive(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			p.mu.Lock()
			p.receiveErr = err
			p.mu.Unlock()
			return
		}
		frame, decodeErr := canbus.DecodeSocketCANRecord(record.Raw)
		status := "raw"
		if decodeErr != nil || frame.DLC() != 8 || frame.RawDLC() != 8 {
			status = "invalid"
		}
		if record.Interface != p.selections[0].CX01Spec().Interface {
			status = "wrong-interface"
		}
		if record.Sequence == 0 || record.Monotonic < 0 {
			status = "invalid"
		}
		p.retain(record, generation, status)
		if status != "raw" {
			continue
		}
		e := canbusreg.Evidence{Frame: frame, Interface: record.Interface, Sequence: record.Sequence, Monotonic: record.Monotonic}
		copy(e.RawRecord[:], record.Raw)
		for n, s := range p.selections {
			c := s.CX01Spec()
			if c.Flavor == "link" && (!frame.ID().Extended() || uint8(frame.ID().Value()) != c.Source) {
				continue
			}
			in := p.associate(s, e)
			// Evidence and exact native bytes always come from the receiver. Selection
			// and epoch remain caller assertions and native admission fails closed.
			in.PCS.Evidence = e
			in.PCS.NativeRecord = append([]byte(nil), record.Raw...)
			o := canbusreg.CX17Observation{Generation: generation, Input: in}
			select {
			case p.sources[n] <- o:
			case <-ctx.Done():
				return
			}
		}
	}
}
func (p *growattProjectProvider) retain(r growattProjectRecord, generation uint64, status string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(r.Raw) > canbus.SocketCANFDRecordSize {
		r.Raw = r.Raw[:canbus.SocketCANFDRecordSize]
		status = "oversize"
	}
	r.Raw = append([]byte(nil), r.Raw...)
	if len(p.raw) == 256 {
		copy(p.raw, p.raw[1:])
		p.raw = p.raw[:255]
		p.evicted++
	}
	p.raw = append(p.raw, growattProjectDiagnostic{Record: r, Generation: generation, Status: status})
}
func (p *growattProjectProvider) Stop() error {
	p.lifecycle.Lock()
	defer p.lifecycle.Unlock()
	return p.stop()
}
func (p *growattProjectProvider) stop() error {
	if p.closeErr != nil {
		return p.closeErr
	}
	if p.cancel != nil {
		p.cancel()
		<-p.done
		p.cancel, p.done = nil, nil
	}
	for _, native := range p.providers {
		native.Stop()
	}
	if p.receiver != nil {
		p.closeErr = p.receiver.Close()
		if p.closeErr != nil {
			return p.closeErr
		}
		p.receiver = nil
	}
	return nil
}
func (p *growattProjectProvider) Snapshot(now time.Duration) growattProjectSnapshot {
	p.lifecycle.Lock()
	defer p.lifecycle.Unlock()
	p.mu.Lock()
	s := growattProjectSnapshot{Raw: append([]growattProjectDiagnostic(nil), p.raw...), Evicted: p.evicted, ReceiveError: p.receiveErr, Quarantined: p.closeErr != nil}
	for n := range s.Raw {
		s.Raw[n].Record.Raw = append([]byte(nil), s.Raw[n].Record.Raw...)
	}
	p.mu.Unlock()
	for _, native := range p.providers {
		s.Providers = append(s.Providers, native.CX17Snapshot(now, p.staleAfter))
	}
	return s
}

func newGrowattPCSProjectProvider(config ebusgateway.GrowattPCSConfig, open growattProjectOpen, associate growattProjectAssociation) (*growattProjectProvider, error) {
	selection, err := config.Selection()
	if err != nil || !config.Enabled {
		return nil, err
	}
	return newGrowattProjectProvider([]canbusreg.CXSelection{selection}, config.StaleAfter, config.ListenOnlyConfirmed, open, associate)
}

func newGrowattLINKProjectProvider(config ebusgateway.GrowattLINKConfig, open growattProjectOpen, associate growattProjectAssociation) (*growattProjectProvider, error) {
	selections, err := config.Selections()
	if err != nil || !config.Enabled {
		return nil, err
	}
	return newGrowattProjectProvider(selections, config.StaleAfter, config.ListenOnlyConfirmed, open, associate)
}
