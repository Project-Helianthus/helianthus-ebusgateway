package semhub

import (
	"fmt"
	"sort"
	"sync"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// EpochRecord is one epoch lifecycle record of one hub incarnation.
type EpochRecord struct {
	Incarnation string               `json:"incarnation"`
	Source      semreg.SourceID      `json:"source"`
	Epoch       semreg.SourceEpochID `json:"epoch"`
	Generation  uint64               `json:"generation"`
	Kind        EventKind            `json:"kind"`
}

// EpochJournal records epoch lifecycle transitions. It is the seam for
// durable epoch storage, which this package does not provide.
type EpochJournal interface {
	Append(EpochRecord) error
	Records() []EpochRecord
}

// MemoryJournal is the in-memory EpochJournal.
type MemoryJournal struct {
	mu      sync.Mutex
	records []EpochRecord
}

// NewMemoryJournal returns an empty in-memory journal.
func NewMemoryJournal() *MemoryJournal { return &MemoryJournal{} }

// Append records one transition.
func (j *MemoryJournal) Append(record EpochRecord) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.records = append(j.records, record)
	return nil
}

// Records returns a copy of every record in append order.
func (j *MemoryJournal) Records() []EpochRecord {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]EpochRecord(nil), j.records...)
}

// Provisional is one legacy source admitted before its qualified source
// identifiers are known. It qualifies into one legacy source per native
// resource tuple; every qualified source shares the provisional epoch and
// generation.
type Provisional struct {
	hub        *Hub
	driver     string
	legacy     LegacyFamily
	epoch      semreg.SourceEpochID
	generation uint64
	mu         sync.Mutex
	handles    map[semreg.SourceID]*Handle
	retired    bool
}

// AdmitProvisional admits one legacy provisional source epoch.
func (h *Hub) AdmitProvisional(driver string, legacy LegacyFamily, epoch semreg.SourceEpochID, generation uint64) (*Provisional, error) {
	if driver == "" || !legacy.valid() || ValidateOpaque(string(epoch)) != nil || generation == 0 {
		return nil, fmt.Errorf("%w: provisional source", ErrAdmission)
	}
	return &Provisional{hub: h, driver: driver, legacy: legacy, epoch: epoch, generation: generation, handles: make(map[semreg.SourceID]*Handle)}, nil
}

// Epoch returns the shared provisional epoch.
func (p *Provisional) Epoch() semreg.SourceEpochID { return p.epoch }

// Generation returns the shared provisional generation.
func (p *Provisional) Generation() uint64 { return p.generation }

// Qualify returns the handle of one qualified legacy source. Repeated
// qualification of the same source is idempotent.
func (p *Provisional) Qualify(id semreg.SourceID) (*Handle, error) {
	if p == nil {
		return nil, ErrAdmission
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.retired {
		return nil, &semreg.Error{ID: semreg.StaleSourceEpoch, Detail: "retired provisional source"}
	}
	if handle := p.handles[id]; handle != nil {
		return handle, nil
	}
	handle, err := p.hub.AdmitSource(Admission{Source: id, Driver: p.driver, Legacy: p.legacy, Epoch: p.epoch, Generation: p.generation})
	if err != nil {
		return nil, err
	}
	p.handles[id] = handle
	return handle, nil
}

// Retire revokes every qualified source of the provisional source at the hub
// level. It submits no SemReg batch: the retained public state is unchanged.
// The error reports a journal failure; the revocation itself always applies.
func (p *Provisional) Retire() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.retired {
		p.mu.Unlock()
		return nil
	}
	p.retired = true
	sources := make([]semreg.SourceID, 0, len(p.handles))
	for id := range p.handles {
		sources = append(sources, id)
	}
	p.mu.Unlock()
	sort.Slice(sources, func(i, j int) bool { return sources[i] < sources[j] })
	var failure error
	for _, id := range sources {
		if err := p.hub.revokeEpoch(id, p.epoch); err != nil && failure == nil {
			failure = err
		}
	}
	return failure
}

func (h *Hub) revokeEpoch(id semreg.SourceID, epoch semreg.SourceEpochID) error {
	h.mu.Lock()
	src := h.sources[id]
	if src == nil || src.epoch != epoch || src.retired[epoch] {
		h.mu.Unlock()
		return nil
	}
	src.retired[epoch] = true
	src.live = false
	src.token++
	generation := src.generation
	h.mu.Unlock()
	h.bus.emit(Event{Kind: EventSourceRetired, Source: id, Epoch: epoch, Generation: generation, NoPublication: true})
	return h.journal.Append(EpochRecord{Incarnation: h.incarnation, Source: id, Epoch: epoch, Generation: generation, Kind: EventSourceRetired})
}
