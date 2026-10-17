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
	// Assets names the assets a fence or retirement covers, in bytewise order.
	Assets []semreg.AssetID `json:"assets,omitempty"`
	// Ordinal is the per-source admission ordinal of a hub-native succession
	// within its incarnation. It is set on both records of the succession's
	// single append (the retired predecessor and the admitted successor) and is
	// zero on every other record.
	Ordinal uint64 `json:"ordinal,omitempty"`
}

// EpochJournal records epoch lifecycle transitions. It is the seam for
// durable epoch storage, which this package does not provide. Append records
// all given records as one write-ahead operation: all or none.
type EpochJournal interface {
	Append(records ...EpochRecord) error
	Records() []EpochRecord
}

// MemoryJournal is the in-memory EpochJournal.
type MemoryJournal struct {
	mu      sync.Mutex
	records []EpochRecord
	// failure, when set, makes Append fail; it models a failing durable store.
	failure error
}

// NewMemoryJournal returns an empty in-memory journal.
func NewMemoryJournal() *MemoryJournal { return &MemoryJournal{} }

// Append records the given transitions atomically.
func (j *MemoryJournal) Append(records ...EpochRecord) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.failure != nil {
		return j.failure
	}
	for _, record := range records {
		j.records = append(j.records, cloneRecord(record))
	}
	return nil
}

func cloneRecord(record EpochRecord) EpochRecord {
	if record.Assets != nil {
		record.Assets = append([]semreg.AssetID(nil), record.Assets...)
	}
	return record
}

// Records returns a copy of every record in append order.
func (j *MemoryJournal) Records() []EpochRecord {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]EpochRecord, 0, len(j.records))
	for _, record := range j.records {
		out = append(out, cloneRecord(record))
	}
	return out
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

// Adopt records the handle an epoch transition (BeginEpoch) produced for one
// of this provisional source's qualified sources.
func (p *Provisional) Adopt(handle *Handle) error {
	if p == nil || handle == nil || handle.epoch != p.epoch {
		return ErrAdmission
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.retired {
		return &semreg.Error{ID: semreg.StaleSourceEpoch, Detail: "retired provisional source"}
	}
	p.handles[handle.source] = handle
	return nil
}

// Retire revokes every qualified source of the provisional source at the hub
// level. It submits no SemReg batch: the retained public state is unchanged.
// The error reports a journal failure; the revocation itself always applies.
func (p *Provisional) Retire() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.retired {
		return nil
	}
	sources := make([]semreg.SourceID, 0, len(p.handles))
	for id := range p.handles {
		sources = append(sources, id)
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i] < sources[j] })
	// Each source retires journal-first; a failure stops here and a retry
	// continues with the sources not yet retired.
	for _, id := range sources {
		if err := p.hub.revokeEpoch(id, p.epoch); err != nil {
			return err
		}
	}
	p.retired = true
	return nil
}

func (h *Hub) revokeEpoch(id semreg.SourceID, epoch semreg.SourceEpochID) error {
	h.lock()
	src := h.sources[id]
	if src == nil || src.epoch != epoch || src.retired[epoch] {
		h.unlock()
		return nil
	}
	generation := src.generation
	affected := []semreg.AssetID{}
	for assetID := range src.assets {
		if snapshot, _, ok := h.assets[assetID].kernel.Current(); ok {
			for _, cursor := range snapshot.Cursors {
				if cursor.SourceID == id && cursor.SourceEpochID == epoch {
					affected = append(affected, assetID)
					break
				}
			}
		}
	}
	sort.Slice(affected, func(i, j int) bool { return affected[i] < affected[j] })
	// Journal first: a failed append leaves the source live and unchanged.
	if err := h.appendEpochLocked(EpochRecord{Incarnation: h.incarnation, Source: id, Epoch: epoch, Generation: generation, Kind: EventSourceRetired, Assets: affected}); err != nil {
		h.unlock()
		return fmt.Errorf("semhub: journal source retirement: %w", err)
	}
	src.retired[epoch] = true
	src.live = false
	src.token++
	h.bus.emit(Event{Kind: EventSourceRetired, Source: id, Epoch: epoch, Generation: generation, NoPublication: true})
	h.unlock()
	return nil
}

// appendEpochLocked preserves the existing journal semantics and retains detached
// recovery history. The caller holds the hub mutex; no new persistence occurs.
func (h *Hub) appendEpochLocked(records ...EpochRecord) error {
	if err := h.journal.Append(records...); err != nil {
		return err
	}
	if h.recovery == nil {
		return nil
	}
	for _, record := range records {
		h.epochHistory = append(h.epochHistory, cloneRecord(record))
	}
	return nil
}
