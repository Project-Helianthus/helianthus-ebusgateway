// Package eebusoutput is the compiled eeBUS output adapter skeleton of the
// eeBUS output binding design (docs/architecture/eebus-output-binding-design.md).
// It projects one coherent shared capture into a loss-aware ledger and an
// always-empty target document, and relays operation requests unchanged to an
// injected semantic operation route. It serves no eeBUS feature, registers no
// SHIP endpoint, constructs no native frame and freezes no mapping: until the
// native docs contract cites STD-01 outcomes no item may map exactly.
package eebusoutput

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"reflect"
	"strings"
	"sync"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticoperations"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticview"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

// Placeholder manifest identity. These are not published target identifiers.
const (
	TargetID        semreg.TargetID     = "target:eebus-output-placeholder"
	TargetVersion   semreg.VersionLabel = "0"
	MappingRevision semreg.Uint64       = "1"
)

// Binding-owned reasons. Source reasons, including unknown_pending_std_01, are
// carried verbatim and never replaced by these.
const (
	ReasonMappingUnknown   semreg.DefinitionID = "eebus.output.reason.mapping_unknown"
	ReasonMappingCandidate semreg.DefinitionID = "eebus.output.reason.mapping_candidate"
)

// Evidence is the conclusion recorded for one mapping row. It is separate from
// runtime qualification and from promotion.
type Evidence string

const (
	EvidenceProven     Evidence = "Proven"
	EvidenceHypothesis Evidence = "Hypothesis"
	EvidenceUnknown    Evidence = "Unknown"
)

// MappingRow records the evidence for mapping one requested SemReg item. A
// Hypothesis row must cite its public Source. No row may be Proven before the
// native docs contract cites STD-01 outcomes.
type MappingRow struct {
	Item     projection.RequestedItem
	Evidence Evidence
	Source   string
}

var (
	ErrMappingFrozen      = errors.New("eebus output: Proven mapping rows are blocked pending STD-01")
	ErrMappingInvalid     = errors.New("eebus output: invalid mapping row")
	ErrUnwired            = errors.New("eebus output: snapshot capture unwired")
	ErrCaptureUnavailable = errors.New("eebus output: shared capture unavailable")
	ErrCaptureSelection   = errors.New("eebus output: shared capture returned another selection")
)

// Document is the eeBUS target document. It stays empty until a native mapping
// is Proven: a withheld, unsupported or unknown item never appears in it.
type Document struct{}

// Result pairs the SemReg projection report with the target document.
type Result struct {
	Report   projection.ProjectionReport `json:"report"`
	Document Document                    `json:"document"`
}

// Asset is one requested asset. An asset withheld by the capture carries the
// capture reason verbatim and no Result.
type Asset struct {
	AssetID        semreg.AssetID `json:"asset_id"`
	Result         *Result        `json:"result,omitempty"`
	WithheldReason string         `json:"withheld_reason,omitempty"`
}

// Projection is the output of exactly one capture.
type Projection struct {
	Assets []Asset `json:"assets"`
}

// Operations is the semantic operation route, satisfied by
// *semanticoperations.Router.
type Operations interface {
	Route(context.Context, semanticoperations.Request) (semanticoperations.Result, error)
}

// Sink receives each accepted asset result. It is never handed a withheld
// asset, so a withheld or failed asset leaves every last accepted result intact.
type Sink interface {
	Accept(semreg.AssetID, Result)
}

// MemorySink is an in-memory Sink keeping the last accepted result per asset.
// It publishes nothing.
type MemorySink struct {
	mu       sync.Mutex
	accepted map[semreg.AssetID]Result
}

func (s *MemorySink) Accept(asset semreg.AssetID, result Result) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.accepted == nil {
		s.accepted = map[semreg.AssetID]Result{}
	}
	kept, ok := cloneResult(result)
	if !ok {
		return
	}
	s.accepted[asset] = kept
}

// Accepted returns the last accepted result of one asset.
func (s *MemorySink) Accepted(asset semreg.AssetID) (Result, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result, ok := s.accepted[asset]
	if !ok {
		return Result{}, false
	}
	return cloneResult(result)
}

// cloneResult deep-copies a result so retained state shares no memory with
// callers. A report that cannot be copied is not retained.
func cloneResult(result Result) (Result, bool) {
	raw, err := json.Marshal(result)
	if err != nil {
		return Result{}, false
	}
	var out Result
	if err := json.Unmarshal(raw, &out); err != nil {
		return Result{}, false
	}
	return out, true
}

// Config composes a Binding. Snapshots is required for projection; a nil
// Operations leaves the relay unwired; a nil Sink keeps no result.
type Config struct {
	Snapshots  semanticoperations.SnapshotCapture
	Operations Operations
	Sink       Sink
	Mappings   []MappingRow
}

// Binding is the adapter skeleton. It is safe for concurrent use.
type Binding struct {
	snapshots  semanticoperations.SnapshotCapture
	operations Operations
	sink       Sink
	mappings   map[projection.RequestedItem]Evidence
}

// NewBinding validates the mapping rows and composes a binding. Typed-nil
// dependencies count as absent.
func NewBinding(config Config) (*Binding, error) {
	binding := &Binding{mappings: map[projection.RequestedItem]Evidence{}}
	for _, row := range config.Mappings {
		if row.Item.Validate() != nil {
			return nil, ErrMappingInvalid
		}
		if _, duplicate := binding.mappings[row.Item]; duplicate {
			return nil, ErrMappingInvalid
		}
		switch row.Evidence {
		case EvidenceProven:
			return nil, ErrMappingFrozen
		case EvidenceHypothesis:
			if !publicSource(row.Source) {
				return nil, ErrMappingInvalid
			}
		case EvidenceUnknown:
		default:
			return nil, ErrMappingInvalid
		}
		binding.mappings[row.Item] = row.Evidence
	}
	if !isNil(config.Snapshots) {
		binding.snapshots = config.Snapshots
	}
	if !isNil(config.Operations) {
		binding.operations = config.Operations
	}
	if !isNil(config.Sink) {
		binding.sink = config.Sink
	}
	return binding, nil
}

// Manifest returns the placeholder target manifest for the given packs.
func Manifest(packs []semreg.PackRef) projection.ProjectionManifest {
	return projection.ProjectionManifest{
		TargetID: TargetID, TargetVersion: TargetVersion, KernelVersion: semreg.ContractKernelV1,
		PackVersions: append([]semreg.PackRef{}, packs...), MappingRevision: MappingRevision,
	}
}

// Project takes exactly one shared capture of the requested assets and
// projects every captured asset. Any error leaves the sink untouched.
func (b *Binding) Project(ctx context.Context, assets []semreg.AssetID) (Projection, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if b == nil || b.snapshots == nil {
		return Projection{}, ErrUnwired
	}
	metadata, inputs, err := b.snapshots.Capture(ctx, append([]semreg.AssetID{}, assets...))
	if err != nil {
		return Projection{}, ErrCaptureUnavailable
	}
	if !sameSelection(assets, inputs) {
		return Projection{}, ErrCaptureSelection
	}
	view, err := semanticview.Build(metadata, inputs)
	if err != nil {
		return Projection{}, err
	}
	out := Projection{Assets: make([]Asset, 0, len(view.Assets))}
	for _, source := range view.Assets {
		asset := Asset{AssetID: source.AssetID}
		if source.UnavailableReason != "" {
			asset.WithheldReason = source.UnavailableReason
			out.Assets = append(out.Assets, asset)
			continue
		}
		report, err := b.project(*source.Snapshot, *source.Projection)
		if err != nil {
			return Projection{}, err
		}
		asset.Result = &Result{Report: report}
		out.Assets = append(out.Assets, asset)
	}
	if b.sink != nil {
		for _, asset := range out.Assets {
			if asset.Result != nil {
				b.sink.Accept(asset.AssetID, *asset.Result)
			}
		}
	}
	return out, nil
}

// project accounts for every item the shared view requested. A non-exact
// source disposition keeps its outcome, reason and loss verbatim. An exact or
// transformed source can only reach the target through a Proven row, which
// cannot exist, so it becomes withheld under a Hypothesis row and unknown
// otherwise. Unrepresentable is a target statement that needs native meaning,
// so it is treated the same way.
func (b *Binding) project(snapshot semreg.Snapshot, source projection.ProjectionReport) (projection.ProjectionReport, error) {
	requested := append([]projection.RequestedItem{}, source.Requested...)
	dispositions := make([]projection.ProjectionDisposition, 0, len(source.Dispositions))
	for _, from := range source.Dispositions {
		d := projection.ProjectionDisposition{
			Kind: from.Kind, ItemID: from.ItemID, Outcome: from.Outcome,
			SourceKeys: append([]semreg.FactKey{}, from.SourceKeys...),
			Loss:       append([]projection.LossDetail{}, from.Loss...),
		}
		if from.Reason != nil {
			reason := *from.Reason
			d.Reason = &reason
		}
		switch from.Outcome {
		case projection.ProjectionWithheld, projection.ProjectionUnsupported, projection.ProjectionUnknown:
		default:
			reason := ReasonMappingUnknown
			d.Outcome = projection.ProjectionUnknown
			if b.mappings[projection.RequestedItem{ItemID: from.ItemID, Kind: from.Kind}] == EvidenceHypothesis {
				reason = ReasonMappingCandidate
				d.Outcome = projection.ProjectionWithheld
			}
			d.Reason = &reason
		}
		dispositions = append(dispositions, d)
	}
	return projection.Project(snapshot, Manifest(source.Manifest.PackVersions), requested, dispositions, nil)
}

// Relay hands one request to the injected operation route and returns its
// Result and error unchanged. It adds no retry, reroute or readback and never
// reports an eeBUS-side acknowledgement as native execution. Without a route
// the relay is unwired and answers UNAVAILABLE with no send.
func (b *Binding) Relay(ctx context.Context, request semanticoperations.Request) (semanticoperations.Result, error) {
	if b == nil || b.operations == nil {
		return semanticoperations.Result{}, &semanticoperations.Error{Code: semanticoperations.CodeUnavailable, Detail: "eebus output relay unwired"}
	}
	return b.operations.Route(ctx, request)
}

func sameSelection(assets []semreg.AssetID, inputs []semanticview.Input) bool {
	if len(assets) != len(inputs) {
		return false
	}
	want := map[semreg.AssetID]int{}
	for _, asset := range assets {
		want[asset]++
	}
	for _, input := range inputs {
		want[input.AssetID]--
		if want[input.AssetID] < 0 {
			return false
		}
	}
	return true
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	}
	return false
}

// publicSource accepts only an absolute https URL with a host and no embedded
// whitespace or credentials.
func publicSource(source string) bool {
	if source == "" || strings.TrimSpace(source) != source || strings.ContainsAny(source, " \t\r\n") {
		return false
	}
	parsed, err := url.Parse(source)
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.User != nil {
		return false
	}
	return true
}
