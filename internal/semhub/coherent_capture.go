package semhub

import (
	"context"
	"errors"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"time"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs"
)

var ErrCaptureUnavailable = errors.New("semhub: coherent capture unavailable")

// CapturedAsset contains detached publication evidence and the admission
// decisions made at the same hub instant. Withheld publications stay private.
type CapturedAsset struct {
	AssetID           semreg.AssetID
	Snapshot          *semreg.Snapshot
	Evaluation        *semreg.EvaluationView
	SourceAdmitted    bool
	ReadAdmitted      bool
	PublicAdmitted    bool
	UnavailableReason string
}

type CoherentCapture struct {
	Metadata    *semreg.PackMetadataRegistry
	EvaluatedAt semreg.TimePoint
	Assets      []CapturedAsset
}

// Capture reads the current admitted epochs without changing publication or
// retention state. The clock is sampled under the read lock and must not
// reenter the hub or block on a hub writer. Inspect remains an independent,
// ungated audit surface.
func (h *Hub) Capture(ctx context.Context, ids []semreg.AssetID, now func() time.Time) (CoherentCapture, error) {
	if h == nil {
		return CoherentCapture{}, ErrCaptureUnavailable
	}
	if err := ctx.Err(); err != nil {
		return CoherentCapture{}, err
	}
	if len(ids) < 1 || len(ids) > 32 {
		return CoherentCapture{}, &semreg.Error{ID: semreg.BoundsExceeded}
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.restoring {
		return CoherentCapture{}, ErrCaptureUnavailable
	}
	at := now()
	metadata, err := packs.NewMetadataRegistry()
	if err != nil {
		return CoherentCapture{}, err
	}
	result := CoherentCapture{Metadata: metadata, EvaluatedAt: semreg.TimePoint{UnixNanoseconds: semreg.Int64(strconv.FormatInt(at.UnixNano(), 10)), ClockID: "clock.utc", UncertaintyNS: "0"}, Assets: make([]CapturedAsset, 0, len(ids))}
	seen := map[semreg.AssetID]bool{}
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return CoherentCapture{}, err
		}
		if id.Validate() != nil || seen[id] {
			return CoherentCapture{}, &semreg.Error{ID: semreg.InvalidValue}
		}
		seen[id] = true
		item := CapturedAsset{AssetID: id}
		target := h.assets[id]
		switch target {
		case nil:
			item.UnavailableReason = "semantic.read.reason.asset_unknown"
		default:
			src := h.sources[target.source]
			item.SourceAdmitted = src != nil && src.admitted && src.live && !src.fenced && !src.retired[src.epoch]
			gate := target.gate
			item.ReadAdmitted = !target.restoreFailed && (gate == nil || semanticSuccess(gate.semantic) && gate.reprojection == ReprojectionSettled)
			snapshot, _, present := target.kernel.Current()
			switch {
			case !item.SourceAdmitted:
				item.UnavailableReason = "semantic.read.reason.source_unavailable"
			case !item.ReadAdmitted:
				item.UnavailableReason = "semantic.read.reason.publication_withheld"
			case !present:
				item.UnavailableReason = "semantic.read.reason.snapshot_unavailable"
			default:
				// A successor admission must not expose its predecessor's snapshot.
				cursor, current := cursorFor(snapshot, src.id, src.epoch, semreg.Uint64(strconv.FormatUint(src.generation, 10)))
				item.PublicAdmitted = current && !cursor.Fenced && publicCaptureEvidence(reflect.ValueOf(snapshot))
				if !item.PublicAdmitted {
					item.UnavailableReason = "semantic.read.reason.publication_withheld"
				} else {
					evaluationContext, err := recordedEvaluationContext(snapshot, result.EvaluatedAt)
					if err != nil {
						return CoherentCapture{}, err
					}
					evaluation, err := semreg.EvaluateSnapshot(snapshot, evaluationContext)
					if err != nil {
						return CoherentCapture{}, err
					}
					item.Snapshot, item.Evaluation = &snapshot, &evaluation
				}
			}
		}
		result.Assets = append(result.Assets, item)
		h.fault("capture-read", id)
	}
	return result, ctx.Err()
}

// recordedEvaluationContext advances the publication's own aging coordinate by
// elapsed serialized wall time. The original Go monotonic receipt is not retained
// here: steps before the recorded wall point fail closed; forward steps age data.
func recordedEvaluationContext(snapshot semreg.Snapshot, at semreg.TimePoint) (semreg.EvaluationContext, error) {
	base, bok := new(big.Int).SetString(string(snapshot.EvaluateMonotonic.Nanoseconds), 10)
	then, tok := new(big.Int).SetString(string(snapshot.EvaluatedAt.UnixNanoseconds), 10)
	now, nok := new(big.Int).SetString(string(at.UnixNanoseconds), 10)
	if !bok || !tok || !nok || !base.IsUint64() || !then.IsInt64() || !now.IsInt64() {
		return semreg.EvaluationContext{}, &semreg.Error{ID: semreg.InvalidTime}
	}
	elapsed := new(big.Int).Sub(now, then)
	ticks := new(big.Int).Add(base, elapsed)
	if elapsed.Sign() < 0 || !ticks.IsUint64() {
		return semreg.EvaluationContext{}, &semreg.Error{ID: semreg.InvalidTime}
	}
	return semreg.EvaluationContext{EvaluatedAt: at, EvaluateMonotonic: semreg.MonotonicPoint{ClockEpochID: snapshot.EvaluateMonotonic.ClockEpochID, Nanoseconds: semreg.Uint64(ticks.String())}}, nil
}

// Evidence references expose only public material or metadata-only references.
// This covers nested retained evidence as well as current native evidence.
func publicCaptureEvidence(v reflect.Value) bool {
	if v.Type() == reflect.TypeOf(semreg.EvidenceRef{}) {
		e := v.Interface().(semreg.EvidenceRef)
		return e.Access == semreg.EvidenceAccessPublic || e.Redaction == semreg.RedactionMetadataOnly
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		return v.IsNil() || publicCaptureEvidence(v.Elem())
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if !publicCaptureEvidence(v.Field(i)) {
				return false
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			if !publicCaptureEvidence(v.Index(i)) {
				return false
			}
		}
	}
	return true
}

// AssetIDs returns a detached selection of all declared assets in bytewise order.
// It does not admit publications; Capture applies the current admission gates.
func (h *Hub) AssetIDs() []semreg.AssetID {
	if h == nil {
		return nil
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	ids := make([]semreg.AssetID, 0, len(h.assets))
	for id := range h.assets {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
