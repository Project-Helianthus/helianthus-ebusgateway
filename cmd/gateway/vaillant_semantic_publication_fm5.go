package main

import (
	"slices"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
)

// vaillantFM5View is the native FM5 state that the poller hands to the
// publication owner: the acquisition verdict and detached copies of the
// retained solar and cylinder snapshots.
type vaillantFM5View struct {
	verdict   vaillantFM5Interpretation
	solar     *vaillantSolarSnapshot
	cylinders map[byte]*vaillantCylinderSnapshot
}

// publishFM5Semantic hands the native FM5 view to the publication owner and
// then republishes the circuits, whose managing device follows the FM5 mode.
func (p *vaillantSemanticPoller) publishFM5Semantic(source semanticSnapshotSource) {
	if p == nil || p.liveProvider() == nil {
		return
	}

	p.mu.Lock()
	view := vaillantFM5View{
		verdict:   p.fm5Interpretation,
		solar:     cloneSolarSnapshot(p.solar),
		cylinders: cloneCylinderSnapshotsMap(p.solarCylinders),
	}
	p.mu.Unlock()

	p.publication.publishFM5View(view, source)
	p.publishCircuits(source)
}

// publishFM5View publishes the FM5 interpretation, solar and cylinders from
// their native view. A structural withdrawal, or a view without solar and
// cylinders, publishes empty solar and cylinder planes. The provider keeps
// its previous interpretation when the mapped verdict is not valid.
func (o *vaillantSemanticPublication) publishFM5View(view vaillantFM5View, source semanticSnapshotSource) {
	verdict := publishedFM5Interpretation(view.verdict)
	switch source {
	case semanticSnapshotSourceCache:
		o.provider.SetFM5InterpretationFromCache(verdict)
	default:
		o.provider.SetFM5Interpretation(verdict)
	}

	var solar *graphql.SolarStatus
	var cylinders []graphql.CylinderStatus
	if isFM5StructuralWithdrawal(view.verdict) || (view.solar == nil && len(view.cylinders) == 0) {
		solar = &graphql.SolarStatus{}
		cylinders = []graphql.CylinderStatus{}
	} else {
		solar = solarStatusFromSnapshot(view.solar)
		cylinders = cylinderStatusesFromSnapshots(view.cylinders)
	}

	switch source {
	case semanticSnapshotSourceCache:
		o.provider.SetSolarFromCache(solar)
		o.provider.SetCylindersFromCache(cylinders)
	default:
		o.provider.SetSolar(solar)
		o.provider.SetCylinders(cylinders)
	}
}

// fm5PublishedAsGPIOOnly reports whether the provider publishes the GPIO-only
// FM5 mode.
func (o *vaillantSemanticPublication) fm5PublishedAsGPIOOnly() bool {
	return o.provider.FM5SemanticMode() == graphql.Fm5SemanticModeGPIOOnly
}

// solarStatusFromSnapshot maps the retained solar snapshot to its published
// status; nil stays nil.
func solarStatusFromSnapshot(snapshot *vaillantSolarSnapshot) *graphql.SolarStatus {
	if snapshot == nil {
		return nil
	}
	return &graphql.SolarStatus{
		CollectorTemperatureC: cloneFloat64Ptr(snapshot.CollectorTemperatureC),
		ReturnTemperatureC:    cloneFloat64Ptr(snapshot.ReturnTemperatureC),
		PumpActive:            cloneBoilerBoolPtr(snapshot.PumpActive),
		CurrentYield:          cloneFloat64Ptr(snapshot.CurrentYield),
		PumpHours:             decodeUint32Float64(snapshot.PumpHours),
		SolarEnabled:          cloneBoilerBoolPtr(snapshot.SolarEnabled),
		FunctionMode:          cloneBoilerBoolPtr(snapshot.FunctionMode),
	}
}

// cylinderStatusesFromSnapshots maps the retained cylinder snapshots with
// live evidence to published statuses in ascending instance order.
func cylinderStatusesFromSnapshots(snapshots map[byte]*vaillantCylinderSnapshot) []graphql.CylinderStatus {
	instances := make([]byte, 0, len(snapshots))
	for instance := range snapshots {
		instances = append(instances, instance)
	}
	slices.Sort(instances)
	cylinders := make([]graphql.CylinderStatus, 0, len(instances))
	for _, instance := range instances {
		snapshot := snapshots[instance]
		if snapshot == nil || !hasLiveCylinderEvidence(snapshot) {
			continue
		}
		cylinders = append(cylinders, graphql.CylinderStatus{
			Index:             int(snapshot.Instance),
			TemperatureC:      cloneFloat64Ptr(snapshot.TemperatureC),
			MaxSetpointC:      cloneFloat64Ptr(snapshot.MaxSetpointC),
			ChargeHysteresisC: cloneFloat64Ptr(snapshot.ChargeHysteresis),
			ChargeOffsetC:     cloneFloat64Ptr(snapshot.ChargeOffset),
		})
	}
	return cylinders
}

// publishedFM5Interpretation maps an acquisition verdict to the published
// interpretation. A mode or reason outside the native sets maps to the zero
// interpretation, which the provider rejects as invalid.
func publishedFM5Interpretation(verdict vaillantFM5Interpretation) graphql.Fm5Interpretation {
	mode, modeOK := publishedFM5Mode(verdict.Mode)
	reason, reasonOK := publishedFM5DegradedReason(verdict.DegradedReason)
	if !modeOK || !reasonOK {
		return graphql.Fm5Interpretation{}
	}
	return graphql.Fm5Interpretation{
		Mode:             mode,
		DegradedReason:   reason,
		EvidenceRevision: verdict.EvidenceRevision,
	}
}

// publishedFM5Mode maps an acquisition mode to the published mode; the zero
// mode maps to the empty published mode.
func publishedFM5Mode(mode vaillantFM5Mode) (graphql.Fm5SemanticMode, bool) {
	switch mode {
	case "":
		return "", true
	case vaillantFM5ModeInterpreted:
		return graphql.Fm5SemanticModeInterpreted, true
	case vaillantFM5ModeGPIOOnly:
		return graphql.Fm5SemanticModeGPIOOnly, true
	case vaillantFM5ModeAbsent:
		return graphql.Fm5SemanticModeAbsent, true
	default:
		return "", false
	}
}

// publishedFM5DegradedReason maps an acquisition degraded reason to the
// published reason; the zero reason maps to the empty published reason.
func publishedFM5DegradedReason(reason vaillantFM5DegradedReason) (graphql.Fm5SemanticDegradedReason, bool) {
	switch reason {
	case "":
		return "", true
	case vaillantFM5DegradedControllerUnreachable:
		return graphql.Fm5SemanticDegradedReasonControllerUnreachable, true
	case vaillantFM5DegradedConfigurationUnavailable:
		return graphql.Fm5SemanticDegradedReasonConfigurationUnavailable, true
	case vaillantFM5DegradedConfigurationNotInterpretable:
		return graphql.Fm5SemanticDegradedReasonConfigurationNotInterpretable, true
	case vaillantFM5DegradedSolarAcquisitionFailed:
		return graphql.Fm5SemanticDegradedReasonSolarAcquisitionFailed, true
	case vaillantFM5DegradedCylinderAcquisitionFailed:
		return graphql.Fm5SemanticDegradedReasonCylinderAcquisitionFailed, true
	case vaillantFM5DegradedEvidenceStale:
		return graphql.Fm5SemanticDegradedReasonEvidenceStale, true
	case vaillantFM5DegradedIncoherentAcquisition:
		return graphql.Fm5SemanticDegradedReasonIncoherentAcquisition, true
	default:
		return "", false
	}
}
