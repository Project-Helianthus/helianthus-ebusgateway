package portalconfig

import "strconv"

// O11ListenerObservation is a trusted owner's applied listener readback, never
// browser input. Binding.Available describes observation, not desired binding.
type O11ListenerObservation struct {
	Surface         string
	AppliedRevision string
	Binding         ListenerBinding
}

// O11DeriveListenerReadback projects a snapshot without applying configuration.
// Callers supply the current accepted record and host-owned planned bindings.
// Its result is detached and does not change DeriveListenerPlan or the HTTP API.
func O11DeriveListenerReadback(r Record, a ListenerAvailability, observations []O11ListenerObservation) ListenerPlan {
	p := DeriveListenerPlan(r, a)
	revision, err := strconv.ParseUint(r.Revision, 10, 64)
	validRevision := err == nil && strconv.FormatUint(revision, 10) == r.Revision
	for i := range p.Surfaces[:4] {
		s := &p.Surfaces[i]
		var observation *O11ListenerObservation
		count := 0
		active := false
		for j := range observations {
			if observations[j].Surface == s.Surface {
				observation = &observations[j]
				count++
				active = active || observations[j].Binding.Available
			}
		}
		if !s.Desired {
			// Still-active readback is withdrawal pending, never proof of disablement.
			if active {
				s.Status, s.Reason = "pending", "listener_withdrawal_pending"
			}
			continue
		}
		// Reuse all planning validation and conflict rules before admission.
		if s.Status != "pending" || s.Binding == nil {
			continue
		}
		if count == 0 {
			s.Status, s.Reason = "withheld", "evidence_unavailable"
			continue
		}
		if count != 1 {
			s.Status, s.Reason = "withheld", "conflicting_readback"
			continue
		}
		b := observation.Binding
		if !validRevision || observation.AppliedRevision != r.Revision ||
			!validListenerBinding(b) || b.Owner != s.Binding.Owner ||
			b.Address.Unmap() != s.Binding.Address || b.Port != s.Binding.Port {
			s.Reason = "listener_application_pending"
			continue
		}
		if !b.Available {
			s.Status, s.Reason = "withheld", "evidence_unavailable"
			continue
		}
		s.Effective, s.Status, s.Reason = true, "effective", "listener_available"
	}
	return p
}
