package catalogv1

import (
	"encoding/json"
	"errors"
	"sort"
	"time"
)

// Catalog is a bounded optimistic detached capture. Sources are called once per
// attempt and must themselves be committed-state readers with no I/O.
func (c *Composer) Catalog(caller any) (Catalog, error) {
	if c == nil {
		return Catalog{}, errors.New("catalog composer missing")
	}
	for attempt := 0; attempt < 3; attempt++ {
		c.mu.RLock()
		rev := c.revision
		descriptors := make([]Descriptor, 0, len(c.descriptors))
		for _, d := range c.descriptors {
			descriptors = append(descriptors, d)
		}
		quarantined := make([]identity, 0, len(c.quarantined))
		for k := range c.quarantined {
			quarantined = append(quarantined, k)
		}
		source, auth, now := c.source, c.auth, c.now
		c.mu.RUnlock()
		fence := ""
		if fenced, ok := source.(FencedSourceCapture); ok {
			fence = fenced.Fence()
		}
		at := now().UTC()
		out := Catalog{Contract: Contract, EvaluationInstant: at, Domains: make([]Domain, 0, len(FivePacks)), Contributions: make([]Identity, 0, len(descriptors)), Resources: []Resource{}, Fields: []Field{}, Actions: []Action{}, Quarantines: make([]Quarantine, 0, len(quarantined))}
		for _, p := range FivePacks {
			out.Domains = append(out.Domains, Domain{Pack: p, SourceState: "ABSENT", Reason: "source_unavailable"})
		}
		if auth != nil {
			scope, err := auth.Scope(caller)
			if err != nil {
				return Catalog{}, ErrUnauthorized
			}
			out.AuthorizationScope = scope
		} else {
			out.AuthorizationScope = "public-read/no-actions"
		}
		if source != nil {
			resources, fields, err := source.Capture(at)
			if err != nil {
				return Catalog{}, err
			}
			out.Resources = append(out.Resources, resources...)
			out.Fields = append(out.Fields, fields...)
			present := map[string]bool{}
			for _, r := range resources {
				present[canonicalDomain(r.Domain)] = true
			}
			for i := range out.Domains {
				if present[out.Domains[i].Pack.ID] {
					out.Domains[i].SourceState = "PRESENT"
					out.Domains[i].Reason = ""
				}
			}
			if fenced, ok := source.(FencedSourceCapture); ok && fence != fenced.Fence() {
				continue
			}
		}
		available := make(map[string]bool, len(out.Resources))
		for _, r := range out.Resources {
			if available[r.ID] {
				return Catalog{}, errors.New("ambiguous detached resource context")
			}
			available[r.ID] = true
		}
		for _, d := range descriptors {
			out.Contributions = append(out.Contributions, Identity{DriverID: d.Manifest.Contributor.DriverID, ManifestID: d.Manifest.ManifestID, ManifestVersion: d.Manifest.ManifestVersion, Digest: d.Digest})
			groups := make(map[string]string, len(d.Manifest.Groups))
			invalid := false
			for _, g := range d.Manifest.Groups {
				if g.ID == "" || g.ResourceContext == "" {
					invalid = true
					break
				}
				if _, exists := groups[g.ID]; exists {
					invalid = true
					break
				}
				groups[g.ID] = g.ResourceContext
			}
			if invalid {
				return Catalog{}, errors.New("contribution group resource mapping is invalid")
			}
			for _, a := range d.Manifest.Actions {
				resourceID, ok := groups[a.Group]
				if !ok {
					return Catalog{}, errors.New("action group has no resource context")
				}
				resource, exists := findResource(out.Resources, resourceID)
				if !exists {
					continue
				}
				action := Action{ID: a.ID, ResourceID: resourceID, ServiceID: a.ServiceRef.ID, CapabilityID: a.CapabilityRef.ID, OperationID: a.OperationRef.ID, ContributionDriverID: d.Manifest.Contributor.DriverID, ContributionManifestID: d.Manifest.ManifestID, ContributionManifestVersion: d.Manifest.ManifestVersion, Source: resource.Source}
				if auth != nil {
					action.Discoverable = auth.Discover(caller, action)
					action.Enabled = action.Discoverable && auth.Invoke(caller, action)
				}
				if action.Discoverable && available[action.ResourceID] {
					out.Actions = append(out.Actions, action)
				}
			}
		}
		for _, k := range quarantined {
			out.Quarantines = append(out.Quarantines, Quarantine{DriverID: k.driver, ManifestID: k.manifest, ManifestVersion: k.version, Reason: "digest_conflict"})
		}
		sortCatalog(&out)
		structural := struct {
			Revision uint64
			Fence    string
			Catalog  Catalog
		}{rev, fence, catalogRevisionView(out)}
		revisionDigest, err := hash(structural)
		if err != nil {
			return Catalog{}, err
		}
		out.CatalogRevision = revisionDigest
		out.CatalogDigest = ""
		catalogDigest, err := hash(out)
		if err != nil {
			return Catalog{}, err
		}
		out.CatalogDigest = catalogDigest
		c.mu.RLock()
		unchanged := rev == c.revision
		c.mu.RUnlock()
		if unchanged {
			return clone(out), nil
		}
	}
	return Catalog{}, ErrCatalogChanged
}

// catalogRevisionView excludes response-time decoration from the action claim.
// It retains descriptors, source records, caller scope, and lifecycle fences;
// native admission still revalidates freshness and expiry immediately before I/O.
func catalogRevisionView(c Catalog) Catalog {
	c.EvaluationInstant = time.Time{}
	c.CatalogRevision = ""
	c.CatalogDigest = ""
	for i := range c.Resources {
		c.Resources[i].Source = sourceRevisionView(c.Resources[i].Source)
	}
	for i := range c.Actions {
		c.Actions[i].Source = sourceRevisionView(c.Actions[i].Source)
	}
	return c
}

func sourceRevisionView(source Source) Source {
	source.EvaluationDigest = ""
	source.Evaluation = normalizeEvaluationClock(source.Evaluation)
	return source
}

// normalizeEvaluationClock removes only the volatile wall/monotonic coordinates
// from the action-stability view.  Facts, quality, provenance, projection
// dispositions and every lifecycle/source identity remain part of the claim.
func normalizeEvaluationClock(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return raw
	}
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return raw
	}
	stripEvaluationClock(value)
	normalized, err := json.Marshal(value)
	if err != nil {
		return raw
	}
	return json.RawMessage(normalized)
}

func stripEvaluationClock(value any) {
	switch typed := value.(type) {
	case map[string]any:
		delete(typed, "evaluated_at")
		delete(typed, "evaluate_monotonic")
		for _, child := range typed {
			stripEvaluationClock(child)
		}
	case []any:
		for _, child := range typed {
			stripEvaluationClock(child)
		}
	}
}

func (c *Composer) Invoke(caller any, claim Claims) (any, error) {
	if c == nil || claim.Deadline.IsZero() || !c.now().Before(claim.Deadline) || claim.IdempotencyKey == "" {
		return nil, ErrStale
	}
	catalog, err := c.Catalog(caller)
	if err != nil {
		return nil, err
	}
	if catalog.CatalogRevision != claim.CatalogRevision {
		return nil, ErrStale
	}
	var action *Action
	for i := range catalog.Actions {
		if matchesClaim(catalog.Actions[i], claim) {
			action = &catalog.Actions[i]
			break
		}
	}
	if action == nil {
		return nil, ErrUnauthorized
	}
	if !matchesDigest(catalog.Contributions, claim) {
		return nil, ErrStale
	}
	c.mu.RLock()
	invoker, revalidator := c.invoker, c.revalidator
	c.mu.RUnlock()
	if invoker == nil {
		return nil, ErrUnauthorized
	}
	if c.auth == nil || !c.auth.Invoke(caller, *action) {
		return nil, ErrUnauthorized
	}
	if revalidator == nil {
		return nil, ErrUnauthorized
	}
	if err := revalidator.Revalidate(*action, claim); err != nil {
		return nil, err
	}
	return invoker.Invoke(*action, claim)
}

func findResource(resources []Resource, id string) (Resource, bool) {
	for _, r := range resources {
		if r.ID == id {
			return r, true
		}
	}
	return Resource{}, false
}
func matchesClaim(a Action, c Claims) bool {
	return a.ID == c.ActionID && a.ResourceID == c.ResourceID && a.CapabilityID == c.CapabilityID && a.ContributionDriverID == c.DriverID && a.ContributionManifestID == c.ManifestID && a.ContributionManifestVersion == c.ManifestVersion && a.Source.SnapshotID == c.SnapshotID && a.Source.Revision == c.Revision && a.Source.BindingID == c.BindingID && a.Source.SourceEpoch == c.SourceEpoch && a.Source.DriverGeneration == c.DriverGeneration && c.Digest != ""
}
func matchesDigest(items []Identity, c Claims) bool {
	for _, i := range items {
		if i.DriverID == c.DriverID && i.ManifestID == c.ManifestID && i.ManifestVersion == c.ManifestVersion && i.Digest == c.Digest {
			return true
		}
	}
	return false
}

// CanonicalJSON is public for transport and fixed wire tests.
func CanonicalJSON(c Catalog) ([]byte, error) { sortCatalog(&c); return canonical(c) }

var _ = sort.Strings
