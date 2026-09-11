package catalogv1

import (
	"errors"
	"sort"
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
		}
		available := make(map[string]bool, len(out.Resources))
		for _, r := range out.Resources {
			available[r.ID] = true
		}
		for _, d := range descriptors {
			out.Contributions = append(out.Contributions, Identity{DriverID: d.Manifest.Contributor.DriverID, ManifestID: d.Manifest.ManifestID, ManifestVersion: d.Manifest.ManifestVersion, Digest: d.Digest})
			for _, a := range d.Manifest.Actions {
				action := Action{ID: a.ID, ResourceID: a.Group, ServiceID: a.ServiceRef.ID, CapabilityID: a.CapabilityRef.ID, OperationID: a.OperationRef.ID}
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
			Revision      uint64
			Domains       []Domain
			Contributions []Identity
			Resources     []Resource
		}{rev, out.Domains, out.Contributions, out.Resources}
		out.CatalogRevision = hash(structural)
		out.CatalogDigest = ""
		out.CatalogDigest = hash(out)
		c.mu.RLock()
		unchanged := rev == c.revision
		c.mu.RUnlock()
		if unchanged {
			return clone(out), nil
		}
	}
	return Catalog{}, ErrCatalogChanged
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
		if catalog.Actions[i].ID == claim.ActionID && catalog.Actions[i].ResourceID == claim.ResourceID && catalog.Actions[i].CapabilityID == claim.CapabilityID {
			action = &catalog.Actions[i]
			break
		}
	}
	if action == nil {
		return nil, ErrUnauthorized
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

// CanonicalJSON is public for transport and fixed wire tests.
func CanonicalJSON(c Catalog) ([]byte, error) { sortCatalog(&c); return canonical(c) }

var _ = sort.Strings
