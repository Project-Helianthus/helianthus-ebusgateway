# Portal configuration diagnostic projection

The accepted [configuration sharing and recovery contract](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/docs/architecture/portal-config-sharing.md)
requires an allowlisted diagnostic export. `O10ProjectDiagnostics` in
[portalconfig](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/portalconfig/o10_diagnostics.go)
implements only the configuration projection for a future export owner. It is
not an HTTP endpoint or a completed Portal diagnostic export. The helper adds
no public API schema, namespace or stability promise before Helianthus 1.0.

The caller supplies an already accepted `Record` and its current load error.
The helper does not establish acceptance, validate host constraints, load or
save storage, inspect listeners, or apply runtime configuration. A future owner
must obtain the accepted record and corresponding load result from the same
configuration snapshot, then explicitly integrate this projection into its
export. It must not substitute a rejected candidate or serialize the ordinary
configuration/API response.

The returned value contains only the fixed configuration `format`,
`selections_available`, closed `status` and `reason`, and desired `graphql`,
`mcp`, `adapter_proxy` and `matter` booleans. It omits the driver collection
entirely, including identities, counts and individual enabled selections.
No record bytes, revisions, raw errors, error fields/next-step text, binding
addresses, paths, native evidence, unknown keys or caller context are copied.
The function accepts neither a listener plan nor arbitrary metadata.

With no load error and the accepted format, selections are available and the
four desired booleans are copied independently. Status/reason are
`pending`/`listener_application_pending`: they describe unapplied configuration
intent, never readiness or effective publication. With an absent or mismatched
format and no load error, selections are unavailable with
`error`/`evidence_unavailable`. With any load error, selections are unavailable
and every selection boolean is false, even when the input retains a last-known
good record. This does not delete or replace that historical record. Consumers
must check availability; false without availability does not mean a known
desired deselection.

Error reasons allow only `invalid_record`, `unsupported_format`,
`unsupported_selection`, `revision_conflict`, `persistence_failed`,
`revision_exhausted`, `evidence_unavailable`, `listener_conflict` and
`invalid_binding` from typed configuration errors, including wrapped ones.
Other errors or unknown codes become `persistence_failed`. No error display
text is evaluated or exported. The returned struct contains only strings and
booleans: it shares no mutable storage with the input and has deterministic
serialization for identical configuration/load results.

**Proven:** focused offline tests cover independent desired selections, closed
output shape, unavailable states, redaction and detachment. **Hypothesis:** a
future export owner can compose this projection with separately allowlisted
operational diagnostics; that integration is not implemented here.
**Unknown:** runtime application, listener readiness, effective publication,
native qualification, freshness and physical acceptance. These remain separate
owner evidence; configuration selections prove none of them.

Export-handler wiring, ordinary passive/cache flag removal, typed connection
editing and runtime apply remain subsequent work under
[issue 1000](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1000).
This slice changes no protocol meaning, native admission, caches, last-known
good evidence, eBUS MCP envelopes or GraphQL parity. Full committed-change CI,
independent review, publication checks and applicable transport/passive gates
remain separate from these focused offline checks.
