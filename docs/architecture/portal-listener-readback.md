# Detached listener owner readback

**Proven**: [O11DeriveListenerReadback](../../internal/portalconfig/o11_readback.go)
adds an offline projection beside the existing
[sharing planner](../../internal/portalconfig/sharing.go), following the
[sharing contract](portal-config-sharing.md). It accepts the current accepted
desired record, host-owned planned bindings and explicit owner observations.
Each observation identifies a surface, applied application revision, listener
owner, address, port and observed availability. These are trusted owner inputs,
never browser assertions. No new persisted or HTTP fields are introduced.

The projection first derives the unchanged plan, retaining its validation,
normalized addresses, scopes, surface order and collision rules. A selected
listener becomes `effective` / `listener_available` only when exactly one
observation matches its surface, current application revision and complete
owner/address/port identity, with positive observed availability. Application
revision must be a canonical unsigned decimal value. IPv4-mapped addresses
normalize as in the planner; zoned observation addresses remain invalid.

Missing observations yield `withheld` / `evidence_unavailable`; duplicate
observations yield `withheld` / `conflicting_readback`, including identical
duplicates. Stale revisions, mismatched identities and invalid observation
bindings stay `pending` / `listener_application_pending`. Revoked availability
yields `withheld` / `evidence_unavailable`. Invalid or conflicting planned
bindings retain their original withholding reasons. A bound listener without
matching applied revision is never effective. The aggregate plan status remains
`pending`: it is not an aggregate runtime application acknowledgement.

Deselection immediately fences effective use. Any still-active observation for
that surface reports `pending` / `listener_withdrawal_pending`; otherwise it
remains `withheld` / `not_selected`. Neither result claims confirmed native
closure. Owner readback must be supplied as a current snapshot on each call;
this pure helper does not retain historical observations or mutate native
last-known-good evidence. Returned bindings, surfaces and conflicts are detached
from caller inputs and other results.

The local surface remains independently selected regardless of external GraphQL
or MCP choices. Unknown external readback cannot alter its observation or disable
shell/internal consumers. Unknown local readback withholds only this projection's
claim of availability; the helper performs no route or runtime operation.
Matter remains an output surface with no listener-derived publication evidence,
even if a caller supplies a Matter-named observation. Output publication and
native admission still require their owning contracts. Listener availability
alone establishes neither qualification nor protocol access permission.

**Unknown**: production owner readback wiring, separate GraphQL/MCP listeners,
actual runtime application and physical acceptance. This helper is a prerequisite
seam, not production listener separation. Socket application, HTTP callbacks,
authentication policy, plain sharing formats and eeBUS/Matter bindings remain
outside this slice. The current adapter listener supports ENH only; no plain
format is added. CX01 uses an ENH client under that current limitation.

**Hypothesis**: no new protocol behavior is proposed. No new namespace or
stability promise is introduced; all namespaces remain pre-stable before 1.0.
Existing eBUS MCP envelopes, invoke safeguards and GraphQL parity are unchanged.
Focused offline tests establish only the projection behavior described here;
full committed-change CI, independent review and any transport or physical
acceptance remain separate.
