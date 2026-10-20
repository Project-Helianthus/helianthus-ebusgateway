# Bidirectional driver registration and northbound hooks

Status: proposed design for [issue #1230](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1230),
a partial contract slice of [parent #1125](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1125).
This page explicitly supersedes the old **M39 planning entry** for bidirectional
registration. Retirement of that entry and its ticket registry is separate work;
this page does not claim the parent is complete. Implementation planning belongs
to the 0.11 roadmap. Nothing is stable before Helianthus 1.0.

All output-hook names and shapes below are proposed extension points, not
implemented hooks, a public provider API, an operation router or production
registration. The existing host rejects every hook declaration. This design
neither wires a hook nor decides who persists Portal selection.

## Authoritative boundaries

Read this extension with the final amendments in the public
[runtime driver lifecycle contract](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/runtime-driver-lifecycle.md)
and the gateway
[driver/provider contract](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/docs/architecture/runtime-driver-provider-contract-v1.md).
The final G5 D1–D5 decisions take precedence over superseded examples in the
provider contract. G5 D1–D5/O8 remain unchanged and retain their existing owners;
this page defines no alternative recovery, control or storage service.

| Owner | Responsibility retained |
| --- | --- |
| Native driver/adapter | Acquisition, discovery, qualification, decoding, native endpoint lifecycle, native retry, ACK/readback and raw evidence beside interpretation. |
| SemReg and its public contracts | Protocol-neutral identity, publication, evaluation, selection, conflict, causal context and projection loss. |
| Gateway G5 composition | Frozen catalog, existing desired/observed lifecycle, source ownership, admission fencing, bounded cleanup and recovery. |
| Output implementation | Target contract validation, endpoint resources, projection/reporting and close proof; eeBUS output remains in `helianthus-eebusreg`. |
| Portal | Presentation and explicit selection intent, without defining native meaning or target eligibility. |

Native protocol facts require the public native owner's contract before semantic
mapping. This design introduces no wire facts or new family mappings. Proven,
Hypothesis and Unknown are evidence labels, separate from qualification,
promotion, freshness, runtime admission and physical acceptance. A connected
peer, readable candidate or accepted descriptor cannot upgrade any of them.
A family-local provider port is not the common production Provider interface
and is not a live gateway integration.

## Descriptor and registration proposal

Reuse the S0 descriptor's explicit direction: exactly `southbound`, `northbound`
or `both`. Direction describes supported roles; it grants no publication,
forwarding or native-write authority. Disposition, durable desired state,
observed lifecycle and effective capabilities remain separate.

| Direction | Permitted declaration | Must remain absent or withheld |
| --- | --- | --- |
| `southbound` | Native acquisition and explicitly owned semantic sources; empty owned-source sets remain legal for lifecycle-only drivers. | Output endpoint hooks and target-publication claims. |
| `northbound` | Output endpoint declarations consuming detached SemReg reads. | Acquisition publisher or invented source ownership merely to satisfy a nonempty-source assumption. An empty owned-source set is valid. |
| `both` | Independently declared acquisition sources and output endpoints with explicit provenance and conflict rules. | Implicit input-to-output forwarding, reflected input promotion or self-selected native routes. |

Retain canonical `DriverID`, configured disposition, exact sanitized
`NativeContractRef`, typed lifecycle adapter and exclusively owned `SourceID`
set. Source identity comes from configured ownership, not source count, array
position, display ID, family label, endpoint location or discovery order.
Driver identity, source identity, epoch, driver generation, native connection
generation, asset, binding and capability identity are distinct.

Proposed output declarations add an opaque configured output-binding identity,
a versioned supported hook contract, exact target contract/profile reference,
and explicit semantic capability/pack requirements. Treat identity keys as typed
tuples, never concatenated labels. Resource callbacks bind the full owning
`DriverID`, source epoch, driver generation and lifecycle correlation plus the
output-binding identity; semantic reads additionally bind exact asset,
capability instance, snapshot and revision identities. A sink's lifecycle epoch
must not fabricate a SemReg source, asset or observation.

Validate and freeze the entire descriptor batch, disabled entries included,
before any factory, semantic declaration, subscription or endpoint effect.
Preserve S0 validation order and existing duplicate-driver, duplicate-source and
hub-owner checks. Then validate output requirements and direction consistency,
unique output ownership and supported hook/target versions before installing
anything. Caller mutation cannot change the frozen registry. List/get remain
coherent detached cached reads, sorted by DriverID, without acquisition.

Reject a source-only descriptor declaring an output hook, a sink-only descriptor
claiming acquisition, missing target requirements, duplicate output ownership,
unknown directions, unknown hook names or unsupported versions before effects.
An empty source set is not contradictory. The proposed host must recognize only
implemented hook contracts; **today even a well-formed proposed output hook is
rejected as unimplemented**. This page reserves no new accepted hook spelling.
Rejection installs no partial catalog, invokes no factory and preserves the
existing valid catalog and last-known-good state.

## Proposed output hooks within the existing lifecycle

The following are conceptual responsibilities, not Go methods or wire fields.
Future DM-HOOKS work must reconcile their exact schemas with G5 before wiring.
Each call has a finite deadline, detached inputs and the exact owning lifecycle
correlation; results must be checked again against the active correlation before
installation. No callback may reopen admission by itself.

| Responsibility | Preconditions and result |
| --- | --- |
| Prepare output | G5 recovery/barrier complete and exact reservation claimed; validate target requirements and stage resources while external publication remains closed. No target endpoint allocation without explicit publication intent and target eligibility; no reporting before lifecycle admission. Return a prepared handle with one close owner, without endpoint exposure. |
| Activate output | Current reservation/correlation, consumed staging and valid target evidence; stage projection from one coherent read. Expose only at the existing serialized activation/admission boundary after proof is validated. |
| Deliver read/projection | Current admitted generation and explicit publication request; consume a detached SemReg view, preserve its identities, evaluation and loss report. Never probe the native driver to fill a gap. |
| Fence/withdraw | Immediately revoke reporting, callback and derived-intent authority for the exact generation, before asynchronous work; retain history and unaffected siblings. |
| Abort/drain/close | Abort failed preparation/activation or close withdrawn output resources under the existing independent native-close obligation. Return truthful keyed close proof, never infer it from cancellation or a notification. |

| Lifecycle transition | Required output behavior |
| --- | --- |
| Registered, disabled or desired STOPPED | No allocation, subscription, reporting or forwarding from registration/list/get alone. Existing G5 bootstrap policy is unchanged. |
| STARTING | Complete recovery before effects; stage output without public eligibility. A late preparation result is discarded and its resources aborted, not installed. |
| RUNNING | Only selected, explicitly published and admitted target projections may report; source capabilities need their own qualification and admission. |
| DEGRADED/BACKOFF | Withhold affected publication, report truthful reasons and retain valid sibling/last-known-good data under original aging; retry never grants authority. |
| STOPPING or failure fence | Reject queued callbacks immediately. Independently retire semantic source paths, if any, and cancel/drain/close output/native resources. Neither obligation waits for the other's success. |
| STOPPED | Require settled retirement/control and proven close. No callback from the stopped generation can publish or reactivate an endpoint. |
| RESTART or START after completed STOP | Final D1 requires old-epoch retirement, settled control and proven close, then exactly one new epoch/reservation. Do not reuse the retired epoch or infer succession from a larger generation. |
| Unconfirmed close or cleanup exhaustion | Retain separate obligations and budgets, withhold the affected path, quarantine uncertain ownership and block successor admission. Never claim STOPPED or erase pending work. |

Keep D2 fail-closed bootstrap/load/repair behavior and its sole durable writer.
D3 keeps desired RUNNING/STOPPED, the eight observed states, optimistic revision
checks, finite in-flight work and list/get readback; no lifecycle receipts,
client idempotency store or completion claim is added. D4 keeps ordinary MCP
lifecycle access without a new principal/kernel-operator authorization gate.
No new authorization gate is introduced before 0.9. Native-write and existing
eeBUS safeguards remain. D5 restores exact accepted/pending recovery material
before admission and retains verified compaction, independent obligations and
original bounded budgets. Output hooks cannot bypass or duplicate that path.

## Sole semantic read and truthful projection loss

Every semantic output reads accepted SemReg snapshots through the
[shared snapshot-read contract](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/api/semantic-snapshot-read.md).
Native diagnostics may retain native evidence but are never an alternative
semantic read source. No native registry fallback, shadow semantic cache,
on-demand acquisition or target-specific reinterpretation may fill missing data.

Capture selected assets once at a coherent read point, with the pack catalog
and one evaluation context. Preserve per-asset snapshot IDs and all revision
components; there is no global SemReg revision. Snapshot, evaluation, selection
and projection for one asset must bind exactly the same revisions and exact
candidate revisions. Mixed revisions fail the read with conflict; a later
publication cannot mutate the detached view. Preserve public-safe provenance,
source/epoch/generation, fences, retired tombstones and evidence access markers;
raw evidence bodies stay native-owned.

A missing/withheld asset carries an explicit disposition; it does not erase a
readable sibling. Unsafe source snapshots are withheld as whole asset entries,
not edited and relabeled as intact snapshots. Preserve exact zero, false and
absent values distinctly. Partial publication retains omitted valid fields and
their original age; retained history never becomes current admission evidence.
Unsupported target fields, ambiguity, stale/unavailable candidates and unknown
mappings produce explicit losses or withholding under the owning projection
contract. Do not invent loss/error codes, select a substitute source or silently
truncate. An eeBUS inbound Unknown cannot become qualified support by reflecting
it through an output.

## Source/sink conflicts and echo prevention

A source may own acquisition while a sink owns a target endpoint. Neither role
owns the other's semantics. Evaluate conflicts before forwarding or exposure:

| Situation | Decision and expected rejection/loss |
| --- | --- |
| Two drivers claim one SourceID, or conflict with hub ownership | Refuse the entire registration before effects; no ownership transfer. |
| Two descriptors claim one output binding | Refuse the entire registration; no last-writer-wins endpoint. |
| Several candidates describe one fact | Preserve alternatives/conflict and SemReg presentation policy; an unresolved projection is withheld with explicit loss, not selected by source count or display ID. |
| Multiple possible native operation routes | Keep operation routing outside this slice; existing admission fails closed on ambiguity. Presentation selection never chooses a route. |
| Bidirectional output returns as an acquisition observation | Preserve genuine native evidence and ancestry; do not relabel the output as independent qualified evidence or automatically derive another command. |
| Incoming causal path re-enters the receiver | `echo_suppressed`, no I/O and unchanged incoming context. This check precedes hop-capacity rejection when both apply. |
| New receiver would exceed causal budget, or context expires | No derived intent; preserve truthful observations with their quality. Keep the owning causal validation result. |
| Same correlation/operation already admitted, throughout its tombstone lifetime | `echo_suppressed`, no duplicate output, including after the echo window ends and before the operation deadline. Only ancestry echo suppression is window-bounded. |
| Same value in a separate admitted intent with new identity/correlation | Value equality alone is no rejection; existing authority, preconditions, route and deadline still apply. |

Use the provider contract's SemReg `CausalContext`: preserve Origin, correlation,
optional parent, expiry and ordered entered-receiver Path. Internal A starts
`[A]/1`; external ingress starts `[]/0`. Egress does not increment. On ingress,
validate the context, check receiver re-entry, check budget, then append the
receiver and increment atomically before processing. Rejection changes neither
path nor correlation. The maximum is eight hops; operation echo windows must be
positive and at most 300 seconds. Retain correlation tombstones through the later
of operation deadline or dispatch plus echo window, with clock uncertainty.
An already-admitted correlation/operation remains rejected as `echo_suppressed`
with no I/O throughout that tombstone lifetime, including after the echo window
ends. Only the ancestry clause for the output binding and operation definition
is bounded by the echo window; duplicate suppression is not.
Unknown/missing ancestry cannot be reset into a new origin to enable forwarding;
withhold derived output until the existing causal contract can validate it.

This specifies boundary behavior only. There is no operation router, automatic
feedback controller, new authority decision or new native write path here.

## Portal selection versus explicit publication

Follow the proposed
[Portal endpoint lifecycle](https://github.com/Project-Helianthus/helianthus-matter-binding/blob/main/docs/PORTAL_ENDPOINT_LIFECYCLE.md).
Discovery and direction declarations only expose options. Portal selection
records publication intent, separately from endpoint/fabric state. Selection
alone must not allocate, commission, report, choose a native route or make
Unknown evidence eligible. A future explicit publication transition must verify
that intent, target eligibility, current lifecycle admission and valid endpoint
restoration/allocation before exposure. This is an admission condition, not a
new authorization gate or a proposed public publish API.

Keep unselected, withheld, eligible and published as target design states,
separate from G5 desired/observed state. Restart, UI refresh and incoming values
do not promote withheld to published. Deselection stops active publication and
reporting while retaining native evidence and inactive endpoint identity where
the target contract permits. Re-selection requires renewed admission; no
substitute target is silently selected. Missing/corrupt/conflicting identity
state withholds affected publication without guessing a replacement. Existing
valid siblings and legitimate shared node state survive. Endpoint/fabric
identity and commissioning remain target-owned; a selection flag never proves
commissioning. This slice chooses no persistence owner, store, operation router
or hook implementation. The Matter binding's current readiness denial is not
changed by this proposal.

Existing eBUS MCP envelopes, deterministic ordering/hashes, schema/output goldens,
invoke intent/safety/idempotency checks and executable MCP/GraphQL parity remain
unchanged. No new namespace or lifecycle GraphQL surface is created. Existing
`core_stable` classifications do not promise namespace stability before 1.0.

## Synthetic-peer acceptance specification

These are manual design vectors and future implementation-test obligations,
not executed runtime tests. Use in-memory synthetic acquisition and output peers
with independent expected traces and counters; never listeners or real peers.
Each rejected case must assert zero endpoint exposure, no forwarding and no
unrelated-state mutation as applicable. Target losses use the owning projection
report rather than successful fabricated values.

| ID | Synthetic stimulus | Expected trace, rejection or loss |
| --- | --- | --- |
| BR-01 | Source-only thermal-family peer with no hooks | Descriptor is role-consistent; only qualified admitted source publication is possible. Output attempts are refused; no endpoint exists. Today registration acceptance still requires all S0 prerequisites. |
| BR-02 | Sink-only infrastructure-family peer with empty source set | Empty source set is valid; no invented semantic source. Today a declared output hook is rejected as unimplemented. Future supported hook stays unexposed until explicit publication and admission. |
| BR-03 | Bidirectional energy-family peer with disjoint owned source and output binding | Role consistency permits the proposal, not forwarding authority. Future output consumes only a detached snapshot; reflected ancestry is suppressed rather than republished as fresh independent evidence. |
| BR-04 | Source-only plus output hook; separately sink-only plus acquisition claim | Each contradictory descriptor rejects the batch before factories, declarations or endpoint calls; valid prior catalog remains. |
| BR-05 | Duplicate source owner; separately duplicate output owner | Each rejects the whole batch with zero effects; no display-ID/count tie-break. |
| BR-06 | Unknown direction, hook name or version; disabled invalid descriptor | Each rejects before effects, including disabled entries; no partial install. |
| BR-07 | Mutate caller descriptor after freeze; permute registration order | Frozen list/get unchanged, sorted and coherent; no factory calls. |
| BR-08 | STOP while a callback is queued, then deliver it after STOP and after successor START | Fence precedes drain; callback remains rejected in both deliveries. Old output cannot report or revive; history and healthy sibling remain. |
| BR-09 | Late prepare or activation result from a canceled correlation | Reject installation, abort its exact resource owner and record truthful close; no exposed endpoint. |
| BR-10 | Block semantic cleanup while close can progress; reverse the block | Each independent obligation progresses. STOPPED appears only when both settle; no sibling loss or overlapping successor. |
| BR-11 | Close timeout, then repeated wake/recovery | Unconfirmed close retains quarantine and original budget; no guessed proof, renewed budget or successor endpoint. |
| BR-12 | Completed STOP then START, or RESTART with duplicate wakes | One new epoch/reservation after retirement and close. Old callbacks remain invalid, new-epoch cursors follow G5, and desired state follows the final D1/D3 rules. |
| BR-13 | Publication changes after read capture; separately splice one revision | Captured output remains detached; spliced snapshot/evaluation/projection is rejected, not repaired by native acquisition. |
| BR-14 | Partial source update, unavailable sibling and retained old observation | Keep unaffected fields with original age; unavailable/lost items are explicit. No unknown-to-zero conversion or historical admission. |
| BR-15 | A to B to C to A, with receiver re-entry and full budget together | Path evolves only on ingress; A rejects `echo_suppressed` first with no I/O or mutation. |
| BR-16 | Expired/over-budget context; missing ancestry; separately redeliver an already-admitted correlation/operation after the echo window ends but before its later operation deadline, and again through tombstone expiry including clock uncertainty | No derived command for invalid context or missing ancestry. Keep truthful observation; each duplicate during the full tombstone lifetime returns `echo_suppressed` with no I/O or redispatch. Only ancestry echo suppression is window-bounded. A separate same-valued newly admitted intent is not rejected merely by value. |
| BR-17 | Discover/select while target evidence is Unknown, then refresh/restart | Withheld reason remains visible; zero endpoint allocation/publication/commissioning. Selection does not supply evidence. |
| BR-18 | Future admitted target deselects/reselects; identity restore is corrupt | Withdraw active reporting, retain permitted inactive identity and sibling state; corrupt restore withholds rather than reallocates or commissions. |

Future identity tests must independently vary DriverID, output-binding identity,
source epoch, driver generation and lifecycle correlation, using the active
reference as an independent oracle: each variant catches production removal of
that exact callback comparison. Acquisition ownership tests independently vary
SourceID and its configured owner; each catches the corresponding ownership-check
removal. Snapshot tests independently vary AssetID, capability instance,
SnapshotID, each of semantic/identity/facts/services/capabilities revision
components, and candidate ID/revision; each catches omission of that exact
binding comparison. Vary native connection generation separately from driver
generation to catch their conflation. Source count and display-ID changes must
leave identity unchanged. These are reasoned mutant mappings only; no mutant
kills or implemented test coverage are claimed here.

## Review and companion disposition

Manual acceptance reviews capability declarations, hook effects, every state
transition, conflict/echo ordering, source/read ownership, selection/publication
separation, losses and all BR examples against the contracts linked above.
Public links identify owner files on `main`; they are not frozen dependency
references or evidence that a future API has shipped.

**MERGE HOLD:** I30, the independent public docs-ebus bidirectional registration
companion, must merge before or with this design and use the same proposed
contract. No sibling output is a build dependency. This partial slice advances
parent #1125 without closing it. Runtime hook wiring, synthetic-peer execution,
independent review, committed-head CI, publication audit, native qualification,
transport, passive/live smoke and physical acceptance remain separate work.
