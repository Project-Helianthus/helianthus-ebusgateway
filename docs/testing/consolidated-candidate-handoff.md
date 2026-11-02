# Consolidated candidate hardware acceptance handoff

Refs [#1118](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1118) /
[#1099](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1099).
This is a partial preparation slice for the **0.7 candidate**, not parent closure,
a hardware verdict, a release decision, or permission to operate equipment.
One consolidated operator session is planned; preparing this document needed no device.

## Order and candidate identity

Follow **INT-17 → INT-19 → INT-20 → INT-21**, downstream of INT-16. INT-12 and
INT-13 sit upstream of INT-16 and are therefore gated through INT-17:

1. **INT-17**: complete offline acceptance and hardware-test preparation against
   the frozen INT-16 BOM; establish `HARDWARE_TEST_READY` using integrated
   composition, fixture/replay, consumer, degraded/restart/reconnect/cleanup,
   exact-BOM CI/review and operator-procedure evidence.
2. **INT-19**: Daybreak Blue independently reviews that final candidate.
3. **INT-20**: run the single consolidated, separately authorized operator session
   below against that same candidate, including retained live obligations.
4. **INT-21**: publish only the BOM covered by final review and required results.

All four packages are pending in the release plan. They are release-plan
IDs, not issue numbers. The plan defines no intervening package.
The [milestone map][plan] and [canonical hard stops][canonical] govern this sequence.

The candidate is bound through the **M44 association**: bundle, association,
component/output artifact IDs, complete architecture inventory and exact-BOM
attestation must refer to the same privately verified input/output inventory.
Use [M44 candidate metadata][m44] and [K25 cohort validation][k25]; labels or
matching cohorts alone do not establish equality. Missing, stale, partial or
mismatched associations withhold readiness. M44 acceptance does not imply
hardware qualification. Keep exact source/build coordinates in the orchestrator's
private verification record; public entries contain only permitted opaque IDs
and public-safe evidence references.

## Preconditions and pending prerequisites

- [ ] Confirm the frozen INT-16 candidate, complete M44 association and all
  owning acceptance records, full required gates and fresh independent review.
  Candidate readiness remains unverified here.
- [ ] Resolve each pending prerequisite below with its owner before its affected
  session segment; missing procedures, contracts or equipment leave results
  Not run / Unknown and prevent complete candidate acceptance.
- [ ] Agree one bounded session plan: family/topology/firmware scope, selected
  manifest cases, normal/degraded paths, observation bounds, recovery and
  cleanup, operator and reviewer roles. Retain exact installation details privately.
- [ ] Obtain action-time operator confirmation immediately before credentials,
  installation/configuration writes, restarts, live mutation, destructive actions
  or safety-relevant control. This checklist supplies no such authority.
- [ ] Read the public native [ENH][enh], [ENS][ens] and applicable native mapping
  contracts before interpreting protocol evidence. Follow K05/K06 obligations;
  do not derive wire facts from UI labels or implementation coverage.
- [ ] Preserve native evidence separately from decoded/promoted views. Publish
  only synthetic examples explicitly labelled synthetic and public-safe reports;
  no observed captures or excerpts, private coordinates, credentials, serials,
  contributor identities, capture times or source revision identifiers.

| Pending prerequisite | Owner and reference | Result | Evidence grade |
|---|---|---|---|
| Frozen INT-16 candidate and complete M44 association | Add-on candidate owner; [M44][m44], [plan][plan] | Not run | Unknown |
| INT-12 eeBUS output-binding acceptance; public contract page not found | Gateway integration owner with native eeBUS owners; [plan][plan], [participation owner][eebus-owner] must supply accepted criteria | Not run | Unknown |
| INT-13 Matter output-binding acceptance | Gateway binding owner; [projection contract](../architecture/matter-output-binding-v1.md), [native binding owner][matter-owner] | Not run | Unknown |
| Explorer Pi operator procedure not found | Public procedure owner undefined; [Explorer repository][explorer-repo] / [offline card][explorer] require owner assignment | Not run | Unknown |
| HA/Ingress physical checks and implementation procedure | Add-on and gateway Portal owners; [offline checklist][ha], [Portal scope][portal-plan] | Not run | Unknown |
| ESERA passive capture remains pending | FOLLOWUP-01 owners; [K05 worksheet][k05] and its parent links | Not run | Unknown |

The K05, K06 and M44 preparation dependencies are merged. Their documents
are references, not completed physical evidence. These pending prerequisites do
not prevent finishing this handoff; they remain visible for operator readiness.

## One operator session

1. **Entry review:** confirm INT-17 then INT-19 completion and unchanged candidate
   association; select family-specific procedures and private evidence retention.
2. **Passive baseline:** assess P01–P06 and the ESERA boundary without borrowing
   authority from active cases. K05 observes only an independently authorized
   passive stream; injected negative controls remain synthetic offline replay.
3. **Transport and adapter-direct:** execute the owning approved T01–T88 and AD
   procedures, including applicable proxy adjuncts. Account for every case and
   expected-failure reason; infrastructure-blocked, unexpected fail or xpass
   cannot become a passing acceptance. K06 owns each AD assertion and artifact gap.
4. **Retained product behavior:** use [retained acceptance][legacy] for the exact
   LEGACY P/N criteria and mux disposition; record each named row below. Reuse
   shared transport observations only with traceable scope, never by implication.
5. **Consumers and outputs:** collect HA/Ingress, Explorer Pi and output-binding
   evidence under their owners' accepted procedures. Cover normal and degraded
   behavior, restart/reconnect, freshness, identity, indeterminate outcomes and
   recovery for each claimed family/profile/firmware and operation. A missing
   device remains an acceptance blocker.
6. **Exit review:** verify cleanup and restored intended state; reconcile every
   result to the same association, record omissions and stop reasons, and return
   unresolved work to #1099 and owning contracts. INT-21 remains pending until
   every required result and final review covers the unchanged BOM.

This session adds no executable device commands. The [add-on smoke runbook][smoke]
is context, not installation authority. The [transport gate](../../scripts/transport_gate.sh)
and [passive gate](../../scripts/passive_smoke_gate.sh) remain authoritative for
classification; this document changes neither gate nor expected-failure inventory.
P01–P06 are not deferred. T01–T88 remain unrun pending authorized physical validation.
PX01–PX12 are additional obligations when proxy wire semantics are in scope.
No gate override is requested; an override never authorizes live writes and never
bypasses a triggered offline Modbus RTU gate.

## Stop conditions and post-fix revalidation

Stop the affected segment on uncertain authorization/isolation, unexpected
observer-originated traffic, observation-bound expiry, evidence loss, unsafe or
indeterminate control outcome, failed cleanup, missing device/procedure, identity
or association mismatch, unexpected fail/xpass, or lost freshness/provenance.
Record the stop reason and unexecuted cases. Withhold new semantic admission for
missing, contradictory, stale, malformed or unqualified evidence. Preserve partial
last-known-good fields with original lineage and freshness; never promote old
observations or fabricated defaults. Native qualification, invalidation and trust
rules continue to apply independently of the public evidence grade.

**Any code, configuration, dependency or package fix during INT-19 or INT-20
invalidates earlier candidate evidence and restarts the chain for the changed BOM.**
Create the new M44 association, return to INT-17 offline acceptance/preparation,
obtain fresh INT-19 Daybreak review, then perform affected INT-20 hardware and
conformance revalidation before INT-21. Owners must document the impact-based
case selection; unchanged evidence may remain historical but is not silently
carried forward as changed-candidate acceptance. A later RTU pin change triggers
the offline RTU composition and pinned endpoint gate plus this revalidation rule.

All MCP namespaces remain pre-stable before 1.0. Existing eBUS invoke safety,
deterministic envelopes/goldens and MCP-to-GraphQL graduation/parity remain owner
contracts; visibility through HA, Portal or an output does not grant write authority.

## Blank candidate result template

Every candidate result below starts **Not run**, with evidence **Unknown** and
qualification and admission **Unassessed**. Evidence grades are
**Proven / Hypothesis / Unknown**; qualification and admission are separate
decisions. Proven for one bounded observation does not qualify a family, codec,
profile, write or binding.

For each later result, record the M44 association, family/topology/firmware and
operation scope, procedure/case reference, expected versus actual observation,
public-safe evidence link and section, omissions/stop reason, independent review,
qualification and admission separately. Every AD, passive, retained and consumer
row that leaves **Not run** links its own per-row evidence record carrying exactly
these fields; the Result cell cites that record, and a row without one stays
**Not run**, whatever the observation. Keep raw native evidence outside the public
package alongside its interpretation. A family summary requires per-case accounting;
it cannot conceal an unexecuted or blocked case. Failed or unverified rows stay
unaccepted. The tables below do not duplicate K05/K06 evidence indexes.

For every individual AD, passive, ESERA, retained, consumer, output and prior-evidence
applicability row below, attach a public-safe per-row evidence record and link its
exact section in that row's governing-reference cell. The record must identify the
row and include every field required above, including the M44 association, actual
observation, evidence link, omissions/stop reason and independent review. Keep the
row Not run / Unknown / Unassessed / Unassessed until its record is complete and
independently reviewed; failed or unverified results remain unaccepted.

The T01–T88 and PX01–PX12 range rows below are summaries only. Before assigning
any aggregate disposition, attach and link a public-safe per-case report covering
every ID in that range, including unexecuted and blocked cases. Record all fields
required above and each case's result, evidence grade, qualification and admission
individually. Keep the summary Not run / Unknown / Unassessed / Unassessed until
that report is complete and independently reviewed. Mixed outcomes must remain
visible in the summary; any blocked, unexecuted, unexpected fail or xpass case
prevents passing acceptance for the range.

| Case / family | Governing reference and scope | Result | Evidence grade | Qualification | Admission |
|---|---|---|---|---|---|
| T01–T04 | [Smoke matrix][matrix]: direct adapter, four transport cases | Not run | Unknown | Unassessed | Unassessed |
| T05–T08 | [Smoke matrix][matrix]: via ebusd-tcp, four cases; retain documented expected-fail inventory | Not run | Unknown | Unassessed | Unassessed |
| T09–T24 | [Smoke matrix][matrix]: proxy single-client, sixteen cases | Not run | Unknown | Unassessed | Unassessed |
| T25–T88 | [Smoke matrix][matrix]: proxy dual-client, sixty-four cases | Not run | Unknown | Unassessed | Unassessed |
| PX01–PX12 | [Proxy adjunct][px]: additional proof when proxy wire semantics are in scope | Not run | Unknown | Unassessed | Unassessed |
| AD01 | [K06 AD01][k06]; use that ID's assertion and evidence requirements | Not run | Unknown | Unassessed | Unassessed |
| AD02 | [K06 AD02][k06]; use that ID's assertion and evidence requirements | Not run | Unknown | Unassessed | Unassessed |
| AD03 | [K06 AD03][k06]; use that ID's assertion and evidence requirements | Not run | Unknown | Unassessed | Unassessed |
| AD04 | [K06 AD04][k06]; use that ID's assertion and evidence requirements | Not run | Unknown | Unassessed | Unassessed |
| AD05 | [K06 AD05][k06]; use that ID's assertion and evidence requirements | Not run | Unknown | Unassessed | Unassessed |
| AD06 | [K06 AD06][k06]; use that ID's assertion and evidence requirements | Not run | Unknown | Unassessed | Unassessed |
| AD07 | [K06 AD07][k06]; use that ID's assertion and evidence requirements | Not run | Unknown | Unassessed | Unassessed |
| AD08 | [K06 AD08][k06]; use that ID's assertion and evidence requirements | Not run | Unknown | Unassessed | Unassessed |
| AD09 | [K06 AD09][k06]; use that ID's assertion and evidence requirements | Not run | Unknown | Unassessed | Unassessed |
| AD10 | [K06 AD10][k06]; use that ID's assertion and evidence requirements | Not run | Unknown | Unassessed | Unassessed |
| AD11 | [K06 AD11][k06]; use that ID's assertion and evidence requirements | Not run | Unknown | Unassessed | Unassessed |
| AD12 | [K06 AD12][k06]; use that ID's assertion and evidence requirements | Not run | Unknown | Unassessed | Unassessed |
| P01 | [Passive gate](../../scripts/passive_smoke_gate.sh): unsupported_or_misconfigured rejection, not passive receipt proof | Not run | Unknown | Unassessed | Unassessed |
| P02 | [Passive gate](../../scripts/passive_smoke_gate.sh): unsupported_or_misconfigured rejection, not passive receipt proof | Not run | Unknown | Unassessed | Unassessed |
| P03 | [Passive gate](../../scripts/passive_smoke_gate.sh): required passive proof within the owning topology scope | Not run | Unknown | Unassessed | Unassessed |
| P04 | [Passive gate](../../scripts/passive_smoke_gate.sh): required passive proof within the owning topology scope | Not run | Unknown | Unassessed | Unassessed |
| P05 | [Passive gate](../../scripts/passive_smoke_gate.sh): required passive proof within the owning topology scope | Not run | Unknown | Unassessed | Unassessed |
| P06 | [Passive gate](../../scripts/passive_smoke_gate.sh): unsupported_or_misconfigured rejection, not passive receipt proof | Not run | Unknown | Unassessed | Unassessed |
| ESERA / FOLLOWUP-01 | [K05][k05]: boundary, four independent decisions and negative controls; capture remains pending | Not run | Unknown | Unassessed | Unassessed |

The P03 [bounded observe-first proof][p03] is specific to its proxy-backed ENS
family; it does not supply ESERA proof or universal passive capability.

Retain the following LEGACY IDs without reproducing installation-specific criteria.
P/N namespaces are local to each retained family. LEGACY-MUX has no P/N IDs in
the retained source: use its existing ID and complete the owning disposition,
positive/negative, live-observable and transport obligations without inventing IDs.

| Retained ID | Governing reference | Result | Evidence grade | Qualification | Admission |
|---|---|---|---|---|---|
| LEGACY-PERSIST P1 | [Retained acceptance][legacy-persist] | Not run | Unknown | Unassessed | Unassessed |
| LEGACY-PERSIST P2 | [Retained acceptance][legacy-persist] | Not run | Unknown | Unassessed | Unassessed |
| LEGACY-PERSIST P3 | [Retained acceptance][legacy-persist] | Not run | Unknown | Unassessed | Unassessed |
| LEGACY-PERSIST P4 | [Retained acceptance][legacy-persist] | Not run | Unknown | Unassessed | Unassessed |
| LEGACY-PERSIST P5 | [Retained acceptance][legacy-persist] | Not run | Unknown | Unassessed | Unassessed |
| LEGACY-PERSIST P6 | [Retained acceptance][legacy-persist] | Not run | Unknown | Unassessed | Unassessed |
| LEGACY-PERSIST N1 | [Retained acceptance][legacy-persist] | Not run | Unknown | Unassessed | Unassessed |
| LEGACY-PERSIST N2 | [Retained acceptance][legacy-persist] | Not run | Unknown | Unassessed | Unassessed |
| LEGACY-PERSIST N3 | [Retained acceptance][legacy-persist] | Not run | Unknown | Unassessed | Unassessed |
| LEGACY-PERSIST N4 | [Retained acceptance][legacy-persist] | Not run | Unknown | Unassessed | Unassessed |
| LEGACY-IDENTITY P1 | [Retained acceptance][legacy-identity] | Not run | Unknown | Unassessed | Unassessed |
| LEGACY-IDENTITY P2 | [Retained acceptance][legacy-identity] | Not run | Unknown | Unassessed | Unassessed |
| LEGACY-IDENTITY P3 | [Retained acceptance][legacy-identity] | Not run | Unknown | Unassessed | Unassessed |
| LEGACY-IDENTITY P4 | [Retained acceptance][legacy-identity] | Not run | Unknown | Unassessed | Unassessed |
| LEGACY-IDENTITY P5 | [Retained acceptance][legacy-identity] | Not run | Unknown | Unassessed | Unassessed |
| LEGACY-IDENTITY P6 | [Retained acceptance][legacy-identity] | Not run | Unknown | Unassessed | Unassessed |
| LEGACY-IDENTITY N1 | [Retained acceptance][legacy-identity] | Not run | Unknown | Unassessed | Unassessed |
| LEGACY-IDENTITY N2 | [Retained acceptance][legacy-identity] | Not run | Unknown | Unassessed | Unassessed |
| LEGACY-IDENTITY N3 | [Retained acceptance][legacy-identity] | Not run | Unknown | Unassessed | Unassessed |
| LEGACY-IDENTITY N4 | [Retained acceptance][legacy-identity] | Not run | Unknown | Unassessed | Unassessed |
| LEGACY-IDENTITY N5 | [Retained acceptance][legacy-identity] | Not run | Unknown | Unassessed | Unassessed |
| LEGACY-MUX | [Retained acceptance][legacy-mux] | Not run | Unknown | Unassessed | Unassessed |

| Consumer / output / prior evidence | Governing reference and candidate scope | Result | Evidence grade | Qualification | Admission |
|---|---|---|---|---|---|
| HA/Ingress routing | [HA checklist][ha]: root/prefix navigation, assets, bootstrap, GraphQL, websocket and SSE, reconnect and session expiry | Not run | Unknown | Unassessed | Unassessed |
| HA/Ingress trust | [HA checklist][ha]: denied untrusted/direct access and spoofed headers; selective publication independently checked | Not run | Unknown | Unassessed | Unassessed |
| HA/Ingress lifecycle and identity | [HA checklist][ha], [smoke runbook][smoke]: onboarding, restart/update, preserved configuration/instance identity, fresh defaults and consumer compatibility | Not run | Unknown | Unassessed | Unassessed |
| Explorer Pi | [Offline card][explorer] is fixture/build evidence only; Pi procedure pending owner assignment | Not run | Unknown | Unassessed | Unassessed |
| INT-12 eeBUS output binding | [Plan][plan] and [native participation owner][eebus-owner]; accepted contract/procedure pending | Not run | Unknown | Unassessed | Unassessed |
| INT-13 Matter output binding | [Projection contract](../architecture/matter-output-binding-v1.md) plus [native binding owner][matter-owner]; projection alone supplies no runtime or fabric evidence | Not run | Unknown | Unassessed | Unassessed |
| Prior RTU evidence applicability | [R01 upstream PR][rtu], [J19 gateway PR][rtu-pin]; historical evidence only, no repeated hardware session scheduled | Not run | Unknown | Unassessed | Unassessed |

The Huawei Modbus TCP rows below cover each gateway family of the
[multi-endpoint contract][modbus-multi]. The gateway registers every Huawei
instance withheld (`FAMILY_NOT_ADMITTED`) until that family's native contract
admits a qualification, so no row can leave **Not run** before then. The
per-row evidence record rule above applies to each row; a family summary
cannot replace its four rows. The governing references are completed by the
issues that compose and publish each family.

| Case / family | Governing reference and scope | Result | Evidence grade | Qualification | Admission |
|---|---|---|---|---|---|
| HUAWEI-SMARTLOGGER identity | [Multi-endpoint contract][modbus-multi], family `huawei.smartlogger`: the declared family's own bounded identity reads on one configured instance; no other family's reads | Not run | Unknown | Unassessed | Unassessed |
| HUAWEI-SMARTLOGGER monitoring publication | [Multi-endpoint contract][modbus-multi], family `huawei.smartlogger`: per-instance publication under the instance source and asset rule; withheld instances publish nothing | Not run | Unknown | Unassessed | Unassessed |
| HUAWEI-SMARTLOGGER degraded and reconnect | [Multi-endpoint contract][modbus-multi], family `huawei.smartlogger`: per-instance failure isolation, one request in flight with the configured gap and settle wait, reconnect backoff; other instances, eBUS, HTTP and MCP unaffected | Not run | Unknown | Unassessed | Unassessed |
| HUAWEI-SMARTLOGGER negative cross-family | [Multi-endpoint contract][modbus-multi], family `huawei.smartlogger`: an instance facing another family's identity is withheld and never switches family | Not run | Unknown | Unassessed | Unassessed |
| HUAWEI-EMMA identity | [Multi-endpoint contract][modbus-multi], family `huawei.emma`: the declared family's own bounded identity reads on one configured instance; no other family's reads | Not run | Unknown | Unassessed | Unassessed |
| HUAWEI-EMMA monitoring publication | [Multi-endpoint contract][modbus-multi], family `huawei.emma`: per-instance publication under the instance source and asset rule; withheld instances publish nothing | Not run | Unknown | Unassessed | Unassessed |
| HUAWEI-EMMA degraded and reconnect | [Multi-endpoint contract][modbus-multi], family `huawei.emma`: per-instance failure isolation, one request in flight with the configured gap and settle wait, reconnect backoff; other instances, eBUS, HTTP and MCP unaffected | Not run | Unknown | Unassessed | Unassessed |
| HUAWEI-EMMA negative cross-family | [Multi-endpoint contract][modbus-multi], family `huawei.emma`: an instance facing another family's identity is withheld and never switches family | Not run | Unknown | Unassessed | Unassessed |
| HUAWEI-SDONGLE identity | [Multi-endpoint contract][modbus-multi], family `huawei.sdongle`: the declared family's own bounded identity reads on one configured instance; no other family's reads | Not run | Unknown | Unassessed | Unassessed |
| HUAWEI-SDONGLE monitoring publication | [Multi-endpoint contract][modbus-multi], family `huawei.sdongle`: per-instance publication under the instance source and asset rule; withheld instances publish nothing | Not run | Unknown | Unassessed | Unassessed |
| HUAWEI-SDONGLE degraded and reconnect | [Multi-endpoint contract][modbus-multi], family `huawei.sdongle`: per-instance failure isolation, one request in flight with the configured gap and settle wait, reconnect backoff; other instances, eBUS, HTTP and MCP unaffected | Not run | Unknown | Unassessed | Unassessed |
| HUAWEI-SDONGLE negative cross-family | [Multi-endpoint contract][modbus-multi], family `huawei.sdongle`: an instance facing another family's identity is withheld and never switches family | Not run | Unknown | Unassessed | Unassessed |

### Prior RTU evidence and its limits

The R01/J19 record (modbus PR #40) reports all checks passing in a one-time read-only
run of function codes 03 and 04 against a standard Modbus RTU server device through a
USB-to-RS-485 adapter family on Linux aarch64, using 8N1 and a deliberately stale
starting speed. Scope covered open/speed inheritance, a long chunked FC03 response,
200 back-to-back reads, absent-unit timeout and recovery, two concurrent callers,
close/reopen and an unsupported-function exception. It is recorded on
[helianthus-modbus PR #40][rtu] and consumed by [gateway PR #1092][rtu-pin]
for the dependency pin only.

This is prior upstream PR-head evidence for one adapter family and read-only
operations; qualification stays fixture-only. It is not a gateway candidate PASS
and is not scheduled for repeat. The blank applicability row above remains
Not run / Unknown for this candidate. Retain the historical result with its scope;
do not convert it into a new live acceptance claim or use it to bypass a triggered
offline RTU gate.

[plan]: https://github.com/Project-Helianthus/helianthus-execution-plans/blob/main/software-stabilization-07.implementing/91-milestone-map.md
[canonical]: https://github.com/Project-Helianthus/helianthus-execution-plans/blob/main/software-stabilization-07.implementing/00-canonical.md
[legacy]: https://github.com/Project-Helianthus/helianthus-execution-plans/blob/main/software-stabilization-07.implementing/92-retained-acceptance.md
[legacy-persist]: https://github.com/Project-Helianthus/helianthus-execution-plans/blob/main/software-stabilization-07.implementing/92-retained-acceptance.md#legacy-persist--execution-plans-27
[legacy-identity]: https://github.com/Project-Helianthus/helianthus-execution-plans/blob/main/software-stabilization-07.implementing/92-retained-acceptance.md#legacy-identity--execution-plans-23
[legacy-mux]: https://github.com/Project-Helianthus/helianthus-execution-plans/blob/main/software-stabilization-07.implementing/92-retained-acceptance.md#legacy-mux--execution-plans-30
[portal-plan]: https://github.com/Project-Helianthus/helianthus-execution-plans/blob/main/software-stabilization-07.implementing/94-portal-configuration-and-ha-ingress.md
[k05]: https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/development/esera-passive-evidence-package.md
[k06]: https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/development/ad01-ad12-evidence-index.md#retained-rows
[matrix]: https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/development/smoke-matrix.md#matrix-coverage
[px]: https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/development/smoke-matrix.md#proxy-semantics-adjunct-gate-px01px12
[p03]: https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/development/smoke-matrix.md#observe-first-proof-note
[enh]: https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/protocols/enh.md
[ens]: https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/protocols/ens.md
[m44]: https://github.com/Project-Helianthus/helianthus-ha-addon/blob/main/docs/candidate-metadata-contract.md
[k25]: https://github.com/Project-Helianthus/helianthus-ha-addon/blob/main/RELEASE_COHORT.md
[ha]: https://github.com/Project-Helianthus/helianthus-ha-addon/blob/main/docs/portal-ingress-offline-acceptance.md
[smoke]: https://github.com/Project-Helianthus/helianthus-ha-addon/blob/main/SMOKE_RUNBOOK.md
[explorer]: https://github.com/Project-Helianthus/helianthus-vrc-explorer/blob/main/docs/offline-acceptance.md
[explorer-repo]: https://github.com/Project-Helianthus/helianthus-vrc-explorer
[eebus-owner]: https://github.com/Project-Helianthus/helianthus-eebusreg/issues/137
[matter-owner]: https://github.com/Project-Helianthus/helianthus-matter-binding/issues/4
[rtu]: https://github.com/Project-Helianthus/helianthus-modbus/pull/40
[rtu-pin]: https://github.com/Project-Helianthus/helianthus-ebusgateway/pull/1092
[modbus-multi]: https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/api/modbus-multi-endpoint-runtime.md
