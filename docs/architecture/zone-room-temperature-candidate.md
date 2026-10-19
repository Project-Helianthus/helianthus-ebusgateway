# Detached zone room-temperature candidate

This partial implementation advances [issue #1231](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1231)
and [parent #952](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/952).
Nothing here is stable before Helianthus 1.0. The mapper is a pure function over
supplied observations and detached previous records; it has no acquisition,
registration, publication, transport or legacy consumer integration.

The public native contract comes first:
[zone room-temperature cutover](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/zone-room-temperature-cutover.md),
[B524 register catalog](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/protocols/vaillant/ebus-vaillant-B524-register-map.md),
and [thermal definitions](https://github.com/Project-Helianthus/helianthus-semreg/blob/main/semreg/v1/packs/thermal/definitions.go).
The implementation is [the detached mapper](../../cmd/gateway/thermal_zone_room_temperature_candidate.go),
with [synthetic vectors](../../cmd/gateway/thermal_zone_room_temperature_candidate_test.go).

## Source and claim review

**Proven**, within the supplied public contract's scope: OP02/GG03 RR000F is
the catalog room-temperature observation; RR0013 is separate sensor-binding
evidence and RR001C is separate association context. The characterized
controller-family byte order does not qualify other families. The existing
thermal air-temperature pack limits are -100..200 Celsius; they are pack
limits, not a native sensor range.

**Hypothesis**: eventual use of the existing air-temperature measurement and
zone service. **Unknown**: the final per-zone temperature FactKey dimension
binding, decimal conversion/rounding policy, native qualification, and a
universal freshness interval. These evidence labels remain independent of
qualification, freshness, runtime admission and physical acceptance.

The result therefore contains no SemReg fact, FactKey, decimal quantity,
capability declaration or admission flag. Every result carries explicit final
binding/decimal-policy withholding. Diagnostic loss strings are local text,
never invented reason DefinitionIDs. A finite native float candidate is not a
promoted measurement or a statement that a sensor association is trusted.

## Inputs and retention

The caller supplies one complete native RR000F slot identity, qualified codec
metadata, explicit observation freshness durations, evaluation points, and
separately qualified binding bytes and their width. There are no defaults for
byte order, codec qualification, binding width or TTL. Only explicitly supplied
f32 little-endian or big-endian, four-byte, Celsius metadata can decode a
candidate. Exact zero survives. Missing values, short/over-length bytes,
NaN/infinities, values outside pack limits, malformed bounds and violations of
supplied narrower profile bounds cannot replace valid native temperature.
No decimal conversion or rounding takes place.

Every identity field participates in matching: native PB/SB, opcode, operation,
group, instance, register, direction, initiator, target and reply source;
asset, service, source, epoch, generation, binding and association revisions;
and family, profile, profile revision, decoder revision and mapping revision.
The two context registers match the same identity with only the explicitly
recognized register changed. Zone display IDs, radio inference and setpoint
substitution have no role here. Unknown identity fields cannot establish a slot.

All attempts and previous observations survive in detached diagnostic history,
including foreign identities, failed values and distinct evidence references.
Previous claimed numeric values and loss flags are revalidated from raw data.
Selected temperature, sensor-binding bytes and association bytes are independent
copies. Sensor binding is retained only against an explicitly qualified width;
its integer/enum meaning is withheld. Association bytes remain uninterpreted.
No RR0013 observation identifies a radio endpoint, and RR001C creates no topology.

Original wall receipt and typed monotonic receipt remain attached to each
observation. Evaluation never becomes receipt. Selection uses monotonic points
within the evaluation clock epoch, after canonical typed validation. Equal
receipt conflicts, including provenance differences, select no winner. Complete
history remains available even when clock continuity is unknown. Sorting full
records makes permutation outcomes deterministic without collapsing evidence
owner, kind, contract, digest, access or redaction differences.

Freshness is separately `fresh`, `stale` or `unknown`: a supplied positive
canonical duration is compared to elapsed monotonic time; equality at expiry is
stale. Malformed durations, future receipts and absent clock continuity are
unknown. No freshness status releases final publication withholding.

## Transition and example review

| Synthetic transition | Result and loss |
| --- | --- |
| First exact four-byte zero, qualified codec | Native Celsius zero, original receipt retained; final publication withheld. |
| Good 20 followed by malformed temperature | Retain 20 at its original receipt; record failed attempt separately. |
| Later valid 21, same complete identity | Independently recover temperature at the new original receipt; context history survives. |
| New binding bytes differ from qualified bytes | Retain raw binding and temperature independently; explicit association loss requires requalification and a new correlated temperature. A native candidate does not relabel the old sensor. |
| Malformed binding width after good binding | Retain prior width-qualified binding; preserve rejected bytes and association loss. |
| Requalification with temperature older than binding | Explicit temperature-predates-binding loss; no current association claim. |
| Equal asset and temperature, different native instance | Foreign record stays in history; it cannot update the requested native slot. No semantic key is constructed. |
| Old duplicate, stale evaluation or another clock epoch | Never renew receipt; stale/unknown status or incomparable history remains explicit. |

`History` is the complete supplied diagnostic window, not a bounded persistent
store. Callers control its size. `Attempts` identifies the current call's supplied
observations independently of historical selections. `LatestLoss` reports the
newest comparable temperature failure or association problem; incomparable or
malformed attempt timing is explicit. It is not a total ordering of clocks.

## Remaining acceptance

Focused synthetic tests establish function behavior only. They do not establish
native qualification, installation suitability, physical acceptance or runtime
publication. Existing MCP envelopes, MCP/GraphQL parity and legacy zones remain
untouched. Any later adapter must preserve identity, receipt, loss, qualification
and freshness consistently before cutover. Public binding and rounding evidence,
independent review, committed-head gates, transport/runtime integration and the
remaining north-south flow remain separate parent work. No eeBUS output belongs
in this mapper.
