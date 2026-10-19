# Detached DHW measured-temperature candidate

This partial slice advances [issue #1232](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1232)
and [parent #952](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/952).
Nothing here is stable before Helianthus 1.0. It is a pure application seam,
not acquisition, registration, a common production provider, or live cutover.

The [public native cutover contract](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/dhw-measured-temperature-cutover.md)
and [B524 native register catalog](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/protocols/vaillant/ebus-vaillant-B524-register-map.md)
precede interpretation. Only B5/24 OP02/GG01/II00 RR0005 is measured input.
Separately supplied RR0001 exact two-byte UIN context determines presence:
nonzero means present under its supplied qualification, zero means absent;
missing, malformed, foreign, stale, or unqualified context stays unavailable.
The mapper supports explicitly supplied little-endian f32 and UIN codecs only.
It does not inspect framing itself: the native owner must supply extracted
value bytes, original request/reply evidence, and exact correlation. Both raw
fields must be non-empty for measurement and presence, including selected history.
`Value` extraction and `Correlated` are trusted caller input: this seam does not
verify that `Value` matches `RawReply`, or that `RawRequest` encodes the selector.
The native owner must verify these relationships before invoking the mapper;
non-empty raw evidence alone does not establish byte consistency or correlation.
Raw and normalized identities must already agree; plus-one reply normalization is
unsupported here. OP06, other instances, cylinder readings, targets, and
charging-state inference cannot substitute for the measurement.

## Evidence and candidate boundary

**Proven** denotes the cited native selector and pack vocabulary at their
respective layers. **Hypothesis** denotes the proposed Thermal binding.
**Unknown** includes unresolved concrete fact identity and decimal policy.
These evidence labels are preserved separately from native qualification and
freshness; even a Proven label supplies neither. Synthetic tests qualify no
controller family or device.

The [Thermal vocabulary](https://github.com/Project-Helianthus/helianthus-semreg/blob/main/semreg/v1/packs/thermal/definitions.go)
contains `thermal.measurement.water_temperature`, `unit.celsius`, and
`thermal.service.dhw`. The seam returns a native float64 candidate, exactly
widened from f32, with local textual losses. It never constructs a SemReg
quantity, decimal, DefinitionID, FactKey dimension, service, or capability.
`Facts` is always empty, and `Withheld` always records that the final fact key
and decimal policy remain unresolved. There is no default TTL or invented
qualification. Unknown binding, codec, bounds, or freshness cannot provide an
eligible candidate. Rounding is not attempted, including for fractional values.

A qualified measurement requires exactly four bytes, a finite value inside
both the pack interval [-100, 200] Celsius and caller-supplied qualified bounds,
an exact identity/evidence match, a successful correlated response, and valid
receipt metadata. Historical native range hints do not supply these bounds.
Known exact zero, including its native sign, survives. Codec metadata must
explicitly assert qualification and bind the complete key and evidence.
The evidence owner, kind, contract, digest, access, and redaction all matter;
none is discarded in matching or detached diagnostic history.

## Transitions and loss

The mapper independently evaluates measurement and presence. Each component
returns a detached latest attempt, detached supplied history, and a selected
last good native record when valid for the exact current identity and codec.
Rejected history remains diagnostic history, never selected under a foreign key.
Raw request, reply, extracted bytes, evidence and original receipt are copied.
A failed attempt cannot erase or rejuvenate a last good measurement.

Typed receipt/evaluation points and positive canonical `FreshForNS` must be
valid. Age uses the same monotonic clock epoch; age equal to the supplied
freshness interval is stale. Future, incomparable, or malformed points are
unknown. A current record replaces selected history only when fresh and strictly
newer in the same clock epoch. Duplicate, older, or incomparable receipts lose
with local text. No wall-clock time is used to order receipts. Unavailable or qualified-absent current
presence withholds the current candidate without
erasing measured history. History remains available when stale; no retention
expiry or freshness duration is invented. An unknown policy cannot label it fresh.
A fresh newer measurement and independently valid present context recover the
candidate; recovery still emits no SemReg fact.

Every selector field (PB, SB, opcode, operation, group, instance, register,
direction), asset, service instance, source, source epoch, generation, binding,
binding/association/semantic revision, native identity, family, profile, profile
revision, and decoder revision participates in identity matching. A changed
binding needs separately qualified evidence, never a relabeled historical receipt.
Each invocation returns one source candidate. Same-asset candidates with differing
service instance IDs remain separate: there is no asset/definition winner or
cross-source projection merge. The controller measurement and boiler mirror
represent this same source and receipt, never two independent facts.

For example, synthetic bytes `00 00 00 00`, qualified matching metadata, a newer
fresh receipt, and exact present context yield native Celsius zero with publication
withheld. Missing or five-byte measurement input after a good 45 Celsius record
preserves that record's bytes and receipt and records a failed attempt. Once its
age reaches the supplied freshness interval, history is stale. A fresh newer
`00 00 38 42` measurement recovers native 46 Celsius without refreshing any old
record or inventing an output binding.

## Acceptance boundary

Focused `TestI32` vectors cover exact zero, widths, finite/profile/pack bounds,
independently varied identity and evidence leaves, missing raw request/reply,
invalid evidence status, supplied presence, typed time,
stale boundaries, receipt ordering, recovery, permutation, and detached history.
Manual documentation review checks sources, claims, transitions, losses, examples,
and public main-branch links. No token/readability test establishes this contract.

Existing legacy consumers, eBUS MCP envelopes and methods, and GraphQL remain
unchanged. No new namespace or projection is exposed. Cutover still requires the
native owner's public family-qualified framing, codec, validity and presence
evidence; final fact identity, decimal and freshness representation decisions;
registration and persistence; and executable MCP/GraphQL/HA parity from one
accepted receipt. Independent review, full CI, publication audit, transport,
passive/live smoke, runtime and physical acceptance are unrun in this slice.
Parent #952 remains open. eeBUS output stays in its native registry owner.
