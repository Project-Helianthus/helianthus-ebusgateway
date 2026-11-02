# Recovery checkpoint validation supplement

This supplement bounds recovery corruption checks for
[issue #1124](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1124),
a partial follow-up under
[issue #1032](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1032).
It describes the internal checkpoint seam; it grants no runtime authority or
public lifecycle API. Nothing is stable before Helianthus 1.0.

Before installing source claims, identity regimes or epoch reservations, restore
requires all of the following:

- Every checkpoint asset's Source and Driver match its existing declaration.
  Forged matching Regimes and Drivers do not override that declaration.
- Every activation, fence, revocation and retirement has an earlier admission
  for the exact source, epoch and incarnation. A control or publication after
  restart may fence or retire an epoch admitted in an earlier incarnation only
  when every named asset carries matching write-ahead Control metadata or a
  validated accepted publication containing that transition in the new
  incarnation, or is explicitly failed and remains closed without replay or
  snapshot installation. Control metadata matches source, epoch, incarnation,
  kind and, for a fence, generation; its attempts remain subject to replay checks.
  This exception requires an earlier admission for the exact source and epoch
  and cannot justify a succession ordinal. A different
  generation can be legitimate; generation equality with the initial admission
  is not required. Later admissions cannot justify earlier events.
- A non-failed asset named in generation-fence history retains a matching Control
  entry, including when the fence is pending or rejected, unless a validated
  accepted publication already carries that asset's exact source, epoch,
  generation and incarnation fence. A generation transition can carry that fence without a separate
  Control on its initiating asset. Dropping an unresolved entry must never create
  a synthetic successful gate that Resettle can open. A failed asset may omit
  both Control and accepted log on re-export and remains closed. Even if every
  named asset failed, the earlier source and epoch admission is still required;
  retaining the inert transition grants no new reservation or live authority.
- Published ownership is derived from accepted batch envelopes whose asset and
  source match the checkpoint entry and whose computed digest matches the
  envelope digest. An entry label alone is insufficient. This ownership check
  does not replace detached kernel replay and exact accepted-result validation.

Succession ordinals belong to a source within an incarnation. A legacy regime
cannot carry ordinal; predecessor is current admitted epoch in that source
incarnation. A succession retirement and admission are adjacent and carry the
same next ordinal. Continuity starts at one and cannot skip from one to three.
Activation, fence and revocation records cannot carry ordinals, including when
appended after valid succession history. Retirement ordinals require a valid
succession pair. Independent sources and new incarnations retain independent
ordinal sequences.

Rejected checkpoint structure closes declared empty assets before installation.
Per-asset replay failures still preserve independent valid assets and do not
install a partial snapshot for the failed asset. Valid restore preserves native
publication evidence and requires fresh reprojection; historical admissions and
settlement never recreate live handles or qualification.

Proven here means deterministic synthetic checkpoint behavior demonstrated by
executed tests. Hypothesis and Unknown remain separate from qualification,
freshness, runtime acceptance and physical acceptance. These rules introduce no
receipts, idempotency-key store or new authority boundary, and change no eBUS MCP
envelope, namespace or GraphQL parity contract.

The shared provider-contract mirror remains a separate documentation companion
owned by the integration work. This supplement does not redefine it.
