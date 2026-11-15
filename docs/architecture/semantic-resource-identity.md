# Semantic resource identity

The gateway allocates every resource identifier of the semantic graph
(`semreg.ResourceID`), as it allocates asset, source and binding identifiers.
The allocator lives in `internal/semgraph`. The graph contract it implements is
[`api/v1/graph.md`](https://github.com/Project-Helianthus/helianthus-docs-semantic/blob/main/api/v1/graph.md)
in docs-semantic.

No publisher uses the allocator yet. Until a family is switched to graph
publication, its sub-asset identity stays in dimension text and the hub still
refuses every graph member.

## Rules

- **The gateway allocates; publishers supply anchors.** A publisher never
  chooses a resource identifier. It hands the allocator a resource kind and a
  typed anchor and publishes the identifier it gets back.
- **Dimension text is never identity.** A dimension value, a display name or
  an identifier built from an asset identifier does not name a resource. Two
  strings that are equal do not make two identities the same.
- **An anchor comes from one of three sources.** `native_token` (basis
  `native_identity`), `native_position` (basis `native_slot`) or
  `operator_label` (basis `operator_slot`). The basis must be the one its source
  establishes.
- **Never an anchor source:** a bus or network address, a routed unit number,
  an entity address path, a credential or certificate fingerprint, a pack
  class, a profile or other native interpretation selector, a display name, or
  an index that is not local to the parent. Each is refused with
  `ErrRefusedAnchorSource`; an unknown source is refused as an invalid anchor.
  Such values may appear in evidence; they never enter a key.

## Anchor and anchor key

An `Anchor` carries its class (`equipment`, `component`, `function` or
`episode`), basis, source, and either a native token (identification scheme,
issuer code, canonical token) or a slot, with an optional parent and 1 through
32 sorted, unique evidence references. A component or episode needs a parent
and a slot, a function needs a slot, and equipment needs a slot unless it has a
native token. The issuer is a registry code, never a name, and is empty for a
globally unique scheme.

The anchor key is `(kind, key)` for a native token and `(kind, parent, slot)`
otherwise. The native key is `sha256:` over the JCS bytes of
`{scheme, issuer, token}`. The SemReg anchor record carries the key and the
scheme; the issuer and token stay native evidence.

## Allocation

`Resolve(kind, anchor)` returns the identifier and the SemReg anchor record.
The identifier is `resource:` followed by the first 32 hexadecimal characters
of a sha256 over the domain `helianthus.semgraph.resource/v1`, the kind and
the anchor key members, each preceded by a zero byte. The same anchor therefore
yields the same identifier in every incarnation, and a restart needs no stored
state. Evidence, the source and the incarnation never enter the hash. A parent
must be an identifier the allocator returned.

## Supersession

`Supersede(previous, next, continuity)` keeps the identifier of the resource
anchored by `previous` while its anchor becomes `next`, and returns the
`supersedes` record: the exact previous anchor and the continuity evidence. The
allowed upgrades are:

- an operator slot to a native slot or a native token;
- a native slot to a native token;
- a native token to a revised canonical token form of the same scheme and
  issuer.

Continuity evidence is a binding that observes both anchors, the native
owner's documented mapping, or, after an operator slot only, an operator
declaration. The allocator refuses a kind change, a class change, a downgrade,
any other change, a previous anchor that was never resolved, is superseded or
is held under another class or basis, and a next key that is already held. A
superseded key is never resolved again. The table records the class and basis
of each current key: `Resolve` refuses an anchor of another class for that key,
refuses a lower basis as a downgrade, and refuses a higher basis, which rises
only through `Supersede`, so every basis change carries continuity evidence and
a `supersedes` record.

The supersession table sits behind the `Store` interface. `MemoryStore` is the
in-memory implementation; the durable store is a later change. Keys held by a
current resource are known to the allocator only for the keys resolved in the
running incarnation and the recorded supersessions; the kernel's
`duplicate_identity` check over the snapshot stays authoritative.

## Home asset

`Home(resource, asset)` records the one home asset of a resource the allocator
returned. Recording the same asset again is accepted; a second asset is refused
with `ErrSecondHome` and the recorded home.

## Identity events

| Event | Expected identifier | Test in `internal/semgraph/identity_events_test.go` |
|---|---|---|
| Driver restart, new source epoch or generation | same | `TestGWID1RestartAndNewEpochKeepIdentifiers` |
| Re-addressing on the bus or network | same; an address is refused as anchor source | `TestGWID2ReaddressingKeepsIdentifier` |
| Credential or certificate rotation | same; a credential is refused as anchor source | `TestGWID3CredentialRotationKeepsIdentifier` |
| Equipment replaced by another unit | new for the equipment and its components; same for functions | `TestGWID4EquipmentReplacedNewForEquipmentAndComponentsSameForFunctions` |
| Operator renames a configuration slot | new, unless the old slot is mapped by supersession | `TestGWID5OperatorRenameNewUnlessMappedBySupersession` |
| Operator label or native slot upgraded to a qualified native token | same, with `supersedes` | `TestGWID7AnchorUpgradeKeepsIdentifierWithSupersedes` |
| Native owner revises the canonical token form | same, with `supersedes` | `TestGWID7RevisedCanonicalTokenFormKeepsIdentifierWithSupersedes` |

An operator slot is mapped only onto a native anchor: the allowed upgrades
contain no operator slot to operator slot change, so a rename from one operator
slot to another is refused as a supersession and yields a new identifier.

One unit observed through two paths (GW-ID-6) needs an asset that accepts
several sources and is tested with that composition.

## Import boundary

`internal/semgraph` is a protected semantic owner in
`scripts/check_semantic_imports.py`: it imports only SemReg and the standard
library.
