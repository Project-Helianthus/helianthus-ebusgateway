// Package semgraph allocates the resource identifiers of the semantic graph.
//
// The gateway allocates every semreg.ResourceID. A publisher supplies a typed
// Anchor: its class, its basis, where it comes from (a native token, a native
// position or an operator label) and, for a native token, the identification
// scheme, issuer code and canonical token. Dimension text is never identity,
// and an address, a routed unit number, an entity address path, a credential,
// a pack class, a profile or other interpretation selector, a display name or
// an index that is not local to the parent is refused as an anchor source.
//
// Allocation is deterministic: the identifier is a domain-separated hash over
// the resource kind and the anchor key, so a driver restart, a new source
// epoch or a new generation needs no stored state to return the same
// identifier. Only an anchor supersession, which keeps an identifier across an
// anchor upgrade, is recorded, behind the Store interface.
//
// The package imports only SemReg and the standard library.
package semgraph
