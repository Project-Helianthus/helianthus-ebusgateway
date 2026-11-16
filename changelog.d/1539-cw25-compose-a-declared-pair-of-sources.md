## Compose a declared pair of sources into one asset

- `cmd/gateway/semantic_pairing_composition.go` gives the startup seam
  `composeSemanticPairings` a pairing declaration: the owner family's asset
  declaration, each contributor's source, driver and packs, and a capacity
  budget for the owner and for each contributor. Every budget is checked
  against the kernel's per-asset bounds on source descriptors, bindings and
  identity links before any pairing is declared; the paired asset is then
  declared once with its contributors. An undeclared second claimant stays a
  fatal collision, and an owner already declared without the contributor is a
  fatal declaration conflict.
- No configuration declares a pairing yet, so the startup preflight declares
  the same assets as before and every publisher output is unchanged.
- A gateway acceptance test composes two synthetic families into one asset and
  checks one unit observed through two paths: one resource, one binding and
  one identity link per path, one fact key with two candidates of which the
  owner-first policy presents exactly one, a contributor fence that keeps the
  resource and the owner's facts, and the undeclared claimant.
- Runtime contract section "Pairing".
