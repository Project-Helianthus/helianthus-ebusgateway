# M7 — AdapterMux conformance cell map (T01–T88 → matrix v2)

Status: planning deliverable (a) of
[#1378](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1378),
written against the operator steer of 2026-10-27 recorded on that issue. It
maps every case of the current 88-case transport matrix to the conformance
matrix v2 defined in
[M7-adaptermux-conformance-matrix.md](M7-adaptermux-conformance-matrix.md).
No case below has a result, and nothing here retires a case from the gate in
force: until the new matrix has run once on hardware, the 88-case
`TRANSPORT_MATRIX_REPORT` check in
[scripts/transport_gate.sh](../scripts/transport_gate.sh), its AD01..AD12 block
and the owner override that cites
[#1099](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1099)
stay as they are. The policy that later re-points them is
[transport-gate-policy-v2.md](../docs/testing/transport-gate-policy-v2.md).

## Source of the old cases

The 88 cases are generated, not hand-listed. Their identity and order come from
`GenerateTopologyCases` in [internal/matrix/topology.go](../internal/matrix/topology.go)
(transports in the order ENS, ENH, UDP-plain, TCP-plain; `T%02d` numbering), and
their default expectation from `defaultExpectedFailure` in
[internal/matrix/runner.go](../internal/matrix/runner.go), asserted by
`TestDefaultExpectedFailureCoverage` in
[internal/matrix/runner_test.go](../internal/matrix/runner_test.go): 40 cases
default to an expected failure (T07, T08 and 38 dual-client cases). The public
runbook in `helianthus-docs-ebus` (`development/smoke-matrix.md`) still states
42 cases and a T06..T08 range; the code is authoritative and that runbook text
is a known discrepancy for its own follow-up.

| Old range | Kind (`TopologyKind`) | Varied legs |
|---|---|---|
| T01–T04 | `direct-adapter` | gateway → adapter: ENS, ENH, UDP-plain, TCP-plain |
| T05–T08 | `via-ebusd-tcp` | gateway → ebusd (ebusd-tcp); ebusd → adapter: four transports |
| T09–T24 | `proxy-single-client` | gateway → proxy (G) × proxy → adapter (S) |
| T25–T88 | `proxy-dual-client` | gateway → proxy (G) × proxy → adapter (S) × ebusd → proxy (E) |

## Findings that decide the map

1. **ebusd as the configured transport is still supported**, so the single
   ebusd-tcp compatibility cell is kept. Evidence in this repository:
   `TransportEbusdTCP` (`"ebusd-tcp"`) in [config.go](../config.go); the
   `-transport` flag in [cmd/gateway/gateway_cli.go](../cmd/gateway/gateway_cli.go)
   accepts `enh, ens, udp-plain, tcp-plain, or ebusd-tcp`; ebusd-tcp timeout
   clamping in [gateway.go](../gateway.go); the ebusd-tcp scan preload in
   [cmd/gateway/startup_scan.go](../cmd/gateway/startup_scan.go); and the fixed
   ebusd-tcp row of the responder capability in
   [cmd/gateway/gateway_responder_capability.go](../cmd/gateway/gateway_responder_capability.go).
   [AGENTS.md](../AGENTS.md) does not withdraw it; its gate rule names the
   T01..T88 result, which includes T05–T08. In ebusd-tcp mode the AdapterMux is
   not in the path (`wireAdapterDirectWithConnectionLost` in
   [cmd/gateway/gateway_adapter_direct.go](../cmd/gateway/gateway_adapter_direct.go)
   composes the mux only for `adapter-direct` and `adapter-direct-ens`), so the
   kept cell sits in the compatibility block, outside the two axes.
2. **The non-mux direct transports are still supported.** `enh`, `ens`,
   `udp-plain` and `tcp-plain` without the `adapter-direct://` scheme do not
   compose the mux, and [internal/adaptermux/mux.go](../internal/adaptermux/mux.go)
   accepts only the `enh` and `ens` upstream protocols. T01–T04 therefore keep
   their subject and are kept as compatibility cells. If a later decision routes
   every configured transport through the mux, these four cells become absorbed
   into the AD01 cell of the same southbound transport.
3. **The gateway → proxy leg (G) has no counterpart.** Inside the AdapterMux the
   gateway is the in-process gateway session, not a network client of the
   multiplexer. The question carried by the G leg (does the gateway's client
   framing work) survives in the direct compatibility cell of that framing.
4. **The external listener speaks ENH only.** `-proxy-listen` is the "TCP listen
   address for ENH proxy clients". On a network connection `ens:` and `enh:` are
   equivalent framing selectors (`helianthus-docs-ebus`, `protocols/ens.md`), so
   an ebusd → proxy leg in ENS or ENH is the external ENH client of CX01. A plain
   UDP or TCP northbound leg has no listener in the mux; that question moves to
   the selective sharing formats of
   [#1001](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1001).
   With the proxy over an ENS or ENH southbound, ebusd over plain TCP was
   expected to pass and over plain UDP was expected to fail.

## Classification rules

- **kept**: the old topology still exists unchanged in a supported
  configuration and continues as a cell under a new id.
- **absorbed**: the old topology is gone, but every leg of its question is asked
  unchanged by one AD-n, PX-n or CX01 cell.
- **retired**: at least one leg of the old question depended on the proxy
  topology (or, for T05/T07/T08, on ebusd's own adapter transport) and has no
  counterpart. The row names the topology it depended on and the cells where
  the remaining legs now live.

No case is classified absorbed. The steer retires T09–T24 and T25–T88 as
topologies, and every one of them carries the G leg, which finding 3 shows has
no counterpart; calling them absorbed would claim an unchanged question. Their
surviving legs are mapped to keepers row by row instead.

Reason codes used in the table:

| Code | Meaning |
|---|---|
| K1 | Kept: non-mux direct composition of the same transport (finding 2). Mux counterpart: the AD01 cell of that transport. |
| K2 | Kept: the single ebusd-tcp compatibility cell (finding 1), with ebusd reaching the adapter over ENH. |
| E1 | Retired: ebusd's own adapter framing. `ens:` on a network adapter is the same ENH framing as `enh:`, so this case repeats `EBUSD-ES01`; the steer keeps one cell. |
| E2 | Retired: ebusd reaching the adapter over plain UDP or TCP is ebusd's own transport, outside the gateway's ebusd-tcp contract. The old expected failure is not carried. |
| S1 | Retired: proxy single-client topology. Southbound S lives in the AD01–AD03 cells of S through the mux; the G leg lives in the direct cell of G (finding 3). |
| D1 | Retired: proxy dual-client topology, ebusd → proxy in ENS or ENH. Coexistence lives in the CX01 cell of S with an external ENH client (finding 4), the ebusd assertions in AD05–AD07 of S; the G leg as in S1. |
| D2 | Retired: proxy dual-client topology, ebusd → proxy in plain UDP or TCP. Same keepers as D1 with an ENH client; the plain-listener leg has no mux counterpart and moves to #1001 (finding 4). |

Expected-failure column: `XF-01` marks an old default expected failure whose
cause (southbound plain UDP/TCP under dual-client load) survives and is carried
to the CX01 cell of that transport as the expected-failure category XF-01 of
the matrix document; "not carried" marks one whose cause leaves with the
retired leg.

## Cell table

| Old | Old topology | Old default | Class | Code | New cell / where the question now lives | Expected failure |
|---|---|---|---|---|---|---|
| T01 | direct adapter, gateway ENS | pass | kept | K1 | `ENS-DS01` | — |
| T02 | direct adapter, gateway ENH | pass | kept | K1 | `ENH-DS01` | — |
| T03 | direct adapter, gateway UDP-plain | pass | kept | K1 | `UDP-DS01` | — |
| T04 | direct adapter, gateway TCP-plain | pass | kept | K1 | `TCP-DS01` | — |
| T05 | gateway ebusd-tcp; ebusd → adapter ENS | pass | retired | E1 | `EBUSD-ES01` | — |
| T06 | gateway ebusd-tcp; ebusd → adapter ENH | pass | kept | K2 | `EBUSD-ES01` | — |
| T07 | gateway ebusd-tcp; ebusd → adapter UDP-plain | xfail | retired | E2 | `EBUSD-ES01`; ebusd's own adapter transport | XF not carried (E2) |
| T08 | gateway ebusd-tcp; ebusd → adapter TCP-plain | xfail | retired | E2 | `EBUSD-ES01`; ebusd's own adapter transport | XF not carried (E2) |
| T09 | proxy single-client; gateway → proxy ENS; proxy → adapter ENS | pass | retired | S1 | `ENS-AD01`, `ENS-AD02`, `ENS-AD03`; client leg `ENS-DS01` | — |
| T10 | proxy single-client; gateway → proxy ENS; proxy → adapter ENH | pass | retired | S1 | `ENH-AD01`, `ENH-AD02`, `ENH-AD03`; client leg `ENS-DS01` | — |
| T11 | proxy single-client; gateway → proxy ENS; proxy → adapter UDP-plain | pass | retired | S1 | `UDP-AD01`, `UDP-AD02`, `UDP-AD03`; client leg `ENS-DS01` | — |
| T12 | proxy single-client; gateway → proxy ENS; proxy → adapter TCP-plain | pass | retired | S1 | `TCP-AD01`, `TCP-AD02`, `TCP-AD03`; client leg `ENS-DS01` | — |
| T13 | proxy single-client; gateway → proxy ENH; proxy → adapter ENS | pass | retired | S1 | `ENS-AD01`, `ENS-AD02`, `ENS-AD03`; client leg `ENH-DS01`; proxy-side baseline of `ENS-AD10` in the transition session | — |
| T14 | proxy single-client; gateway → proxy ENH; proxy → adapter ENH | pass | retired | S1 | `ENH-AD01`, `ENH-AD02`, `ENH-AD03`; client leg `ENH-DS01`; proxy-side baseline of `ENH-AD10` in the transition session | — |
| T15 | proxy single-client; gateway → proxy ENH; proxy → adapter UDP-plain | pass | retired | S1 | `UDP-AD01`, `UDP-AD02`, `UDP-AD03`; client leg `ENH-DS01`; proxy-side baseline of `UDP-AD10` in the transition session | — |
| T16 | proxy single-client; gateway → proxy ENH; proxy → adapter TCP-plain | pass | retired | S1 | `TCP-AD01`, `TCP-AD02`, `TCP-AD03`; client leg `ENH-DS01`; proxy-side baseline of `TCP-AD10` in the transition session | — |
| T17 | proxy single-client; gateway → proxy UDP-plain; proxy → adapter ENS | pass | retired | S1 | `ENS-AD01`, `ENS-AD02`, `ENS-AD03`; client leg `UDP-DS01` | — |
| T18 | proxy single-client; gateway → proxy UDP-plain; proxy → adapter ENH | pass | retired | S1 | `ENH-AD01`, `ENH-AD02`, `ENH-AD03`; client leg `UDP-DS01` | — |
| T19 | proxy single-client; gateway → proxy UDP-plain; proxy → adapter UDP-plain | pass | retired | S1 | `UDP-AD01`, `UDP-AD02`, `UDP-AD03`; client leg `UDP-DS01` | — |
| T20 | proxy single-client; gateway → proxy UDP-plain; proxy → adapter TCP-plain | pass | retired | S1 | `TCP-AD01`, `TCP-AD02`, `TCP-AD03`; client leg `UDP-DS01` | — |
| T21 | proxy single-client; gateway → proxy TCP-plain; proxy → adapter ENS | pass | retired | S1 | `ENS-AD01`, `ENS-AD02`, `ENS-AD03`; client leg `TCP-DS01` | — |
| T22 | proxy single-client; gateway → proxy TCP-plain; proxy → adapter ENH | pass | retired | S1 | `ENH-AD01`, `ENH-AD02`, `ENH-AD03`; client leg `TCP-DS01` | — |
| T23 | proxy single-client; gateway → proxy TCP-plain; proxy → adapter UDP-plain | pass | retired | S1 | `UDP-AD01`, `UDP-AD02`, `UDP-AD03`; client leg `TCP-DS01` | — |
| T24 | proxy single-client; gateway → proxy TCP-plain; proxy → adapter TCP-plain | pass | retired | S1 | `TCP-AD01`, `TCP-AD02`, `TCP-AD03`; client leg `TCP-DS01` | — |
| T25 | proxy dual-client; gateway → proxy ENS; proxy → adapter ENS; ebusd → proxy ENS | pass | retired | D1 | `ENS-CX01`, `ENS-AD05`, `ENS-AD06`, `ENS-AD07`; client leg `ENS-DS01` | — |
| T26 | proxy dual-client; gateway → proxy ENS; proxy → adapter ENS; ebusd → proxy ENH | pass | retired | D1 | `ENS-CX01`, `ENS-AD05`, `ENS-AD06`, `ENS-AD07`; client leg `ENS-DS01` | — |
| T27 | proxy dual-client; gateway → proxy ENS; proxy → adapter ENS; ebusd → proxy UDP-plain | xfail | retired | D2 | `ENS-CX01`, `ENS-AD05`, `ENS-AD06`, `ENS-AD07`; client leg `ENS-DS01`; plain listener question with #1001 | XF not carried (D2) |
| T28 | proxy dual-client; gateway → proxy ENS; proxy → adapter ENS; ebusd → proxy TCP-plain | pass | retired | D2 | `ENS-CX01`, `ENS-AD05`, `ENS-AD06`, `ENS-AD07`; client leg `ENS-DS01`; plain listener question with #1001 | — |
| T29 | proxy dual-client; gateway → proxy ENS; proxy → adapter ENH; ebusd → proxy ENS | pass | retired | D1 | `ENH-CX01`, `ENH-AD05`, `ENH-AD06`, `ENH-AD07`; client leg `ENS-DS01` | — |
| T30 | proxy dual-client; gateway → proxy ENS; proxy → adapter ENH; ebusd → proxy ENH | pass | retired | D1 | `ENH-CX01`, `ENH-AD05`, `ENH-AD06`, `ENH-AD07`; client leg `ENS-DS01` | — |
| T31 | proxy dual-client; gateway → proxy ENS; proxy → adapter ENH; ebusd → proxy UDP-plain | xfail | retired | D2 | `ENH-CX01`, `ENH-AD05`, `ENH-AD06`, `ENH-AD07`; client leg `ENS-DS01`; plain listener question with #1001 | XF not carried (D2) |
| T32 | proxy dual-client; gateway → proxy ENS; proxy → adapter ENH; ebusd → proxy TCP-plain | pass | retired | D2 | `ENH-CX01`, `ENH-AD05`, `ENH-AD06`, `ENH-AD07`; client leg `ENS-DS01`; plain listener question with #1001 | — |
| T33 | proxy dual-client; gateway → proxy ENS; proxy → adapter UDP-plain; ebusd → proxy ENS | xfail | retired | D1 | `UDP-CX01`, `UDP-AD05`, `UDP-AD06`, `UDP-AD07`; client leg `ENS-DS01` | XF-01 → `UDP-CX01` |
| T34 | proxy dual-client; gateway → proxy ENS; proxy → adapter UDP-plain; ebusd → proxy ENH | xfail | retired | D1 | `UDP-CX01`, `UDP-AD05`, `UDP-AD06`, `UDP-AD07`; client leg `ENS-DS01` | XF-01 → `UDP-CX01` |
| T35 | proxy dual-client; gateway → proxy ENS; proxy → adapter UDP-plain; ebusd → proxy UDP-plain | xfail | retired | D2 | `UDP-CX01`, `UDP-AD05`, `UDP-AD06`, `UDP-AD07`; client leg `ENS-DS01`; plain listener question with #1001 | XF-01 → `UDP-CX01` |
| T36 | proxy dual-client; gateway → proxy ENS; proxy → adapter UDP-plain; ebusd → proxy TCP-plain | xfail | retired | D2 | `UDP-CX01`, `UDP-AD05`, `UDP-AD06`, `UDP-AD07`; client leg `ENS-DS01`; plain listener question with #1001 | XF-01 → `UDP-CX01` |
| T37 | proxy dual-client; gateway → proxy ENS; proxy → adapter TCP-plain; ebusd → proxy ENS | xfail | retired | D1 | `TCP-CX01`, `TCP-AD05`, `TCP-AD06`, `TCP-AD07`; client leg `ENS-DS01` | XF-01 → `TCP-CX01` |
| T38 | proxy dual-client; gateway → proxy ENS; proxy → adapter TCP-plain; ebusd → proxy ENH | xfail | retired | D1 | `TCP-CX01`, `TCP-AD05`, `TCP-AD06`, `TCP-AD07`; client leg `ENS-DS01` | XF-01 → `TCP-CX01` |
| T39 | proxy dual-client; gateway → proxy ENS; proxy → adapter TCP-plain; ebusd → proxy UDP-plain | xfail | retired | D2 | `TCP-CX01`, `TCP-AD05`, `TCP-AD06`, `TCP-AD07`; client leg `ENS-DS01`; plain listener question with #1001 | XF-01 → `TCP-CX01` |
| T40 | proxy dual-client; gateway → proxy ENS; proxy → adapter TCP-plain; ebusd → proxy TCP-plain | xfail | retired | D2 | `TCP-CX01`, `TCP-AD05`, `TCP-AD06`, `TCP-AD07`; client leg `ENS-DS01`; plain listener question with #1001 | XF-01 → `TCP-CX01` |
| T41 | proxy dual-client; gateway → proxy ENH; proxy → adapter ENS; ebusd → proxy ENS | pass | retired | D1 | `ENS-CX01`, `ENS-AD05`, `ENS-AD06`, `ENS-AD07`; client leg `ENH-DS01` | — |
| T42 | proxy dual-client; gateway → proxy ENH; proxy → adapter ENS; ebusd → proxy ENH | pass | retired | D1 | `ENS-CX01`, `ENS-AD05`, `ENS-AD06`, `ENS-AD07`; client leg `ENH-DS01` | — |
| T43 | proxy dual-client; gateway → proxy ENH; proxy → adapter ENS; ebusd → proxy UDP-plain | xfail | retired | D2 | `ENS-CX01`, `ENS-AD05`, `ENS-AD06`, `ENS-AD07`; client leg `ENH-DS01`; plain listener question with #1001 | XF not carried (D2) |
| T44 | proxy dual-client; gateway → proxy ENH; proxy → adapter ENS; ebusd → proxy TCP-plain | pass | retired | D2 | `ENS-CX01`, `ENS-AD05`, `ENS-AD06`, `ENS-AD07`; client leg `ENH-DS01`; plain listener question with #1001 | — |
| T45 | proxy dual-client; gateway → proxy ENH; proxy → adapter ENH; ebusd → proxy ENS | pass | retired | D1 | `ENH-CX01`, `ENH-AD05`, `ENH-AD06`, `ENH-AD07`; client leg `ENH-DS01` | — |
| T46 | proxy dual-client; gateway → proxy ENH; proxy → adapter ENH; ebusd → proxy ENH | pass | retired | D1 | `ENH-CX01`, `ENH-AD05`, `ENH-AD06`, `ENH-AD07`; client leg `ENH-DS01` | — |
| T47 | proxy dual-client; gateway → proxy ENH; proxy → adapter ENH; ebusd → proxy UDP-plain | xfail | retired | D2 | `ENH-CX01`, `ENH-AD05`, `ENH-AD06`, `ENH-AD07`; client leg `ENH-DS01`; plain listener question with #1001 | XF not carried (D2) |
| T48 | proxy dual-client; gateway → proxy ENH; proxy → adapter ENH; ebusd → proxy TCP-plain | pass | retired | D2 | `ENH-CX01`, `ENH-AD05`, `ENH-AD06`, `ENH-AD07`; client leg `ENH-DS01`; plain listener question with #1001 | — |
| T49 | proxy dual-client; gateway → proxy ENH; proxy → adapter UDP-plain; ebusd → proxy ENS | xfail | retired | D1 | `UDP-CX01`, `UDP-AD05`, `UDP-AD06`, `UDP-AD07`; client leg `ENH-DS01` | XF-01 → `UDP-CX01` |
| T50 | proxy dual-client; gateway → proxy ENH; proxy → adapter UDP-plain; ebusd → proxy ENH | xfail | retired | D1 | `UDP-CX01`, `UDP-AD05`, `UDP-AD06`, `UDP-AD07`; client leg `ENH-DS01` | XF-01 → `UDP-CX01` |
| T51 | proxy dual-client; gateway → proxy ENH; proxy → adapter UDP-plain; ebusd → proxy UDP-plain | xfail | retired | D2 | `UDP-CX01`, `UDP-AD05`, `UDP-AD06`, `UDP-AD07`; client leg `ENH-DS01`; plain listener question with #1001 | XF-01 → `UDP-CX01` |
| T52 | proxy dual-client; gateway → proxy ENH; proxy → adapter UDP-plain; ebusd → proxy TCP-plain | xfail | retired | D2 | `UDP-CX01`, `UDP-AD05`, `UDP-AD06`, `UDP-AD07`; client leg `ENH-DS01`; plain listener question with #1001 | XF-01 → `UDP-CX01` |
| T53 | proxy dual-client; gateway → proxy ENH; proxy → adapter TCP-plain; ebusd → proxy ENS | xfail | retired | D1 | `TCP-CX01`, `TCP-AD05`, `TCP-AD06`, `TCP-AD07`; client leg `ENH-DS01` | XF-01 → `TCP-CX01` |
| T54 | proxy dual-client; gateway → proxy ENH; proxy → adapter TCP-plain; ebusd → proxy ENH | xfail | retired | D1 | `TCP-CX01`, `TCP-AD05`, `TCP-AD06`, `TCP-AD07`; client leg `ENH-DS01` | XF-01 → `TCP-CX01` |
| T55 | proxy dual-client; gateway → proxy ENH; proxy → adapter TCP-plain; ebusd → proxy UDP-plain | xfail | retired | D2 | `TCP-CX01`, `TCP-AD05`, `TCP-AD06`, `TCP-AD07`; client leg `ENH-DS01`; plain listener question with #1001 | XF-01 → `TCP-CX01` |
| T56 | proxy dual-client; gateway → proxy ENH; proxy → adapter TCP-plain; ebusd → proxy TCP-plain | xfail | retired | D2 | `TCP-CX01`, `TCP-AD05`, `TCP-AD06`, `TCP-AD07`; client leg `ENH-DS01`; plain listener question with #1001 | XF-01 → `TCP-CX01` |
| T57 | proxy dual-client; gateway → proxy UDP-plain; proxy → adapter ENS; ebusd → proxy ENS | pass | retired | D1 | `ENS-CX01`, `ENS-AD05`, `ENS-AD06`, `ENS-AD07`; client leg `UDP-DS01` | — |
| T58 | proxy dual-client; gateway → proxy UDP-plain; proxy → adapter ENS; ebusd → proxy ENH | pass | retired | D1 | `ENS-CX01`, `ENS-AD05`, `ENS-AD06`, `ENS-AD07`; client leg `UDP-DS01` | — |
| T59 | proxy dual-client; gateway → proxy UDP-plain; proxy → adapter ENS; ebusd → proxy UDP-plain | xfail | retired | D2 | `ENS-CX01`, `ENS-AD05`, `ENS-AD06`, `ENS-AD07`; client leg `UDP-DS01`; plain listener question with #1001 | XF not carried (D2) |
| T60 | proxy dual-client; gateway → proxy UDP-plain; proxy → adapter ENS; ebusd → proxy TCP-plain | pass | retired | D2 | `ENS-CX01`, `ENS-AD05`, `ENS-AD06`, `ENS-AD07`; client leg `UDP-DS01`; plain listener question with #1001 | — |
| T61 | proxy dual-client; gateway → proxy UDP-plain; proxy → adapter ENH; ebusd → proxy ENS | pass | retired | D1 | `ENH-CX01`, `ENH-AD05`, `ENH-AD06`, `ENH-AD07`; client leg `UDP-DS01` | — |
| T62 | proxy dual-client; gateway → proxy UDP-plain; proxy → adapter ENH; ebusd → proxy ENH | pass | retired | D1 | `ENH-CX01`, `ENH-AD05`, `ENH-AD06`, `ENH-AD07`; client leg `UDP-DS01` | — |
| T63 | proxy dual-client; gateway → proxy UDP-plain; proxy → adapter ENH; ebusd → proxy UDP-plain | xfail | retired | D2 | `ENH-CX01`, `ENH-AD05`, `ENH-AD06`, `ENH-AD07`; client leg `UDP-DS01`; plain listener question with #1001 | XF not carried (D2) |
| T64 | proxy dual-client; gateway → proxy UDP-plain; proxy → adapter ENH; ebusd → proxy TCP-plain | pass | retired | D2 | `ENH-CX01`, `ENH-AD05`, `ENH-AD06`, `ENH-AD07`; client leg `UDP-DS01`; plain listener question with #1001 | — |
| T65 | proxy dual-client; gateway → proxy UDP-plain; proxy → adapter UDP-plain; ebusd → proxy ENS | pass | retired | D1 | `UDP-CX01`, `UDP-AD05`, `UDP-AD06`, `UDP-AD07`; client leg `UDP-DS01` | — |
| T66 | proxy dual-client; gateway → proxy UDP-plain; proxy → adapter UDP-plain; ebusd → proxy ENH | pass | retired | D1 | `UDP-CX01`, `UDP-AD05`, `UDP-AD06`, `UDP-AD07`; client leg `UDP-DS01` | — |
| T67 | proxy dual-client; gateway → proxy UDP-plain; proxy → adapter UDP-plain; ebusd → proxy UDP-plain | xfail | retired | D2 | `UDP-CX01`, `UDP-AD05`, `UDP-AD06`, `UDP-AD07`; client leg `UDP-DS01`; plain listener question with #1001 | XF-01 → `UDP-CX01` |
| T68 | proxy dual-client; gateway → proxy UDP-plain; proxy → adapter UDP-plain; ebusd → proxy TCP-plain | xfail | retired | D2 | `UDP-CX01`, `UDP-AD05`, `UDP-AD06`, `UDP-AD07`; client leg `UDP-DS01`; plain listener question with #1001 | XF-01 → `UDP-CX01` |
| T69 | proxy dual-client; gateway → proxy UDP-plain; proxy → adapter TCP-plain; ebusd → proxy ENS | xfail | retired | D1 | `TCP-CX01`, `TCP-AD05`, `TCP-AD06`, `TCP-AD07`; client leg `UDP-DS01` | XF-01 → `TCP-CX01` |
| T70 | proxy dual-client; gateway → proxy UDP-plain; proxy → adapter TCP-plain; ebusd → proxy ENH | xfail | retired | D1 | `TCP-CX01`, `TCP-AD05`, `TCP-AD06`, `TCP-AD07`; client leg `UDP-DS01` | XF-01 → `TCP-CX01` |
| T71 | proxy dual-client; gateway → proxy UDP-plain; proxy → adapter TCP-plain; ebusd → proxy UDP-plain | xfail | retired | D2 | `TCP-CX01`, `TCP-AD05`, `TCP-AD06`, `TCP-AD07`; client leg `UDP-DS01`; plain listener question with #1001 | XF-01 → `TCP-CX01` |
| T72 | proxy dual-client; gateway → proxy UDP-plain; proxy → adapter TCP-plain; ebusd → proxy TCP-plain | xfail | retired | D2 | `TCP-CX01`, `TCP-AD05`, `TCP-AD06`, `TCP-AD07`; client leg `UDP-DS01`; plain listener question with #1001 | XF-01 → `TCP-CX01` |
| T73 | proxy dual-client; gateway → proxy TCP-plain; proxy → adapter ENS; ebusd → proxy ENS | pass | retired | D1 | `ENS-CX01`, `ENS-AD05`, `ENS-AD06`, `ENS-AD07`; client leg `TCP-DS01` | — |
| T74 | proxy dual-client; gateway → proxy TCP-plain; proxy → adapter ENS; ebusd → proxy ENH | pass | retired | D1 | `ENS-CX01`, `ENS-AD05`, `ENS-AD06`, `ENS-AD07`; client leg `TCP-DS01` | — |
| T75 | proxy dual-client; gateway → proxy TCP-plain; proxy → adapter ENS; ebusd → proxy UDP-plain | xfail | retired | D2 | `ENS-CX01`, `ENS-AD05`, `ENS-AD06`, `ENS-AD07`; client leg `TCP-DS01`; plain listener question with #1001 | XF not carried (D2) |
| T76 | proxy dual-client; gateway → proxy TCP-plain; proxy → adapter ENS; ebusd → proxy TCP-plain | pass | retired | D2 | `ENS-CX01`, `ENS-AD05`, `ENS-AD06`, `ENS-AD07`; client leg `TCP-DS01`; plain listener question with #1001 | — |
| T77 | proxy dual-client; gateway → proxy TCP-plain; proxy → adapter ENH; ebusd → proxy ENS | pass | retired | D1 | `ENH-CX01`, `ENH-AD05`, `ENH-AD06`, `ENH-AD07`; client leg `TCP-DS01` | — |
| T78 | proxy dual-client; gateway → proxy TCP-plain; proxy → adapter ENH; ebusd → proxy ENH | pass | retired | D1 | `ENH-CX01`, `ENH-AD05`, `ENH-AD06`, `ENH-AD07`; client leg `TCP-DS01` | — |
| T79 | proxy dual-client; gateway → proxy TCP-plain; proxy → adapter ENH; ebusd → proxy UDP-plain | xfail | retired | D2 | `ENH-CX01`, `ENH-AD05`, `ENH-AD06`, `ENH-AD07`; client leg `TCP-DS01`; plain listener question with #1001 | XF not carried (D2) |
| T80 | proxy dual-client; gateway → proxy TCP-plain; proxy → adapter ENH; ebusd → proxy TCP-plain | pass | retired | D2 | `ENH-CX01`, `ENH-AD05`, `ENH-AD06`, `ENH-AD07`; client leg `TCP-DS01`; plain listener question with #1001 | — |
| T81 | proxy dual-client; gateway → proxy TCP-plain; proxy → adapter UDP-plain; ebusd → proxy ENS | xfail | retired | D1 | `UDP-CX01`, `UDP-AD05`, `UDP-AD06`, `UDP-AD07`; client leg `TCP-DS01` | XF-01 → `UDP-CX01` |
| T82 | proxy dual-client; gateway → proxy TCP-plain; proxy → adapter UDP-plain; ebusd → proxy ENH | xfail | retired | D1 | `UDP-CX01`, `UDP-AD05`, `UDP-AD06`, `UDP-AD07`; client leg `TCP-DS01` | XF-01 → `UDP-CX01` |
| T83 | proxy dual-client; gateway → proxy TCP-plain; proxy → adapter UDP-plain; ebusd → proxy UDP-plain | xfail | retired | D2 | `UDP-CX01`, `UDP-AD05`, `UDP-AD06`, `UDP-AD07`; client leg `TCP-DS01`; plain listener question with #1001 | XF-01 → `UDP-CX01` |
| T84 | proxy dual-client; gateway → proxy TCP-plain; proxy → adapter UDP-plain; ebusd → proxy TCP-plain | xfail | retired | D2 | `UDP-CX01`, `UDP-AD05`, `UDP-AD06`, `UDP-AD07`; client leg `TCP-DS01`; plain listener question with #1001 | XF-01 → `UDP-CX01` |
| T85 | proxy dual-client; gateway → proxy TCP-plain; proxy → adapter TCP-plain; ebusd → proxy ENS | xfail | retired | D1 | `TCP-CX01`, `TCP-AD05`, `TCP-AD06`, `TCP-AD07`; client leg `TCP-DS01` | XF-01 → `TCP-CX01` |
| T86 | proxy dual-client; gateway → proxy TCP-plain; proxy → adapter TCP-plain; ebusd → proxy ENH | xfail | retired | D1 | `TCP-CX01`, `TCP-AD05`, `TCP-AD06`, `TCP-AD07`; client leg `TCP-DS01` | XF-01 → `TCP-CX01` |
| T87 | proxy dual-client; gateway → proxy TCP-plain; proxy → adapter TCP-plain; ebusd → proxy UDP-plain | xfail | retired | D2 | `TCP-CX01`, `TCP-AD05`, `TCP-AD06`, `TCP-AD07`; client leg `TCP-DS01`; plain listener question with #1001 | XF-01 → `TCP-CX01` |
| T88 | proxy dual-client; gateway → proxy TCP-plain; proxy → adapter TCP-plain; ebusd → proxy TCP-plain | xfail | retired | D2 | `TCP-CX01`, `TCP-AD05`, `TCP-AD06`, `TCP-AD07`; client leg `TCP-DS01`; plain listener question with #1001 | XF-01 → `TCP-CX01` |

## Totals

| Class | Cases | Reason codes |
|---|---|---|
| kept | 5 | K1 × 4 (T01–T04), K2 × 1 (T06) |
| absorbed | 0 | — |
| retired | 83 | E1 × 1 (T05), E2 × 2 (T07, T08), S1 × 16 (T09–T24), D1 × 32, D2 × 32 (T25–T88) |
| **total** | **88** | 4 + 1 + 1 + 2 + 16 + 32 + 32 = 88 |

Every row names at least one new cell, so no case leaves without a keeper. Of
the 40 old default expected failures, 30 are carried as XF-01 to `UDP-CX01` and
`TCP-CX01`, 8 (ebusd → proxy plain UDP over an ENS or ENH southbound) leave with
the plain-listener leg, and 2 (T07, T08) leave with ebusd's own adapter
transport: 30 + 8 + 2 = 40. T65 and T66 were the only plain-southbound
dual-client cases expected to pass, and only with a gateway → proxy UDP leg that
no longer exists; `UDP-CX01` therefore starts with the carried category.

## Keepers, reverse view

| New cell (per southbound S where shown) | Old cases whose question it carries |
|---|---|
| `ENS-DS01`, `ENH-DS01`, `UDP-DS01`, `TCP-DS01` | T01–T04 (kept), plus the G leg of every T09–T88 case with that G |
| `EBUSD-ES01` | T06 (kept); T05, T07, T08 (retired) |
| `S-AD01`, `S-AD02`, `S-AD03` | the four T09–T24 cases with southbound S (T09/T13/T17/T21 for ENS, T10/T14/T18/T22 for ENH, T11/T15/T19/T23 for UDP-plain, T12/T16/T20/T24 for TCP-plain) |
| `S-AD10` | T13, T14, T15, T16 as the additional proxy-side baseline for ENS, ENH, UDP-plain, TCP-plain, in the transition session only; outside it the replaced side is `S-DS01` |
| `S-CX01`, `S-AD05`, `S-AD06`, `S-AD07` | the sixteen T25–T88 cases with southbound S |

The UART-direct rows and the PX01–PX12 columns have no predecessor among
T01–T88: the UART rows are new and reserved, and PX01–PX12 was a separate
adjunct of the proxy repository whose cases move into the mux's arbitration
(see the matrix document).

## Adjacent passive suite (outside the 88; not decided here)

The passive smoke suite P01–P06 (`GeneratePassiveSmokeCases`, checked by
[scripts/passive_smoke_gate.sh](../scripts/passive_smoke_gate.sh)) is not part
of T01–T88 and the steer does not re-point it. Three of its cases depend on the
same proxy topologies:

| Case | Topology | Proxy-dependent | Candidate keeper, for a separate decision |
|---|---|---|---|
| P01, P02 | direct adapter ENS, ENH; `unsupported_or_misconfigured` | no | unchanged |
| P03 | proxy single-client ENS/ENS; `required` | yes | `ENS-AD03` with `ENS-AD02` |
| P04 | proxy dual-client ENS; `required` | yes | `ENS-AD03` under `ENS-CX01` load |
| P05 | proxy dual-client ENH; `required` | yes | `ENH-AD03` under `ENH-CX01` load |
| P06 | via ebusd-tcp ENS; `unsupported_or_misconfigured` | no | unchanged |

Until that decision, the passive gate and its own owner override
(`PASSIVE_SMOKE_GATE_OWNER_OVERRIDE`, `PASSIVE_SMOKE_GATE_OWNER_REASON`) are
untouched by this plan.
