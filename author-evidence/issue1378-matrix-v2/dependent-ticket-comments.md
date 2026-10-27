# Issue #1378 — dependent-ticket amendments (drafts)

Deliverable (e) of
[#1378](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1378).
Each section below is a ready-to-post comment for an open ticket that names
T01–T88, PX01–PX12, AD01–AD12 or a proxy topology in its acceptance, or a draft
for a new issue. The amendment is always a comment naming the cells; no merged
gate or issue body is edited. Nothing here has been posted.

Cell notation used in every comment: a cell is `<TRANSPORT>-<INVARIANT>`.
Southbound transports (axis 1, where the mux's symbol stream comes from): `ENS`,
`ENH` (executable through the mux), `UDP`, `TCP` (planned: the mux has no plain
southbound yet), `UARTENS`, `UARTENH`, `UARTRAW` (reserved, blocked-infra).
Northbound invariants (axis 2, what the mux does with the stream): AD01–AD10 and
AD12 (AD11 retired), PX01–PX12, CX01 (gateway and an external ENH client,
ebusd or VRC Explorer, simultaneously through the mux). Compatibility cells for
transports that bypass the mux: `ENS-DS01`, `ENH-DS01`, `UDP-DS01`, `TCP-DS01`
(direct adapter, no mux) and `EBUSD-ES01` (ebusd-tcp). Schema:
`matrix/M7-adaptermux-conformance-matrix.md`; old-to-new map:
`matrix/M7-adaptermux-conformance-cell-map.md`; gate policy:
`docs/testing/transport-gate-policy-v2.md` (all in the #1378 pull request).

Search scope used to find dependents: open issues in the organization naming
T01–T88, PX01–PX12, AD01–AD12 or "proxy topology", the open gateway issues on
the AdapterMux and its listener, and the open helianthus-vrc-explorer issues
(no certification-run issue exists, so a new one is drafted).

| Target | Kind | Section |
|---|---|---|
| helianthus-ebusgateway#1003 | comment | 1 |
| helianthus-ebusgateway#1099 | comment | 2 |
| helianthus-ebusgateway#476 | comment | 3 |
| helianthus-ebusgateway#1032 (AdapterMux lifecycle) | comment | 4 |
| helianthus-ebusgateway#1001 (AdapterMux listener sharing) | comment | 5 |
| helianthus-ebusgateway#1345 | comment | 6 |
| helianthus-ebusgateway#938 | comment | 7 |
| helianthus-ebusgateway#551 | comment | 8 |
| helianthus-docs-ebus#250 | comment | 9 |
| helianthus-execution-plans#130 | comment | 10 |
| helianthus-vrc-explorer#312 | comment | 11 |
| helianthus-ebusgateway#991 (deferred; informational) | comment | 12 |
| helianthus-vrc-explorer (Explorer certification run) | new issue | 13 |
| helianthus-ebus-adapter-proxy (proxy deprecation) | new issue | 14 |
| helianthus-ebusgateway (runner support for the new suite) | new issue, proposed | 15 |
| helianthus-ebusgateway (mux plain southbound framing) | new issue, proposed | 16 |

---

## 1. helianthus-ebusgateway#1003 — comment

> Amendment from the transport conformance matrix v2 planning (#1378, operator
> steer 2026-10-27). The acceptance above is unchanged; this names the matrix
> cells that replace the T01..T88 reference in "transport conformance" and in
> the scope addition recorded on #1099.
>
> Southbound side: `ENS` and `ENH` through the mux (`adapter-direct-ens://`,
> `adapter-direct://`). `UDP` and `TCP` cells are planned (the mux has no plain
> southbound); UART-direct cells are reserved (blocked-infra).
>
> Northbound side, cells for this change:
>
> - AD12, no 0xA9/0xAA escape corruption, now worded as this issue's contract
>   (raw symbols at the mux boundary, host-side escaping on both paths, a
>   logical view with provenance for internal consumers): `ENS-AD12`,
>   `ENH-AD12`.
> - AD05 and AD06, external session receives raw pairs and sends START/SEND:
>   `ENS-AD05`, `ENH-AD05`, `ENS-AD06`, `ENH-AD06`.
> - AD08, per-session echo per raw symbol, including a client waiting for each
>   echoed symbol: `ENS-AD08`, `ENH-AD08`.
> - AD01, gateway requests with reserved CRC bytes through the mux:
>   `ENS-AD01`, `ENH-AD01`.
> - AD03, passive reconstruction from the logical view: `ENS-AD03`,
>   `ENH-AD03`.
> - PX01 and PX02, grant visibility and stale STARTED under queued passive
>   traffic: `ENS-PX01`, `ENH-PX01`, `ENS-PX02`, `ENH-PX02`.
> - CX01, gateway and an external ENH client concurrently: `ENS-CX01`,
>   `ENH-CX01`.
>
> Scope addition on #1099, mapped: raw escape pairs and structural SYN to an
> external session → `ENS-AD12`, `ENH-AD12`, `ENS-AD05`, `ENH-AD05`; a client
> waiting for each echoed symbol with a reserved CRC byte → `ENS-AD08`,
> `ENH-AD08`, `ENS-AD06`, `ENH-AD06`; gateway requests with reserved CRC bytes
> → `ENS-AD01`, `ENH-AD01`, `ENS-AD12`, `ENH-AD12`.
>
> The multiplexer architecture's AD12 text in helianthus-docs-ebus is replaced
> by this contract in a prepared change. Until the new matrix has run once on
> hardware, the T01..T88 gate and the #1099 override stay in force and this
> change remains covered by the consolidated session.
>
> 🤖 Generated with [Claude Code](https://claude.com/claude-code)

## 2. helianthus-ebusgateway#1099 — comment

> Cells for the consolidated session, from the matrix v2 planning (#1378). This
> session runs the old form (`full88`, T01–T88) for the last time and the new
> form (`adaptermux-v2`) for the first time, on the same hardware session and
> the same per-transport setups. The override citing this issue stays in force
> until the new form has run here.
>
> New form, 173 cells:
>
> - Southbound side, one hardware setup per transport: `ENS`, `ENH`, `UDP`,
>   `TCP`; UART-direct rows `UARTENS`, `UARTENH`, `UARTRAW` are reserved and
>   reported blocked-infra (`uart_direct_hardware_absent`).
> - Northbound side, on every southbound: AD01–AD10, AD12, PX01–PX12, CX01.
>   AD11 is retired (its return target, the standalone proxy, is deprecated).
> - Compatibility cells on the setup of their transport: `ENS-DS01`,
>   `ENH-DS01`, `UDP-DS01`, `TCP-DS01`, `EBUSD-ES01`.
>
> Projected with today's mux scope: 47 executed (`ENS`/`ENH` × AD01–AD10,
> AD12, PX01–PX08, PX12, CX01, plus the five compatibility cells), 54 planned
> (`UDP-*`, `TCP-*`; `*-PX09` to `*-PX11` with no local responder in the mux),
> 72 blocked-infra (UART-direct). Accepted non-pass verdicts only through the
> categories XF-01, BI-01, BI-02, PL-01, PL-02 of the matrix page.
>
> Old-form runs T13–T16 (proxy single-client with an ENH gateway leg) are the
> additional proxy-side baseline of `ENS-AD10`, `ENH-AD10`, `UDP-AD10`,
> `TCP-AD10`. Every pull request listed above as deferred is covered by the
> old-form report as before. The scope addition for #1003 maps to `ENS-AD12`,
> `ENH-AD12`, `ENS-AD05`, `ENH-AD05`, `ENS-AD06`, `ENH-AD06`, `ENS-AD08`,
> `ENH-AD08`, `ENS-AD01`, `ENH-AD01`. After the session, the gate switch of
> `docs/testing/transport-gate-policy-v2.md` re-points the gate and the ship
> override; an override that cites this issue is not valid once it closes.
>
> 🤖 Generated with [Claude Code](https://claude.com/claude-code)

## 3. helianthus-ebusgateway#476 — comment

> Matrix v2 planning (#1378) gives the formal AD01..AD12 matrix report a home:
> the AD cells of the `adaptermux-v2` report. Proposed reading of the close
> condition, not an edit of it:
>
> - Southbound side: every axis-1 row (`ENS`, `ENH` executable; `UDP`, `TCP`
>   planned; UART-direct reserved).
> - Northbound side: `<T>-AD01` to `<T>-AD10` and `<T>-AD12` on all seven
>   rows, 77 cells, of which `ENS-AD01`…`ENS-AD10`, `ENS-AD12` and
>   `ENH-AD01`…`ENH-AD10`, `ENH-AD12` (22 cells) execute in the first run.
> - AD10 keeps an equivalence test plan: per southbound, the replaced
>   configuration (`<T>-DS01`, or the proxy route T13–T16 in the transition
>   session) against the mux, compared per semantic field with MCP/GraphQL
>   parity.
> - AD11 is retired with an explicit note: a return to the deprecated
>   standalone proxy is not an accepted rollback target; configurations that
>   bypass the mux are accepted by the compatibility cells.
> - AD12 is worded as the wire-symbol contract of #1003.
>
> The evidence index in helianthus-docs-ebus is updated accordingly in a
> prepared change. The T01..T88 release record of this issue maps to the
> `adaptermux-v2` report plus the last `full88` report of the consolidated
> session (#1099). The stale M6a test pointer noted above is unaffected.
>
> 🤖 Generated with [Claude Code](https://claude.com/claude-code)

## 4. helianthus-ebusgateway#1032 — comment (AdapterMux lifecycle)

> Matrix v2 planning (#1378): cells that replace the T01..T88/AD01..AD12
> reference in the transport gate statement above, for the `ebus.primary`
> lifecycle of the mux generation and its external listener.
>
> - Southbound side: the mux generation on `ENS` and `ENH` (`UDP`, `TCP`
>   planned; UART-direct reserved); the non-mux driver paths through
>   `ENS-DS01`, `ENH-DS01`, `UDP-DS01`, `TCP-DS01` and `EBUSD-ES01`.
> - Northbound side, lifecycle-relevant cells on each executable southbound:
>   AD04 (RESETTED to all consumers) `ENS-AD04`, `ENH-AD04`; AD09 (disconnect
>   and reconnection recovery) `ENS-AD09`, `ENH-AD09`; AD05 (external client
>   connects to the listener) `ENS-AD05`, `ENH-AD05`; CX01 (gateway and external
>   client together) `ENS-CX01`, `ENH-CX01`; PX12 (non-owner sends rejected)
>   `ENS-PX12`, `ENH-PX12`.
>
> The statement "eBUS T01..T88/AD01..AD12 and passive smoke are not
> filename-triggered" reads, after the gate switch, "the eBUS `adaptermux-v2`
> report (AD cells included) and passive smoke are not filename-triggered"; the
> classifier file lists do not change. Until the new matrix has run once on
> hardware, the current gate and the #1099 override apply.
>
> 🤖 Generated with [Claude Code](https://claude.com/claude-code)

## 5. helianthus-ebusgateway#1001 — comment (AdapterMux listener sharing)

> Matrix v2 planning (#1378): the external listener of the mux is the
> northbound side of every coexistence cell, so its supported formats decide
> which external-client cells exist.
>
> - Southbound side: `ENS`, `ENH` (`UDP`, `TCP` planned; UART-direct reserved).
> - Northbound side, listener-relevant cells on each executable southbound:
>   AD05 `ENS-AD05`, `ENH-AD05`; AD06 `ENS-AD06`, `ENH-AD06`; AD08
>   `ENS-AD08`, `ENH-AD08`; PX05–PX08 (FIFO within the external class)
>   `ENS-PX05`…`ENS-PX08`, `ENH-PX05`…`ENH-PX08`; PX12 `ENS-PX12`,
>   `ENH-PX12`; CX01 `ENS-CX01`, `ENH-CX01`.
>
> The listener speaks ENH only today. The old dual-client cases with ebusd
> reaching the proxy over plain UDP or TCP (over an ENS or ENH southbound, TCP
> was expected to pass and UDP expected to fail) have no mux counterpart and
> are retired; their question moves here. If a plain sharing format is supported, the matrix gains
> a CX01 variant for that format; until then CX01 uses an ENH client.
>
> 🤖 Generated with [Claude Code](https://claude.com/claude-code)

## 6. helianthus-ebusgateway#1345 — comment

> Matrix v2 planning (#1378) changes the physical gate list this catalogue will
> publish. The five range gates (T01–T04, T05–T08, T09–T24, T25–T88,
> PX01–PX12) are replaced by the `adaptermux-v2` inventory of 173 case IDs:
>
> - Southbound rows `ENS`, `ENH`, `UDP`, `TCP`, `UARTENS`, `UARTENH`,
>   `UARTRAW`, each with the northbound families AD (AD01–AD10, AD12), PX
>   (PX01–PX12) and CX (CX01);
> - the compatibility block `ENS-DS01`, `ENH-DS01`, `UDP-DS01`, `TCP-DS01`,
>   `EBUSD-ES01`;
> - AD11 as a retired, non-gate row;
> - acceptance categories XF-01, BI-01 (`adapter_no_signal`), BI-02
>   (`uart_direct_hardware_absent`), PL-01, PL-02, with `planned` accepted only
>   inside PL-01 and PL-02.
>
> For the 0.7 consolidated session the catalogue must carry both forms (the
> last `full88` and the first `adaptermux-v2`); after the gate switch only the
> new one. Each change bumps the catalogue version, which the add-on index
> (helianthus-ha-addon#274) then consumes instead of its hard-coded ranges.
>
> 🤖 Generated with [Claude Code](https://claude.com/claude-code)

## 7. helianthus-ebusgateway#938 — comment

> Matrix v2 planning (#1378): "T01-T88 live parity remain INT-20" maps to the
> `adaptermux-v2` cells of the configured transport, run in the same
> consolidated session (#1099).
>
> - Southbound side: the configured transport's row (`ENS`, `ENH`; `UDP`,
>   `TCP` planned) and, for non-mux configurations, `ENS-DS01`, `ENH-DS01`,
>   `UDP-DS01`, `TCP-DS01` or `EBUSD-ES01`.
> - Northbound side relevant to identity corroboration: AD01 (active
>   exchanges) and AD02/AD03 (passive companion evidence) on `ENS` and `ENH`:
>   `ENS-AD01`, `ENH-AD01`, `ENS-AD02`, `ENH-AD02`, `ENS-AD03`, `ENH-AD03`.
>
> The separate P01..P06 passive report requirement stated above is not changed
> by the matrix plan.
>
> 🤖 Generated with [Claude Code](https://claude.com/claude-code)

## 8. helianthus-ebusgateway#551 — comment

> Matrix v2 planning (#1378): the two capture families of this issue map to
> hardware setups of the new matrix.
>
> - adapter-direct captures → the mux southbound setups `ENS` and `ENH`; the
>   matching cells are `ENS-AD01`, `ENH-AD01` (active B503 exchanges),
>   `ENS-AD12`, `ENH-AD12` (raw escape pairs in captured frames) and
>   `ENS-CX01`, `ENH-CX01` when an external client is present.
> - ebusd_tcp captures → the compatibility cell `EBUSD-ES01` on the ENH setup.
>
> The release-wide T01..T88 ownership referenced above (#476) becomes the
> `adaptermux-v2` report plus the last `full88` report of the consolidated
> session (#1099). The bridge-PASS/bridge-LIVE-PASS rows of the B503 matrix
> are unaffected.
>
> 🤖 Generated with [Claude Code](https://claude.com/claude-code)

## 9. helianthus-docs-ebus#250 — comment

> Index completion for the AD rows, from the gateway's matrix v2 planning
> (helianthus-ebusgateway#1378), is prepared on the branch
> `issue/1378-ad-index-matrix-v2` of this repository (not yet published):
>
> - AD10: equivalence test plan; per southbound transport, the replaced
>   configuration against the multiplexer, plus the proxy route once in the
>   transition session; cells `<T>-AD10` on `ENS`, `ENH`, `UDP`, `TCP` and the
>   reserved UART rows.
> - AD11: retired with an explicit note; no artifact required, never counted
>   as PASS.
> - AD12: the multiplexer architecture text "logical bytes (no escape
>   sequences)" is replaced by the wire-symbol contract of
>   helianthus-ebusgateway#1003; cells `<T>-AD12` on every southbound.
>
> Southbound side of all AD rows: `ENS`, `ENH`, `UDP`, `TCP`, `UARTENS`,
> `UARTENH`, `UARTRAW`. Northbound side: AD01–AD10, AD12.
>
> 🤖 Generated with [Claude Code](https://claude.com/claude-code)

## 10. helianthus-execution-plans#130 — comment

> Gateway matrix v2 planning (helianthus-ebusgateway#1378) changes two of the
> items collected here:
>
> - "the T01-T88 matrix" becomes the `adaptermux-v2` matrix: southbound rows
>   `ENS`, `ENH`, `UDP`, `TCP` and reserved UART-direct rows, each with
>   northbound invariants AD01–AD10, AD12, PX01–PX12 and CX01, plus the
>   compatibility cells `ENS-DS01`, `ENH-DS01`, `UDP-DS01`, `TCP-DS01`,
>   `EBUSD-ES01`; 173 cells, hardware once per transport.
> - "AD01-AD12 evidence" becomes the AD cells of that report (AD11 retired,
>   AD10 with an equivalence plan, AD12 as the wire-symbol contract).
>
> The consolidated session (helianthus-ebusgateway#1099) runs the last
> T01–T88 and the first `adaptermux-v2` on the same hardware session; the old
> gate and its override stay in force until then.
>
> 🤖 Generated with [Claude Code](https://claude.com/claude-code)

## 11. helianthus-vrc-explorer#312 — comment

> The transport clause above ("HELD pending the T01..T88 hardware gate") maps,
> after the gateway's matrix v2 planning (helianthus-ebusgateway#1378), to the
> gateway cells on the Explorer path through the AdapterMux:
>
> - Southbound side: `ENS`, `ENH` (`UDP`, `TCP` planned; UART-direct reserved).
> - Northbound side: `ENS-CX01`, `ENH-CX01` with VRC Explorer as the external
>   client, and `ENS-AD05`, `ENH-AD05`, `ENS-AD06`, `ENH-AD06`, `ENS-AD08`,
>   `ENH-AD08`, `ENS-AD12`, `ENH-AD12`.
>
> Until the new matrix has run once on hardware, the T01..T88 gate stays the
> one in force. The certification run of the Explorer through the mux is
> drafted as a separate issue.
>
> 🤖 Generated with [Claude Code](https://claude.com/claude-code)

## 12. helianthus-ebusgateway#991 — comment (informational)

> Matrix v2 planning (#1378): PX09, PX10 and PX11 (local target sees the request
> only from echoed RECEIVED; local emulated response inside the responder
> window; late responder bytes rejected and counted) became cells of the mux on
> every southbound, `<T>-PX09` to `<T>-PX11`. The mux has no local responder
> today, so all 21 cells are planned (category PL-02, reason
> `mux_local_responder_not_implemented`) and leave that state when the
> registered responder ownership of this issue exists. Whether that is wanted
> for 0.7 is an operator decision; this comment changes neither the deferral
> nor the scope above.
>
> 🤖 Generated with [Claude Code](https://claude.com/claude-code)

## 13. helianthus-vrc-explorer — new issue (Explorer certification run)

Title: `[0.7] Certification run: VRC Explorer as the external ENH client through the gateway AdapterMux`

> ## Observation
>
> No issue records a certification run of VRC Explorer against the 0.7
> candidate. The Explorer ships in the same 0.7 candidate as the gateway and the
> add-on (helianthus-ha-addon#246), and the gateway's consolidated hardware
> handoff lists an Explorer Pi row with no assigned procedure owner. The
> gateway's transport conformance matrix v2 (helianthus-ebusgateway#1378) makes
> VRC Explorer one of the two external ENH clients of its coexistence
> invariant CX01.
>
> ## Scope
>
> In the consolidated hardware session (helianthus-ebusgateway#1099), with the
> Explorer connected to the gateway's AdapterMux listener:
>
> - Southbound side: each executable mux row, `ENS` and `ENH`. `UDP` and `TCP`
>   are planned in the gateway matrix and UART-direct rows are reserved; they
>   are recorded as not run here.
> - Northbound side: CX01 records with `northbound_client = vrc-explorer`
>   (`ENS-CX01`, `ENH-CX01`): an Explorer scan completes while the gateway
>   smoke runs; plus Explorer workload evidence for `ENS-AD05`, `ENH-AD05`
>   (receives broadcasts), `ENS-AD06`, `ENH-AD06` (START/SEND),
>   `ENS-AD08`, `ENH-AD08` (echo per raw symbol) and `ENS-AD12`, `ENH-AD12`
>   (raw escape pairs, including reserved bytes in a CRC position;
>   helianthus-ebusgateway#1003 and the diagnostic of PR #287).
> - A written Pi procedure for this run, owned in this repository.
>
> ## Acceptance
>
> - One public-safe record per cell above, with family wording, evidence grade
>   (Proven, Hypothesis, Unknown) separate from qualification, and no captures,
>   addresses, serials or capture times.
> - Unexpected fail or xpass returns to the owning repository; blocked or
>   unexecuted cells stay visible.
> - No device writes beyond what the operator authorizes at action time.
>
> ## Out of scope
>
> Explorer feature work, the direct (non-mux) Explorer path against an adapter,
> and gateway gate changes.
>
> 🤖 Generated with [Claude Code](https://claude.com/claude-code)

## 14. helianthus-ebus-adapter-proxy — new issue (proxy deprecation)

Title: `Retire the PX01–PX12 adjunct and the T01..T88 proxy topologies in favor of the gateway AdapterMux matrix`

> ## Observation
>
> The repository is marked deprecated (README; #107 made the maintenance
> boundary self-contained), but `profiles/proxy-wire-semantics/px-cases.md`
> still says the PX01–PX12 set "must be validated alongside the transport
> matrix (`T01..T88`)" and calls T01..T88 the primary gate, and
> `scripts/run_proxy_semantics_matrix.py` still writes a gate report. The
> gateway operator steer of 2026-10-27 (helianthus-ebusgateway#1378) retires
> the proxy topologies T09–T88 and moves PX01–PX12 into the AdapterMux as
> cells of the gateway's `adaptermux-v2` matrix (`<T>-PX01` to `<T>-PX12` on
> every southbound transport).
>
> ## Scope
>
> 1. `px-cases.md`: mark the case set historical; name the gateway matrix cells
>    that carry each PX case now; record that PX09–PX11 have no mux
>    counterpart yet (planned until gateway #991).
> 2. `scripts/run_proxy_semantics_matrix.py`: state in its header and output
>    that the report is historical and feeds no Helianthus gate; keep it
>    runnable for reproduction.
> 3. README and AGENTS: name the replacement matrix and the deprecation of the
>    T09–T88 topologies.
> 4. Archival: the consolidated gateway session (helianthus-ebusgateway#1099)
>    runs T01–T88 one last time and needs the proxy for T09–T88. Archive the
>    repository only after that session's reports exist.
>
> ## Acceptance
>
> - No document in this repository presents PX01–PX12 or T01..T88 as a current
>   gate.
> - The PX tests and their runner still pass as historical evidence of the
>   proxy implementation.
> - Markdown and link validation pass.
>
> ## Out of scope
>
> Runtime, transport or protocol changes; gateway gate changes; the
> docs-ebus smoke-matrix runbook (its own companion).
>
> 🤖 Generated with [Claude Code](https://claude.com/claude-code)

## 15. helianthus-ebusgateway — new issue, proposed (runner support)

Title: `[0.7] Generate the adaptermux-v2 suite in matrix-runner (case naming, inventory, reserved cells)`

> The consolidated session (#1099) must produce the first `adaptermux-v2`
> report, so the runner needs the suite before that session. Scope: the
> proposal in §7 of `matrix/M7-adaptermux-conformance-matrix.md` (suite name,
> `<TRANSPORT>-<INVARIANT>` case IDs, order, kinds, verdict and index fields,
> environment, flags, reserved UART and planned cells, XF-01 inventory) with
> tests; `full88` unchanged and still the default. Not in scope: the gate
> switch of `docs/testing/transport-gate-policy-v2.md`. The change touches
> `internal/matrix/*` and `cmd/matrix-runner/*` and is therefore under the
> transport gate (the #1099 override applies until the session).
>
> 🤖 Generated with [Claude Code](https://claude.com/claude-code)

## 16. helianthus-ebusgateway — new issue, proposed (mux plain southbound)

Title: `AdapterMux southbound UDP-plain and TCP-plain framing`

> The matrix v2 planning (#1378) puts UDP-plain and TCP-plain on the mux's
> southbound axis, but the mux accepts only `enh` and `ens` upstream, so the 48
> `UDP-*` and `TCP-*` cells stay planned (PL-01). With the proxy, plain
> southbound existed; dual-client runs over it were expected to fail (carried
> as XF-01 to `UDP-CX01` and `TCP-CX01`). Scope: define and implement the
> mux's own arbitration, echo verification, escape handling and loss boundary
> on a raw symbol stream (the public UDP-plain contract defines none of them),
> with deterministic tests; then the cells execute. Until then the plain
> transports are covered by `UDP-DS01` and `TCP-DS01` without the mux.
>
> 🤖 Generated with [Claude Code](https://claude.com/claude-code)
