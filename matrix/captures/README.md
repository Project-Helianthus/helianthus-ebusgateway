# M7 BENCH-REPLACE — Capture artefact directory

**Milestone:** `M7_BENCH_REPLACE`
**Repo:** `helianthus-ebusgateway`
**Issue:** [#551](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/551)
**Plan:** [`vaillant-b503-namespace-w17-26.implementing`](https://github.com/Project-Helianthus/helianthus-execution-plans/blob/main/vaillant-b503-namespace-w17-26.implementing/13-amendment-1-dispatcher-portal-ux.md) §M7
**Plan canonical SHA:** `86495340799be9340dc191c371a49a958f65c357c76a1e0a2974502c8489b508`
**Predecessor:** M6_DISPATCHER_BRIDGE merged ([squash `25ae909`](https://github.com/Project-Helianthus/helianthus-ebusgateway/pull/550))

---

## Purpose

This directory holds **operator-attested live-bus capture artefacts** used to flip
`matrix/M6a-vaillant-b503.md` §9 rows from `[bridge-PASS]` (mocked-transport pass) to
`[bridge-LIVE-PASS]` (live-hardware pass). Per AD17, capture artefacts are one of three
concurrent attestation gates; the trailer (`BENCH-REPLACE-SIGNOFF: <YYYY-MM-DD>` on the PR
HEAD commit) and the GitHub PR label (`bench-replace-signoff`) are the other two.

Without capture artefacts in this directory matching the §6 BENCH-REPLACE protocol of
`matrix/M6a-vaillant-b503.md`, the §9 row flip is a defect, regardless of trailer or label
state.

---

## File-naming convention

```
matrix/captures/M7-<transport>-<YYYY-MM-DD>.txt
```

`<transport>` values for v1:

| Transport | Filename |
|-----------|----------|
| adapter-direct | `M7-adapter-direct-<YYYY-MM-DD>.txt` |
| ebusd_tcp | `M7-ebusd-tcp-<YYYY-MM-DD>.txt` |

`ebusd_serial` is `ESCALATE` per matrix §5 (no lab rig); no capture file expected for v1.

Date format is ISO-8601 `YYYY-MM-DD` matching the day on which the capture was performed
on the live gateway (`192.168.100.4`). Multiple capture rounds in a single day land as
separate commits on the same file, not date-suffixed duplicates.

---

## Required content per capture file

Each capture file MUST be reproducible from the live bus and MUST include the following
fields. Format is structured key-value (one field per line, `key: value`); section
headers in `## ...` markdown form keep the artefact human-readable while remaining
greppable. See `M7-EXAMPLE-template.txt` for the canonical layout.

### Header fields (all required)

| Key | Description |
|-----|-------------|
| `transport_family` | One of `adapter-direct`, `ebusd_tcp`. |
| `gateway_address` | Source byte the gateway uses on this transport (`0x71` per project memory). |
| `target_address` | Target eBUS device address. v1 expects `BAI 0x08` and/or `BASV2 0x15`. |
| `gateway_build_sha` | Short SHA of the gateway binary running on `192.168.100.4` during the capture. |
| `capture_date` | ISO-8601 `YYYY-MM-DD` matching filename. |
| `capture_operator` | GitHub handle of the operator running the capture. |

### §A — 5 read-selector round-trips (REQUIRED)

For each B503 read selector, capture the gateway-side MCP envelope (so
error-mapping discipline per `helianthus-docs-ebus` B503.md §12.4 is
verifiable end-to-end). The B503 selector pairs (authoritative source:
`helianthus-ebusgo/protocol/vaillant/b503/encode.go`) are:

1. `errors.get` — selector pair `00 01` (Currenterror)
2. `errors.history.get` — selector pair `01 01 <hex-index>` (Errorhistory)
3. `service.current.get` — selector pair `00 02` (Currentservice)
4. `service.history.get` — selector pair `01 02 <hex-index>` (Servicehistory)
5. `live_monitor.get` — selector pair `00 03` (HMU LiveMonitor; covered also under §B)

Each entry MUST include (REQUIRED for falsification):
- decoded MCP envelope `meta.capabilities.vaillant_b503.reason`
- decoded MCP envelope `meta.data_hash` — sha256 over decoded payload;
  the authoritative wire-fingerprint for §A
- decoded payload from the typed `ebus.v1.vaillant.<selector>.get` tool
  `data` section (slots / history record / live-monitor frame)
- timestamp (ISO-8601 with seconds resolution)

OPTIONAL auxiliary evidence (capture if convenient via tcpdump / socat
on the gateway's transport socket; NOT required because the v1 MCP
surface does not expose raw frames for §A1..§A4 — the typed handlers
in `mcp/vaillant_b503.go` return decoded payload maps via `slotsToMap`
/ `historyToMap`, and `ebus.v1.rpc.invoke` v1 takes a typed plane/method
shape, not raw bytes):
- raw request bytes (hex, comma-separated)
- raw response bytes (hex, comma-separated, including ACK/NAK)

For §A.5 only, `live_monitor.get` action=`read` exposes `data.raw_hex`
— that field SHOULD be recorded as `A5.response_bytes` (it is wire
bytes, available without auxiliary tooling).

### §B — Live-monitor round-trip (REQUIRED)

Capture the full `enable → read (≥2 frames) → disable` lifecycle as three distinct events:

1. `enable` (write-class frame; SERVICE_WRITE class on adapter-direct)
2. `read` — at least two consecutive observations
3. `disable` — operator-initiated (NOT idle-timeout) with matching `issuer_token`

Required side-evidence:
- `meta.capabilities.vaillant_b503.reason` transitions: `UNKNOWN`/last-known →
  `AVAILABLE` (post-first success after enable) → `AVAILABLE` (held during reads) →
  back to `AVAILABLE`/last-known after disable.
- Idle-timer is NOT relied on for disable; explicit disable RPC is exercised.

### §C — Mixed-traffic regression sample (REQUIRED)

Concurrent B524 + B503 traffic to demonstrate AD16 lock-order invariant
(`liveMonitorMu → readMu`) holds on real hardware. This is the live-bus ratification of
matrix §3 RB-02 / RB-03 / RB-04.

Required:
- ≥1 B524 read-poll observation captured **during** an active B503 live-monitor read.
- evidence the B524 poller did NOT stall (timestamp delta vs. baseline poll cadence
  within 2× tolerance).
- evidence no stale-epoch B503 frame escaped after a synthetic disconnect/reconnect (if
  reconnect is exercised in this capture round).

### §D — Verdict block (REQUIRED)

Closing block listing each row from `matrix/M6a-vaillant-b503.md` §9 covered by this
capture file with explicit `bridge-LIVE-PASS` or `bridge-FAIL` verdict per row. Any
`bridge-FAIL` verdict in any capture file blocks the PR per AD17 — the matrix flip is
not authorised and the BENCH-REPLACE round must be re-run after the regression is
fixed.

---

## Workflow

1. Operator copies `M7-EXAMPLE-template.txt` to `M7-<transport>-<YYYY-MM-DD>.txt`.
2. Operator runs the capture protocol from `matrix/M6a-vaillant-b503.md` §6.1 against
   the live gateway at `192.168.100.4`.
3. Operator fills the template with real wire bytes + envelope output.
4. Operator commits the capture file on this branch (`issue/551-m7-bench-replace`).
5. Operator edits `matrix/M6a-vaillant-b503.md` §9: replace `[bridge-PASS]` →
   `[bridge-LIVE-PASS]` for each row covered by this capture.
6. Operator adds `BENCH-REPLACE-SIGNOFF: <YYYY-MM-DD>` trailer to the signoff commit.
7. cruise-merge-gate validates trailer + label + capture artefacts present before
   permitting WAIT_OPERATOR lift.

The orchestrator-side scaffold (this PR's initial commit) MUST NOT pre-flip §9 rows
and MUST NOT include the `BENCH-REPLACE-SIGNOFF` trailer — both are reserved for the
operator's signoff commit on this branch.

---

## Cross-references

- AD17 — three-gate attestation contract (plan
  [`10-scope-decisions.md`](https://github.com/Project-Helianthus/helianthus-execution-plans/blob/main/vaillant-b503-namespace-w17-26.implementing/10-scope-decisions.md) §AD17).
- §6 BENCH-REPLACE protocol — [`matrix/M6a-vaillant-b503.md`](../M6a-vaillant-b503.md) §6.
- §6.1 execution protocol — [`matrix/M6a-vaillant-b503.md`](../M6a-vaillant-b503.md) §6.1.
- §9 production-dispatcher rows — [`matrix/M6a-vaillant-b503.md`](../M6a-vaillant-b503.md) §9.
