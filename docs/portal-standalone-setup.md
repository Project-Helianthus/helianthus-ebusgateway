# Standalone Portal setup and recovery

The Portal can show configuration, health and driver status even when managed
drivers are disabled or fail. It still needs a successfully bootstrapped gateway
and HTTP listener: a process-level configuration or listener failure can prevent
it from opening. This guide covers the delivered portion of
[#1002](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1002);
ordinary setup without daemon YAML tuning and full minimal-install acceptance
remain incomplete. Home Assistant is optional.

**Proven** below means traced to the current implementation, not a physical
installation result. **Hypothesis** covers planned behavior; **Unknown** covers
unverified installation and native binding outcomes. These labels are separate
from native qualification, freshness and physical acceptance. No interface is
stable before Helianthus 1.0.

## Prepare the host, then open Portal

1. Have the host operator provision an existing, trusted, writable local
   configuration directory and select it with `-portal-config-root` (default
   `/data`). The gateway does not create the directory. It owns
   `portal-config.json` and an exclusive `portal-config.lock` there; allow only
   one gateway writer. Keep lifecycle and recovery storage separate. The browser
   cannot choose this location. Host provisioning is outside the Portal editor.
2. Retain the installation's supported adapter bootstrap. Adapter location,
   transport and protocol are still host settings: `-address`, `-network` and
   `-transport`, including ENS/ENH where supported. Local UART setup or a remote
   TCP/UDP adapter connection is not configured by the Portal checkboxes; use
   the supported host adapter path. This guide adds no transport support.
   HTTP mounting, proxy interface/port/format, TLS/origin/trust configuration,
   native driver settings and existing M2M configuration also remain with the
   host. See the [gateway configuration reference](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/README.md#gateway-flag-cheat-sheet).
3. After host startup, open the configured Portal mount (normally `/portal/`)
   on the provisioned gateway HTTP service. `-http-addr` must enable that service
   and `-portal-path` must enable Portal. Keep the host's intended access boundary:
   the default HTTP bind is wildcard, and unselected sharing boxes do not close
   existing host listeners. No Home Assistant instance or working driver is
   required to read the shell. Missing native data remains unavailable.
4. Open **Configuration**, then **Reload saved choices (discard edits)** to
   read the accepted record. On first valid startup with no record, the gateway
   persists revision `0` from host driver eligibility with sharing, proxy and
   Matter choices false. Revision `0` is not local adoption. The configuration
   API is `api/v1/config` relative to the Portal directory, using GET to read
   and PUT to save; use the UI rather than treating the file as an import API.
5. Change the desired checkboxes and select **Save desired choices**. A
   successful save confirms durable choices, not driver startup or exposure.
   Reload discards unsaved edits. Saving does not START, STOP or RESTART a
   driver; it does not change the separate durable lifecycle intent.

## Read the result before taking another action

| What you see | Meaning and next step |
| --- | --- |
| Saved desired selection | Accepted preference. An unsaved checkbox is only a draft. |
| `restart_required` | Saved driver eligibility differs from startup eligibility. Arrange an operator-controlled gateway restart, with action-time confirmation where device effects are possible; then reload status. Saving alone leaves the current driver running or stopped as before. |
| `disabled` / `not_selected` | Startup applied deselection and owner readback confirms disabled. After an adopted edit, the driver is neither constructed nor started at the next startup. |
| `effective` / `running` | Current readback reports an enabled, running driver outside safety quarantine. This does not prove native qualification, fresh data or output publication. |
| `withheld` | Inspect the reason. `unsupported_selection` cannot widen host enablement; `evidence_unavailable` supplies no positive owner evidence; `lifecycle_stopped` requires the separate lifecycle owner, not another checkbox save. |
| `pending` or `error` | Inspect the driver reason. During scheduled `BACKOFF`, the row may show the next attempt, attempt number and sanitized last failure. This is the owner's retry schedule, not permission to force a restart or repair native trust. |

After an adopted save, a false driver choice narrows host eligibility at the next
startup; a true choice cannot enable a host-disabled driver. Reselecting a driver
and restarting can resume retained `RUNNING` lifecycle intent. Plan that device
effect explicitly. No record choice bypasses native admission or safety quarantine.

GraphQL and MCP sharing, adapter proxy and Matter output boxes save desired
choices only. `listener_application` remains `pending`; planned surfaces remain
`effective: false`. A valid planned binding can show
`pending` / `listener_application_pending`; absent binding evidence is withheld.
Selected Matter remains `withheld` / `evidence_unavailable`. Restart does not
complete these unimplemented applications. Existing host-controlled endpoints
may operate independently of these reported planned states. A checkbox cannot
create an eeBUS binding, select Matter resources or commission an output.

Passive observation and cache have no switches in this desired record. Sharing
choices do not refresh cached values or grant evidence; unavailable drivers
cannot observe. Host-level passive/cache tuning still exists and its removal
from ordinary setup remains follow-up work. Retained observations stay historical
with their provenance and age; a partial failure does not make valid sibling
fields fresh or erase their last-known evidence.

## Recover without resetting accepted state

- For `revision_conflict`, reload saved choices, review the newer record and
  reapply the intended edit. Do not force an old revision or reset its counter.
- For `unsupported_selection`, choose a provisioned option; for
  `listener_conflict` or `invalid_binding`, have the host operator repair
  bindings. The rejected edit is not applied. The Portal retains input on an
  error; **Reload saved choices (discard edits)** explicitly discards it.
- For `invalid_record`, `unsupported_format` or `persistence_failed`, inspect the
  sanitized code and safe next step. Have the operator check directory access,
  storage health, format compatibility and single-writer ownership locally.
  Preserve the failed record for controlled repair; do not publish its contents,
  unlink an active lock, delete the record to get defaults, or reset revisions.
  There is no browser repair/import wizard. Cold-load repair requires a supported
  local repair and a gateway restart, followed by fresh configuration/status
  readback. A reread alone cannot establish durability after a failed directory
  flush. `revision_exhausted` needs a future supported format migration.
- A record that may exist but fails to load withholds host-enabled drivers
  without constructing or opening them; shell and error/status remain available
  when the HTTP service starts. A definitely absent record, such as under an
  unprovisioned root, leaves host eligibility in force while the API reports its
  storage error. Missing storage is therefore not a universal driver-off switch.
- An error may return the last accepted choices as `historical: true`; no valid
  retained record means desired choices are unavailable. Historical display is
  not acceptance of the rejected edit or proof of current operation. Preserve
  separate native evidence and lifecycle/recovery state during local repair.

## Remaining acceptance for #1002

These are open requirements, not delivered setup steps:

- [ ] Deliver adapter location/transport/protocol editing and remove ordinary
  daemon YAML tuning, while retaining diagnostics and native safeguards.
- [ ] Apply independent GraphQL, MCP and proxy sharing choices with positive
  owner readback, proxy interface/port/format control and preserved private
  consumers; finish always-on passive/cache setup migration.
- [ ] Integrate native eeBUS participation/trust/device selection through its
  owner and selective Matter publication/commissioning through its owner.
- [ ] Complete optional Home Assistant Ingress/minimal-install packaging under
  the [add-on owner](https://github.com/Project-Helianthus/helianthus-ha-addon/issues/239).
  HA persistence, reattachment, continuity and stale-grace choices remain Unknown.
- [ ] Validate coherent standalone and optional HA paths, retained identities and
  state migration, recovery, and full diagnostics across all linked outcomes.
- [ ] Obtain candidate-level independent review, full CI and applicable release
  and physical evidence. This offline guide closes none of those requirements.

The owning [configuration and recovery contract](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/docs/architecture/portal-config-sharing.md)
and [contribution and embedding contract](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/docs/architecture/portal-contributions.md)
define the detailed boundaries and URL construction. Embedding changes paths,
not native trust or action admission. This guide grants no new authorization,
namespace, binding or lifecycle API; authentication/authorization framework work
remains on the 0.11 roadmap.
