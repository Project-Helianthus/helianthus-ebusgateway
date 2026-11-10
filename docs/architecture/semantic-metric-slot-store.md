# Semantic metric slot store

This internal gateway API implements durable assignment mechanics for the
[Thermal and Infrastructure renderer](../semantic-prometheus-thermal-infrastructure-v1.md)
and the accepted
[coverage contract](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/deployment/prometheus-semantic-coverage.md).
It adds no public metric schema or native protocol meaning. Nothing here is a
stability promise before Helianthus 1.0.

## Private API

`internal/semanticmetrics.O21Persistence` injects `Load(target)` and
`Commit(target, document)`. The target is an opaque local configuration identity,
never a metric label or a network address. One adapter/store owns one target
exclusively: the adapter must fence competing writers, including other processes
and store instances. It must bound reads, return detached bytes, and neither
retain nor mutate committed bytes. A successful commit means the whole document
has been atomically replaced and durably completed, including any required
filesystem metadata durability. Partial writes, uncertain acknowledgements and
lost writer fencing must return errors. `SlotFileStore` implements these adapter obligations on Linux and Darwin; no
deployment path is selected.

`O21Create` explicitly provisions a definitely absent target. Only
`fs.ErrNotExist` permits creation; corruption and other read errors never reset
reservations. Creation itself must complete durably. Missing state on restart
must be investigated by composition, never automatically recreated.

`O21Open` reads the document with version, exact target and complete `Binding`
identities, including retired entries. It rejects documents above 256 KiB,
unknown properties, trailing data, invalid version/target, unsupported domains,
out-of-range or duplicate slots, duplicate tuples, invalid identity members,
invalid dimensions and invalid, duplicate or cross-domain selected fields. It
uses the existing renderer configuration validator and admits at most 16 slots
in each of the thermal and infrastructure domains. The target is nonempty and
at most 256 bytes. It never imports a saved validation flag.

`Assign(target, binding)` reserves a caller-selected slot; it does not allocate
on scrape. Assignments are immutable. A changed identity uses a new unused slot.
`Retire(target, binding)` requires the complete original non-retired binding and
persists its tombstone. Neither an active nor a retired slot can be reused for
that target. Tombstones have no expiry or deletion API. Each domain eventually
exhausts after 16 reservations, independently of the other domain.

`Export()` returns a detached `Registry`. `Assign` and `Retire` also return
detached registries only after durable completion, with `Validated=false`.
Before invoking persistence, a transition clears validation; concurrent exports
retain the last accepted assignments but withhold all values. Successful
persistence installs the proposed document. Any persistence error leaves the
last accepted assignments intact, latches uncertainty and prohibits further
writes or revalidation in that instance. Reopen through the adapter is required
to discover what actually persisted; corrupt or missing state still fails
closed. Previously returned registries are detached snapshots, so composition
must not cache an earlier validated registry across transitions.

`Revalidate(target, bindings)` is an explicit current-owner attestation of the
entire exact ordered binding list, including tombstones, slot, domain, asset,
source, source epoch, native binding, driver generation, service instance,
dimensions and selected fields. The caller must obtain current owner evidence
before calling it. Exact equality deliberately rejects reorderings. A mismatch
clears validation without changing assignments or writing persistence. An
uncertain instance cannot be revalidated. This API does not itself consult a
native owner or turn stored identities into qualified observations. The renderer
still requires current admitted, qualified, fresh and valid semantic evidence
for each field and preserves its existing independent missing-value behavior.

## Filesystem adapter

`NewSlotFileStore(directory, target)` requires an existing caller-selected
local directory and an explicit target. It does not load, create or repair a
document. Targets must be valid UTF-8, nonempty, at most 256 bytes, contain no
control characters or path separators, and differ from `.` and `..`. A computed
target fingerprint maps each identity to a bounded filename; document identity
is still checked by `O21Open`. The target never becomes a metric label.

On Linux and Darwin the adapter opens the directory without following a final
symlink, then uses directory-relative operations. Other platforms reject
construction explicitly. The caller must control the directory and its ancestor
directories, use a local filesystem supporting advisory exclusive locks, atomic
same-directory rename and file/directory sync, and ensure every writer uses the
adapter. Network/distributed filesystems are outside this contract. Lock files
are persistent and must never be removed or replaced, even after Close.

A nonblocking exclusive per-target lock fences other instances and processes.
Operations verify the directory and regular lock inode still match their held
descriptors; lost fencing latches uncertainty. Different targets have independent
locks and documents. `Close` waits for an operation in progress, releases file
descriptors and fencing, is idempotent, and permanently rejects later reads and
commits. The caller must close the adapter before reopening for recovery and
must not use a previously exported registry after closing its persistence owner.

Reads reject symlinks, nonregular objects and documents above 256 KiB. Reads are
bounded even if a file grows and return detached bytes. Only an absent document
returns `fs.ErrNotExist`; invalid objects and other errors never permit
`O21Create` to repair state. JSON and binding validation remain owned by
`O21Open`, rather than being duplicated in the filesystem layer.

Commit checks the target and size, writes a private temporary regular file in
the held directory, syncs the file, closes it, rechecks fencing, atomically
renames over the document, syncs the directory, then rechecks fencing before
acknowledging success. It does not retain the input buffer. Temporary files are
cleaned up on ordinary error paths; a process crash may leave unused temporary
files, which are never loaded as documents. No cleanup of unrelated files is
performed. After persistence starts, every error latches uncertainty, including
rename errors and errors after rename. Reopening discovers the document that
actually survived; an error never implies that replacement did not happen.
Target mismatch and an oversized input are rejected before persistence starts.

## Composition still required

`gatewaySemanticMetricRegistry` remains empty. Production binding selection,
deployment state directory, explicit provisioning and missing-store policy,
current-owner revalidation, and registry refresh/invalidation at the renderer
boundary remain separate composition work. Composition must serialize transitions
with consumption of exported registry snapshots. No configuration, metrics
route, automatic repair, acquisition, dependency or public API is added.
MCP compatibility, GraphQL parity, native evidence and per-field last-known-good
state remain unchanged.

## Evidence classification

**Proven** by focused synthetic `TestO21` and `TestSlotFileStore` fixtures when
run successfully: bounded detached reads, refusal of unsafe objects, explicit
absence and provisioning, cross-target isolation, exclusive instance/process
fencing, close/reopen behavior, ordered file-write/sync/close/rename/directory-sync
acknowledgement, individual injected operation failures, post-rename uncertainty,
last accepted assignment retention, unvalidated restart, full identity rejection
and persistent retirement. These are durable-adapter evidence only.

**Hypothesis**: operating-system sync acknowledgements provide the required
power-loss durability on a selected deployment filesystem. Offline fault seams
exercise acknowledged operation ordering and failure policy, rather than power
interruption or storage-controller behavior.

**Unknown**: production composition, actual filesystem power-loss qualification,
live owner reattachment, device behavior and physical acceptance.
Proven/Hypothesis/Unknown are evidence grades, separate from qualification,
freshness and physical acceptance. This structural runtime change can alter
release binaries and requires renewal of affected release, security and hardware
evidence. Full repository CI, applicable transport/conformance gates and
independent committed-change review remain shipping obligations. Offline fixtures
do not establish deployed acceptance, physical qualification or binary neutrality.
