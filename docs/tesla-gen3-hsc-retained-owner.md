# Tesla Gen3 HSC retained owner

The Tesla Gen3 HSC retained owner is a disabled-by-default, non-send gateway
component for the exact `wc3_24_44_3` current-limit profile. It accepts only
completed FC100 outcomes supplied by an external transport owner. It has no
serial path, stream, request builder, executor, retry callback, activation
sequence, credential, or write authorization.

## Configuration

Enabling the owner requires explicit public endpoint, asset, source,
source-epoch, clock-epoch, EVSE, connector, driver-generation, node, and exact
profile identities. Disabled configuration must be all-zero. The endpoint ID is
a public label and must never contain a private serial path.

The command flags use the `tesla-gen3-hsc-*` prefix. There are no Tesla serial,
activation, request, write, credential, or authorization flags.

## Completed-outcome ingestion

The owner accepts two record shapes:

- persistent configured current: one t7 request and its t8 terminal response;
- provisional allocated current: one t25 request/t26 acknowledgement followed
  by one later t27 request/t28 terminal response.

Every exchange must carry the configured endpoint, source, epoch, generation,
node, `wc3_24_44_3` operation version, a strictly increasing correlation ID,
wall and monotonic receipt coordinates, and a successful terminal outcome. The
owner validates request and response RTU ADUs, including unit, function and
CRC, against the retained payloads. It then delegates operation tags and typed
field validation to the pinned `helianthus-modbusreg` constructors. This rejects
forged typed values, malformed bodies, partial sequences, mismatched ADUs,
application errors, duplicate outcomes, and mixed identity or generation.

Persistent and provisional records remain separate. A rejected update does not
replace either accepted sibling. The retained evidence is bounded to the latest
persistent exchange and the latest provisional set/readback pair. All returned
payload and ADU slices are detached copies. A persistent-only update retains
the native provisional record but withdraws it from the new semantic
publication so an unchanged sibling cannot receive a new lifetime. A later
correlated provisional outcome can publish allocated current again with its
own lifecycle. When a provisional update includes the retained persistent
record, each record keeps its own receipt coordinates, so the provisional
update cannot refresh configured current. Provisional allocation lifetime starts
at the completed t25/t26 set/ack receipt. The later t27/t28 readback qualifies
the value and advances aggregate evaluation, but does not restart that lifetime.

## Lifecycle and read surfaces

A generation fence makes native and semantic reads unavailable before a
successor can be admitted. A successor requires the same stable endpoint,
asset, source, clock, EVSE, connector, profile and node, a different source
epoch, and exactly the next driver generation. The fence reserves one successor
atomically; successful construction consumes it, while construction failure
restores one retry.

Accepted records feed the existing read-only surfaces:

- MCP `modbus.v1.tesla.gen3.evse.current_limit.get` for native records;
- MCP `semantic.v1.evse.current.get` for the evaluated SemReg view;
- authenticated mTLS GraphQL `SemanticEVSECurrent` for the same detached SemReg
  publication;
- the EVSE domain on the configured semantic Prometheus listener, evaluated at
  the scrape instant from that same detached publication.

The two Tesla MCP tools are registered from the retained-owner capability even
when the unrelated Modbus TCP core is disabled. That composition does not
register TCP raw/profile tools or Growatt tools; Growatt native, semantic, and
Portal availability remain conditional on a successfully started Growatt
runtime.

Reads access retained memory only. They cannot ingest a record, open an
endpoint, activate a profile, construct a request, retry, recover, or write. A
consumer read may advance only the public evaluation high-water. If an already
buffered, later-correlated completed outcome is delivered afterward, the owner
retains its original native receipt and evidence while the new public aggregate
evaluation clamps to that high-water.

## Remaining boundary

This slice does not implement production or live acquisition and does not close
gateway issue #965. A future acquisition owner must arrive through a separately
accepted safe contract. This retained-record component remains independent of
that acquisition work.
