# Echo-mismatch counting and adapter mux diagnostics

**Proven** means visible in the current source renderer; **Hypothesis** means an
operator interpretation to investigate; **Unknown** means evidence is missing.
These terms do not establish codec or profile qualification.

## Counting

**Proven:** one echo-mismatch observation adds exactly one to
`ebus_errors_total{class="echo_mismatch",phase="request",scope="active"}` and
exactly one to `ebus_active_echo_mismatch_subclass_total{subclass=...}`. Series
names and the existing `class="echo_mismatch"` selectors are unchanged.

The protocol layer can report one mismatch twice: once with the observed byte and
once without a byte. The store treats a byte-less event that follows a
byte-bearing one within 250 ms, with no other event between, as the same
observation and does not count it again. Without a repeat, nothing changes.

## Subclass labels

New values of `subclass` (the existing values are unchanged):

| Value | Meaning |
| --- | --- |
| `byte_unknown` | The event carries no observed byte. It is never labelled `post_grant_ack`. |
| `escape_pair_verify` | The adapter mux failed the echo verification of a gateway escape pair and no byte is attributed. |

**Proven:** `post_grant_ack` now means an observed byte equal to the ACK symbol.
**Unknown:** the bus event has no byte-provenance flag in the pinned library.
A zero byte on an event that carries the protocol echo-mismatch outcome is
treated as not observed and reported as `byte_unknown`, so a real observed zero
byte from the protocol layer is also `byte_unknown` until the flag is
available. An event without that outcome carries its byte literally, so a zero
byte there stays `post_grant_ack`.

## Adapter mux counters

All carry bounded labels only; none carries an address or a serial.

| Metric | Labels | Meaning |
| --- | --- | --- |
| `ebus_adaptermux_escape_pair_verify_failures_total` | `step` = `lead` or `suffix` | Failed gateway escape-pair echo verifications by failing symbol. |
| `ebus_adaptermux_transaction_endings_total` | `reason` | Gateway-owned transactions by ending reason. |
| `ebus_adaptermux_stale_active_bytes_discarded_total` | none | Bytes left by a finished or failed transaction and discarded. |
| `ebus_adaptermux_inter_write_drained_bytes_total` | none | Stale bytes drained between two writes of one transaction. |
| `ebus_adaptermux_foreign_initiator_releases_total` | none | Ownerships released after a foreign initiator byte. |

The mux also writes a rate-limited log line per escape-pair failure with the
written and observed symbols.

## Release notes

The echo-mismatch count falls where a generic mismatch was previously counted
twice, and events previously labelled `post_grant_ack` without an observed byte
move to `byte_unknown` or `escape_pair_verify`. Alerts on
`class="echo_mismatch"` keep working but may see a lower rate.
