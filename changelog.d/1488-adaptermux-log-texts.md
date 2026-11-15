## Work-item codes removed from the AdapterMux log texts

Log texts only; behaviour, timing, counters, metric names, snapshot fields and configuration are unchanged. The `adaptermux: ` log and error texts no longer carry work-item codes: each text keeps its sentence and every formatted value in the same order, and the code is dropped, or replaced by the behaviour it stood for (`(debounced)` in the absorb timeout reset line). `scripts/rename_adaptermux_log_codes.py` holds the table and its `--check` mode fails if a coded string literal remains in the package. Old and new texts:

- `adaptermux: Close: timed out sending to pendingStart.notify (AM53)` becomes `adaptermux: Close: timed out sending to pendingStart.notify`
- `adaptermux: reconnect: timed out sending to pendingStart.notify (AM53)` becomes `adaptermux: reconnect: timed out sending to pendingStart.notify`
- `adaptermux: consecutive timeouts for %v (threshold %v), triggering reconnect (AM27)` becomes `adaptermux: consecutive timeouts for %v (threshold %v), triggering reconnect`
- `adaptermux: ownership released for session %d remote=%s (terminal SYN, F-21)` becomes `adaptermux: ownership released for session %d remote=%s (terminal SYN)`
- `adaptermux: ownership released for session %d remote=%s (SYN timeout) (AM6)` becomes `adaptermux: ownership released for session %d remote=%s (SYN timeout)`
- `adaptermux: ownership released for session %d remote=%s (F-26 external START pending: elapsed %v skipping grace %v) (AM6)` becomes `adaptermux: ownership released for session %d remote=%s (external START pending: elapsed %v skipping grace %v)`
- `adaptermux: ownership released for session %d remote=%s (idle grace expired: %v ≥ %v) (AM6)` becomes `adaptermux: ownership released for session %d remote=%s (idle grace expired: %v ≥ %v)`
- `adaptermux: handleReset: timed out sending to pendingStart.notify (AM53)` becomes `adaptermux: handleReset: timed out sending to pendingStart.notify`
- `adaptermux: pendingStart deadline expired for session %d (AM8, wasBlocking=%v, cancelledInFlight=%v)` becomes `adaptermux: pendingStart deadline expired for session %d (wasBlocking=%v, cancelledInFlight=%v)`
- `adaptermux: deadline triggered transport reconnect to unstick hung StartArbitration (C2)` becomes `adaptermux: deadline triggered transport reconnect to unstick hung StartArbitration`
- `adaptermux: F-34 abandon-watchdog firing for session %d gen=%d (no SEND within %v after STARTED reached the client) — force-releasing ownership` becomes `adaptermux: abandon-watchdog firing for session %d gen=%d (no SEND within %v after STARTED reached the client) — force-releasing ownership`
- `adaptermux: suppressing STARTED-mismatch(0x%02X, pending 0x%02X) for session %d — request was cancelled/replaced; absorbed (C4/R4 AM56-half)` becomes `adaptermux: suppressing STARTED-mismatch(0x%02X, pending 0x%02X) for session %d — request was cancelled/replaced; absorbed`
- `adaptermux: STARTED mismatch: got initiator 0x%02X, pending 0x%02X — failing pending session %d (AM56)` becomes `adaptermux: STARTED mismatch: got initiator 0x%02X, pending 0x%02X — failing pending session %d`
- `adaptermux: suppressing STARTED for session %d — request was cancelled/replaced; absorbed (C4/R4)` becomes `adaptermux: suppressing STARTED for session %d — request was cancelled/replaced; absorbed`
- `adaptermux: suppressing FAILED(0x%02X) for session %d — request was cancelled/replaced; absorbed (C4/R4 FAILED-half)` becomes `adaptermux: suppressing FAILED(0x%02X) for session %d — request was cancelled/replaced; absorbed`
- `adaptermux: absorb timeout reset reason=%s (was waiting for %d stale arbitration response(s)) (F-22 debounce) (%d suppressed)` becomes `adaptermux: absorb timeout reset reason=%s (was waiting for %d stale arbitration response(s)) (debounced) (%d suppressed)`
- `adaptermux: absorb timeout reset reason=%s (was waiting for %d stale arbitration response(s)) (F-22 debounce)` becomes `adaptermux: absorb timeout reset reason=%s (was waiting for %d stale arbitration response(s)) (debounced)`
- `adaptermux: broadcastResetToSessions timed out after 1s — %d session(s) still pending (AM4)` becomes `adaptermux: broadcastResetToSessions timed out after 1s — %d session(s) still pending`
- `adaptermux: rejecting session — managed generation retired (AM13)` becomes `adaptermux: rejecting session — managed generation retired`
- `adaptermux: rejecting session — mux not available (AM13)` becomes `adaptermux: rejecting session — mux not available`
- `adaptermux: rejecting session — max sessions (%d) reached (AM50)` becomes `adaptermux: rejecting session — max sessions (%d) reached`
- `adaptermux: session %d close timed out — wg.Wait pending, will self-resolve on I/O unblock (AM14)` becomes `adaptermux: session %d close timed out — wg.Wait pending, will self-resolve on I/O unblock`
- `adaptermux: session %d INFO id=0x%02X payload too large (%d bytes, max 255) (AM39)` becomes `adaptermux: session %d INFO id=0x%02X payload too large (%d bytes, max 255)`

Log consumers that match one of the old texts must match the new one; there is no second spelling. The gateway binary changes.
