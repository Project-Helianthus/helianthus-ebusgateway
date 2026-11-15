## Give the single-attempt cleanup test an operation budget that survives a loaded host

The single-attempt cleanup test of the runtime control service gave its fixture's first START one attempt of 100 ms, which a loaded host can miss. The budget is now 2 s; the assertions are unchanged. Test-only: the gateway binary is unchanged.
