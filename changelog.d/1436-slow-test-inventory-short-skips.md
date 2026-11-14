## Record the measured slow-test inventory and skip the slowest replay tests in short mode

A measured inventory of slow tests is recorded in `docs/testing/slow-test-inventory.md`,
and the three slowest replay tests of the gateway command package are skipped in short
mode (the local gate). GitHub CI still runs them in the full suite. Assertions are
unchanged; this is a test and documentation change only, and the gateway binary is
unchanged.
