# Daemon update status

The daemon `firmwareVersion` in GraphQL (`daemonStatus` and
`daemon_status`) and MCP (`ebus.v1.runtime.status.get`) is the validated
embedded gateway release version supplied at build time.

`updatesAvailable` is a read-only comparison. The gateway's background release
checker finds the newest successful `push` run on `main` for public
[`helianthus-ha-addon` `build.yml`](https://github.com/Project-Helianthus/helianthus-ha-addon/actions/workflows/build.yml), then reads
`helianthus/config.json` at that run's immutable `head_sha`. It compares the
two versions with Go module semantic-version ordering. A moving `main` file and
GitHub Release tags are not used as release evidence.

The checker has bounded response sizes, request time, and a six-hour refresh
interval. GraphQL and MCP handlers only read its in-memory result: before the
first successful refresh they return `updatesAvailable: false`, and a failed
refresh retains the last successful comparison. No status request downloads,
installs, or changes an add-on or device.

Adapter status intentionally remains different. It reports the observed adapter
firmware only and keeps `adapterStatus.updatesAvailable` false until a public
catalogue supplies model, hardware revision, bootloader compatibility, exact
latest version, and provenance. Comparing adapter firmware with the add-on
release is not valid.
