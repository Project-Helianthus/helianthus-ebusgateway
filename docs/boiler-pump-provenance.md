# Boiler pump evidence

The gateway's fast eBUS boiler refresh does not derive
`CentralHeatingPumpActive` from circuit 0's `PumpStatusRaw`. A circuit pump
observation identifies the circuit pump; it does not establish boiler pump
state. Circuit pump evidence remains available in its native circuit snapshot.

Without independent boiler pump evidence, the boiler pump field remains
unknown. Independent true and false values survive circuit changes and partial
updates that omit boiler pump evidence. A later independent boiler pump update
may replace the retained value. Other supported mirror fields continue to
refresh independently.

Cached boiler pump values have Unknown provenance: older caches may contain a
value derived from circuit 0. Cache loading discards that field before provider
publication and poller hydration, while retaining other cached boiler fields.
Pump state is no longer persisted; independent live evidence is required after
each restart. Live partial updates continue to retain independent pump values.

This correction adds no new register qualification, wire mapping, or device
support. The regression fixtures use synthetic state and establish gateway
assembly behavior only.

Tracking: https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1094
