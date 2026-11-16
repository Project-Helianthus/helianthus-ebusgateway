## Publish zones, hot water, circuits and radio devices from native views

Zones, hot water, circuits and radio devices are published by the `vaillantSemanticPublication` owner from native views that the acquisition side builds under its snapshot lock and hands over; the owner maps them to GraphQL values and pushes them to the provider and hub. Three acquisition functions over the size budget (the radio device refresh, the circuit refresh and the grab zone parser) are split into helpers without reordering reads. Published output is unchanged; the gateway binary changes.
