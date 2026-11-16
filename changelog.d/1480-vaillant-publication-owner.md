## Move the publication state into a vaillantSemanticPublication owner

The Vaillant poller's publication collaborators (GraphQL provider, broadcast hub, cache persister and the boiler publication lock) move into a `vaillantSemanticPublication` owner reached through nil-safe accessors. Published output is unchanged; the gateway binary changes.
