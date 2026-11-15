## Give the retained eBUS consumers one owner

The consumers that live across eBUS generations (listener, passive reconstructor, semantic poller, promoter, semantic barrier, address-table subscription, admitted source) get one owner, `retainedEBusConsumers`, with a documented contract and a constructor that rejects missing collaborators. The order of effects, the log lines and the shutdown order are unchanged; the gateway binary changes.
