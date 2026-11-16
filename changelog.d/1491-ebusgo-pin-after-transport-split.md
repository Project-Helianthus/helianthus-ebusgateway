## Re-pin ebusgo after the bus and ENH transport split and the local worker string renames

The ebusgo pin moves to the main-branch revision that splits the bus echo, transaction and retry paths and the ENH transport into named units and renames the local worker and GPIO transport texts. Gateway source, wire behaviour and the protocol and transport API the gateway uses are unchanged; the AdapterMux, matrix and PTY tests pass on it. The gateway binary changes.
