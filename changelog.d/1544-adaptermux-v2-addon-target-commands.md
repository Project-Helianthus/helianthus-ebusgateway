## Add verify-only start and stop commands for adaptermux-v2 cells on an add-on target

Verify-only start and stop commands let `matrix-runner` run `adaptermux-v2` cells against a gateway that runs as an add-on: they check that the gateway and the external client are in the state the cell needs and change nothing; tooling only, the gateway binary is unchanged.
