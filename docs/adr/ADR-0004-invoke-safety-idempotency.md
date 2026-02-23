# ADR-0004: Invoke Safety and Idempotency

## Status
Accepted

## Context
Mutating operations need explicit safety controls.

## Decision
`ebus.v1.rpc.invoke` must require intent, dangerous override for mutating/unknown methods, and idempotency key for mutate intent.

## Consequences
Makes side-effectful calls explicit and auditable.
