# ADR-0002: MCP v1 Contract Envelope

## Status
Accepted

## Context
Current MCP responses are inconsistent across tools.

## Decision
Standardize `ebus.v1.*` responses as `meta/data/error`, including deterministic `data_hash`.

## Consequences
Improves determinism, testing, and parity verification with GraphQL.
