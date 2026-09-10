#!/usr/bin/env python3
"""Fail closed if injected Tesla EVSE semantics reach production dispatch/config."""
from pathlib import Path

FORBIDDEN = "TeslaGen3EVSESemantic"
PRODUCTION_PATHS = (
    Path("config.go"), Path("modbus_config.go"),
    Path("cmd/gateway/gateway_run_lifecycle.go"),
    Path("cmd/gateway/gateway_run_setup.go"),
    Path("cmd/gateway/modbus_mcp_provider.go"),
    Path("cmd/gateway/m2m_graphql_runtime.go"),
)

def main() -> int:
    offenders = [str(path) for path in PRODUCTION_PATHS if FORBIDDEN in path.read_text()]
    if offenders:
        raise SystemExit("injected Tesla EVSE semantic seam reached production path: " + ", ".join(offenders))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
