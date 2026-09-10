#!/usr/bin/env python3
"""Keep Tesla EVSE production composition on the retained non-send owner."""
from pathlib import Path

PRODUCTION_PATHS = (
    Path("config.go"), Path("modbus_config.go"),
    Path("cmd/gateway/gateway_run_lifecycle.go"),
    Path("cmd/gateway/gateway_run_setup.go"),
    Path("cmd/gateway/modbus_mcp_provider.go"),
    Path("cmd/gateway/m2m_graphql_runtime.go"),
)
OWNER = Path("cmd/gateway/tesla_hsc_retained_owner.go")
FORBIDDEN_PRODUCTION = (
    "NewTeslaGen3EVSESemanticPublication(",
    "TeslaGen3EVSESemanticEvidence{",
    ".IngestPersistent(",
    ".IngestProvisional(",
)
FORBIDDEN_OWNER = (
    "OpenRTUSerial(", ".Exchange(", "WriteRTU(",
    "BuildTeslaFC100OperationRequest(", "TESLA\\x00", "PASS\\x00",
)

def main() -> int:
    offenders = [
        f"{path}:{token}"
        for path in PRODUCTION_PATHS
        for token in FORBIDDEN_PRODUCTION
        if token in path.read_text()
    ]
    if offenders:
        raise SystemExit("Tesla EVSE injection reached production composition: " + ", ".join(offenders))
    owner = OWNER.read_text()
    unsafe = [token for token in FORBIDDEN_OWNER if token in owner]
    if unsafe:
        raise SystemExit("Tesla retained owner gained send or activation code: " + ", ".join(unsafe))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
