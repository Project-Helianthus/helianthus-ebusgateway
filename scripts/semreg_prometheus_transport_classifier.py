#!/usr/bin/env python3
"""Accept only PROM-SEMREG's exact detached lifecycle wiring hunk."""
from __future__ import annotations

import difflib
import subprocess
import sys
from pathlib import Path


def base_text(base_ref: str, path: str) -> str:
    result = subprocess.run(
        ["git", "show", f"{base_ref}:{path}"], text=True, capture_output=True, check=False
    )
    if result.returncode:
        raise ValueError("cannot read base lifecycle source")
    return result.stdout


def changed_lines(before: str, after: str) -> tuple[list[str], list[str]]:
    removed: list[str] = []
    added: list[str] = []
    for line in difflib.unified_diff(before.splitlines(), after.splitlines(), n=0):
        if line.startswith("---") or line.startswith("+++") or line.startswith("@@"):
            continue
        if line.startswith("-"):
            removed.append(line[1:])
        elif line.startswith("+"):
            added.append(line[1:])
    return removed, added


def main() -> int:
    if len(sys.argv) != 3:
        return 2
    base_ref, path = sys.argv[1:]
    try:
        before = base_text(base_ref, path)
    except ValueError as exc:
        print(f"transport gate: {exc}", file=sys.stderr)
        return 1
    after = Path(path).read_text(encoding="utf-8")
    removed, added = changed_lines(before, after)
    expected_removed = [
        "\tif busObservability != nil && (cfg.ModbusTCPConfig.Enabled || cfg.ModbusTCPConfig.GrowattBMSRS485.Enabled) {",
        "\t\t\treturn semanticPrometheusDomains(modbusAdapter, growattBMSRuntime, cfg.ModbusTCPConfig.Enabled, cfg.ModbusTCPConfig.GrowattBMSRS485.Enabled, cfg.ModbusTCPConfig.GrowattBMSRS485.AssetID, at)",
    ]
    expected_added = [
        "\tif busObservability != nil && (cfg.ModbusTCPConfig.Enabled || cfg.ModbusTCPConfig.GrowattBMSRS485.Enabled || cfg.PrometheusEVSEEnabled) {",
        "\t\t\t// EVSE acquisition remains outside this issue. A future accepted owner",
        "\t\t\t// may supply the detached generic read seam; nil truthfully reports the",
        "\t\t\t// configured runtime as unavailable without creating Tesla composition.",
        "\t\t\treturn semanticPrometheusDomains(modbusAdapter, growattBMSRuntime, nil, cfg.ModbusTCPConfig.Enabled, cfg.ModbusTCPConfig.GrowattBMSRS485.Enabled, cfg.PrometheusEVSEEnabled, cfg.ModbusTCPConfig.GrowattBMSRS485.AssetID, at)",
    ]
    if path == "cmd/gateway/gateway_cli.go":
        expected_removed = []
        expected_added = [
            '\tfs.BoolVar(&cfg.PrometheusEVSEEnabled, "semantic-prometheus-evse-enabled", cfg.PrometheusEVSEEnabled, "append detached EVSE SemReg metrics when an EVSE semantic runtime is configured")',
        ]
    if removed == expected_removed and added == expected_added:
        return 0
    print("transport gate: lifecycle diff is not the exact detached SemReg Prometheus hunk", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
