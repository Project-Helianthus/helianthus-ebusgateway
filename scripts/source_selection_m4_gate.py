#!/usr/bin/env python3
"""Reject legacy source-selection API names after SAS M4.

The gate is intentionally split by surface:

* active code rejects old selector API symbols and legacy join-era vocabulary;
* public text/schema/script surfaces reject old admission/status field names;
* Go code string literals are scanned for removed public GraphQL/MCP aliases.

Some old CLI/config spellings are intentionally retained by the locked plan as
compatibility inputs with new semantics. Those are listed in RETAINED_PUBLIC
with the owning rationale instead of being silently skipped.
"""

from __future__ import annotations

import dataclasses
import pathlib
import re
import subprocess
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]

PUBLIC_SUFFIXES = {
    ".graphql",
    ".json",
    ".md",
    ".sh",
    ".yaml",
    ".yml",
}

SELF_TEST_ALLOWLIST = {
    pathlib.Path("source_selection_m4_test.go"),
    pathlib.Path("scripts/source_selection_m4_gate.py"),
    pathlib.Path("scripts/transport_gate.sh"),
    pathlib.Path("scripts/passive_smoke_gate.sh"),
    pathlib.Path("docs/transport-gate-evidence/2026-04-25-adapter-direct-enh.yaml"),
}

HISTORICAL_ALLOWLIST_PREFIXES = (
    pathlib.Path("docs/transport-gate-evidence"),
)


@dataclasses.dataclass(frozen=True)
class LegacyPattern:
    name: str
    regex: re.Pattern[str]
    scope: str


def rx(pattern: str) -> re.Pattern[str]:
    return re.compile(pattern)


def literal(text: str) -> re.Pattern[str]:
    return re.compile(re.escape(text))


ACTIVE_CODE_PATTERNS = [
    LegacyPattern("Joiner", rx(r"\bJoiner\b"), "active code"),
    LegacyPattern("JoinBus", rx(r"\bJoinBus\b"), "active code"),
    LegacyPattern("JoinConfig", rx(r"\bJoinConfig\b"), "active code"),
    LegacyPattern("JoinResult", rx(r"\bJoinResult\b"), "active code"),
    LegacyPattern("JoinMetrics", rx(r"\bJoinMetrics\b"), "active code"),
    LegacyPattern("JoinStateStore", rx(r"\bJoinStateStore\b"), "active code"),
    LegacyPattern("NewJoiner", rx(r"\bNewJoiner\b"), "active code"),
    LegacyPattern("joiner", rx(r"\bjoiner\b"), "active code"),
    LegacyPattern("joinbus", rx(r"\bjoinbus\b"), "active code"),
    LegacyPattern("gentlejoin", literal("gentle" + "join"), "active code"),
    LegacyPattern("gentle-join", literal("gentle" + "-join"), "active code"),
    LegacyPattern("join-capable", literal("join" + "-capable"), "active code"),
    LegacyPattern("join_result", literal("join" + "_result"), "active code"),
    LegacyPattern("joiner_selection", literal("joiner" + "_selection"), "active code"),
    LegacyPattern("admission_path_selected", literal("admission" + "_path_selected"), "active code"),
    LegacyPattern(
        "startup_admission_consecutive_rejoin_failures",
        literal("startup" + "_admission_consecutive_rejoin_failures"),
        "active code",
    ),
]

PUBLIC_SURFACE_PATTERNS = [
    LegacyPattern("admission_path_selected", literal("admission" + "_path_selected"), "public surface"),
    LegacyPattern("startup_admission_degraded_total", literal("startup" + "_admission_degraded_total"), "public surface"),
    LegacyPattern("startup_admission_state", literal("startup" + "_admission_state"), "public surface"),
    LegacyPattern(
        "startup_admission_override_active",
        literal("startup" + "_admission_override_active"),
        "public surface",
    ),
    LegacyPattern(
        "startup_admission_warmup_events_seen",
        literal("startup" + "_admission_warmup_events_seen"),
        "public surface",
    ),
    LegacyPattern(
        "startup_admission_warmup_cycles_total",
        literal("startup" + "_admission_warmup_cycles_total"),
        "public surface",
    ),
    LegacyPattern(
        "startup_admission_override_bypass_total",
        literal("startup" + "_admission_override_bypass_total"),
        "public surface",
    ),
    LegacyPattern(
        "startup_admission_override_conflict_detected",
        literal("startup" + "_admission_override_conflict_detected"),
        "public surface",
    ),
    LegacyPattern(
        "startup_admission_degraded_escalated",
        literal("startup" + "_admission_degraded_escalated"),
        "public surface",
    ),
    LegacyPattern(
        "startup_admission_degraded_since_ms",
        literal("startup" + "_admission_degraded_since_ms"),
        "public surface",
    ),
    LegacyPattern(
        "startup_admission_degraded_cumulative_ms",
        literal("startup" + "_admission_degraded_cumulative_ms"),
        "public surface",
    ),
    LegacyPattern("busAdmission", literal("bus" + "Admission"), "public surface"),
    LegacyPattern("companionTarget", literal("companion" + "Target"), "public surface"),
    LegacyPattern("sourceSelection", literal("source" + "Selection"), "public surface"),
    LegacyPattern("explicitValidateOnly", literal("explicit" + "ValidateOnly"), "public surface"),
    LegacyPattern("gentlejoin", literal("gentle" + "join"), "public surface"),
    LegacyPattern("gentle-join", literal("gentle" + "-join"), "public surface"),
    LegacyPattern("join-capable", literal("join" + "-capable"), "public surface"),
    LegacyPattern("join_result", literal("join" + "_result"), "public surface"),
    LegacyPattern("joiner_selection", literal("joiner" + "_selection"), "public surface"),
    LegacyPattern("source-addr", rx(r"(?<!-)\bsource" + r"-addr\b"), "public surface"),
    LegacyPattern("-source-addr", literal("-source" + "-addr"), "public surface"),
    LegacyPattern("source_addr", rx(r"\bsource" + r"_addr\b"), "public surface"),
    LegacyPattern("source_addr_state_file", literal("source" + "_addr_state_file"), "public surface"),
]

GO_STRING_PUBLIC_PATTERNS = [
    LegacyPattern("busAdmission", literal("bus" + "Admission"), "Go public string"),
    LegacyPattern("companionTarget", literal("companion" + "Target"), "Go public string"),
    LegacyPattern("sourceSelection", literal("source" + "Selection"), "Go public string"),
    LegacyPattern("explicitValidateOnly", literal("explicit" + "ValidateOnly"), "Go public string"),
    LegacyPattern("admission_path_selected", literal("admission" + "_path_selected"), "Go public string"),
]

RETAINED_PUBLIC = {
    pathlib.Path("cmd/gateway/main.go"): {
        "source-addr": "locked plan keeps CLI -source-addr as explicit source config input; it no longer bypasses selection",
        "-source-addr": "locked plan keeps CLI -source-addr as explicit source config input; it no longer bypasses selection",
    },
    pathlib.Path("cmd/gateway/main_test.go"): {
        "source-addr": "CLI compatibility tests document retained explicit source config input",
        "-source-addr": "CLI compatibility tests document retained explicit source config input",
    },
    pathlib.Path("scripts/passive_matrix_case_ha.sh"): {
        "source-addr": "HA matrix script passes auto so current gateway selects/validates the source",
        "-source-addr": "HA matrix script passes auto so current gateway selects/validates the source",
    },
}


def retained_rationale(path: pathlib.Path, pattern: LegacyPattern) -> str | None:
    return RETAINED_PUBLIC.get(path, {}).get(pattern.name)


def tracked_files() -> list[pathlib.Path]:
    out = subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard", "--cached"], cwd=REPO_ROOT, text=True)
    return [pathlib.Path(line) for line in out.splitlines() if line]


def is_historical(path: pathlib.Path) -> bool:
    return any(path.parts[: len(prefix.parts)] == prefix.parts for prefix in HISTORICAL_ALLOWLIST_PREFIXES)


def should_scan_public(path: pathlib.Path) -> bool:
    if path in SELF_TEST_ALLOWLIST:
        return False
    if is_historical(path) and path.name != "TEMPLATE.yaml":
        return False
    if path.suffix in PUBLIC_SUFFIXES:
        return True
    return False


def should_scan_active_code(path: pathlib.Path) -> bool:
    if path in SELF_TEST_ALLOWLIST:
        return False
    if is_historical(path):
        return False
    if path.suffix in {".go", ".py", ".sh", ".graphql", ".json", ".md", ".yaml", ".yml"}:
        return True
    return False


def is_allowed_dot_join(line: str) -> bool:
    return any(
        allowed in line
        for allowed in (
            "filepath.Join(",
            "strings.Join(",
            "path.Join(",
            "errors.Join(",
        )
    )


def go_string_literals(text: str) -> list[tuple[int, str]]:
    literals: list[tuple[int, str]] = []
    i = 0
    line = 1
    while i < len(text):
        ch = text[i]
        if ch == "\n":
            line += 1
            i += 1
            continue
        if ch == "`":
            start_line = line
            i += 1
            start = i
            while i < len(text) and text[i] != "`":
                if text[i] == "\n":
                    line += 1
                i += 1
            literals.append((start_line, text[start:i]))
            if i < len(text):
                i += 1
            continue
        if ch == '"':
            start_line = line
            i += 1
            chars: list[str] = []
            while i < len(text):
                if text[i] == "\\" and i + 1 < len(text):
                    chars.append(text[i])
                    chars.append(text[i + 1])
                    i += 2
                    continue
                if text[i] == '"':
                    break
                if text[i] == "\n":
                    line += 1
                    break
                chars.append(text[i])
                i += 1
            literals.append((start_line, "".join(chars)))
            if i < len(text) and text[i] == '"':
                i += 1
            continue
        i += 1
    return literals


def main() -> int:
    failures: list[str] = []
    retained: list[str] = []
    for rel in tracked_files():
        if not (should_scan_active_code(rel) or should_scan_public(rel)):
            continue
        text = (REPO_ROOT / rel).read_text(encoding="utf-8", errors="ignore")
        for lineno, line in enumerate(text.splitlines(), start=1):
            if should_scan_active_code(rel):
                if ".Join(" in line and not is_allowed_dot_join(line):
                    failures.append(f"{rel}:{lineno}: legacy selector call .Join(...): {line.strip()}")
                for pattern in ACTIVE_CODE_PATTERNS:
                    if pattern.regex.search(line):
                        rationale = retained_rationale(rel, pattern)
                        if rationale is not None:
                            retained.append(f"{rel}:{lineno}: retained {pattern.name}: {rationale}")
                            continue
                        failures.append(
                            f"{rel}:{lineno}: legacy {pattern.scope} term {pattern.name}: {line.strip()}"
                        )

            if should_scan_public(rel):
                for pattern in PUBLIC_SURFACE_PATTERNS:
                    if pattern.regex.search(line):
                        rationale = retained_rationale(rel, pattern)
                        if rationale is not None:
                            retained.append(f"{rel}:{lineno}: retained {pattern.name}: {rationale}")
                            continue
                        failures.append(
                            f"{rel}:{lineno}: legacy {pattern.scope} term {pattern.name}: {line.strip()}"
                        )

        if rel.suffix == ".go":
            for literal_lineno, literal_text in go_string_literals(text):
                for pattern in GO_STRING_PUBLIC_PATTERNS + PUBLIC_SURFACE_PATTERNS:
                    if pattern.regex.search(literal_text):
                        rationale = retained_rationale(rel, pattern)
                        if rationale is not None:
                            retained.append(f"{rel}:{literal_lineno}: retained {pattern.name}: {rationale}")
                            continue
                        failures.append(
                            f"{rel}:{literal_lineno}: legacy {pattern.scope} term {pattern.name}: {literal_text.strip()}"
                        )

    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    if retained:
        print("Source-selection M4 retained legacy spellings:")
        for item in retained:
            print(f"  - {item}")
    print("Source-selection M4 public API gate passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
