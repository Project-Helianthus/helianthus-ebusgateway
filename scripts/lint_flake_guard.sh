#!/usr/bin/env bash
# Classify only the known SA5011 signature; never change the lint exit status.
# Usage: bash scripts/lint_flake_guard.sh LOG {pr CHANGED_FILES_NUL|push}
set -euo pipefail

python3 - "$@" <<'PY'
import os
import re
import sys
from pathlib import Path

if len(sys.argv) not in (3, 4) or sys.argv[2] not in ("pr", "push"):
    sys.exit("usage: lint_flake_guard.sh LOG {pr CHANGED_FILES_NUL|push}")

mode = sys.argv[2]
if (mode == "pr") != (len(sys.argv) == 4):
    sys.exit("PR classification requires a changed-file list; push takes none")

try:
    log = Path(sys.argv[1]).read_text()
    changed = set()
    if mode == "pr":
        raw = Path(sys.argv[3]).read_bytes()
        if raw and not raw.endswith(b"\0"):
            sys.exit("changed-file list must be NUL-delimited")
        changed = {os.fsdecode(path) for path in raw.split(b"\0") if path}
except (OSError, UnicodeError) as error:
    sys.exit(f"lint flake guard input unavailable: {error}")

# Both the action annotations and golangci-lint's text output are accepted.
# Source snippets are disabled by the workflow; unexpected output fails closed.
finding = re.compile(
    r"(?P<path>[^:\r\n]+):[1-9][0-9]*:[1-9][0-9]*: "
    r"(?:SA5011\(related information\): this check suggests that the pointer can be nil"
    r"|SA5011: possible nil pointer dereference) \(staticcheck\)"
)
count = 0
for line in log.splitlines():
    if not line.strip():
        continue
    if line.startswith("##[error]"):
        line = line[len("##[error]"):]
    if line == "issues found":
        continue
    match = finding.fullmatch(line)
    if match is None:
        sys.exit(1)
    path = match["path"]
    if path.startswith("./"):
        path = path[2:]
    # Absolute or non-canonical paths cannot be compared safely to git output.
    if path.startswith("/") or any(part in ("", ".", "..") for part in path.split("/")):
        sys.exit(1)
    if path in changed:
        sys.exit(1)
    count += 1

if not count:
    sys.exit(1)

message = "lint runner flake: staticcheck lost noreturn facts — rerun"
print(f"::warning::{message}")
if os.environ.get("GITHUB_STEP_SUMMARY"):
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as summary:
        summary.write(message + "\n")
PY
