#!/usr/bin/env python3
"""Offline move of the gateway packages with underscores and adoption of the
library package renames.

Run without arguments to move the four gateway package directories (files are
moved, never copied), rewrite the package clauses, import paths, package
qualifiers and package references in comments of FILES, drop import names that
equal the new package name, gofmt the files and update
the moved paths in the source_files list of docs/architecture/naming-inventory.json.
Pass --require MODULE@VERSION (repeatable) to also re-pin a library with
`go mod edit` and an offline `go mod tidy` against the local module cache.

String, rune and raw-string literals are never rewritten: error texts, MCP
method strings and Portal API sections that start with an old package name or
name the eBUS standard service family stay unchanged, and so do file names.
Comment references to a historical revision of the ebusreg catalog keep the
path of that revision.

--check lists any Go package directory, package clause, import path or
identifier in the repository's Go sources that still uses an old name and fails.
"""
import json
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GATEWAY = "github.com/Project-Helianthus/helianthus-ebusgateway/"
EBUSREG = "github.com/Project-Helianthus/helianthus-ebusreg/"
EBUSGO = "github.com/Project-Helianthus/helianthus-ebusgo/"

MOVES = {
    "internal/execution_policy": "internal/executionpolicy",
    "internal/nm_runtime": "internal/nmruntime",
    "internal/rpc_source": "internal/rpcsource",
    "mcp/ebus_standard": "mcp/ebusstandard",
}
IMPORTS = {GATEWAY + old: GATEWAY + new for old, new in MOVES.items()}
IMPORTS.update({
    EBUSREG + "catalog/ebus_standard": EBUSREG + "catalog/ebusstandard",
    EBUSGO + "protocol/telegram_fsm": EBUSGO + "protocol/telegramfsm",
    EBUSGO + "protocol/ebus_standard": EBUSGO + "protocol/ebusstandard",
})
PACKAGES = {
    "execution_policy": "executionpolicy",
    "nm_runtime": "nmruntime",
    "rpc_source": "rpcsource",
    "ebus_standard": "ebusstandard",
    "telegram_fsm": "telegramfsm",
}
IDENTIFIERS = dict(PACKAGES)
IDENTIFIERS.update({old + "_test": new + "_test" for old, new in PACKAGES.items()})

# Files that import both the gateway MCP package and the ebusreg catalog, now
# both named ebusstandard, use import names that state the owner.
IMPORT_NAMES = {
    "mcp/ebus_standard_wiring.go": {"estd": "mcpstandard", "ebusstd": "ebusregstandard"},
    "mcp/ebus_standard_wiring_test.go": {"estd": "mcpstandard", "ebusstd": "ebusregstandard"},
    "mcp/ebusstandard/server_test.go": {"estd": "mcpstandard", "ebusstd": "ebusregstandard"},
}

COMMENT_RULES = [
    (re.compile(r"see telegram_fsm\.go\."), "see protocol/telegramfsm/fsm.go."),
    (re.compile(r"helianthus-ebusreg/catalog/ebus_standard(?!\w)"),
     "helianthus-ebusreg/catalog/ebusstandard"),
    (re.compile(r"(?<![\w/])mcp/ebus_standard(?!\w)"), "mcp/ebusstandard"),
    (re.compile(r"\bPackage ebus_standard\b"), "Package ebusstandard"),
    (re.compile(r"(?<![\w-])(execution_policy|nm_runtime|rpc_source|telegram_fsm)(?!\w)"),
     lambda match: PACKAGES[match[1]]),
]

FILES = [
    "cmd/gateway/gateway_http_server.go",
    "cmd/gateway/gateway_responder_capability.go",
    "cmd/gateway/wiring_test.go",
    "internal/adaptermux/arbitration_recovery.go",
    "internal/adaptermux/issue1402_exit_test.go",
    "internal/adaptermux/live_telegram_fence_test.go",
    "internal/adaptermux/v8classifier/admin_event.go",
    "internal/adaptermux/v8classifier/admin_event_aa_drop_test.go",
    "internal/adaptermux/v8classifier/admin_event_test.go",
    "internal/adaptermux/v8classifier/classifier.go",
    "internal/adaptermux/v8classifier/classifier_fsm_test.go",
    "internal/adaptermux/v8classifier_wiring_test.go",
    "internal/executionpolicy/policy.go",
    "internal/executionpolicy/policy_test.go",
    "internal/executionpolicy/whitelist.go",
    "internal/executionpolicy/whitelist_catalog_integration_test.go",
    "internal/nmruntime/nm_runtime.go",
    "internal/nmruntime/nm_runtime_catalog_integration_test.go",
    "internal/nmruntime/nm_runtime_test.go",
    "internal/nmruntime/responder_runtime.go",
    "internal/nmruntime/responder_runtime_test.go",
    "internal/rpcsource/source.go",
    "internal/rpcsource/source_test.go",
    "mcp/ebus_standard_wiring.go",
    "mcp/ebus_standard_wiring_test.go",
    "mcp/ebusstandard/envelope.go",
    "mcp/ebusstandard/envelope_test.go",
    "mcp/ebusstandard/forward_compat_test.go",
    "mcp/ebusstandard/forward_compat_unknown_active_transport_test.go",
    "mcp/ebusstandard/golden_catalog_test.go",
    "mcp/ebusstandard/golden_test.go",
    "mcp/ebusstandard/responder_capability.go",
    "mcp/ebusstandard/responder_capability_test.go",
    "mcp/ebusstandard/server.go",
    "mcp/ebusstandard/server_test.go",
    "mcp/rpc_source_guard.go",
    "mcp/rpc_source_guard_test.go",
    "mcp/server.go",
    "mcp/server_construction.go",
    "mcp/server_test.go",
    "mcp/tool_classification_test.go",
    "portal/explorer_ebus_standard.go",
    "portal/explorer_ebus_standard_test.go",
]
INVENTORY = "docs/architecture/naming-inventory.json"

TOKEN = re.compile(r"//[^\n]*|/\*[\s\S]*?\*/|\"(?:\\.|[^\"\\\n])*\"|`[^`]*`|'(?:\\.|[^'\\\n])*'|\b[A-Za-z_]\w*\b")
IMPORT_SPEC = re.compile(r"^(\t)(\w+) (\"([^\"]+)\")$", re.M)
GO_ENV = dict(os.environ, GOWORK="off", GOPROXY="off", GOFLAGS="-mod=mod", GOTOOLCHAIN="local")


def drop_redundant_import_name(match):
    # An import name equal to the renamed package's own name is dropped.
    if match[4] in IMPORTS.values() and match[4].rsplit("/", 1)[1] == match[2]:
        return match[1] + match[3]
    return match[0]


def rewrite(source, names):
    def token(match):
        text = match[0]
        if text.startswith(("//", "/*")):
            for pattern, replacement in COMMENT_RULES:
                text = pattern.sub(replacement, text)
            return text
        if text[0] == '"':
            return '"' + IMPORTS[text[1:-1]] + '"' if text[1:-1] in IMPORTS else text
        if text[0] in "'`":
            return text
        return names.get(text, IDENTIFIERS.get(text, text))

    return IMPORT_SPEC.sub(drop_redundant_import_name, TOKEN.sub(token, source))


def move():
    for old, new in MOVES.items():
        source, target = ROOT / old, ROOT / new
        if target.exists():
            raise SystemExit(f"{new} already exists")
        source.rename(target)


def update_inventory():
    path = ROOT / INVENTORY
    lines = path.read_text().split("\n")
    start = lines.index('  "source_files": [')
    end = lines.index("  ],", start)
    entries = [json.loads(line.strip().rstrip(",")) for line in lines[start + 1:end]]
    moved = []
    for entry in entries:
        for old, new in MOVES.items():
            if entry.startswith(old + "/"):
                entry = new + entry[len(old):]
        moved.append(entry)
    moved.sort()
    body = ["    " + json.dumps(entry) + "," for entry in moved]
    body[-1] = body[-1][:-1]
    lines[start + 1:end] = body
    path.write_text("\n".join(lines))


def repin(requirements):
    for requirement in requirements:
        subprocess.run(["go", "mod", "edit", "-require=" + requirement], cwd=ROOT, env=GO_ENV, check=True)
    subprocess.run(["go", "mod", "tidy"], cwd=ROOT, env=GO_ENV, check=True)


def go_sources():
    for path in sorted(ROOT.rglob("*.go")):
        parts = path.relative_to(ROOT).parts
        if not any(part.startswith(".") or part == "node_modules" for part in parts):
            yield path


def remaining():
    for path in go_sources():
        relative = path.relative_to(ROOT)
        if "_" in relative.parent.as_posix() and "testdata" not in relative.parts:
            yield f"{relative}: package directory with an underscore"
        source = path.read_text()
        for match in TOKEN.finditer(source):
            text = match[0]
            line = source.count("\n", 0, match.start()) + 1
            if text[0] == '"' and text[1:-1] in IMPORTS:
                yield f"{relative}:{line}: import {text}"
            elif text in IDENTIFIERS:
                yield f"{relative}:{line}: {text}"


def main(argv):
    if argv == ["--check"]:
        found = list(remaining())
        print("\n".join(found) if found else "no old package path or name remains")
        return 1 if found else 0
    requirements = []
    while argv:
        if len(argv) < 2 or argv[0] != "--require":
            raise SystemExit("usage: rename_underscore_packages.py [--check | --require MODULE@VERSION ...]")
        requirements.append(argv[1])
        argv = argv[2:]
    move()
    for name in FILES:
        path = ROOT / name
        source = rewrite(path.read_text(), IMPORT_NAMES.get(name, {}))
        formatted = subprocess.run(["gofmt"], input=source, text=True, check=True,
                                   stdout=subprocess.PIPE).stdout
        path.write_text(formatted)
    update_inventory()
    if requirements:
        repin(requirements)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
