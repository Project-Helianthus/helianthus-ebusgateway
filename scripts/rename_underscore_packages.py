#!/usr/bin/env python3
"""Offline move of the gateway packages with underscores and adoption of the
library package renames.

Run without arguments to move the four gateway package directories (files are
moved, never copied), rewrite the package clauses, import paths, package
qualifiers and package references in comments of FILES, drop import names that
equal the new package name, gofmt the files, rewrite every string value of
docs/architecture/naming-inventory.json and docs/architecture/naming-consumers.json
and every path in TEXT_FILES that names a moved directory, and rebuild the
Portal assets with the repository's own build step.
Pass --require MODULE@VERSION (repeatable) to also re-pin a library with
`go mod edit` and an offline `go mod tidy` against the local module cache.

DIRECTORIES is the one table of moved directories: the four gateway moves and
the three library moves. A moved directory matches only as a whole path (a
path prefix or a path segment), so mcp/ebus_standard_wiring.go, the
ebus_standard MCP method strings, JSON keys and Go identifiers keep their
names. String, rune and raw-string literals of the Go sources are never
rewritten: error texts, MCP method strings and Portal API sections that start
with an old package name or name the eBUS standard service family stay
unchanged, and so do file names. A path cited together with an explicit
library revision ("ebusreg@<revision> catalog/...") keeps the path of that
revision.

--check lists any Go package directory, package clause, import path or
identifier that still uses an old name, and any tracked text file that still
names a moved directory outside the exact keep-list (KEEP_FILES, revision-cited
library paths and recorded `go test` result lines), and fails.
--verify-library checks every rewritten library path against the directories
of the pinned library modules (`go list -m`, offline) and lists the paths that
do not exist.
"""
import json
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULE_PREFIX = "github.com/Project-Helianthus/"
GATEWAY = "helianthus-ebusgateway"
EBUSREG = "helianthus-ebusreg"
EBUSGO = "helianthus-ebusgo"

# Every moved directory, relative to the root of the repository that owns it.
DIRECTORIES = {
    "internal/execution_policy": (GATEWAY, "internal/executionpolicy"),
    "internal/nm_runtime": (GATEWAY, "internal/nmruntime"),
    "internal/rpc_source": (GATEWAY, "internal/rpcsource"),
    "mcp/ebus_standard": (GATEWAY, "mcp/ebusstandard"),
    "catalog/ebus_standard": (EBUSREG, "catalog/ebusstandard"),
    "protocol/telegram_fsm": (EBUSGO, "protocol/telegramfsm"),
    "protocol/ebus_standard": (EBUSGO, "protocol/ebusstandard"),
}
MOVES = {old: new for old, (repository, new) in DIRECTORIES.items() if repository == GATEWAY}
IMPORTS = {
    MODULE_PREFIX + repository + "/" + old: MODULE_PREFIX + repository + "/" + new
    for old, (repository, new) in DIRECTORIES.items()
}
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
CONSUMERS = "docs/architecture/naming-consumers.json"
TEXT_FILES = [
    "matrix/M6a-transport-matrix.md",
    "portal/web/src/app.js",
    "portal/web/test/l7-catalog.test.mjs",
]
PORTAL_SOURCE = "portal/web/src/app.js"
# Files that name the old directories on purpose.
KEEP_FILES = frozenset({
    "CHANGELOG.md",
    "scripts/rename_underscore_packages.py",
    "tests/test_rename_underscore_packages.py",
})

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


DIRECTORY_END = r"(?![\w-]|\.\w)"
OLD_PATH = re.compile(r"(?<![\w.-])(" + "|".join(map(re.escape, DIRECTORIES)) + ")" + DIRECTORY_END)
NEW_LIBRARY_PATH = re.compile(
    r"(?<![\w.-])(" + "|".join(re.escape(new) for repository, new in DIRECTORIES.values()
                               if repository != GATEWAY) + ")" + DIRECTORY_END + r"[\w./-]*")
# A library path cited together with an explicit library revision describes the
# library at that revision, also when the comment wraps before the path.
REVISION_CITED = re.compile(
    r"(?<![\w.-])(?:helianthus-)?(?:ebusreg|ebusgo)@[0-9a-f]{7,40}\s+(?:(?://|#)[ \t]*)?[\w./-]+")
# Recorded `go test` result lines name the package path of the run they record.
GO_TEST_RESULT = re.compile(
    r"^(?:ok|FAIL|\?)[ \t]+" + re.escape(MODULE_PREFIX + GATEWAY) + r"/[\w./-]+", re.M)


def kept_spans(text):
    return [match.span() for pattern in (REVISION_CITED, GO_TEST_RESULT) for match in pattern.finditer(text)]


def stale_paths(text):
    spans = kept_spans(text)
    for match in OLD_PATH.finditer(text):
        if not any(start <= match.start() < end for start, end in spans):
            yield match


def moved_path(text):
    # A moved directory matches only as a whole path prefix or path segment, so
    # files such as mcp/ebus_standard_wiring.go keep their names.
    spans = {match.start() for match in stale_paths(text)}
    return OLD_PATH.sub(lambda match: DIRECTORIES[match[1]][1] if match.start() in spans else match[0], text)


def rewrite_paths(node):
    if isinstance(node, dict):
        return {key: rewrite_paths(value) for key, value in node.items()}
    if isinstance(node, list):
        return [rewrite_paths(value) for value in node]
    return moved_path(node) if isinstance(node, str) else node


def inventory_strings(node):
    if isinstance(node, dict):
        node = list(node.values())
    if isinstance(node, list):
        for value in node:
            yield from inventory_strings(value)
    elif isinstance(node, str):
        yield node


def update_inventory(root=ROOT):
    # Every string value of both documents that names a moved directory follows
    # the move; keys, key order and formatting stay. The sorted lists (the
    # inventory's source_files and the consumers' reference_source_files
    # listings) are re-sorted; every other list keeps its order, so the diff
    # carries only the path changes.
    path = root / INVENTORY
    inventory = rewrite_paths(json.loads(path.read_text()))
    inventory["source_files"].sort()
    path.write_text(json.dumps(inventory, indent=2) + "\n")
    path = root / CONSUMERS
    if path.exists():
        consumers = rewrite_paths(json.loads(path.read_text()))
        for listing in consumers.get("reference_source_files", {}).values():
            listing.sort()
        path.write_text(json.dumps(consumers, indent=2) + "\n")


def update_text_files(root=ROOT):
    changed = []
    for name in TEXT_FILES:
        path = root / name
        text = path.read_text()
        rewritten = moved_path(text)
        if rewritten != text:
            path.write_text(rewritten)
            changed.append(name)
    return changed


def build_portal_assets(root=ROOT):
    # portal/static/assets is generated from portal/web/src with its manifest.
    subprocess.run(["bash", "scripts/build_portal_assets.sh"], cwd=root, check=True)


def library_paths(root=ROOT):
    # Library paths in the rewritten documents, relative to the library root:
    # cut after the file segment, so identities such as
    # declaration/.../bcd.go/go_identifier/... name the file.
    texts = []
    for name in (INVENTORY, CONSUMERS):
        if (root / name).exists():
            texts.extend(inventory_strings(json.loads((root / name).read_text())))
    texts.extend((root / name).read_text() for name in TEXT_FILES if (root / name).exists())
    owners = {new: repository for repository, new in DIRECTORIES.values() if repository != GATEWAY}
    for text in texts:
        for match in NEW_LIBRARY_PATH.finditer(text):
            segments = match[0].rstrip("./").split("/")
            for index, segment in enumerate(segments[2:], 2):
                if "." in segment:
                    segments = segments[:index + 1]
                    break
            yield owners[match[1]], "/".join(segments)


def module_directory(repository):
    return Path(subprocess.run(
        ["go", "list", "-m", "-f", "{{.Dir}}", MODULE_PREFIX + repository], cwd=ROOT, env=GO_ENV,
        check=True, stdout=subprocess.PIPE, text=True).stdout.strip())


def verify_library(root=ROOT, directories=None):
    found = list(library_paths(root))
    directories = directories or {repository: module_directory(repository) for repository, _ in set(found)}
    missing = sorted({f"{repository}/{path}" for repository, path in found
                      if not (directories[repository] / path).exists()})
    unique = {repository: len({path for owner, path in found if owner == repository})
              for repository in sorted({repository for repository, _ in found})}
    return len(found), unique, missing


def repin(requirements):
    for requirement in requirements:
        subprocess.run(["go", "mod", "edit", "-require=" + requirement], cwd=ROOT, env=GO_ENV, check=True)
    subprocess.run(["go", "mod", "tidy"], cwd=ROOT, env=GO_ENV, check=True)


def go_sources(root=ROOT):
    for path in sorted(root.rglob("*.go")):
        parts = path.relative_to(root).parts
        if not any(part.startswith(".") or part == "node_modules" for part in parts):
            yield path


def text_files(root=ROOT):
    if (root / ".git").exists():
        listed = subprocess.run(["git", "-C", str(root), "ls-files", "-z"], check=True,
                                stdout=subprocess.PIPE).stdout.decode().split("\0")
        paths = [root / name for name in listed if name]
    else:
        paths = [path for path in sorted(root.rglob("*")) if path.is_file()
                 and not any(part.startswith(".") or part == "node_modules"
                             for part in path.relative_to(root).parts)]
    for path in paths:
        if not path.is_file():
            continue
        data = path.read_bytes()
        if b"\0" in data:
            continue
        try:
            yield path.relative_to(root).as_posix(), data.decode()
        except UnicodeDecodeError:
            continue


def remaining_go(root):
    for path in go_sources(root):
        relative = path.relative_to(root)
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


def remaining_paths(root):
    for name in (INVENTORY, CONSUMERS):
        if (root / name).exists():
            for text in inventory_strings(json.loads((root / name).read_text())):
                if any(stale_paths(text)):
                    yield f"{name}: moved path {text}"
    for name, text in text_files(root):
        if name in KEEP_FILES or name in (INVENTORY, CONSUMERS):
            continue
        for match in stale_paths(text):
            line = text.count("\n", 0, match.start()) + 1
            yield f"{name}:{line}: moved path {match[1]}"


def remaining(root=ROOT):
    yield from remaining_go(root)
    yield from remaining_paths(root)


def report_library(root=ROOT):
    count, unique, missing = verify_library(root)
    print(f"library paths checked: {count} ({', '.join(f'{name}: {number} distinct' for name, number in unique.items())})")
    print("\n".join("missing in the pinned library: " + path for path in missing)
          if missing else "every rewritten library path exists in the pinned library")
    return 1 if missing else 0


def main(argv):
    if argv == ["--check"]:
        found = list(remaining())
        print("\n".join(found) if found else "no old package path or name remains")
        return 1 if found else 0
    if argv == ["--verify-library"]:
        return report_library()
    requirements = []
    while argv:
        if len(argv) < 2 or argv[0] != "--require":
            raise SystemExit("usage: rename_underscore_packages.py "
                             "[--check | --verify-library | --require MODULE@VERSION ...]")
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
    if PORTAL_SOURCE in update_text_files():
        build_portal_assets()
    if requirements:
        repin(requirements)
    return report_library()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
