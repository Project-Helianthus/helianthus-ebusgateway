#!/usr/bin/env python3
"""Fail closed on native dependencies of the protocol-neutral semantic boundary.

Rules live here. The Go standard-library parser reads every source variant,
including tests and testdata, without loading modules or evaluating build tags.
Only filename suffixes distinguish tests; package names cannot exempt sources.
"""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

MODULE = "github.com/Project-Helianthus/helianthus-ebusgateway"
PROTECTED = (
    "internal/semhub",
    "internal/semanticmetrics",
    "internal/semanticoperations",
    "internal/semanticview",
)
# Exact local semantic owners; other local helpers are traversed but rejected
# until an explicit review adds an owner. Subpackages do not inherit admission.
ALLOWED_LOCAL = frozenset(f"{MODULE}/{name}" for name in PROTECTED)
# Existing synthetic operation fixtures are admitted only from test files; their
# production Go sources remain graph roots and receive the full import checks.
ALLOWED_TEST_LOCAL = frozenset({f"{MODULE}/internal/semanticoperations/semoptest"})
# SemReg owns protocol-neutral contracts. No other external owner is implicit.
ALLOWED_EXTERNAL = ("github.com/Project-Helianthus/helianthus-semreg",)
# Native libraries and all gateway implementation outside the semantic owners
# must stay behind the composition boundary. Unknown dependencies also fail.
FORBIDDEN_PREFIXES = tuple(
    f"github.com/Project-Helianthus/helianthus-{name}"
    for name in (
        "ebusgo", "ebusreg", "modbus", "modbusreg", "canbus", "canbusreg",
        "eebusreg", "eebus-go", "ship-go", "spine-go",
    )
) + tuple(f"{MODULE}/internal/{name}" for name in (
    "adaptermux", "vaillant", "modbusadapter", "canbusadapter", "eebusadapter",
    "transport",
))


def under(path, prefix):
    return path == prefix or path.startswith(prefix + "/")


def parse_sources(root):
    """Stage the non-shipped Go parser under scratch and use cached Go only."""
    scratch = Path(__file__).resolve().parents[1] / ".tmp"
    scratch.mkdir(exist_ok=True)
    env = dict(os.environ, GOWORK="off", GOPROXY="off", GOSUMDB="off",
               GOTOOLCHAIN="local", GOFLAGS="-buildvcs=false",
               TMPDIR=str(scratch), GOCACHE=str(scratch / "gocache"))
    with tempfile.TemporaryDirectory(prefix="semantic-imports-", dir=scratch) as work:
        module_file = root / "go.mod"
        if module_file.exists():
            module = subprocess.run(
                ["go", "mod", "edit", "-json", str(module_file)], cwd=work, env=env,
                capture_output=True, text=True, timeout=30, check=True,
            )
            for replacement in json.loads(module.stdout).get("Replace") or []:
                name = replacement["Old"]["Path"]
                if any(under(name, owner) for owner in ALLOWED_EXTERNAL):
                    raise ValueError(f"SemReg replacement is not allowed: {name}")
        parser = Path(work) / "parser.go"
        parser.write_bytes((Path(__file__).parent / "semantic_imports/parser.go.txt").read_bytes())
        completed = subprocess.run(
            ["go", "run", str(parser), str(root)], cwd=work, env=env,
            capture_output=True, text=True, timeout=150, check=True,
        )
    return json.loads(completed.stdout)


def violations(parsed):
    """Check both production closure and test closure, retaining import chains."""
    standard = set(parsed["Standard"])
    production, tests = {}, {}
    production_packages = set()
    for source in parsed["Sources"]:
        path = Path(source["File"])
        package = f"{MODULE}/{path.parent.as_posix()}"
        if not path.name.endswith("_test.go"):
            production_packages.add(package)
        target = tests if path.name.endswith("_test.go") else production
        target.setdefault(package, []).extend((imp, source["File"]) for imp in source["Imports"])
        # Include even packages without imports in the graph.
        production.setdefault(package, [])
    errors = set()
    for relative in PROTECTED:
        owner = f"{MODULE}/{relative}"
        roots = sorted(p for p in production if under(p, owner))
        if owner not in production_packages:
            errors.add(f"missing protected package: {owner}")
        for origin in roots:
            # Test imports are checked too, but cannot supply production edges.
            pending = [(origin, (origin,), True)]
            visited = set()
            while pending:
                package, chain, include_tests = pending.pop()
                if package in visited:
                    continue
                visited.add(package)
                edges = production[package] + (tests.get(package, []) if include_tests else [])
                for dependency, filename in edges:
                    route = " -> ".join((*chain, dependency))
                    if any(under(dependency, p) for p in FORBIDDEN_PREFIXES):
                        errors.add(f"{filename}: forbidden native dependency: {route}")
                    elif (dependency in ALLOWED_LOCAL or dependency in standard
                          or (filename.endswith("_test.go") and dependency in ALLOWED_TEST_LOCAL)):
                        pass
                    elif any(under(dependency, p) for p in ALLOWED_EXTERNAL):
                        pass
                    else:
                        errors.add(f"{filename}: dependency requires reviewed rule: {route}")
                    if under(dependency, MODULE):
                        if dependency not in production_packages:
                            errors.add(f"{filename}: unresolved local dependency: {route}")
                        else:
                            pending.append((dependency, (*chain, dependency), False))
    return sorted(errors)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    try:
        parsed = parse_sources(args.root.resolve())
        errors = violations(parsed)
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        print(f"semantic import check failed: {error}", file=sys.stderr)
        if isinstance(error, subprocess.CalledProcessError):
            print(error.stderr, file=sys.stderr)
        return 1
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"Semantic import boundary OK: {len(PROTECTED)} owners; all source variants and tests checked")
    return 0


if __name__ == "__main__":
    sys.exit(main())
