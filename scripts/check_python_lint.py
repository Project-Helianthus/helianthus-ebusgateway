#!/usr/bin/env python3
"""Offline, exact-symbol Ruff debt ratchet; regenerate after removing debt."""
import argparse
import ast
from collections import Counter
import json
import os
from pathlib import Path
import re
import subprocess
import sys

VERSION = "0.15.10"
# .claude is the gitignored local agent directory; it can hold nested worktrees.
EXCLUDED = {".git", ".tmp", "__pycache__", ".venv", "venv", "node_modules", "vendor",
            ".claude"}
ROOT = Path(__file__).resolve().parents[1]


def inventory(root):
    paths = []
    for directory, dirs, files in os.walk(root):
        dirs[:] = sorted(name for name in dirs if name not in EXCLUDED)
        for name in dirs + files:
            if (Path(directory) / name).is_symlink():
                raise ValueError("source symlinks are not supported")
        paths.extend(Path(directory) / name for name in sorted(files) if name.endswith(".py"))
    return sorted(paths)


def symbol_at(tree, line):
    matches = []

    def visit(node, parents):
        named = isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
        names = parents + [node.name] if named else parents
        if named and node.lineno <= line <= node.end_lineno:
            matches.append(".".join(names))
        for child in ast.iter_child_nodes(node):
            visit(child, names)

    visit(tree, [])
    return matches[-1] if matches else "<module>"


def measure(root):
    paths = inventory(root)
    version = subprocess.run([sys.executable, "-m", "ruff", "--version"],
                             capture_output=True, text=True, check=True, timeout=30)
    if version.stdout.strip() != "ruff " + VERSION:
        raise ValueError("required Ruff version: " + VERSION)
    rows = []
    for path in paths:
        result = subprocess.run([sys.executable, "-m", "ruff", "check", "--no-cache",
                                 "--config", str(ROOT / "pyproject.toml"),
                                 "--select", "N,C90,PLR0915", "--output-format", "json",
                                 "--no-respect-gitignore", "--ignore-noqa", str(path)],
                                capture_output=True, text=True, timeout=30)
        if result.returncode not in (0, 1):
            raise ValueError(result.stderr)
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for item in json.loads(result.stdout):
            message = item["message"]
            measured = re.search(r"\((\d+) > (\d+)\)", message)
            rows.append({"path": path.relative_to(root).as_posix(), "rule": item["code"],
                         "symbol": symbol_at(tree, item["location"]["row"]),
                         "detail": re.sub(r"\(\d+ > \d+\)", "(measured)", message),
                         "value": int(measured[1]) if measured else None})
    return sorted(rows, key=lambda row: json.dumps(row, sort_keys=True))


def document(rows):
    return {"generator_version": 1, "ruff_version": VERSION,
            "thresholds": {"complexity": 10, "statements": 50},
            "regenerate": "python3 scripts/check_python_lint.py --generate", "violations": rows}


def compare(expected, actual):
    # Exact multisets catch growth, duplicate occurrences, reductions and stale debt.
    return Counter(json.dumps(row, sort_keys=True) for row in expected) == Counter(
        json.dumps(row, sort_keys=True) for row in actual)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generate", action="store_true")
    args = parser.parse_args()
    baseline = ROOT / "scripts/python-lint-baseline.json"
    current = document(measure(ROOT))
    if args.generate:
        baseline.write_text(json.dumps(current, indent=2) + "\n")
        return 0
    saved = json.loads(baseline.read_text())
    if saved != current:
        print("Python lint debt changed: fix new/growing debt or regenerate removed debt.")
        return 1
    print("Python lint baseline matches complete inventory.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(2)
