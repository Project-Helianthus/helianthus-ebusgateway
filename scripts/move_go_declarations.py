#!/usr/bin/env python3
"""Move top-level Go declarations between the files of one package.

The script is offline and uses the standard library only (plus the local
``gofmt`` binary to format the files it writes).

Modes:

``--manifest FILE``
    Apply the manifest: every listed declaration moves, with its leading
    comments, into the file that lists it. Each written file gets the imports
    its code uses; imports a file no longer uses are dropped. ``gofmt`` runs on
    every written file.

``--manifest FILE --check``
    Verify the current tree against the manifest: every listed declaration
    resolves to exactly one top-level declaration, located in the file that
    lists it; no declaration of the listed files is unlisted; no ``init``
    function and no ``var`` with a call initializer is listed outside the
    manifest's source file.

``--compare BEFORE_DIR AFTER_DIR``
    Compare the normalized multiset of top-level declarations (imports
    excluded) of the non-test Go files of two directories.

``--report FILE...``
    Print lines and function count per file and every function longer than
    the function budget. A non-test file over the file budgets fails.

Manifest format::

    {
      "package_dir": "cmd/gateway",
      "source": "big.go",
      "files": {"big.go": ["type:poller", ...], "small.go": ["func:helper", ...]}
    }

Declaration keys are ``func:Name``, ``method:Receiver.Name``, ``type:Name``,
``var:Name`` and ``const:Name``; a grouped declaration is keyed by its first
name. Optional ``import_names`` maps an import path to its package name when
the name differs from the last path element.
"""

from __future__ import annotations

import argparse
import bisect
import collections
import dataclasses
import json
import pathlib
import re
import shutil
import subprocess
import sys

MAX_FILE_LINES = 1000
MAX_FILE_FUNCTIONS = 60
MAX_FUNCTION_LINES = 150
KEYWORDS = ("func", "type", "var", "const", "import")
CONTINUATION = (",", "=", "+", "-", "*", "/", "|", "&", "^", "%", "<", ">", "!",
                "(", "[", "{", ".", ":")
CONVERSIONS = {
    "bool", "byte", "rune", "string", "int", "int8", "int16", "int32", "int64",
    "uint", "uint8", "uint16", "uint32", "uint64", "uintptr", "float32",
    "float64", "complex64", "complex128", "func",
}
IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
CALL = re.compile(r"(?:([A-Za-z_][A-Za-z0-9_]*)|[\])}])\s*\(")
SELECTOR = re.compile(r"(?<![A-Za-z0-9_.])([A-Za-z_][A-Za-z0-9_]*)\s*\.")
IMPORT_SPEC = re.compile(r'^\s*(?:([A-Za-z_][A-Za-z0-9_]*|\.)\s+)?"([^"]+)"')


class MoveError(Exception):
    """A manifest, source or comparison violation."""


@dataclasses.dataclass
class Unit:
    """One top-level declaration with the comments that precede it."""

    kind: str
    key: str
    lead: list[str]
    lines: list[str]
    masked: str
    start: int

    def text(self) -> str:
        return "\n".join(self.lead + self.lines)

    def is_init(self) -> bool:
        return self.key.startswith("func:init#")

    def has_call_initializer(self, shadowed=frozenset()) -> bool:
        return self.kind == "var" and initializer_calls(self.masked, shadowed)


@dataclasses.dataclass
class GoFile:
    """A parsed Go file: header, import units, declaration units, trailer."""

    path: pathlib.Path
    header: list[str]
    imports: list[Unit]
    units: list[Unit]
    trailer: list[str]


# ---------------------------------------------------------------- lexing


def _skip_quoted(text: str, index: int, quote: str) -> int:
    index += 1
    while index < len(text) and text[index] != quote and text[index] != "\n":
        index += 2 if text[index] == "\\" else 1
    return index + 1


def _literal_end(text: str, index: int) -> int:
    """Return the end of the comment or literal that starts at index, or -1."""
    pair = text[index:index + 2]
    char = text[index]
    if pair == "//":
        end = text.find("\n", index)
        return len(text) if end < 0 else end
    if pair == "/*":
        end = text.find("*/", index + 2)
        return len(text) if end < 0 else end + 2
    if char == "`":
        end = text.find("`", index + 1)
        return len(text) if end < 0 else end + 1
    if char in "\"'":
        return min(_skip_quoted(text, index, char), len(text))
    return -1


def mask(text: str) -> str:
    """Blank comments and fill string/rune literals with zeros.

    Newlines and offsets are kept; a filled literal still ends a line as an
    operand, so a declaration ending in a string is not taken as continued.
    """
    out = []
    index = 0
    while index < len(text):
        end = _literal_end(text, index)
        if end < 0:
            out.append(text[index])
            index += 1
            continue
        fill = " " if text[index] == "/" else "0"
        out.append("".join("\n" if c == "\n" else fill for c in text[index:end]))
        index = end
    return "".join(out)


def literal_line_starts(text: str) -> set[int]:
    """Return the line numbers (0-based) that start inside a comment or literal."""
    newlines = [i for i, c in enumerate(text) if c == "\n"]
    inside = set()
    index = 0
    while index < len(text):
        end = _literal_end(text, index)
        if end < 0:
            index += 1
            continue
        first = bisect.bisect_left(newlines, index)
        last = bisect.bisect_left(newlines, end)
        inside.update(range(first + 1, last + 1))
        index = end
    return inside


def depth_change(masked_line: str) -> int:
    opened = sum(masked_line.count(c) for c in "([{")
    return opened - sum(masked_line.count(c) for c in ")]}")


# ---------------------------------------------------------------- parsing


def _group_first_name(masked: str) -> str:
    body = masked[masked.index("(") + 1:]
    match = IDENT.search(body)
    if not match:
        raise MoveError("empty declaration group: " + masked.splitlines()[0])
    return match.group(0)


def _func_key(masked: str) -> str:
    match = re.match(r"func\s*(?:\(([^)]*)\))?\s*([A-Za-z_][A-Za-z0-9_]*)", masked)
    if not match:
        raise MoveError("unparsable function: " + masked.splitlines()[0])
    if match.group(1) is None:
        return "func:" + match.group(2)
    receiver = match.group(1).split()[-1].lstrip("*")
    receiver = receiver.split("[", 1)[0]
    return "method:" + receiver + "." + match.group(2)


def declaration_key(kind: str, masked: str) -> str:
    if kind == "func":
        return _func_key(masked)
    rest = masked[len(kind):].lstrip()
    if rest.startswith("("):
        return kind + ":" + _group_first_name(masked)
    match = IDENT.match(rest)
    if not match:
        raise MoveError("unparsable declaration: " + masked.splitlines()[0])
    return kind + ":" + match.group(0)


def initializer_calls(masked: str, shadowed=frozenset()) -> bool:
    """Report whether a var declaration's initializers contain a call.

    Everything after the first initializer is scanned, so a call nested in a
    multi-line composite literal counts; conversions to predeclared types and
    function types do not. A predeclared type name that the package redeclares
    as a function or variable is a call, not a conversion.
    """
    assign = re.search(r"(?<![=!<>:])=(?!=)", masked)
    if not assign:
        return False
    for match in CALL.finditer(masked[assign.end():]):
        if match.group(1) not in CONVERSIONS or match.group(1) in shadowed:
            return True
    return False


_NAME_LIST = re.compile(r"\s*([A-Za-z_][A-Za-z0-9_]*(?:\s*,\s*[A-Za-z_][A-Za-z0-9_]*)*)")


def _ends_statement(masked: str, index: int) -> bool:
    """Report whether Go inserts a semicolon at the newline at index."""
    head = masked[:index].rstrip()
    return bool(head) and bool(_SEMICOLON_END.search(head))


def _group_spec_names(body: str) -> list[str]:
    """Return the names each spec of a parenthesized group declares."""
    names = []
    depth = 0
    at_start = True
    index = 0
    while index < len(body):
        if depth == 0 and at_start:
            match = _NAME_LIST.match(body, index)
            if match:
                names.extend(IDENT.findall(match.group(1)))
                index = match.end()
                at_start = False
                continue
        char = body[index]
        if char in "([{":
            depth += 1
        elif char in ")]}":
            depth -= 1
            if depth < 0:
                break
        elif depth == 0 and (char == ";" or (char == "\n" and _ends_statement(body, index))):
            at_start = True
        index += 1
    return names


def declared_names(unit: Unit) -> list[str]:
    """Return the package-level names a declaration unit declares.

    Every spec of a grouped declaration counts, not only the first one that
    keys the unit. Methods declare no package-level name.
    """
    if unit.kind == "func":
        return [unit.key.split(":", 1)[1]] if unit.key.startswith("func:") else []
    rest = unit.masked[len(unit.kind):].lstrip()
    if rest.startswith("("):
        return _group_spec_names(rest[1:])
    match = _NAME_LIST.match(rest)
    return IDENT.findall(match.group(1)) if match else []


def _keyword_at(line: str) -> str:
    for keyword in KEYWORDS:
        if line.startswith(keyword) and not IDENT.match(line[len(keyword):len(keyword) + 1] or " "):
            return keyword
    return ""


def _declaration_end(lines, masked_lines, literal_starts, start):
    depth = 0
    index = start
    while index < len(lines):
        depth += depth_change(masked_lines[index])
        ends_open = (index + 1) in literal_starts
        tail = masked_lines[index].rstrip()
        if depth <= 0 and not ends_open and not tail.endswith(CONTINUATION):
            return index
        index += 1
    raise MoveError("unterminated declaration at line %d" % (start + 1))


class _Parser:
    def __init__(self, path: pathlib.Path, text: str):
        self.path = path
        self.lines = text.split("\n")
        if self.lines and self.lines[-1] == "":
            self.lines.pop()
        self.masked = mask(text).split("\n")
        self.literal_starts = literal_line_starts(text)
        self.counts = collections.Counter()

    def _header_end(self) -> int:
        for index, line in enumerate(self.lines):
            if index not in self.literal_starts and self.masked[index].startswith("package "):
                return index
        raise MoveError("%s: no package clause" % self.path)

    def _unit(self, kind, lead, start, end) -> Unit:
        masked = "\n".join(self.masked[start:end + 1])
        key = declaration_key(kind, masked) if kind != "import" else "import"
        if key == "func:init":
            self.counts[key] += 1
            key = "func:init#%d" % self.counts[key]
        return Unit(kind, key, lead, self.lines[start:end + 1], masked, start)

    def parse(self) -> GoFile:
        header_end = self._header_end()
        result = GoFile(self.path, self.lines[:header_end + 1], [], [], [])
        pending: list[str] = []
        index = header_end + 1
        while index < len(self.lines):
            line = self.lines[index]
            keyword = "" if index in self.literal_starts else _keyword_at(line)
            if not keyword:
                if self.masked[index].strip() and index not in self.literal_starts:
                    raise MoveError("%s:%d: unexpected top-level text" % (self.path, index + 1))
                pending.append(line)
                index += 1
                continue
            end = _declaration_end(self.lines, self.masked, self.literal_starts, index)
            unit = self._unit(keyword, _trim_blank(pending), index, end)
            (result.imports if keyword == "import" else result.units).append(unit)
            pending = []
            index = end + 1
        result.trailer = _trim_blank(pending)
        return result


def _trim_blank(lines: list[str]) -> list[str]:
    start, end = 0, len(lines)
    while start < end and not lines[start].strip():
        start += 1
    while end > start and not lines[end - 1].strip():
        end -= 1
    return lines[start:end]


def parse_file(path: pathlib.Path) -> GoFile:
    return _Parser(path, path.read_text(encoding="utf-8")).parse()


# ---------------------------------------------------------------- imports


@dataclasses.dataclass(frozen=True)
class ImportSpec:
    name: str
    path: str
    explicit: bool

    def render(self) -> str:
        if self.explicit:
            return '%s "%s"' % (self.name, self.path)
        return '"%s"' % self.path

    def is_std(self) -> bool:
        return "." not in self.path.split("/", 1)[0]


def default_package_name(path: str, overrides: dict) -> str:
    if path in overrides:
        return overrides[path]
    parts = path.split("/")
    last = parts[-1]
    if re.fullmatch(r"v[0-9]+", last) and len(parts) > 1:
        last = parts[-2]
    last = re.sub(r"\.v[0-9]+$", "", last)
    last = re.sub(r"^go-|-go$", "", last)
    return last.rsplit("-", 1)[-1].replace(".", "_")


def import_specs(go_file: GoFile, overrides: dict) -> list[ImportSpec]:
    specs = []
    for unit in go_file.imports:
        for line in unit.lines:
            match = IMPORT_SPEC.match(line[len("import"):] if line.startswith("import") else line)
            if match:
                name = match.group(1) or default_package_name(match.group(2), overrides)
                specs.append(ImportSpec(name, match.group(2), bool(match.group(1))))
    return specs


def used_names(units: list[Unit]) -> set[str]:
    names = set()
    for unit in units:
        names.update(m.group(1) for m in SELECTOR.finditer(unit.masked))
    return names


def render_imports(specs: list[ImportSpec]) -> list[str]:
    if not specs:
        return []
    groups = [[s for s in specs if s.is_std()], [s for s in specs if not s.is_std()]]
    lines = ["import ("]
    for group in (g for g in groups if g):
        if len(lines) > 1:
            lines.append("")
        lines.extend("\t" + spec.render() for spec in sorted(group, key=lambda s: s.path))
    lines.append(")")
    return lines


def required_imports(go_file: GoFile, units: list[Unit], pool: dict) -> list[ImportSpec]:
    names = used_names(units)
    kept = [s for s in pool.values() if s.name in names]
    own = import_specs(go_file, {}) if go_file else []
    kept.extend(s for s in own if s.name in ("_", ".") and s not in kept)
    return kept


# ---------------------------------------------------------------- manifest


@dataclasses.dataclass
class Plan:
    package_dir: pathlib.Path
    source: str
    targets: dict
    overrides: dict
    parsed: dict


def load_plan(manifest_path: pathlib.Path, root: pathlib.Path) -> Plan:
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    package_dir = root / data["package_dir"]
    targets = {name: list(keys) for name, keys in data["files"].items()}
    if data["source"] not in targets:
        raise MoveError("the source file must list the declarations it keeps")
    parsed = {}
    for name in targets:
        path = package_dir / name
        if path.exists():
            parsed[name] = parse_file(path)
    return Plan(package_dir, data["source"], targets, data.get("import_names", {}), parsed)


def _locate(plan: Plan) -> dict:
    located = collections.defaultdict(list)
    for name, go_file in plan.parsed.items():
        for unit in go_file.units:
            located[unit.key].append((name, unit))
    return located


def _listing_errors(plan: Plan, located: dict) -> list[str]:
    errors = []
    listed = collections.Counter(k for keys in plan.targets.values() for k in keys)
    errors.extend("listed more than once: %s" % k for k, n in sorted(listed.items()) if n > 1)
    for key in sorted(listed):
        if len(located.get(key, [])) != 1:
            errors.append("%s resolves to %d declarations" % (key, len(located.get(key, []))))
    errors.extend("unlisted declaration: %s" % k for k in sorted(located) if k not in listed)
    return errors


def _pinned_errors(plan: Plan, located: dict) -> list[str]:
    errors = []
    shadowed = frozenset(
        name for entries in located.values() for _, unit in entries
        if unit.kind in ("func", "var", "const")
        for name in declared_names(unit) if name in CONVERSIONS)
    for name, keys in plan.targets.items():
        for key in keys:
            units = [u for _, u in located.get(key, [])]
            pinned = any(u.is_init() or u.has_call_initializer(shadowed) for u in units)
            if pinned and name != plan.source:
                errors.append("%s must stay in %s (init or call initializer)" % (key, plan.source))
    return errors


def validate(plan: Plan, require_placed: bool) -> list[str]:
    located = _locate(plan)
    errors = _listing_errors(plan, located) + _pinned_errors(plan, located)
    if require_placed:
        for name, keys in plan.targets.items():
            errors.extend("%s is not in %s" % (key, name) for key in keys
                          if len(located.get(key, [])) == 1 and located[key][0][0] != name)
    return errors


def _import_pool(plan: Plan) -> dict:
    pool = {}
    for go_file in plan.parsed.values():
        for spec in import_specs(go_file, plan.overrides):
            if spec.name in ("_", "."):
                continue
            previous = pool.get(spec.name)
            if previous and previous.path != spec.path:
                raise MoveError("import name %s maps to two paths" % spec.name)
            pool[spec.name] = spec
    return pool


def _new_header(plan: Plan) -> list[str]:
    source = plan.parsed[plan.source]
    constraints = [line for line in source.header if line.startswith("//go:build")]
    package = source.header[-1]
    return constraints + ([""] if constraints else []) + [package]


def render_file(header, imports, units, trailer) -> str:
    blocks = ["\n".join(header)]
    if imports:
        blocks.append("\n".join(imports))
    blocks.extend(unit.text() for unit in units)
    if trailer:
        blocks.append("\n".join(trailer))
    return "\n\n".join(blocks) + "\n"


def _unchanged_import_text(go_file: GoFile, specs: list[ImportSpec], overrides: dict):
    if go_file is None or not go_file.imports:
        return None
    if set(import_specs(go_file, overrides)) != set(specs):
        return None
    return [line for unit in go_file.imports for line in unit.lead + unit.lines]


def plan_contents(plan: Plan) -> dict:
    located = _locate(plan)
    pool = _import_pool(plan)
    order = list(plan.targets)
    contents = {}
    for name, keys in plan.targets.items():
        units = [located[key][0] for key in keys]
        # A file's own declarations stay first; moved ones follow by origin.
        units.sort(key=lambda item, own=name: (item[0] != own, order.index(item[0]),
                                                 item[1].start))
        units = [unit for _, unit in units]
        existing = plan.parsed.get(name)
        specs = required_imports(existing, units, pool)
        imports = _unchanged_import_text(existing, specs, plan.overrides) or render_imports(specs)
        header = existing.header if existing else _new_header(plan)
        trailer = existing.trailer if existing else []
        contents[name] = render_file(header, imports, units, trailer)
    return contents


def gofmt_binary() -> str:
    found = shutil.which("gofmt")
    if found:
        return found
    go = shutil.which("go")
    if go:
        root = subprocess.run([go, "env", "GOROOT"], capture_output=True, text=True,
                              check=True, timeout=60).stdout.strip()
        candidate = pathlib.Path(root) / "bin" / "gofmt"
        if candidate.exists():
            return str(candidate)
    raise MoveError("gofmt not found")


def apply_manifest(manifest_path: pathlib.Path, root: pathlib.Path) -> list[pathlib.Path]:
    plan = load_plan(manifest_path, root)
    errors = validate(plan, require_placed=False)
    if errors:
        raise MoveError("\n".join(errors))
    written = []
    for name, text in plan_contents(plan).items():
        path = plan.package_dir / name
        if not path.exists() or path.read_text(encoding="utf-8") != text:
            path.write_text(text, encoding="utf-8")
            written.append(path)
    if written:
        subprocess.run([gofmt_binary(), "-w"] + [str(p) for p in written], check=True, timeout=300)
    return written


def check_manifest(manifest_path: pathlib.Path, root: pathlib.Path) -> list[str]:
    return validate(load_plan(manifest_path, root), require_placed=True)


# ---------------------------------------------------------------- compare


_SEMICOLON_END = re.compile(r"(?:[A-Za-z0-9_)\]}\"'`]|\+\+|--)$")


def _separator(last_code: str, newline: bool) -> str:
    if newline and _SEMICOLON_END.search(last_code):
        return "\n"
    return " "


def normalize(text: str) -> str:
    """Collapse whitespace outside string and rune literals; keep literal text.

    Comments are kept with their inner whitespace collapsed, so only
    formatting whitespace is ignored. A newline that follows a token which
    makes Go insert a semicolon is kept, because it separates statements.
    Comments are transparent to that rule, as in Go: the token before a
    comment decides, and a line comment or a block comment spanning lines
    acts as a newline.
    """
    out = []
    last_code = ""
    pending = False
    newline = False
    index = 0
    while index < len(text):
        char = text[index]
        end = _literal_end(text, index)
        if end >= 0 or not char.isspace():
            separated = pending and bool(out)
            if separated:
                out.append(_separator(last_code, newline))
                newline = newline and out[-1] == " "
            pending = False
            if end >= 0 and char == "/":
                comment = text[index:end]
                out.append(re.sub(r"\s+", " ", comment))
                pending = True
                newline = newline or comment.startswith("//") or "\n" in comment
                index = end
                continue
            newline = False
            token = text[index:end] if end >= 0 else char
            out.append(token)
            # Two adjacent characters are kept so "++" and "--" are seen.
            last_code = token if separated else (last_code + token)[-2:]
            index = end if end >= 0 else index + 1
            continue
        pending = True
        newline = newline or char == "\n"
        index += 1
    return "".join(out)


def declaration_multiset(directory: pathlib.Path) -> collections.Counter:
    counter = collections.Counter()
    for path in sorted(directory.glob("*.go")):
        if path.name.endswith("_test.go"):
            continue
        for unit in parse_file(path).units:
            counter[normalize(unit.text())] += 1
    return counter


def compare_dirs(before: pathlib.Path, after: pathlib.Path) -> list[str]:
    left = declaration_multiset(before)
    right = declaration_multiset(after)
    problems = []
    for text, count in sorted((left - right).items()):
        problems.append("missing after (%d): %s" % (count, text[:160]))
    for text, count in sorted((right - left).items()):
        problems.append("added after (%d): %s" % (count, text[:160]))
    return problems


# ---------------------------------------------------------------- report


def file_report(path: pathlib.Path) -> dict:
    go_file = parse_file(path)
    lines = len(path.read_text(encoding="utf-8").splitlines())
    functions = [u for u in go_file.units if u.kind == "func"]
    long_functions = [(u.key, len(u.lines)) for u in functions if len(u.lines) > MAX_FUNCTION_LINES]
    return {"path": str(path), "lines": lines, "functions": len(functions),
            "long_functions": long_functions, "test": path.name.endswith("_test.go")}


def report(paths: list[pathlib.Path]) -> int:
    status = 0
    for path in paths:
        row = file_report(path)
        over = not row["test"] and (row["lines"] > MAX_FILE_LINES
                                     or row["functions"] > MAX_FILE_FUNCTIONS)
        status = 1 if over else status
        marker = " OVER-BUDGET" if over else (" (test)" if row["test"] else "")
        print("%s lines=%d functions=%d%s" % (row["path"], row["lines"], row["functions"], marker))
        for key, size in row["long_functions"]:
            print("  long function %s lines=%d" % (key, size))
    return status


# ---------------------------------------------------------------- main


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--manifest", type=pathlib.Path)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--compare", nargs=2, type=pathlib.Path, metavar=("BEFORE", "AFTER"))
    parser.add_argument("--report", nargs="+", type=pathlib.Path, metavar="FILE")
    parser.add_argument("--root", type=pathlib.Path, default=pathlib.Path.cwd())
    return parser


def run(args) -> int:
    if args.report:
        return report(args.report)
    if args.compare:
        problems = compare_dirs(*args.compare)
        print("\n".join(problems) if problems else "declaration multisets are equal")
        return 1 if problems else 0
    if args.manifest and args.check:
        errors = check_manifest(args.manifest, args.root)
        print("\n".join(errors) if errors else "manifest matches the tree")
        return 1 if errors else 0
    if args.manifest:
        written = apply_manifest(args.manifest, args.root)
        print("wrote %d files" % len(written))
        return 0
    build_parser().print_usage()
    return 2


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return run(args)
    except MoveError as error:
        print("move_go_declarations: %s" % error, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
