#!/usr/bin/env python3
"""Relocate the Vaillant poller's publication state to its publication owner.

The Vaillant semantic poller (``vaillantSemanticPoller``) no longer holds the
GraphQL provider, the broadcast hub, the cache persister or the boiler
publication lock. They live in ``vaillantSemanticPublication`` and are reached
through nil-safe accessors on the poller. This offline script rewrites the
remaining direct uses in Go sources of package ``main`` under ``cmd/gateway``:

* a read ``X.provider``, ``X.hub`` or ``X.cache`` of a poller ``X`` becomes
  ``X.liveProvider()``, ``X.broadcastHub()`` or ``X.cachePersister()``;
* ``X.boilerPublishMu`` becomes ``X.boilerPublishLock()``;
* an assignment ``X.provider = E`` (likewise ``hub`` and ``cache``) becomes
  ``X.publication = newVaillantSemanticPublication(...)`` that keeps the two
  other collaborators;
* the keys ``provider``, ``hub`` and ``cache`` of a ``vaillantSemanticPoller``
  composite literal become one ``publication`` key built by
  ``newVaillantSemanticPublication``.

A poller ``X`` is a name the same top-level declaration binds to a poller: a
receiver or parameter of type ``*vaillantSemanticPoller``, a ``var`` of that
type (every name of a grouped declaration such as ``first, second`` counts), or a variable assigned from a poller literal, from
``newVaillantSemanticPoller``, from ``startVaillantSemanticPolling`` or from a
package function whose first result is a poller, with or without explicit type
arguments at the call. Comments and string literals
are never rewritten.

Usage:
  relocate_poller_publication_state.py [--root DIR] [FILE ...]
      rewrite FILE ... (default: every cmd/gateway/*.go), then gofmt them
  relocate_poller_publication_state.py --check [--root DIR] [FILE ...]
      list every remaining direct use; exit 1 when one remains
"""

from __future__ import annotations

import argparse
import glob
import os
import re
import shutil
import subprocess
import sys

POLLER = "vaillantSemanticPoller"
CONSTRUCTOR = "newVaillantSemanticPublication"
COLLABORATORS = ("provider", "hub", "cache")
ACCESSORS = {
    "provider": "liveProvider()",
    "hub": "broadcastHub()",
    "cache": "cachePersister()",
    "boilerPublishMu": "boilerPublishLock()",
}
IDENT = r"[A-Za-z_][A-Za-z0-9_]*"
# An optional type-parameter or type-argument list such as "[T any]" or
# "[map[string]int]"; one level of nested brackets is accepted.
TYPE_LIST = r"(?:\[(?:[^\[\]]|\[[^\[\]]*\])*\])?"


def mask(src: str) -> str:
    """Return src with comment and literal contents blanked, offsets kept."""
    out = list(src)
    i, n = 0, len(src)

    def blank(a: int, b: int) -> None:
        for k in range(a, b):
            if out[k] != "\n":
                out[k] = " "

    while i < n:
        c = src[i]
        if src.startswith("//", i):
            j = src.find("\n", i)
            j = n if j < 0 else j
            blank(i, j)
            i = j
        elif src.startswith("/*", i):
            j = src.find("*/", i + 2)
            j = n if j < 0 else j + 2
            blank(i, j)
            i = j
        elif c == "`":
            j = src.find("`", i + 1)
            j = n if j < 0 else j + 1
            blank(i + 1, j - 1)
            i = j
        elif c in "\"'":
            j = i + 1
            while j < n and src[j] != c and src[j] != "\n":
                j += 2 if src[j] == "\\" else 1
            blank(i + 1, j)
            i = j + 1
        else:
            i += 1
    return "".join(out)


def poller_functions(masked_sources: list[str]) -> set[str]:
    """Package functions whose first result is a poller."""
    pattern = re.compile(
        r"^func\s+(" + IDENT + r")\s*" + TYPE_LIST + r"\((?:[^()]|\([^()]*\))*\)\s*\(?\s*(?:" + IDENT + r"\s+)?\*" + POLLER + r"\b",
        re.M,
    )
    names = {"newVaillantSemanticPoller", "startVaillantSemanticPolling"}
    for text in masked_sources:
        names.update(m.group(1) for m in pattern.finditer(text))
    return names


def declarations(masked: str) -> list[tuple[int, int]]:
    """Spans of top-level declarations: each runs to the next top-level line."""
    starts = [m.start() for m in re.finditer(r"^(?:func|type|var|const|import)\b", masked, re.M)]
    starts.append(len(masked))
    return [(starts[k], starts[k + 1]) for k in range(len(starts) - 1)]


def bare_item_start(text: str) -> bool:
    """Whether a name ending text's prefix starts a bare list item.

    In a grouped declaration such as ``first, second *T`` or ``var a, b *T``
    every name before the typed one stands alone between separators; a name
    that follows another word (``n int, p *T``) is a type, not a variable.
    """
    stripped = text.rstrip()
    gap = text[len(stripped):]
    return (
        not stripped
        or "\n" in gap
        or stripped.endswith(("(", ","))
        or re.search(r"(?<![A-Za-z0-9_])var$", stripped) is not None
    )


def grouped_names(decl: str, start: int) -> list[str]:
    """Names that share the type of the typed name at start, nearest first."""
    names: list[str] = []
    before = re.compile(r"(?<![A-Za-z0-9_.])(" + IDENT + r")\s*,\s*$")
    while True:
        m = before.search(decl, max(0, start - 512), start)
        if m is None or not bare_item_start(decl[: m.start(1)]):
            return names
        names.append(m.group(1))
        start = m.start(1)


def poller_names(decl: str, functions: set[str]) -> set[str]:
    names: set[str] = set()
    typed = re.compile(r"(?:^|[(,\s])(" + IDENT + r")\s+\*" + POLLER + r"\b")
    for m in typed.finditer(decl):
        names.add(m.group(1))
        names.update(grouped_names(decl, m.start(1)))
    literal = re.compile(r"\b(" + IDENT + r")\s*:?=\s*&" + POLLER + r"\s*\{")
    names.update(m.group(1) for m in literal.finditer(decl))
    call = re.compile(
        r"\b(" + IDENT + r")\s*(?:,\s*" + IDENT + r"\s*)*:?=\s*(" + IDENT + r")\s*" + TYPE_LIST + r"\s*\("
    )
    names.update(m.group(1) for m in call.finditer(decl) if m.group(2) in functions)
    names.discard("_")
    return names


def matching_brace(masked: str, open_at: int) -> int:
    depth = 0
    for k in range(open_at, len(masked)):
        ch = masked[k]
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
            if depth == 0:
                return k
    raise ValueError("unbalanced literal")


def literal_elements(masked: str, lo: int, hi: int) -> list[tuple[int, int, int]]:
    """Top-level elements in masked[lo:hi] as (start, end, comma_or_hi)."""
    elements = []
    depth, start = 0, lo
    for k in range(lo, hi + 1):
        ch = masked[k] if k < hi else ","
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif ch == "," and depth == 0:
            seg = masked[start:k]
            if seg.strip():
                a = start + (len(seg) - len(seg.lstrip()))
                b = start + len(seg.rstrip())
                elements.append((a, b, k))
            start = k + 1
    return elements


class Edit:
    def __init__(self, start: int, end: int, text: str) -> None:
        self.start, self.end, self.text = start, end, text


def literal_keys(masked: str, open_at: int, close_at: int) -> dict[str, tuple[int, int, int]]:
    """Elements of one poller literal keyed by a publication field name."""
    keyed: dict[str, tuple[int, int, int]] = {}
    for a, b, comma in literal_elements(masked, open_at + 1, close_at):
        key = re.match(r"(" + IDENT + r")\s*:", masked[a:b])
        if key and key.group(1) in ACCESSORS:
            keyed[key.group(1)] = (a, b, comma)
    return keyed


def literal_replacement(src: str, keyed: dict[str, tuple[int, int, int]]) -> str:
    """The publication key that replaces the collaborator keys of a literal."""
    values = []
    for name in COLLABORATORS:
        if name in keyed:
            a, b, _ = keyed[name]
            values.append(src[a:b].split(":", 1)[1].strip())
        else:
            values.append("nil")
    return "publication: " + CONSTRUCTOR + "(" + ", ".join(values) + ")"


def removal_edit(src: str, element: tuple[int, int, int], close_at: int) -> Edit:
    """Remove one literal element: its whole line when it stands alone."""
    a, b, comma = element
    line_start = src.rfind("\n", 0, a) + 1
    end = comma + 1 if comma < close_at else b
    if src[line_start:a].strip() == "" and comma < close_at:
        nl = src.find("\n", end)
        if nl >= 0 and src[end:nl].strip() == "":
            return Edit(line_start, nl + 1, "")
    while end < close_at and src[end] in " \t":
        end += 1
    return Edit(a, end, "")


def literal_edits(src: str, masked: str, findings: list[tuple[int, str]]) -> list[Edit]:
    edits: list[Edit] = []
    # A return type such as "*vaillantSemanticPoller {" opens a body, not a
    # literal, so a preceding "*" excludes the match.
    for m in re.finditer(r"(?<![A-Za-z0-9_.*])" + POLLER + r"\s*\{", masked):
        open_at = m.end() - 1
        close_at = matching_brace(masked, open_at)
        keyed = literal_keys(masked, open_at, close_at)
        if not keyed:
            continue
        for key, (a, _, _) in keyed.items():
            findings.append((a, "literal key " + key))
        if "boilerPublishMu" in keyed:
            raise ValueError("a literal sets boilerPublishMu; rewrite it by hand")
        ordered = sorted(keyed.values())
        first = ordered[0]
        edits.append(Edit(first[0], first[1], literal_replacement(src, keyed)))
        edits.extend(removal_edit(src, element, close_at) for element in ordered[1:])
    return edits


def selector_edits(src: str, masked: str, functions: set[str], findings: list[tuple[int, str]]) -> list[Edit]:
    edits: list[Edit] = []
    field = "|".join(ACCESSORS)
    for lo, hi in declarations(masked):
        decl = masked[lo:hi]
        names = poller_names(decl, functions)
        if not names:
            continue
        selector = re.compile(
            r"(?<![A-Za-z0-9_.])(" + "|".join(sorted(names)) + r")\.(" + field + r")(?![A-Za-z0-9_])"
        )
        for m in selector.finditer(decl):
            start, end = lo + m.start(), lo + m.end()
            owner, name = m.group(1), m.group(2)
            findings.append((start, owner + "." + name))
            assign = re.match(r"[ \t]*=(?!=)[ \t]*", masked[end:])
            if assign and name in COLLABORATORS:
                line_end = masked.find("\n", end)
                line_end = len(masked) if line_end < 0 else line_end
                # The masked source blanks comments but keeps literal quotes,
                # so its last non-blank character ends the expression and a
                # trailing comment stays outside the replacement.
                value_start = end + assign.end()
                value_end = value_start + len(masked[value_start:line_end].rstrip())
                value = src[value_start:value_end].strip()
                expr = masked[value_start:value_end]
                if not value or expr.count("(") != expr.count(")"):
                    raise ValueError("unsupported assignment to " + owner + "." + name)
                args = []
                for other in COLLABORATORS:
                    args.append(value if other == name else owner + "." + ACCESSORS[other])
                edits.append(Edit(start, value_end, owner + ".publication = " + CONSTRUCTOR + "(" + ", ".join(args) + ")"))
                continue
            edits.append(Edit(start, end, owner + "." + ACCESSORS[name]))
    return edits


def apply(src: str, edits: list[Edit]) -> str:
    out = src
    previous = None
    for edit in sorted(edits, key=lambda e: e.start, reverse=True):
        if previous is not None and edit.end > previous:
            raise ValueError("overlapping rewrites")
        out = out[: edit.start] + edit.text + out[edit.end :]
        previous = edit.start
    return out


def line_of(src: str, offset: int) -> int:
    return src.count("\n", 0, offset) + 1


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--check", action="store_true", help="list remaining direct uses; exit 1 when one remains")
    parser.add_argument("--root", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
    parser.add_argument("files", nargs="*", help="Go files to process (default: every cmd/gateway/*.go)")
    return parser.parse_args(argv)


def rewrite(src: str, msk: str, functions: set[str]) -> tuple[str, list[tuple[int, str]]]:
    """Rewrite one source; return the result and the direct uses it found."""
    findings: list[tuple[int, str]] = []
    # Selectors first, so a literal value that reads a collaborator is
    # rewritten before the literal moves it; neither pass adds lines
    # before the other's findings.
    selected = apply(src, selector_edits(src, msk, functions, findings))
    rewritten = apply(selected, literal_edits(selected, mask(selected), findings))
    return rewritten, findings


def report_remaining(remaining: int) -> int:
    if remaining:
        print("%d direct uses of the poller's publication state remain" % remaining)
        return 1
    print("no direct use of the poller's publication state remains")
    return 0


def format_changed(changed: list[str]) -> int:
    if changed:
        gofmt = shutil.which("gofmt")
        if gofmt is None:
            print("gofmt not found: run gofmt -w on the rewritten files", file=sys.stderr)
            return 1
        subprocess.run([gofmt, "-w", *changed], check=True)
    print("%d files rewritten" % len(changed))
    return 0


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    root = os.path.abspath(args.root)
    package = sorted(glob.glob(os.path.join(root, "cmd", "gateway", "*.go")))
    if not package:
        print("no Go files under cmd/gateway", file=sys.stderr)
        return 2
    sources = {path: open(path, encoding="utf-8").read() for path in package}
    masked = {path: mask(text) for path, text in sources.items()}
    functions = poller_functions(list(masked.values()))

    targets = [os.path.abspath(f) for f in args.files] if args.files else package
    unknown = [path for path in targets if path not in sources]
    if unknown:
        print("not a Go file of cmd/gateway: " + unknown[0], file=sys.stderr)
        return 2

    remaining = 0
    changed: list[str] = []
    for path in targets:
        src = sources[path]
        rel = os.path.relpath(path, root)
        try:
            rewritten, findings = rewrite(src, masked[path], functions)
        except ValueError as err:
            print(rel + ": " + str(err), file=sys.stderr)
            return 2
        if args.check:
            for offset, what in sorted(findings):
                print("%s:%d: %s" % (rel, line_of(src, offset), what))
            remaining += len(findings)
        elif rewritten != src:
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(rewritten)
            changed.append(path)
            print("rewrote %s (%d uses)" % (rel, len(findings)))

    if args.check:
        return report_remaining(remaining)
    return format_changed(changed)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
