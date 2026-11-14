#!/usr/bin/env python3
"""Changelog fragments: one entry file per pull request, assembled at release.

A pull request adds one file changelog.d/<issue-number>-<slug>.md and leaves
CHANGELOG.md alone, so open pull requests never edit the same changelog lines.

  check                 validate every fragment
  check --require-new   also require that the change against the base adds one
  assemble              print the complete changelog (fragments, then CHANGELOG.md)
  assemble --write      move the fragments into CHANGELOG.md (release pull request)

Offline. Only --require-new calls Git, and only to read.
"""
import argparse
import os
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
DIRECTORY = "changelog.d"
CHANGELOG = "CHANGELOG.md"
README = "README.md"
NAME = re.compile(r"[0-9]+-[a-z0-9]+(?:-[a-z0-9]+)*\.md")
PATTERN = DIRECTORY + "/<issue-number>-<slug>.md"
DEFAULT_BASE = "origin/main"


def order_key(name):
    """Newest issue first; the name breaks ties, so order never depends on merge time."""
    return (-int(name.split("-", 1)[0]), name)


def fragment_names(root):
    """Return (fragment names in assembly order, problems with the directory)."""
    directory = Path(root) / DIRECTORY
    if not directory.is_dir():
        return [], [DIRECTORY + "/ is missing"]
    names, problems = [], []
    for entry in sorted(directory.iterdir(), key=lambda item: item.name):
        regular = entry.is_file() and not entry.is_symlink()
        if regular and entry.name == README:
            continue
        if regular and NAME.fullmatch(entry.name):
            names.append(entry.name)
        else:
            problems.append(f"{DIRECTORY}/{entry.name}: not a fragment; expected {PATTERN}")
    return sorted(names, key=order_key), problems


def is_conflict_marker(line):
    return line == "=======" or line.startswith(("<<<<<<< ", ">>>>>>> ", "||||||| "))


def body_problems(lines):
    """Problems on the lines after the title: whitespace, markers, extra titles."""
    problems, fenced = [], False
    for number, line in enumerate(lines, start=2):
        if line.startswith("```"):
            fenced = not fenced
        if line != line.rstrip():
            problems.append(f"line {number}: trailing whitespace")
        if is_conflict_marker(line):
            problems.append(f"line {number}: conflict marker")
        if not fenced and re.match(r"#{1,2} ", line):
            problems.append(f"line {number}: a fragment holds one '## ' title only")
    return problems


def text_problems(text):
    """Return the format problems of one fragment text."""
    problems = []
    if "\r" in text:
        problems.append("carriage return")
    if not text.endswith("\n") or text.endswith("\n\n"):
        problems.append("the file ends with exactly one newline")
    lines = text.rstrip("\n").split("\n")
    if not re.fullmatch(r"## \S(?:.*\S)?", lines[0]):
        problems.append("line 1: expected '## <title>'")
    if len(lines) < 3 or lines[1] != "" or not lines[2].strip():
        problems.append("expected the title, one blank line, then the entry text")
    return problems + body_problems(lines[1:])


def check_fragments(root):
    """Return (fragment names in assembly order, every problem found)."""
    names, problems = fragment_names(root)
    for name in names:
        try:
            text = (Path(root) / DIRECTORY / name).read_bytes().decode("utf-8")
        except UnicodeDecodeError:
            problems.append(f"{DIRECTORY}/{name}: not UTF-8")
            continue
        problems.extend(f"{DIRECTORY}/{name}: {problem}" for problem in text_problems(text))
    return names, problems


def assemble(changelog, entries):
    """Insert the entries, in the given order, before the first '## ' section.

    The existing sections keep every byte, their final newlines included.
    """
    if not entries:
        return changelog
    match = re.search(r"^## ", changelog, flags=re.M)
    cut = match.start() if match else len(changelog)
    title = changelog[:cut].rstrip("\n")
    head = title + "\n\n" if title else ""
    body = "\n".join(entry.rstrip("\n") + "\n" for entry in entries)
    sections = changelog[cut:]
    return head + body + ("\n" + sections if sections else "")


def git(root, *arguments):
    """Run one read-only Git command; a missing or hung Git counts as a failure."""
    try:
        result = subprocess.run(["git", "-C", str(root), *arguments], capture_output=True,
                                text=True, timeout=120, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return 1, ""
    return result.returncode, result.stdout


def resolves(root, revision):
    return git(root, "rev-parse", "--verify", "--quiet", revision + "^{commit}")[0] == 0


def change_set(root, base, head=None):
    """Return {path: status} against the merge base, or None when it is unknown.

    Without head the working tree is compared, untracked files included, so the
    rule holds before the commit exists.
    """
    code, merge_base = git(root, "merge-base", base, head or "HEAD")
    if code != 0:
        return None
    revisions = [merge_base.strip()] + ([head] if head else [])
    code, output = git(root, "diff", "--name-status", "--no-renames", "-z", *revisions)
    if code != 0:
        return None
    fields = output.split("\0")
    changes = dict(zip(fields[1::2], fields[0::2]))
    if not head:
        code, output = git(root, "ls-files", "--others", "--exclude-standard", "-z")
        if code != 0:
            return None
        changes.update({path: "A" for path in output.split("\0") if path})
    return changes


def new_fragment_problems(changes):
    """Apply the pull-request rule to a change set ({path: status})."""
    prefix = DIRECTORY + "/"
    fragments = {path: status for path, status in changes.items()
                 if path.startswith(prefix) and path != prefix + README}
    added = [path for path, status in fragments.items() if status == "A"]
    removed = [path for path, status in fragments.items() if status == "D"]
    if CHANGELOG in changes:
        if removed and not added:
            return []
        return [f"{CHANGELOG} changes only in a release assembly, which removes fragments "
                f"and adds none; put the entry in {PATTERN}"]
    if removed:
        return [f"fragments are removed only by a release assembly, together with {CHANGELOG}"]
    if not added:
        return [f"the change adds no {PATTERN}"]
    return []


def require_new(root, base, head):
    """Return (problems, notice) for the new-fragment rule."""
    explicit = base or os.environ.get("CHANGELOG_BASE")
    reference = explicit or DEFAULT_BASE
    if head and not resolves(root, head):
        return [f"head {head} is not available"], None
    changes = change_set(root, reference, head)
    if changes is None:
        if explicit:
            return [f"base {reference} is not available"], None
        return [], f"base {reference} is not available; the new-fragment rule was not checked"
    if not changes:
        return [], f"no change against {reference}; the new-fragment rule does not apply"
    return new_fragment_problems(changes), None


def run_check(root, arguments):
    names, problems = check_fragments(root)
    if arguments.require_new:
        more, notice = require_new(root, arguments.base, arguments.head)
        problems.extend(more)
        if notice:
            print("changelog fragments: " + notice)
    for problem in problems:
        print("changelog fragments: " + problem, file=sys.stderr)
    if problems:
        return 1
    print(f"changelog fragments: {len(names)} valid")
    return 0


def run_assemble(root, arguments):
    names, problems = check_fragments(root)
    target = Path(root) / CHANGELOG
    if not target.is_file():
        problems.append(f"{CHANGELOG} is missing")
    for problem in problems:
        print("changelog fragments: " + problem, file=sys.stderr)
    if problems:
        return 1
    directory = Path(root) / DIRECTORY
    entries = [(directory / name).read_bytes().decode("utf-8") for name in names]
    text = assemble(target.read_bytes().decode("utf-8"), entries)
    if not arguments.write:
        sys.stdout.write(text)
        return 0
    target.write_bytes(text.encode("utf-8"))
    for name in names:
        (directory / name).unlink()
    print(f"changelog fragments: {len(names)} moved into {CHANGELOG}")
    return 0


def main(argv=None, root=ROOT):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("check", help="validate the fragments")
    check.add_argument("--require-new", action="store_true",
                       help="require that the change against the base adds a fragment")
    check.add_argument("--base", help="base revision (default: CHANGELOG_BASE, then origin/main)")
    check.add_argument("--head", help="head revision (default: the working tree)")
    build = commands.add_parser("assemble", help="print or write the complete changelog")
    build.add_argument("--write", action="store_true",
                       help="rewrite CHANGELOG.md and remove the fragments")
    arguments = parser.parse_args(argv)
    if arguments.command == "check":
        return run_check(root, arguments)
    return run_assemble(root, arguments)


if __name__ == "__main__":
    sys.exit(main())
