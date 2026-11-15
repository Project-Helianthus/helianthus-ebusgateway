#!/usr/bin/env python3
"""Offline removal of work-item codes from the AdapterMux log and error texts.

RENAMES maps each exact old Go string literal of internal/adaptermux to its
new text: the sentence stays and the code goes, or the code is replaced by the
behaviour it stood for. Only string literals change; comments and identifiers
are left alone. Run without arguments to rewrite the files; --check fails if a
coded string literal remains in a non-test Go file of internal/adaptermux or if
a table entry no longer matches the sources.
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = "internal/adaptermux"
MUX = PACKAGE + "/mux.go"
SESSION = PACKAGE + "/session.go"
DASH = "—"
GE = "≥"
PREFIX = "adaptermux: "
# (file, old literal, new literal, occurrences); texts without the quotes.
RENAMES = [
    (MUX, "adaptermux: Close: timed out sending to pendingStart.notify (AM53)",
     "adaptermux: Close: timed out sending to pendingStart.notify", 1),
    (MUX, "adaptermux: reconnect: timed out sending to pendingStart.notify (AM53)",
     "adaptermux: reconnect: timed out sending to pendingStart.notify", 1),
    (MUX, "adaptermux: consecutive timeouts for %v (threshold %v), triggering reconnect (AM27)",
     "adaptermux: consecutive timeouts for %v (threshold %v), triggering reconnect", 1),
    (MUX, "adaptermux: ownership released for session %d remote=%s (terminal SYN, F-21)",
     "adaptermux: ownership released for session %d remote=%s (terminal SYN)", 1),
    (MUX, "adaptermux: ownership released for session %d remote=%s (SYN timeout) (AM6)",
     "adaptermux: ownership released for session %d remote=%s (SYN timeout)", 1),
    (MUX, "adaptermux: ownership released for session %d remote=%s (F-26 external START pending:"
     " elapsed %v skipping grace %v) (AM6)",
     "adaptermux: ownership released for session %d remote=%s (external START pending:"
     " elapsed %v skipping grace %v)", 1),
    (MUX, "adaptermux: ownership released for session %d remote=%s (idle grace expired: %v "
     + GE + " %v) (AM6)",
     "adaptermux: ownership released for session %d remote=%s (idle grace expired: %v "
     + GE + " %v)", 1),
    (MUX, "adaptermux: handleReset: timed out sending to pendingStart.notify (AM53)",
     "adaptermux: handleReset: timed out sending to pendingStart.notify", 1),
    (MUX, "adaptermux: pendingStart deadline expired for session %d (AM8, wasBlocking=%v,"
     " cancelledInFlight=%v)",
     "adaptermux: pendingStart deadline expired for session %d (wasBlocking=%v,"
     " cancelledInFlight=%v)", 1),
    (MUX, "adaptermux: deadline triggered transport reconnect to unstick hung StartArbitration (C2)",
     "adaptermux: deadline triggered transport reconnect to unstick hung StartArbitration", 1),
    (MUX, "adaptermux: F-34 abandon-watchdog firing for session %d gen=%d (no SEND within %v"
     " after STARTED reached the client) " + DASH + " force-releasing ownership",
     "adaptermux: abandon-watchdog firing for session %d gen=%d (no SEND within %v"
     " after STARTED reached the client) " + DASH + " force-releasing ownership", 1),
    (MUX, "adaptermux: suppressing STARTED-mismatch(0x%02X, pending 0x%02X) for session %d "
     + DASH + " request was cancelled/replaced; absorbed (C4/R4 AM56-half)",
     "adaptermux: suppressing STARTED-mismatch(0x%02X, pending 0x%02X) for session %d "
     + DASH + " request was cancelled/replaced; absorbed", 1),
    (MUX, "adaptermux: STARTED mismatch: got initiator 0x%02X, pending 0x%02X " + DASH
     + " failing pending session %d (AM56)",
     "adaptermux: STARTED mismatch: got initiator 0x%02X, pending 0x%02X " + DASH
     + " failing pending session %d", 1),
    (MUX, "adaptermux: suppressing STARTED for session %d " + DASH
     + " request was cancelled/replaced; absorbed (C4/R4)",
     "adaptermux: suppressing STARTED for session %d " + DASH
     + " request was cancelled/replaced; absorbed", 1),
    (MUX, "adaptermux: suppressing FAILED(0x%02X) for session %d " + DASH
     + " request was cancelled/replaced; absorbed (C4/R4 FAILED-half)",
     "adaptermux: suppressing FAILED(0x%02X) for session %d " + DASH
     + " request was cancelled/replaced; absorbed", 1),
    (MUX, "adaptermux: absorb timeout reset reason=%s (was waiting for %d stale arbitration"
     " response(s)) (F-22 debounce) (%d suppressed)",
     "adaptermux: absorb timeout reset reason=%s (was waiting for %d stale arbitration"
     " response(s)) (debounced) (%d suppressed)", 1),
    (MUX, "adaptermux: absorb timeout reset reason=%s (was waiting for %d stale arbitration"
     " response(s)) (F-22 debounce)",
     "adaptermux: absorb timeout reset reason=%s (was waiting for %d stale arbitration"
     " response(s)) (debounced)", 1),
    (MUX, "adaptermux: broadcastResetToSessions timed out after 1s " + DASH
     + " %d session(s) still pending (AM4)",
     "adaptermux: broadcastResetToSessions timed out after 1s " + DASH
     + " %d session(s) still pending", 1),
    (SESSION, "adaptermux: rejecting session " + DASH + " managed generation retired (AM13)",
     "adaptermux: rejecting session " + DASH + " managed generation retired", 2),
    (SESSION, "adaptermux: rejecting session " + DASH + " mux not available (AM13)",
     "adaptermux: rejecting session " + DASH + " mux not available", 1),
    (SESSION, "adaptermux: rejecting session " + DASH + " max sessions (%d) reached (AM50)",
     "adaptermux: rejecting session " + DASH + " max sessions (%d) reached", 1),
    (SESSION, "adaptermux: session %d close timed out " + DASH
     + " wg.Wait pending, will self-resolve on I/O unblock (AM14)",
     "adaptermux: session %d close timed out " + DASH
     + " wg.Wait pending, will self-resolve on I/O unblock", 1),
    (SESSION, "adaptermux: session %d INFO id=0x%02X payload too large (%d bytes, max 255) (AM39)",
     "adaptermux: session %d INFO id=0x%02X payload too large (%d bytes, max 255)", 1),
]
TOKEN = re.compile(r"//[^\n]*|/\*[\s\S]*?\*/|\"(?:\\.|[^\"\\\n])*\"|`[^`]*`|'(?:\\.|[^'\\\n])*'")
CODE = re.compile(r"(?<![A-Za-z0-9_])(?:[A-Z]{1,2}[0-9]{1,2}|F-(?:NEW-)?[0-9]+)(?![A-Za-z0-9_])")
VERB = re.compile(r"%[-+# 0-9.]*[a-zA-Z%]")


def literals(source):
    """Yield the text of every Go string literal outside comments."""
    for match in TOKEN.finditer(source):
        token = match[0]
        if token[0] in "\"`":
            yield token[1:-1]


def entry_problems(old, new):
    """Report a table entry that would change more than the code."""
    problems = []
    if not new.startswith(PREFIX) or not old.startswith(PREFIX):
        problems.append("prefix")
    if VERB.findall(old) != VERB.findall(new):
        problems.append("formatted values")
    if [c for c in old if ord(c) > 127] != [c for c in new if ord(c) > 127]:
        problems.append("non-ASCII text added or removed")
    if CODE.search(new) or not CODE.search(old) or "\\" in old + new:
        problems.append("code")
    return problems


def table_problems():
    problems = []
    for path, old, new, _count in RENAMES:
        for problem in entry_problems(old, new):
            problems.append(f"{path}: table entry {new!r}: {problem}")
    return problems


def replace(source, table):
    counts = {old: 0 for old, _new, _count in table}
    mapping = {old: new for old, new, _count in table}

    def swap(match):
        token = match[0]
        if token[0] == "\"" and token[1:-1] in mapping:
            counts[token[1:-1]] += 1
            return "\"" + mapping[token[1:-1]] + "\""
        return token

    return TOKEN.sub(swap, source), counts


def by_file():
    files = {}
    for path, old, new, count in RENAMES:
        files.setdefault(path, []).append((old, new, count))
    return files


def file_problems(path, table, source):
    present = list(literals(source))
    problems = []
    for old, new, count in table:
        if present.count(old):
            problems.append(f"{path}: old text remains: {old!r}")
        if present.count(new) != count:
            problems.append(f"{path}: {present.count(new)} of {count} new text(s): {new!r}")
    return problems


def coded_literals():
    problems = []
    for path in sorted((ROOT / PACKAGE).rglob("*.go")):
        if path.name.endswith("_test.go"):
            continue
        for text in literals(path.read_text(encoding="utf-8")):
            if CODE.search(text):
                problems.append(f"{path.relative_to(ROOT)}: coded string literal: {text!r}")
    return problems


def check():
    problems = table_problems()
    for path, table in by_file().items():
        problems += file_problems(path, table, (ROOT / path).read_text(encoding="utf-8"))
    return problems + coded_literals()


def rewrite():
    problems = table_problems()
    updates = {}
    for path, table in by_file().items():
        source = (ROOT / path).read_text(encoding="utf-8")
        updated, counts = replace(source, table)
        for old, new, count in table:
            if counts[old] not in (0, count):
                problems.append(f"{path}: {counts[old]} of {count} old text(s): {old!r}")
        updates[path] = updated
    if problems:
        return problems
    for path, updated in updates.items():
        (ROOT / path).write_text(updated, encoding="utf-8")
    return check()


def main(argv):
    if argv not in ([], ["--check"]):
        print("usage: rename_adaptermux_log_codes.py [--check]", file=sys.stderr)
        return 2
    problems = check() if argv else rewrite()
    for problem in problems:
        print(problem, file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
