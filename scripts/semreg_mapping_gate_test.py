#!/usr/bin/env python3
"""Offline execution coverage; public docs and Go commands are controlled doubles.

These tests prove shell orchestration and fail-closed module selection. They do
not qualify the remote documentation or replace the complete mapping gates.
"""

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent
MODULE = "github.com/Project-Helianthus/helianthus-semreg"
GATES = (
    ("growatt_storage_semreg_gate.sh", "storage"),
    ("tesla_evse_semreg_mapping_gate.sh", "evse"),
)

# Each double records its arguments. Unexpected commands fail instead of
# accidentally falling through to the host's network or Go environment.
DOUBLE = r'''
import json
import os
from pathlib import Path
import sys

name = Path(sys.argv[0]).name
args = sys.argv[1:]
with open(os.environ["GATE_TEST_LOG"], "a") as log:
    log.write(json.dumps([name, *args]) + "\n")
if name == "git":
    if args[:2] == ["init", "-q"]:
        Path(args[2]).mkdir()
    elif args[0] == "-C":
        command = args[2:]
        if command[:3] == ["remote", "add", "origin"]:
            pass
        elif command[:4] == ["fetch", "-q", "--depth=1", "origin"]:
            Path(args[1], "requested-ref").write_text(command[4])
        elif command == ["rev-parse", "FETCH_HEAD"]:
            print(Path(args[1], "requested-ref").read_text())
        elif command == ["checkout", "-q", "--detach", "FETCH_HEAD"]:
            pass
        else:
            sys.exit(90)
    else:
        sys.exit(90)
elif name == "python3":
    if len(args) != 1 or not Path(args[0]).name.startswith("validate_"):
        sys.exit(90)
    sys.exit(int(os.environ.get("GATE_TEST_DOCS_STATUS", "0")))
elif name == "go":
    if os.environ.get("GOWORK") != "off":
        sys.exit(90)
    if args == ["list", "-m", "-f", "{{.Version}}", os.environ["GATE_TEST_MODULE"]]:
        print(os.environ["GATE_TEST_VERSION"])
        sys.exit(int(os.environ.get("GATE_TEST_LIST_STATUS", "0")))
    elif args == ["list", "-m", "-f", "{{if .Replace}}replaced{{end}}", os.environ["GATE_TEST_MODULE"]]:
        print(os.environ.get("GATE_TEST_REPLACED", ""))
    elif args == ["test", os.environ["GATE_TEST_PACKAGE"], "-count=1"]:
        sys.exit(int(os.environ.get("GATE_TEST_PACK_STATUS", "0")))
    else:
        sys.exit(90)
else:
    sys.exit(90)
'''


class SemregMappingGateTests(unittest.TestCase):
    def run_gate(self, gate, pack, **overrides):
        version = re.search(
            rf"^\s*{re.escape(MODULE)}\s+(\S+)",
            (ROOT / "go.mod").read_text(), re.MULTILINE,
        ).group(1)
        with tempfile.TemporaryDirectory(prefix="semreg-gate-test-") as directory:
            work = Path(directory)
            log = work / "commands.jsonl"
            for command in ("git", "go", "python3"):
                executable = work / command
                executable.write_text(f"#!{sys.executable}\n" + DOUBLE)
                executable.chmod(0o755)
            env = dict(os.environ)
            env.update(
                PATH=str(work) + os.pathsep + env["PATH"],
                TMPDIR=str(work),
                GATE_TEST_LOG=str(log),
                GATE_TEST_MODULE=MODULE,
                GATE_TEST_PACKAGE=f"{MODULE}/semreg/v1/packs/{pack}",
                GATE_TEST_VERSION=version,
            )
            env.update(overrides)
            result = subprocess.run(
                ["bash", str(ROOT / "scripts" / gate)], cwd=ROOT,
                env=env, capture_output=True, text=True, timeout=30,
            )
            calls = [json.loads(line) for line in log.read_text().splitlines()]
        return result, calls

    def test_selected_module_runs_docs_and_exact_pack(self):
        for gate, pack in GATES:
            with self.subTest(gate=gate):
                result, calls = self.run_gate(gate, pack)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual([call[0] for call in calls],
                                 ["git"] * 5 + ["python3", "go", "go", "go"])
                self.assertEqual(calls[-1],
                                 ["go", "test", f"{MODULE}/semreg/v1/packs/{pack}", "-count=1"])

    def test_wrong_module_fails_before_pack_tests(self):
        for gate, pack in GATES:
            with self.subTest(gate=gate):
                result, calls = self.run_gate(gate, pack, GATE_TEST_VERSION="v0.0.0-unselected")
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("runtime is not selected", result.stderr)
                self.assertFalse(any(call[:2] == ["go", "test"] for call in calls))

    def test_replacement_fails_even_with_selected_version(self):
        for gate, pack in GATES:
            with self.subTest(gate=gate):
                result, calls = self.run_gate(gate, pack, GATE_TEST_REPLACED="replaced")
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("runtime must not be replaced", result.stderr)
                self.assertFalse(any(call[:2] == ["go", "test"] for call in calls))

    def test_docs_failure_stops_before_module_selection(self):
        for gate, pack in GATES:
            with self.subTest(gate=gate):
                result, calls = self.run_gate(gate, pack, GATE_TEST_DOCS_STATUS="17")
                self.assertEqual(result.returncode, 17, result.stderr)
                self.assertFalse(any(call[0] == "go" for call in calls))

    def test_module_query_failure_stops_before_pack_tests(self):
        for gate, pack in GATES:
            with self.subTest(gate=gate):
                result, calls = self.run_gate(gate, pack, GATE_TEST_LIST_STATUS="19")
                self.assertEqual(result.returncode, 19, result.stderr)
                self.assertEqual(calls[-1][:2], ["go", "list"])

    def test_pack_failure_is_preserved(self):
        for gate, pack in GATES:
            with self.subTest(gate=gate):
                result, _ = self.run_gate(gate, pack, GATE_TEST_PACK_STATUS="23")
                self.assertEqual(result.returncode, 23, result.stderr)


if __name__ == "__main__":
    unittest.main()
