#!/usr/bin/env python3
"""Offline execution coverage; public docs, Python and Go commands are doubles.

These tests prove the shell orchestration of the Huawei PV gate: it fetches the
pinned public docs-semantic commit itself, refuses every identity form other
than the resource-based one before any Go build, fails closed on module
selection and preserves the focused Go test outcome. They do not qualify the
remote documentation.
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
GATE = ROOT / "scripts" / "huawei_pv_semreg_gate.sh"
MODULE = "github.com/Project-Helianthus/helianthus-semreg"
FOCUSED_GO_TEST = ["go", "test", "-race", "./cmd/gateway", "./internal/modbusadapter", "./mcp",
                   "-run", "^TestH10", "-count=1", "-timeout=120s"]
ALLOCATED = "gateway_allocated_resource_id"
FINALIZED_CONTRACT = {
    "identity": {
        "asset_id": {"source": "gateway_allocated_asset_id"},
        "source_id": {"source": "gateway_managed_instance_source_id"},
        "binding_id": {"stable_inputs": ["asset_id", "source_id", "profile_id"]},
        "resources": {
            "system": {"dimension": "pv.dimension.system", "source": ALLOCATED},
            "inverter": {"dimension": "pv.dimension.inverter", "source": ALLOCATED},
            "inputs": {"dimension": "pv.dimension.input", "source": ALLOCATED},
            "phases": {"dimension": "pv.dimension.phase", "source": "not_allocated_synthetic"},
        },
    },
    "graph": {
        "resources": [
            {"kind": "resource.pv_system", "anchor_class": "function", "anchor_basis": "operator_slot",
             "anchor_source": "operator_label", "parent_kind": None},
            {"kind": "resource.inverter", "anchor_class": "equipment", "anchor_basis": "operator_slot",
             "anchor_source": "operator_label", "parent_kind": None},
            {"kind": "resource.dc_input", "anchor_class": "component", "anchor_basis": "native_slot",
             "anchor_source": "native_position", "parent_kind": "resource.inverter"},
        ],
        "relations": [
            {"definition": "pv.relation.system_inverter", "basis_class": "gate_rule",
             "from_kind": "resource.pv_system", "to_kind": "resource.inverter"},
            {"definition": "pv.relation.inverter_input", "basis_class": "gate_rule",
             "from_kind": "resource.inverter", "to_kind": "resource.dc_input"},
        ],
    },
}

# Each double records its arguments. Unexpected commands fail instead of
# accidentally falling through to the host's network or Go environment. The
# git double materialises the contract under test at checkout; the python3
# double executes the gate's inline identity check with the real interpreter
# and treats the public validator as a controlled exit status.
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
        repo = Path(args[1])
        command = args[2:]
        if command[:3] == ["remote", "add", "origin"]:
            pass
        elif command[:4] == ["fetch", "-q", "--depth=1", "origin"]:
            (repo / "requested-ref").write_text(command[4])
        elif command == ["rev-parse", "FETCH_HEAD"]:
            print((repo / "requested-ref").read_text())
        elif command == ["checkout", "-q", "--detach", "FETCH_HEAD"]:
            contract = repo / "api/v1/mappings/huawei-sun2000-pv-v1.json"
            contract.parent.mkdir(parents=True)
            contract.write_text(os.environ["GATE_TEST_CONTRACT"])
        else:
            sys.exit(90)
    else:
        sys.exit(90)
elif name == "python3":
    if args[:1] == ["-"]:
        sys.argv = ["-", *args[1:]]
        exec(compile(sys.stdin.read(), "<gate>", "exec"), {"__name__": "__main__"})
        sys.exit(0)
    if len(args) != 1 or Path(args[0]).name != "validate_huawei_sun2000_pv_mapping.py":
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
    elif args == json.loads(os.environ["GATE_TEST_GO_TEST"]):
        sys.exit(int(os.environ.get("GATE_TEST_GO_STATUS", "0")))
    else:
        sys.exit(90)
else:
    sys.exit(90)
'''


class HuaweiPVSemregGateTests(unittest.TestCase):
    def run_gate(self, *arguments, contract=FINALIZED_CONTRACT, **overrides):
        version = re.search(
            rf"^\s*{re.escape(MODULE)}\s+(\S+)",
            (ROOT / "go.mod").read_text(), re.MULTILINE,
        ).group(1)
        with tempfile.TemporaryDirectory(prefix="huawei-gate-test-") as directory:
            work = Path(directory)
            log = work / "commands.jsonl"
            log.touch()
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
                GATE_TEST_VERSION=version,
                GATE_TEST_CONTRACT=json.dumps(contract),
                GATE_TEST_GO_TEST=json.dumps(FOCUSED_GO_TEST[1:]),
            )
            env.update(overrides)
            result = subprocess.run(
                ["bash", str(GATE), *arguments], cwd=ROOT,
                env=env, capture_output=True, text=True, timeout=30,
            )
            calls = [json.loads(line) for line in log.read_text().splitlines()]
        return result, calls

    def pinned_commit(self):
        return re.search(r'^docs_commit="([0-9a-f]{40})"$', GATE.read_text(), re.MULTILINE).group(1)

    def test_fetches_pinned_public_commit_then_identity_vectors_and_focused_go(self):
        result, calls = self.run_gate()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([call[0] for call in calls],
                         ["git"] * 5 + ["python3", "python3", "go", "go", "go"])
        fetch = calls[2]
        self.assertEqual(fetch[3:7], ["fetch", "-q", "--depth=1", "origin"])
        self.assertEqual(fetch[7], self.pinned_commit())
        self.assertEqual(calls[5][1], "-")
        self.assertTrue(calls[6][1].endswith("/scripts/validate_huawei_sun2000_pv_mapping.py"))
        self.assertEqual(calls[-1], FOCUSED_GO_TEST)

    def test_arguments_are_refused_before_any_command(self):
        result, calls = self.run_gate("some/docs/root")
        self.assertEqual(result.returncode, 2)
        self.assertIn("no arguments", result.stderr)
        self.assertEqual(calls, [])

    def refused(self, mutate, message):
        contract = json.loads(json.dumps(FINALIZED_CONTRACT))
        mutate(contract)
        result, calls = self.run_gate(contract=contract)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(message, result.stderr)
        self.assertEqual([call[0] for call in calls], ["git"] * 5 + ["python3"])

    def test_older_identity_forms_refused_before_vectors_and_go(self):
        # The derived-asset form this gate pinned before and any other source
        # of the asset or the source identifier are refused.
        for field, value in (("asset_id", "gateway_derived_semantic_asset_id"),
                             ("asset_id", "old_identity"),
                             ("source_id", "configured_source_id")):
            with self.subTest(field=field, value=value):
                self.refused(lambda c: c["identity"][field].update(source=value),
                             "Huawei resource identity companion required")

    def test_binding_input_drift_refused_before_vectors_and_go(self):
        self.refused(lambda c: c["identity"]["binding_id"].update(stable_inputs=["asset_id", "source_id"]),
                     "Huawei binding inputs differ")

    def test_resource_identity_drift_refused_before_vectors_and_go(self):
        # Every role the gateway keys facts by, its dimension and its
        # allocation are checked; a phase is never allocated.
        mutations = []
        for role in ("system", "inverter", "inputs"):
            mutations.append((role, "dimension", "pv.dimension.array"))
            mutations.append((role, "source", "asset_text"))
        mutations.append(("phases", "source", ALLOCATED))
        for role, field, value in mutations:
            with self.subTest(role=role, field=field):
                self.refused(lambda c: c["identity"]["resources"][role].update({field: value}),
                             "Huawei resource identity companion required")

    def test_anchor_drift_refused_before_vectors_and_go(self):
        # Each member of each anchor the gateway publishes is varied.
        for index in range(3):
            for field, value in (("kind", "resource.equipment"), ("anchor_class", "episode"),
                                 ("anchor_basis", "native_identity"), ("anchor_source", "routed_unit"),
                                 ("parent_kind", "resource.pv_system")):
                with self.subTest(index=index, field=field):
                    self.refused(lambda c: c["graph"]["resources"][index].update({field: value}),
                                 "Huawei resource anchors differ")

    def test_relation_drift_refused_before_vectors_and_go(self):
        # Each member of each relation the gateway publishes is varied.
        for index in range(2):
            for field, value in (("definition", "relation.relays"), ("basis_class", "configuration"),
                                 ("from_kind", "resource.equipment"), ("to_kind", "resource.phase")):
                with self.subTest(index=index, field=field):
                    self.refused(lambda c: c["graph"]["relations"][index].update({field: value}),
                                 "Huawei resource relations differ")

    def test_vector_failure_stops_before_module_selection(self):
        result, calls = self.run_gate(GATE_TEST_DOCS_STATUS="17")
        self.assertEqual(result.returncode, 17, result.stderr)
        self.assertFalse(any(call[0] == "go" for call in calls))

    def test_wrong_module_fails_before_focused_go_test(self):
        result, calls = self.run_gate(GATE_TEST_VERSION="v0.0.0-unselected")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("runtime is not selected", result.stderr)
        self.assertFalse(any(call[:2] == ["go", "test"] for call in calls))

    def test_replacement_fails_even_with_selected_version(self):
        result, calls = self.run_gate(GATE_TEST_REPLACED="replaced")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("runtime must not be replaced", result.stderr)
        self.assertFalse(any(call[:2] == ["go", "test"] for call in calls))

    def test_focused_go_failure_is_preserved(self):
        result, _ = self.run_gate(GATE_TEST_GO_STATUS="23")
        self.assertEqual(result.returncode, 23, result.stderr)


if __name__ == "__main__":
    unittest.main()
