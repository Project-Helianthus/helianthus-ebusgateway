"""Synthetic inventories exercise the moved-path rewrite and its --check."""
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "rename_underscore_packages", ROOT / "scripts/rename_underscore_packages.py")
RENAME = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RENAME)

WIRING = "mcp/ebus_standard_wiring.go"


def row(source_file, name):
    return {
        "consumer_files": [source_file, "ebusgo/emulation/framework.go"],
        "current_name": name,
        "full_identity": f"declaration/{source_file}/go_identifier/0/{name}",
        "source_file": source_file,
        "status": "unchanged",
    }


def inventory():
    return {
        "bounded_declarations": [
            {"kind": "tool", "source_file": "mcp/ebus_standard/server.go", "status": "ready"},
            {"kind": "tool", "source_file": WIRING, "status": "ready"},
        ],
        "rows": [
            row("internal/execution_policy/policy.go", "Policy"),
            row("internal/nm_runtime/nm_runtime.go", "Runtime"),
            row("internal/rpc_source/source.go", "Source"),
            row("mcp/ebus_standard/envelope.go", "NewEnvelope"),
            row(WIRING, "RegisterEbusStandardTools"),
            row("mcp/ebus_standard_wiring_test.go", "TestRegisterEbusStandardTools"),
        ],
        "source_files": ["internal/rpc_source/source.go", "mcp/ebus_standard/envelope.go", WIRING],
    }


class RenameInventoryTest(unittest.TestCase):
    def setUp(self):
        scratch = ROOT / ".tmp"
        scratch.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=scratch)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.path = self.root / RENAME.INVENTORY
        self.path.parent.mkdir(parents=True)

    def write(self, document):
        self.path.write_text(json.dumps(document, indent=2) + "\n")

    def test_nested_paths_follow_the_move(self):
        self.write(inventory())
        RENAME.update_inventory(self.root)
        result = json.loads(self.path.read_text())
        self.assertEqual(
            [entry["source_file"] for entry in result["rows"]],
            ["internal/executionpolicy/policy.go", "internal/nmruntime/nm_runtime.go",
             "internal/rpcsource/source.go", "mcp/ebusstandard/envelope.go", WIRING,
             "mcp/ebus_standard_wiring_test.go"])
        for entry in result["rows"]:
            self.assertEqual(entry["full_identity"],
                             f"declaration/{entry['source_file']}/go_identifier/0/{entry['current_name']}")
            self.assertEqual(entry["consumer_files"], [entry["source_file"], "ebusgo/emulation/framework.go"])
        self.assertEqual([entry["source_file"] for entry in result["bounded_declarations"]],
                         ["mcp/ebusstandard/server.go", WIRING])
        self.assertEqual(result["source_files"],
                         ["internal/rpcsource/source.go", WIRING, "mcp/ebusstandard/envelope.go"])
        self.assertEqual(list(RENAME.remaining(self.root)), [])

    def test_check_reports_a_partial_move(self):
        document = inventory()
        # Only the top-level list moved, as the earlier inventory update did.
        document["source_files"] = [RENAME.moved_path(entry) for entry in document["source_files"]]
        self.write(document)
        found = list(RENAME.remaining(self.root))
        self.assertIn(f"{RENAME.INVENTORY}: moved path mcp/ebus_standard/envelope.go", found)
        self.assertIn(f"{RENAME.INVENTORY}: moved path "
                      "declaration/internal/nm_runtime/nm_runtime.go/go_identifier/0/Runtime", found)
        self.assertIn(f"{RENAME.INVENTORY}: moved path mcp/ebus_standard/server.go", found)
        self.assertFalse([line for line in found if "ebus_standard_wiring" in line])

    def test_repository_inventory_is_complete_and_stable(self):
        shutil.copyfile(ROOT / RENAME.INVENTORY, self.path)
        before = self.path.read_bytes()
        self.assertFalse([text for text in RENAME.inventory_strings(json.loads(before))
                          if RENAME.OLD_PATH.search(text)])
        RENAME.update_inventory(self.root)
        self.assertEqual(self.path.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
