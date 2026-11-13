"""Synthetic inventories, consumer lists and text files exercise the moved-path
rewrite, its keep-list and its --check."""
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
# A synthetic library revision, built at run time.
REVISION = "0" * 7


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


    def test_repository_consumers_are_complete_and_stable(self):
        path = self.root / RENAME.CONSUMERS
        shutil.copyfile(ROOT / RENAME.INVENTORY, self.path)
        shutil.copyfile(ROOT / RENAME.CONSUMERS, path)
        before = path.read_bytes()
        self.assertFalse([text for text in RENAME.inventory_strings(json.loads(before))
                          if RENAME.OLD_PATH.search(text)])
        RENAME.update_inventory(self.root)
        self.assertEqual(path.read_bytes(), before)

    def test_library_forms_follow_the_move(self):
        document = inventory()
        document["rows"][0]["consumer_files"] = [
            "ebusgo/protocol/telegram_fsm/fsm.go", "ebusgo/protocol/ebus_standard/types/bcd.go",
            "helianthus-ebusreg/catalog/ebus_standard/identity.go", "ebusgo/protocol/telegram_fsm_notes.md"]
        self.write(document)
        consumers = {
            "consumer_paths": {
                "ebus_standard": ["vrc-explorer/ebus_standard.py", "ebusgo/protocol/telegram_fsm/fsm.go"],
                "Bcd": ["ebusgo/protocol/ebus_standard/types/bcd.go", "ha-integration/coordinator.py"],
            },
            "reference_source_files": {
                "ebusgo": ["protocol/ebus_standard/types/bcd.go", "protocol/telegram_fsm/fsm.go",
                           "protocol/telegram_x.go"],
                "vrc-explorer": ["ebus_standard.py"],
            },
            "tool": "ebus.v1.ebus_standard.services.list",
        }
        path = self.root / RENAME.CONSUMERS
        path.write_text(json.dumps(consumers, indent=2) + "\n")
        found = list(RENAME.remaining(self.root))
        self.assertIn(f"{RENAME.CONSUMERS}: moved path protocol/telegram_fsm/fsm.go", found)
        self.assertIn(f"{RENAME.CONSUMERS}: moved path ebusgo/protocol/ebus_standard/types/bcd.go", found)
        self.assertIn(f"{RENAME.INVENTORY}: moved path helianthus-ebusreg/catalog/ebus_standard/identity.go", found)
        RENAME.update_inventory(self.root)
        self.assertEqual(json.loads(self.path.read_text())["rows"][0]["consumer_files"], [
            "ebusgo/protocol/telegramfsm/fsm.go", "ebusgo/protocol/ebusstandard/types/bcd.go",
            "helianthus-ebusreg/catalog/ebusstandard/identity.go", "ebusgo/protocol/telegram_fsm_notes.md"])
        text = path.read_text()
        result = json.loads(text)
        self.assertEqual(text, json.dumps(result, indent=2) + "\n")
        self.assertEqual(list(result), list(consumers))
        self.assertEqual(list(result["consumer_paths"]), ["ebus_standard", "Bcd"])
        self.assertEqual(result["consumer_paths"]["ebus_standard"],
                         ["vrc-explorer/ebus_standard.py", "ebusgo/protocol/telegramfsm/fsm.go"])
        self.assertEqual(result["consumer_paths"]["Bcd"],
                         ["ebusgo/protocol/ebusstandard/types/bcd.go", "ha-integration/coordinator.py"])
        # The sorted listing stays sorted: telegram_x.go now sorts before telegramfsm/.
        self.assertEqual(result["reference_source_files"]["ebusgo"],
                         ["protocol/ebusstandard/types/bcd.go", "protocol/telegram_x.go",
                          "protocol/telegramfsm/fsm.go"])
        self.assertEqual(result["reference_source_files"]["vrc-explorer"], ["ebus_standard.py"])
        self.assertEqual(result["tool"], "ebus.v1.ebus_standard.services.list")
        self.assertEqual(list(RENAME.remaining(self.root)), [])

    def test_text_files_follow_the_move_outside_the_keep_list(self):
        self.write(inventory())
        RENAME.update_inventory(self.root)
        files = {
            "matrix/M6a-transport-matrix.md":
                "| CM-01 | `mcp/ebus_standard/golden_test.go` | `internal/nm_runtime/nm_runtime_test.go` |\n"
                "Goldens in `helianthus-ebusgateway/mcp/ebus_standard/testdata/` and "
                "`mcp/ebus_standard_wiring.go` (`ebus_standard` tools).\n",
            "portal/web/src/app.js":
                "// helianthus-ebusreg/catalog/ebus_standard/identity.go - Direction\n"
                "// Read-only surface over the in-process mcp/ebus_standard sub-server.\n"
                "const section = \"ebus_standard\";\n",
            "portal/web/test/l7-catalog.test.mjs": "  //     from helianthus-ebusreg/catalog/ebus_standard/identity.go.\n",
            "internal/executionpolicy/whitelist.go":
                f"package executionpolicy\n\n// sourced from ebusreg@{REVISION}\n"
                "// catalog/ebus_standard/catalog.yaml, and see\n"
                f"// ebusreg@{REVISION} catalog/ebus_standard/catalog.yaml too.\n"
                "// Current: catalog/ebus_standard/catalog.yaml.\n",
            "CHANGELOG.md": "`internal/execution_policy` becomes `internal/executionpolicy`.\n",
            "scripts/rename_underscore_packages.py": "MOVES = {'internal/rpc_source': 'internal/rpcsource'}\n",
            "tests/test_rename_underscore_packages.py": "PATH = 'mcp/ebus_standard/server.go'\n",
            "author-evidence/run/ci.log":
                "ok  \tgithub.com/Project-Helianthus/helianthus-ebusgateway/internal/nm_runtime\t0.2s\n"
                "see internal/nm_runtime for details\n",
            "docs/mcp-tool-inventory.md": "| ebus.v1.ebus_standard.decode | mcp/ebus_standard_wiring.go |\n",
        }
        for name, text in files.items():
            (self.root / name).parent.mkdir(parents=True, exist_ok=True)
            (self.root / name).write_text(text)
        found = list(RENAME.remaining(self.root))
        self.assertEqual(sorted(found), sorted([
            "author-evidence/run/ci.log:2: moved path internal/nm_runtime",
            "internal/executionpolicy/whitelist.go:6: moved path catalog/ebus_standard",
            "matrix/M6a-transport-matrix.md:1: moved path mcp/ebus_standard",
            "matrix/M6a-transport-matrix.md:1: moved path internal/nm_runtime",
            "matrix/M6a-transport-matrix.md:2: moved path mcp/ebus_standard",
            "portal/web/src/app.js:1: moved path catalog/ebus_standard",
            "portal/web/src/app.js:2: moved path mcp/ebus_standard",
            "portal/web/test/l7-catalog.test.mjs:1: moved path catalog/ebus_standard",
        ]))
        self.assertEqual(RENAME.update_text_files(self.root), RENAME.TEXT_FILES)
        self.assertEqual((self.root / "matrix/M6a-transport-matrix.md").read_text(),
                         "| CM-01 | `mcp/ebusstandard/golden_test.go` | `internal/nmruntime/nm_runtime_test.go` |\n"
                         "Goldens in `helianthus-ebusgateway/mcp/ebusstandard/testdata/` and "
                         "`mcp/ebus_standard_wiring.go` (`ebus_standard` tools).\n")
        self.assertEqual((self.root / "portal/web/src/app.js").read_text(),
                         "// helianthus-ebusreg/catalog/ebusstandard/identity.go - Direction\n"
                         "// Read-only surface over the in-process mcp/ebusstandard sub-server.\n"
                         "const section = \"ebus_standard\";\n")
        self.assertEqual((self.root / "portal/web/test/l7-catalog.test.mjs").read_text(),
                         "  //     from helianthus-ebusreg/catalog/ebusstandard/identity.go.\n")
        self.assertEqual(sorted(RENAME.remaining(self.root)), [
            "author-evidence/run/ci.log:2: moved path internal/nm_runtime",
            "internal/executionpolicy/whitelist.go:6: moved path catalog/ebus_standard",
        ])

    def test_rewritten_library_paths_are_checked_against_the_pinned_library(self):
        document = inventory()
        document["rows"][0]["consumer_files"] = [
            "ebusgo/protocol/telegramfsm/fsm.go", "ebusgo/protocol/ebusstandard/types/gone.go"]
        document["rows"][0]["full_identity"] = (
            "declaration/ebusgo/protocol/ebusstandard/types/bcd.go/go_identifier/0/Bcd")
        self.write(document)
        (self.root / "portal/web/src").mkdir(parents=True)
        (self.root / "portal/web/src/app.js").write_text(
            "// helianthus-ebusreg/catalog/ebusstandard/identity.go - Direction.\n")
        modules = {repository: self.root / "modules" / repository
                   for repository in (RENAME.EBUSGO, RENAME.EBUSREG)}
        for repository, path in [(RENAME.EBUSGO, "protocol/telegramfsm/fsm.go"),
                                 (RENAME.EBUSGO, "protocol/ebusstandard/types/bcd.go"),
                                 (RENAME.EBUSREG, "catalog/ebusstandard/identity.go")]:
            (modules[repository] / path).parent.mkdir(parents=True, exist_ok=True)
            (modules[repository] / path).write_text("package x\n")
        count, unique, missing = RENAME.verify_library(self.root, modules)
        self.assertEqual(count, 4)
        self.assertEqual(unique, {RENAME.EBUSGO: 3, RENAME.EBUSREG: 1})
        self.assertEqual(missing, [RENAME.EBUSGO + "/protocol/ebusstandard/types/gone.go"])


if __name__ == "__main__":
    unittest.main()
