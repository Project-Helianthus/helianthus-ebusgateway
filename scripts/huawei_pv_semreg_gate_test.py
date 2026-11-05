"""Offline gate argument and finalized-identity refusal checks."""
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

GATE = Path(__file__).with_name("huawei_pv_semreg_gate.sh")

class HuaweiGateTest(unittest.TestCase):
    def test_root_required(self):
        result = subprocess.run(["bash", str(GATE)], capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 2)
        self.assertIn("PUBLIC_DOCS_SEMANTIC_ROOT", result.stderr)

    def test_old_asset_identity_refused_before_go(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as root:
            contract = Path(root) / "api/v1/mappings/huawei-sun2000-pv-v1.json"
            contract.parent.mkdir(parents=True)
            contract.write_text(json.dumps({"identity": {"asset_id": {"source": "old_identity"}, "source_id": {"source": "gateway_managed_instance_source_id"}}}))
            result = subprocess.run(["bash", str(GATE), root], capture_output=True, text=True, timeout=5)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Huawei identity companion required", result.stderr)
            self.assertNotIn("go-build", result.stderr)

if __name__ == "__main__":
    unittest.main()
