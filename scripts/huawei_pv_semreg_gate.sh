#!/usr/bin/env bash
set -euo pipefail

# Supply a standalone public docs-semantic checkout containing the merged
# Huawei identity companion. This offline gate never fetches dependencies.
if [[ $# -ne 1 ]]; then
  echo 'usage: bash scripts/huawei_pv_semreg_gate.sh PUBLIC_DOCS_SEMANTIC_ROOT' >&2
  exit 2
fi
public_docs_root="$1"
python3 - "$public_docs_root" <<'PY'
import json
import pathlib
import sys
root = pathlib.Path(sys.argv[1])
contract = json.loads((root / 'api/v1/mappings/huawei-sun2000-pv-v1.json').read_text())
identity = contract['identity']
if identity['asset_id']['source'] != 'gateway_derived_semantic_asset_id' or identity['source_id']['source'] != 'gateway_managed_instance_source_id':
    raise SystemExit('Huawei identity companion required')
derivation = identity['asset_id']['derivation']
if derivation['domain'] != 'helianthus.modbus.asset.v1' or derivation['field_encoding'] != 'utf8_each_followed_by_nul':
    raise SystemExit('Huawei asset derivation contract differs')
PY
python3 "$public_docs_root/scripts/validate_huawei_sun2000_pv_mapping.py"
GOWORK=off go test -race ./cmd/gateway ./internal/modbusadapter ./mcp -run '^TestH10' -count=1 -timeout=120s
