#!/usr/bin/env bash
set -euo pipefail

# This gate consumes only public, immutable inputs: the pinned public
# docs-semantic commit carrying the merged Huawei identity companion and the
# SemReg runtime selected by go.mod. It depends on neither a sibling checkout,
# hardware, a network endpoint nor credentials, so it runs from a standalone
# clone exactly like the storage and EVSE mapping gates.
docs_repo="https://github.com/Project-Helianthus/helianthus-docs-semantic.git"
docs_commit="35e1a0e05383e0abf6a3c6f61f05845adb0d0808"
semreg_commit="5fe3a96fd9aea2cfcc276db87c58019b2f60949d"
if [[ $# -ne 0 ]]; then
  echo 'usage: bash scripts/huawei_pv_semreg_gate.sh (no arguments; the pinned public docs-semantic commit is fetched)' >&2
  exit 2
fi
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/helianthus-huawei-pv-semreg.XXXXXX")"
trap 'rm -rf "$tmp_dir"' EXIT

git init -q "$tmp_dir/docs"
git -C "$tmp_dir/docs" remote add origin "$docs_repo"
git -C "$tmp_dir/docs" fetch -q --depth=1 origin "$docs_commit"
test "$(git -C "$tmp_dir/docs" rev-parse FETCH_HEAD)" = "$docs_commit"
git -C "$tmp_dir/docs" checkout -q --detach FETCH_HEAD
public_docs_root="$tmp_dir/docs"

# Refuse the older identity form before any Go build so a stale companion
# cannot be masked by passing gateway tests.
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

module_version="$(GOWORK=off go list -m -f '{{.Version}}' github.com/Project-Helianthus/helianthus-semreg)"
replaced="$(GOWORK=off go list -m -f '{{if .Replace}}replaced{{end}}' github.com/Project-Helianthus/helianthus-semreg)"
test -z "$replaced" || { echo "SemReg PV runtime must not be replaced" >&2; exit 1; }
case "$module_version" in *"${semreg_commit:0:12}"*) ;; *) echo "pinned SemReg PV runtime is not selected" >&2; exit 1 ;; esac
GOWORK=off go test -race ./cmd/gateway ./internal/modbusadapter ./mcp -run '^TestH10' -count=1 -timeout=120s
