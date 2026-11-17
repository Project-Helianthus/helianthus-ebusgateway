#!/usr/bin/env bash
set -euo pipefail

# This gate consumes only public, immutable inputs: the pinned public
# docs-semantic commit carrying the merged Huawei gate re-based on resources
# and the SemReg runtime selected by go.mod. It depends on neither a sibling checkout,
# hardware, a network endpoint nor credentials, so it runs from a standalone
# clone exactly like the storage and EVSE mapping gates.
docs_repo="https://github.com/Project-Helianthus/helianthus-docs-semantic.git"
docs_commit="699a3924e665abaa96668ffd76873f9f3b701bb6"
semreg_commit="6624e6a810d70917f356b5f93c41e8dc314d814a"
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

# Refuse any identity form other than the resource-based one before any Go
# build, so a stale gate cannot be masked by passing gateway tests: allocated
# asset and resource identifiers, the binding inputs, and the anchors and
# relations the gateway publishes.
python3 - "$public_docs_root" <<'PY'
import json
import pathlib
import sys
root = pathlib.Path(sys.argv[1])
contract = json.loads((root / 'api/v1/mappings/huawei-sun2000-pv-v1.json').read_text())
identity = contract['identity']
if identity['asset_id']['source'] != 'gateway_allocated_asset_id' or identity['source_id']['source'] != 'gateway_managed_instance_source_id':
    raise SystemExit('Huawei resource identity companion required')
if identity['binding_id']['stable_inputs'] != ['asset_id', 'source_id', 'profile_id']:
    raise SystemExit('Huawei binding inputs differ')
resources = identity['resources']
for role, dimension in (('system', 'pv.dimension.system'), ('inverter', 'pv.dimension.inverter'), ('inputs', 'pv.dimension.input')):
    if resources[role]['dimension'] != dimension or resources[role]['source'] != 'gateway_allocated_resource_id':
        raise SystemExit('Huawei resource identity companion required')
if resources['phases']['source'].startswith('gateway_allocated'):
    raise SystemExit('Huawei resource identity companion required')
anchors = {(entry['kind'], entry['anchor_class'], entry['anchor_basis'], entry['anchor_source'], entry['parent_kind']) for entry in contract['graph']['resources']}
for anchor in (('resource.pv_system', 'function', 'operator_slot', 'operator_label', None),
               ('resource.inverter', 'equipment', 'operator_slot', 'operator_label', None),
               ('resource.dc_input', 'component', 'native_slot', 'native_position', 'resource.inverter')):
    if anchor not in anchors:
        raise SystemExit('Huawei resource anchors differ')
relations = {(entry['definition'], entry['basis_class'], entry['from_kind'], entry['to_kind']) for entry in contract['graph']['relations']}
for relation in (('pv.relation.system_inverter', 'gate_rule', 'resource.pv_system', 'resource.inverter'),
                 ('pv.relation.inverter_input', 'gate_rule', 'resource.inverter', 'resource.dc_input')):
    if relation not in relations:
        raise SystemExit('Huawei resource relations differ')
PY
python3 "$public_docs_root/scripts/validate_huawei_sun2000_pv_mapping.py"

module_version="$(GOWORK=off go list -m -f '{{.Version}}' github.com/Project-Helianthus/helianthus-semreg)"
replaced="$(GOWORK=off go list -m -f '{{if .Replace}}replaced{{end}}' github.com/Project-Helianthus/helianthus-semreg)"
test -z "$replaced" || { echo "SemReg PV runtime must not be replaced" >&2; exit 1; }
case "$module_version" in *"${semreg_commit:0:12}"*) ;; *) echo "pinned SemReg PV runtime is not selected" >&2; exit 1 ;; esac
GOWORK=off go test -race ./cmd/gateway ./internal/modbusadapter ./mcp -run '^TestH10' -count=1 -timeout=120s
