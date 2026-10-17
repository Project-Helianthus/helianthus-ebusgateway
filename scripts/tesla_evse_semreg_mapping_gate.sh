#!/usr/bin/env bash
set -euo pipefail

# Public immutable mapping evidence for the injected Tesla EVSE SemReg seam.
# This intentionally consumes neither a sibling checkout nor native hardware.
docs_repo="https://github.com/Project-Helianthus/helianthus-docs-semantic.git"
docs_commit="88a422896e1dc8c45a6bf629f08b8bff6115c009"
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/helianthus-tesla-evse-semreg.XXXXXX")"
trap 'rm -rf "$tmp_dir"' EXIT

git init -q "$tmp_dir/docs"
git -C "$tmp_dir/docs" remote add origin "$docs_repo"
git -C "$tmp_dir/docs" fetch -q --depth=1 origin "$docs_commit"
test "$(git -C "$tmp_dir/docs" rev-parse FETCH_HEAD)" = "$docs_commit"
git -C "$tmp_dir/docs" checkout -q --detach FETCH_HEAD
python3 "$tmp_dir/docs/scripts/validate_tesla_gen3_wc3_24443_evse_current_limit_mapping.py"

# go.mod is the runtime pin; reject workspace/local replacements and drift.
# Keep this gate valid when an accepted SemReg dependency advances.
semreg_module="github.com/Project-Helianthus/helianthus-semreg"
pinned_version="$(GOWORK=off go mod edit -json | python3 -c '
import json, sys
requirements = json.load(sys.stdin)["Require"]
print(next(item["Version"] for item in requirements if item["Path"] == sys.argv[1]))
' "$semreg_module")"
module_version="$(GOWORK=off go list -m -f '{{if .Replace}}replaced{{else}}{{.Version}}{{end}}' "$semreg_module")"
if [ -z "$pinned_version" ] || [ "$module_version" != "$pinned_version" ]; then
  echo "pinned SemReg EVSE runtime is not selected" >&2
  exit 1
fi
GOWORK=off go test github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/evse -count=1
