#!/usr/bin/env bash
set -euo pipefail

# This gate consumes only public, immutable inputs. It does not depend on a
# sibling checkout, hardware, a serial endpoint, or credentials.
docs_repo="https://github.com/Project-Helianthus/helianthus-docs-semantic.git"
docs_commit="f830ace6c2b9dd1af0e87ce808fa545662578418"
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/helianthus-growatt-storage.XXXXXX")"
trap 'rm -rf "$tmp_dir"' EXIT

git init -q "$tmp_dir/docs"
git -C "$tmp_dir/docs" remote add origin "$docs_repo"
git -C "$tmp_dir/docs" fetch -q --depth=1 origin "$docs_commit"
test "$(git -C "$tmp_dir/docs" rev-parse FETCH_HEAD)" = "$docs_commit"
git -C "$tmp_dir/docs" checkout -q --detach FETCH_HEAD
python3 "$tmp_dir/docs/scripts/validate_growatt_bms_rs485_v202_storage_mapping.py"

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
  echo "pinned SemReg storage runtime is not selected" >&2
  exit 1
fi
GOWORK=off go test github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/storage -count=1
