#!/usr/bin/env bash
set -euo pipefail

# This gate consumes only public, immutable inputs. It does not depend on a
# sibling checkout, hardware, a serial endpoint, or credentials.
docs_repo="https://github.com/Project-Helianthus/helianthus-docs-semantic.git"
docs_commit="f830ace6c2b9dd1af0e87ce808fa545662578418"
semreg_commit="f3f761bc67e10d6a65eba6c13cb4dc51002d6955"
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/helianthus-growatt-storage.XXXXXX")"
trap 'rm -rf "$tmp_dir"' EXIT

git init -q "$tmp_dir/docs"
git -C "$tmp_dir/docs" remote add origin "$docs_repo"
git -C "$tmp_dir/docs" fetch -q --depth=1 origin "$docs_commit"
test "$(git -C "$tmp_dir/docs" rev-parse FETCH_HEAD)" = "$docs_commit"
git -C "$tmp_dir/docs" checkout -q --detach FETCH_HEAD
python3 "$tmp_dir/docs/scripts/validate_growatt_bms_rs485_v202_storage_mapping.py"

module_version="$(GOWORK=off go list -m -f '{{.Version}}' github.com/Project-Helianthus/helianthus-semreg)"
case "$module_version" in *"${semreg_commit:0:12}"*) ;; *) echo "pinned SemReg storage runtime is not selected" >&2; exit 1 ;; esac
GOWORK=off go test github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/storage -count=1
