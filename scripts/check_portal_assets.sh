#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

./scripts/build_portal_assets.sh

if ! git diff --quiet -- portal/static/assets; then
  echo "portal assets are out of date. run: ./scripts/build_portal_assets.sh"
  git status --short -- portal/web/src portal/static/assets
  exit 1
fi

echo "portal assets are up to date"
