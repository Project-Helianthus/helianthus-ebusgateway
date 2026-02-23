#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if ! command -v npm >/dev/null 2>&1; then
  echo "npm is required to build portal assets"
  exit 1
fi

echo "==> portal asset build"
npm --prefix portal/web run build
