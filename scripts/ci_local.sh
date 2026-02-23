#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

echo "==> terminology gate"
if git grep -nIwiE 'm[a]ster|s[l]ave'; then
  echo "Found legacy terminology."
  exit 1
fi

echo "==> gofmt"
unformatted="$(git ls-files '*.go' | xargs -n 50 gofmt -l || true)"
if [ -n "${unformatted}" ]; then
  echo "gofmt required for:"
  echo "${unformatted}"
  exit 1
fi

if command -v npm >/dev/null 2>&1; then
  echo "==> portal assets"
  ./scripts/check_portal_assets.sh
else
  echo "==> npm not found; skipping portal asset check"
fi

echo "==> go vet"
go vet ./...

echo "==> go build"
go build ./...

echo "==> go test (race)"
go test -race -count=1 ./...

if command -v golangci-lint >/dev/null 2>&1; then
  echo "==> golangci-lint"
  golangci-lint run ./...
else
  echo "==> golangci-lint not found; skipping"
fi

echo "==> transport gate"
./scripts/transport_gate.sh
