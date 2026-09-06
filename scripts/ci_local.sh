#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

echo "==> terminology gate"
if git grep -nIwiE 'm[a]ster|s[l]ave'; then
  echo "Found legacy terminology."
  exit 1
fi
python3 scripts/source_selection_m4_gate.py

echo "==> gofmt"
unformatted="$(
  git ls-files --cached --others --exclude-standard '*.go' |
    while IFS= read -r path; do
      [ ! -f "${path}" ] || printf '%s\0' "${path}"
    done |
    xargs -0 -n 50 gofmt -l || true
)"
if [ -n "${unformatted}" ]; then
  echo "gofmt required for:"
  echo "${unformatted}"
  exit 1
fi

if command -v npm >/dev/null 2>&1; then
  echo "==> portal node tests"
  node --test portal/web/test/*.test.mjs
  echo "==> portal assets"
  ./scripts/check_portal_assets.sh
else
  echo "==> npm not found; skipping portal asset check"
fi

echo "==> go vet"
go vet ./...

echo "==> go build"
go build ./...

echo "==> linux 32-bit build"
GOOS=linux GOARCH=386 go build -o /dev/null ./cmd/gateway
GOOS=linux GOARCH=arm GOARM=7 go build -o /dev/null ./cmd/gateway
GOOS=linux GOARCH=arm GOARM=6 go build -o /dev/null ./cmd/gateway

echo "==> go test (race)"
go test -race -count=1 ./...

echo "==> canonical PV SemReg shadow"
(
  cd integrationtests/canonicalpvshadow
  GOWORK=off go test -race -count=1 ./...
)

echo "==> validating source-selection artifact schema coverage"
go test ./... -run "TestSourceSelectionArtifact_EmitValidatesAgainstSchema" -count=1

echo "==> python script tests"
python3 scripts/passive_canary_verifier_test.py
python3 scripts/source_selection_m4_gate_test.py
python3 scripts/transport_gate_test.py
python3 scripts/passive_smoke_gate_test.py
python3 scripts/m8_source_clock_test.py
python3 scripts/capture_m8_source_window_test.py

if command -v golangci-lint >/dev/null 2>&1; then
  echo "==> golangci-lint"
  golangci-lint run ./...
else
  echo "==> golangci-lint not found; skipping"
fi

echo "==> transport gate"
./scripts/transport_gate.sh

echo "==> passive smoke gate"
./scripts/passive_smoke_gate.sh
