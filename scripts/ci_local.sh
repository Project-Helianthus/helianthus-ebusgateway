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

echo "==> semantic import boundary"
python3 scripts/check_semantic_imports.py
python3 -m unittest discover -s tests -p test_semantic_imports.py -v

echo "==> Python lint baseline"
python3 scripts/check_python_lint.py
python3 -m unittest discover -s tests -p test_python_lint.py -v

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
go build -tags nomatter -o /dev/null ./cmd/gateway

echo "==> linux 32-bit build"
GOOS=linux GOARCH=386 go build -o /dev/null ./cmd/gateway
GOOS=linux GOARCH=arm GOARM=7 go build -o /dev/null ./cmd/gateway
GOOS=linux GOARCH=arm GOARM=6 go build -o /dev/null ./cmd/gateway

echo "==> go test (race)"
go test -race -short -count=1 -timeout 40m ./...
go test -race -short -count=1 -timeout 20m -tags nomatter ./cmd/gateway
GOMODCACHE="$(go env GOMODCACHE)" python3 scripts/check_no_matter_profile.py

echo "==> validating source-selection artifact schema coverage"
go test ./... -run "TestSourceSelectionArtifact_EmitValidatesAgainstSchema" -count=1

echo "==> python script tests"
python3 -m venv .tmp/metrics-inventory-venv
.tmp/metrics-inventory-venv/bin/python -m pip install pytest
.tmp/metrics-inventory-venv/bin/python -m pytest tests/test_metrics_inventory_docs.py -q -p no:cacheprovider
python3 scripts/passive_canary_verifier_test.py
python3 scripts/source_selection_m4_gate_test.py
python3 scripts/transport_gate_test.py
python3 scripts/passive_smoke_gate_test.py
python3 scripts/m8_source_clock_test.py
python3 scripts/capture_m8_source_window_test.py
python3 scripts/tesla_evse_semreg_boundary_test.py
python3 scripts/semreg_mapping_gate_test.py
python3 scripts/huawei_pv_semreg_gate_test.py
python3 scripts/test_reset_runtime_control.py

if command -v golangci-lint >/dev/null 2>&1; then
  echo "==> golangci-lint"
  # Keep in sync with the version pinned in .github/workflows/ci.yml.
  GOLANGCI_LINT_EXPECTED_VERSION="2.11.4"
  golangci_lint_installed="$(golangci-lint --version 2>/dev/null | sed -nE 's/.*version v?([0-9]+\.[0-9]+\.[0-9]+).*/\1/p' | head -n1)"
  if [ "${golangci_lint_installed%%.*}" != "${GOLANGCI_LINT_EXPECTED_VERSION%%.*}" ]; then
    echo "WARNING: golangci-lint ${golangci_lint_installed:-unknown} installed; CI pins v${GOLANGCI_LINT_EXPECTED_VERSION} (major v${GOLANGCI_LINT_EXPECTED_VERSION%%.*}). Local lint results may differ from CI." >&2
  fi
  golangci-lint run ./...
else
  echo "==> golangci-lint not found; skipping"
fi

echo "==> transport gate"
./scripts/transport_gate.sh

echo "==> Growatt storage SemReg mapping gate"
./scripts/growatt_storage_semreg_gate.sh

echo "==> Tesla EVSE SemReg mapping gate"
./scripts/tesla_evse_semreg_mapping_gate.sh

echo "==> Huawei PV SemReg mapping gate"
./scripts/huawei_pv_semreg_gate.sh

echo "==> passive smoke gate"
./scripts/passive_smoke_gate.sh
