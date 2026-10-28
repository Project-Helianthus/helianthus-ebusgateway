#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
test_dir="$(mktemp -d)"
trap 'rm -rf "$test_dir"' EXIT
export GITHUB_STEP_SUMMARY="$test_dir/summary"
guard="$script_dir/lint_flake_guard.sh"
message='lint runner flake: staticcheck lost noreturn facts — rerun'
checks=0

expect_flake() {
  : > "$GITHUB_STEP_SUMMARY"
  bash "$guard" "$@" > "$test_dir/output"
  [ "$(cat "$test_dir/output")" = "::warning::$message" ]
  [ "$(cat "$GITHUB_STEP_SUMMARY")" = "$message" ]
  checks=$((checks + 1))
}

expect_rejected() {
  : > "$GITHUB_STEP_SUMMARY"
  if bash "$guard" "$@" > "$test_dir/output" 2> "$test_dir/error"; then
    echo "unexpected flake classification: $*" >&2
    exit 1
  fi
  [ ! -s "$test_dir/output" ]
  [ ! -s "$GITHUB_STEP_SUMMARY" ]
  checks=$((checks + 1))
}

# The issue's first and rebased failure signatures are identical, deliberately
# exercised separately. Only public repository paths and diagnostics are used.
cat > "$test_dir/first.log" <<'LOG'
##[error]cmd/gateway/boiler_circuit_zero_isolation_test.go:257:5: SA5011(related information): this check suggests that the pointer can be nil (staticcheck)
##[error]cmd/gateway/boiler_circuit_zero_isolation_test.go:260:58: SA5011: possible nil pointer dereference (staticcheck)
##[error]cmd/gateway/boiler_circuit_zero_isolation_test.go:261:19: SA5011: possible nil pointer dereference (staticcheck)
##[error]cmd/gateway/boiler_pump_provenance_test.go:180:5: SA5011(related information): this check suggests that the pointer can be nil (staticcheck)
##[error]cmd/gateway/boiler_pump_provenance_test.go:183:16: SA5011: possible nil pointer dereference (staticcheck)
##[error]cmd/gateway/composition_projection_acceptance_test.go:1142:5: SA5011(related information): this check suggests that the pointer can be nil (staticcheck)
##[error]issues found
LOG
cp "$test_dir/first.log" "$test_dir/rebased.log"
printf 'docs/ci.md\0' > "$test_dir/changed"
expect_flake "$test_dir/first.log" pr "$test_dir/changed"
expect_flake "$test_dir/rebased.log" pr "$test_dir/changed"
sed 's/^##\[error\]//' "$test_dir/first.log" > "$test_dir/text.log"
expect_flake "$test_dir/text.log" pr "$test_dir/changed"

for pair in '255 258' '268 271' '334 337'; do
  read -r related dereference <<< "$pair"
  printf '##[error]cmd/gateway/address_table_insertion_test.go:%s:5: SA5011(related information): this check suggests that the pointer can be nil (staticcheck)\n' "$related"
  printf '##[error]cmd/gateway/address_table_insertion_test.go:%s:19: SA5011: possible nil pointer dereference (staticcheck)\n' "$dereference"
done > "$test_dir/push.log"
printf '##[error]issues found\n' >> "$test_dir/push.log"
expect_flake "$test_dir/push.log" push

# Path is the comparison key: vary each reported file independently, including
# one represented only by related information. Matching any one must reject.
for path in boiler_circuit_zero_isolation boiler_pump_provenance composition_projection_acceptance; do
  printf 'cmd/gateway/%s_test.go\0' "$path" > "$test_dir/touched"
  expect_rejected "$test_dir/first.log" pr "$test_dir/touched"
done
printf 'cmd/gateway/address_table_insertion_test.go\0' > "$test_dir/touched"
expect_rejected "$test_dir/push.log" pr "$test_dir/touched"
: > "$test_dir/empty"
expect_flake "$test_dir/first.log" pr "$test_dir/empty"
expect_rejected "$test_dir/empty" push
printf '##[error]issues found\n' > "$test_dir/summary-only.log"
expect_rejected "$test_dir/summary-only.log" push
expect_rejected "$test_dir/missing" push
expect_rejected "$test_dir/first.log" pr
expect_rejected "$test_dir/first.log" pr "$test_dir/missing"
expect_rejected "$test_dir/first.log" unknown
printf 'docs/ci.md\n' > "$test_dir/not-nul"
expect_rejected "$test_dir/first.log" pr "$test_dir/not-nul"

for extra in \
  '##[error]cmd/gateway/other_test.go:1:2: SA4006: unused value (staticcheck)' \
  '##[error]cmd/gateway/other_test.go:1:2: SA5011: different message (staticcheck)' \
  '##[error]cmd/gateway/other_test.go:1:2: SA5011: possible nil pointer dereference (other)' \
  '##[error]context deadline exceeded' \
  'unexpected unstructured diagnostic'; do
  cp "$test_dir/first.log" "$test_dir/mixed.log"
  printf '%s\n' "$extra" >> "$test_dir/mixed.log"
  expect_rejected "$test_dir/mixed.log" push
done

# Normalize only the conventional ./ prefix; reject unsafe path spellings.
sed 's|cmd/gateway/|./cmd/gateway/|' "$test_dir/first.log" > "$test_dir/dot.log"
expect_flake "$test_dir/dot.log" push
printf 'cmd/gateway/boiler_pump_provenance_test.go\0' > "$test_dir/touched"
expect_rejected "$test_dir/dot.log" pr "$test_dir/touched"
for prefix in / ../ cmd//; do
  sed "s|cmd/gateway/|${prefix}cmd/gateway/|" "$test_dir/first.log" > "$test_dir/path.log"
  expect_rejected "$test_dir/path.log" push
done

# The literal filename comparison must retain spaces and newlines from git -z.
printf '##[error]synthetic file_test.go:1:2: SA5011: possible nil pointer dereference (staticcheck)\n' > "$test_dir/spaces.log"
printf 'synthetic file_test.go\0unrelated\nfile.go\0' > "$test_dir/touched"
expect_rejected "$test_dir/spaces.log" pr "$test_dir/touched"
printf 'synthetic file_test.go.extra\0' > "$test_dir/touched"
expect_flake "$test_dir/spaces.log" pr "$test_dir/touched"

printf 'lint flake guard: %s checks passed\n' "$checks"
