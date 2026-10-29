#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Exercise both checkout branches with Actions' truthiness for string/number
# literals: numeric zero is falsy, but the string '0' must select full history.
python3 - "$script_dir/../.github/workflows/ci.yml" <<'PY'
import ast
import re
import sys
from pathlib import Path

workflow = Path(sys.argv[1]).read_text()
expression = re.search(r"^\s+fetch-depth: \$\{\{ (.*?) \}\}$", workflow, re.M)
assert expression is not None, "missing checkout fetch-depth expression"
branches = re.fullmatch(
    r"runner.environment != 'github-hosted' && ('[01]'|[01]) \|\| ('[01]'|[01])",
    expression[1],
)
assert branches is not None, "unexpected checkout fetch-depth expression"
full, shallow = (ast.literal_eval(value) for value in branches.groups())
for environment, expected in (("self-hosted", "0"), ("github-hosted", "1")):
    depth = (environment != "github-hosted" and full) or shallow
    assert str(depth) == expected, f"{environment}: expected {expected}, got {depth}"
print("lint checkout fetch-depth: 2 checks passed")
PY

# Execute cache preparation twice to verify fresh directories even when the
# runner already has a persistent linter cache.
python3 - "$script_dir/../.github/workflows/ci.yml" <<'PYCACHE'
import os
import subprocess
import sys
import tempfile
from pathlib import Path

workflow = Path(sys.argv[1]).read_text()
step = workflow.split("      - name: Prepare isolated self-hosted lint cache\n", 1)[1]
step = step.split("      - uses:", 1)[0]
assert "if: runner.environment != 'github-hosted'" in step
run = step.split("        run: |\n", 1)[1]
run = "\n".join(line[10:] for line in run.splitlines())
runner_temp = Path(tempfile.mkdtemp())
env_file = runner_temp / "env"
previous_cache = runner_temp / "persistent-cache"
previous_cache.mkdir()
(previous_cache / "sentinel").write_text("existing cache")
caches = []
for _ in range(2):
    subprocess.run(
        ["bash", "-eo", "pipefail", "-c", run],
        env={**os.environ, "RUNNER_TEMP": str(runner_temp),
             "GITHUB_ENV": str(env_file), "GOLANGCI_LINT_CACHE": str(previous_cache)},
        check=True,
    )
    key, value = env_file.read_text().splitlines()[-1].split("=", 1)
    assert key == "GOLANGCI_LINT_CACHE"
    cache = Path(value)
    assert cache.is_absolute() and cache.parent == runner_temp
    assert cache.is_dir() and not list(cache.iterdir())
    caches.append(cache)
assert caches[0] != caches[1]
assert (previous_cache / "sentinel").read_text() == "existing cache"
print("lint cache isolation: 2 checks passed")
PYCACHE

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

# Execute the workflow failure handler so ordinary findings remain visible even
# when classification rejects them. The preceding lint step must stay failed.
python3 - "$script_dir/../.github/workflows/ci.yml" "$test_dir" <<'PYTEST'
import os
import subprocess
import sys
from pathlib import Path

workflow = Path(sys.argv[1]).read_text()
test_dir = Path(sys.argv[2])
handler = workflow.split("      - name: Identify self-hosted staticcheck runner flake\n", 1)[1]
run = handler.split("        run: |\n", 1)[1]
run = "\n".join(line[10:] for line in run.splitlines())
assert "continue-on-error:" not in workflow, "lint failures must remain failures"
ordinary = "other_test.go:1:2: SA4006: unused value (staticcheck)\n"
(test_dir / "ordinary.log").write_text(ordinary)
for filename, accepted in (("ordinary.log", False), ("first.log", True)):
    log = test_dir / filename
    result = subprocess.run(
        ["bash", "-eo", "pipefail", "-c", run],
        env={**os.environ, "EVENT_NAME": "push", "LINT_LOG": str(log)},
        text=True, capture_output=True, check=False,
    )
    assert result.stdout.startswith(log.read_text()), result.stdout + result.stderr
    assert (result.returncode == 0) == accepted, result.stdout + result.stderr
    assert ("::warning::" in result.stdout) == accepted, result.stdout
print("lint failure diagnostics: 2 checks passed")
PYTEST

printf 'lint flake guard: %s checks passed\n' "$checks"
