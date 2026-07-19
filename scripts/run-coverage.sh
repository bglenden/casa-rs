#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/run-coverage.sh [--ci-like]

Runs the workspace LLVM source-coverage gate.

Options:
  --ci-like  Rebuild with `PKG_CONFIG=/usr/bin/false` so coverage matches the
             GitHub Actions minimal environment where casacore C++ interop is
             unavailable.
EOF
}

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

ci_like="false"
for arg in "$@"; do
  case "$arg" in
    --ci-like)
      ci_like="true"
      ;;
    -h|--help|help)
      usage
      exit 0
      ;;
    *)
      usage >&2
      exit 1
      ;;
  esac
done

coverage_fail_under="${COVERAGE_FAIL_UNDER:-75}"
coverage_target="${COVERAGE_TARGET:-78}"

if [[ "$ci_like" == "true" ]]; then
  echo "==> Running CI-like coverage (forcing pkg-config casacore lookup to fail)"
  # `has_casacore_cpp` is decided in build.rs, so clear existing artifacts before
  # switching to the CI-like no-casacore configuration.
  cargo llvm-cov clean --workspace
  export PKG_CONFIG=/usr/bin/false
fi

export CARGO_INCREMENTAL=0

run_llvm_cov() {
  # Keep the coverage gate focused on shipped code plus functional tests.
  # Large perf/profile harnesses are useful for benchmarking, but they make
  # line-coverage drift when benchmarks are added, renamed, or explicitly ignored.
  # Thin binary entrypoints are exercised indirectly through library/runtime
  # tests and otherwise add denominator without meaningful extra signal.
  # The casars-imager library root is the runtime orchestration layer for the
  # shipped imager app; its algorithmic work is covered in casa-imaging,
  # casa-images, task-contract tests, and slow fixture runs, while the remaining
  # direct CLI/workflow plumbing is already checked by functional tests.
  # The casars library root owns alternate-screen setup, terminal event-loop
  # lifecycle, and direct terminal overlay plumbing that is not meaningfully
  # line-coverable in a CI coverage run; the underlying app/runtime behavior
  # remains covered through focused module tests.
  local ignored_files
  ignored_files='(^|/)src/bin/|(^|/)src/main\.rs$|(^|/)examples/|(^|/)tests/.*perf.*\.rs$|(^|/)crates/casars-imager/src/lib\.rs$|(^|/)crates/casars/src/lib\.rs$|(^|/)crates/casa-test-support/src/|(^|/)crates/casars-python/src/'

  local feature_args=()
  if cargo run -q -p casa-test-support --bin casatestdata-preflight -- \
    --tier slow-parity \
    --require measurementset/vla/ngc5921.ms; then
    # Imaging parity is a separate release-quality gate in test-slow.sh. Do not
    # make source coverage depend on whether this host happens to expose CASA:
    # casars-imager/src/lib.rs is excluded from the coverage denominator, and
    # enabling its slow tests here can execute host-only parity modes that do not
    # contribute coverage to the measured source set.
    echo "==> Including slow msexplore parity suite in coverage"
    feature_args=(--features casa-ms/slow-tests)
  else
    echo "coverage warning: slow msexplore dataset unavailable; coverage omits its parity suite" >&2
  fi

  cargo llvm-cov \
    --workspace \
    "${feature_args[@]}" \
    --exclude casars-python \
    --exclude casa-test-support \
    --ignore-filename-regex "$ignored_files" \
    --fail-under-lines "$coverage_fail_under" \
    --color never \
    -- \
    --test-threads=1
}

coverage_log="$(mktemp -t casa-rs-llvm-cov.XXXXXX.log)"
cleanup() {
  rm -f "$coverage_log"
}
trap cleanup EXIT

: > "$coverage_log"
set +e
run_llvm_cov 2>&1 | tee "$coverage_log"
coverage_status=${PIPESTATUS[0]}
set -e

if (( coverage_status != 0 )); then
  exit "$coverage_status"
fi

line_coverage="$(
  awk '/^TOTAL[[:space:]]/ { value = $10; gsub(/%/, "", value); print value }' "$coverage_log" | tail -n 1
)"

if [[ -z "$line_coverage" ]]; then
  echo "coverage warning: could not parse TOTAL line coverage from cargo llvm-cov output" >&2
  exit 0
fi

if awk -v got="$line_coverage" -v target="$coverage_target" 'BEGIN { exit !((got + 0) < (target + 0)) }'; then
  echo "coverage warning: line coverage ${line_coverage}% is below target ${coverage_target}% but above enforced ${coverage_fail_under}%" >&2
else
  echo "coverage target satisfied: line coverage ${line_coverage}% >= ${coverage_target}%"
fi
