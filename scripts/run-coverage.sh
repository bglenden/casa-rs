#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/run-coverage.sh [--ci-like]

Runs the workspace tarpaulin coverage gate.

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

tarpaulin_timeout="${TARPAULIN_TIMEOUT:-300}"
tarpaulin_attempts="${TARPAULIN_ATTEMPTS:-2}"

if [[ "$ci_like" == "true" ]]; then
  echo "==> Running CI-like coverage (forcing pkg-config casacore lookup to fail)"
  # `has_casacore_cpp` is decided in build.rs, so clear existing artifacts before
  # switching to the CI-like no-casacore configuration.
  cargo clean
  export PKG_CONFIG=/usr/bin/false
fi

run_tarpaulin() {
  # Keep the coverage gate focused on shipped code plus functional tests.
  # Large perf/profile harnesses are useful for benchmarking, but they make
  # line-coverage drift when benchmarks are added, renamed, or explicitly ignored.
  # Thin binary entrypoints are exercised indirectly through library/runtime tests
  # and otherwise add denominator without meaningful extra signal in tarpaulin.
  #
  # The real-fixture tablebrowser traversal test is exercised in the normal
  # `cargo test --workspace` gate. Under tarpaulin it can complete successfully
  # and then leave the coverage runner in a generic post-test abort state, so
  # skip it only for coverage collection.
  cargo tarpaulin \
    --workspace \
    --exclude casars-python \
    --timeout "$tarpaulin_timeout" \
    --out Stdout \
    --fail-under 75 \
    --exclude-files \
    '*/src/bin/*' \
    '*/src/main.rs' \
    '*/examples/*' \
    '*/tests/*perf*.rs' \
    -- \
    --skip tablebrowser::tests::browser_traverses_real_fixture_tables_and_cells
}

tarpaulin_log="$(mktemp -t casa-rs-tarpaulin.XXXXXX.log)"
cleanup() {
  rm -f "$tarpaulin_log"
}
trap cleanup EXIT

attempt=1
while (( attempt <= tarpaulin_attempts )); do
  if (( attempt > 1 )); then
    echo "==> Retrying tarpaulin coverage (attempt $attempt/$tarpaulin_attempts)"
    rm -rf target/tarpaulin
  fi

  : > "$tarpaulin_log"
  set +e
  run_tarpaulin 2>&1 | tee "$tarpaulin_log"
  tarpaulin_status=${PIPESTATUS[0]}
  set -e

  if (( tarpaulin_status == 0 )); then
    exit 0
  fi

  if (( attempt == tarpaulin_attempts )); then
    exit "$tarpaulin_status"
  fi

  echo "==> cargo tarpaulin exited with status $tarpaulin_status"
  if grep -q 'Error: "Test failed during run"' "$tarpaulin_log"; then
    echo "==> tarpaulin reported a generic run failure; retrying once"
  else
    echo "==> retrying coverage once before treating this as a hard failure"
  fi

  attempt=$((attempt + 1))
done
