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

if [[ "$ci_like" == "true" ]]; then
  echo "==> Running CI-like coverage (forcing pkg-config casacore lookup to fail)"
  # `has_casacore_cpp` is decided in build.rs, so clear existing artifacts before
  # switching to the CI-like no-casacore configuration.
  cargo clean
  export PKG_CONFIG=/usr/bin/false
fi

# Keep the coverage gate focused on shipped code plus functional tests.
# Large perf/profile harnesses are useful for benchmarking, but they make
# line-coverage drift when benchmarks are added, renamed, or explicitly ignored.
cargo tarpaulin \
  --workspace \
  --timeout "$tarpaulin_timeout" \
  --out Stdout \
  --fail-under 75 \
  --exclude-files \
  '*/examples/*' \
  '*/tests/*perf*.rs'
