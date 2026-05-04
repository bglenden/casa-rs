#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
out_root="${1:-$repo_root/target/frontend-bindings}"
python_out="$out_root/python"
swift_out="$out_root/swift"

cd "$repo_root"

case "$(uname -s)" in
  Darwin)
    lib_name="libcasars_frontend_services.dylib"
    ;;
  Linux)
    lib_name="libcasars_frontend_services.so"
    ;;
  *)
    echo "unsupported platform for frontend binding generation: $(uname -s)" >&2
    exit 2
    ;;
esac

lib_path="$repo_root/target/debug/$lib_name"

echo "==> Building Rust frontend services"
cargo build -p casars-frontend-services

echo "==> Generating Python UniFFI bindings"
mkdir -p "$python_out"
cargo run -p casars-frontend-services --bin casars-frontend-bindgen -- python "$lib_path" "$python_out"
cp "$lib_path" "$python_out/$lib_name"

echo "==> Generating Swift UniFFI bindings"
mkdir -p "$swift_out"
cargo run -p casars-frontend-services --bin casars-frontend-bindgen -- swift "$lib_path" "$swift_out"

if [[ -d "$repo_root/apps/casars-mac/Sources/CasarsFrontendServices" ]]; then
  cp "$swift_out/CasarsFrontendServices.swift" \
    "$repo_root/apps/casars-mac/Sources/CasarsFrontendServices/CasarsFrontendServices.swift"
  cp "$swift_out/CasarsFrontendServicesFFI.h" \
    "$repo_root/apps/casars-mac/Sources/CasarsFrontendServicesFFI/CasarsFrontendServicesFFI.h"
  cp "$swift_out/CasarsFrontendServicesFFI.modulemap" \
    "$repo_root/apps/casars-mac/Sources/CasarsFrontendServicesFFI/module.modulemap"
fi

echo "generated bindings under $out_root"
