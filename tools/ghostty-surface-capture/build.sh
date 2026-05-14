#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
source_file="$script_dir/ghostty_surface_capture.swift"
output="${1:-/private/tmp/ghostty-surface-capture}"
xcframework="${GHOSTTYKIT_XCFRAMEWORK:-/private/tmp/ghostty-source/macos/GhosttyKit.xcframework}"
slice="$xcframework/macos-arm64_x86_64"

if [[ ! -f "$slice/ghostty-internal.a" ]]; then
  echo "missing GhosttyKit static library: $slice/ghostty-internal.a" >&2
  echo "set GHOSTTYKIT_XCFRAMEWORK=/path/to/GhosttyKit.xcframework" >&2
  exit 2
fi

mkdir -p /private/tmp/ghostty-swift-module-cache "$(dirname "$output")"

CLANG_MODULE_CACHE_PATH=/private/tmp/ghostty-swift-module-cache \
swiftc "$source_file" \
  -I "$slice/Headers" \
  "$slice/ghostty-internal.a" \
  -framework AppKit \
  -framework Carbon \
  -framework QuartzCore \
  -framework CoreImage \
  -framework ImageIO \
  -framework IOSurface \
  -framework UniformTypeIdentifiers \
  -lc++ \
  -o "$output"

echo "$output"
