#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later

# Build the release FFTW dependency, including the architecture's SIMD kernels.
# Keep the exact upstream source and this recipe beside the installed archives.
set -euo pipefail

if [[ $# != 1 || "$1" != /* || "$1" == / ]]; then
  echo "usage: bash scripts/build-fftw.sh /absolute/durable/install-prefix" >&2
  exit 2
fi

prefix="$1"
version=3.3.11
source_sha256=5630c24cdeb33b131612f7eb4b1a9934234754f9f388ff8617458d0be6f239a1
case "$(uname -m)" in
  arm64|aarch64) simd=(--enable-neon) ;;
  x86_64|amd64) simd=(--enable-sse2 --enable-avx --enable-avx2) ;;
  *) echo "No validated FFTW SIMD configuration for $(uname -m)" >&2; exit 1 ;;
esac

# Never replace another installation or reuse a partly built dependency.
if [[ -e "$prefix" ]]; then
  echo "FFTW prefix already exists; choose a new prefix: $prefix" >&2
  exit 1
fi
mkdir -p "$prefix/source"
archive="$prefix/source/fftw-$version.tar.gz"
curl --fail --location --retry 3 "https://www.fftw.org/fftw-$version.tar.gz" --output "$archive"
if command -v sha256sum >/dev/null 2>&1; then
  actual_sha256="$(sha256sum "$archive" | awk '{print $1}')"
else
  actual_sha256="$(shasum -a 256 "$archive" | awk '{print $1}')"
fi
if [[ "$actual_sha256" != "$source_sha256" ]]; then
  echo "FFTW source checksum mismatch" >&2
  exit 1
fi
tar -xzf "$archive" -C "$prefix/source"
source_dir="$prefix/source/fftw-$version"
cp "$0" "$prefix/source/build-fftw.sh"

for precision in double single; do
  mkdir "$prefix/build-$precision"
  args=(--prefix="$prefix" --enable-static --disable-shared --with-pic
        --enable-threads --disable-fortran "${simd[@]}")
  if [[ "$precision" == single ]]; then
    args+=(--enable-single)
  fi
  (
    cd "$prefix/build-$precision"
    "$source_dir/configure" "${args[@]}"
    make -j2
    make install
  )
done

echo "FFTW $version installed with ${simd[*]}"
echo "Use PKG_CONFIG_PATH=$prefix/lib/pkgconfig when building casa-rs."
