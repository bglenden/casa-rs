#!/usr/bin/env bash
set -euo pipefail

tutorial_root="${CASA_RS_TUTORIAL_DATA_ROOT:-$HOME/SoftwareProjects/casa-tutorial-data}"
raw_dir="$tutorial_root/tutorial-parity/alma/m100/band3-combine/raw"
extract_dir="$raw_dir/extracted"
manifest="$raw_dir/SHA256SUMS"

usage() {
  cat <<USAGE
Usage:
  $(basename "$0") status
  $(basename "$0") verify [KEY ...]
  $(basename "$0") download [KEY ...]
  $(basename "$0") extract [KEY ...]

Keys:
  m100-12m
  m100-7m
  m100-tp
  m100-aca-reference

Environment:
  CASA_RS_TUTORIAL_DATA_ROOT  Tutorial data root.

Notes:
  status and verify are safe to run while external downloads are in progress.
  download uses ordinary curl resume. For the 12m archive, the GLENDENNING
  downloader may be better because it can resume durable byte-range chunks.
  verify and extract only run for archives whose final size matches the
  registry entry. For the M100 guide route, the TP image can come from
  m100-aca-reference; m100-tp is needed only when reproducing TP calibration
  rather than using the guide's downloadable TP image.
USAGE
}

dataset_manifest() {
  cat <<'DATASETS'
# key|url|filename|bytes|expected_root
m100-12m|https://almascience.nrao.edu/almadata/sciver/M100Band3_12m/M100_Band3_12m_CalibratedData.tgz|M100_Band3_12m_CalibratedData.tgz|15580494468|M100_Band3_12m_CalibratedData
m100-7m|https://almascience.nrao.edu/almadata/sciver/M100Band3ACA/M100_Band3_7m_CalibratedData.tgz|M100_Band3_7m_CalibratedData.tgz|9774558254|M100_Band3_7m_CalibratedData
m100-tp|https://almascience.nrao.edu/almadata/sciver/M100Band3ACA/M100_Band3_TP_CalibratedData_5.1.tgz|M100_Band3_TP_CalibratedData_5.1.tgz|14372792248|M100_Band3_TP_CalibratedData_5.1
m100-aca-reference|https://almascience.nrao.edu/almadata/sciver/M100Band3ACA/M100_Band3_ACA_ReferenceImages_5.1.tgz|M100_Band3_ACA_ReferenceImages_5.1.tgz|24775689|M100_Band3_ACA_ReferenceImages_5.1
DATASETS
}

file_size() {
  if [[ -f "$1" ]]; then
    stat -f '%z' "$1"
  else
    echo 0
  fi
}

selected() {
  local key="$1"
  shift
  if (($# == 0)); then
    return 0
  fi
  local wanted
  for wanted in "$@"; do
    [[ "$key" == "$wanted" ]] && return 0
  done
  return 1
}

status_one() {
  local key="$1"
  local filename="$2"
  local expected_bytes="$3"
  local expected_root="$4"
  local archive="$raw_dir/$filename"
  local actual_bytes
  actual_bytes="$(file_size "$archive")"

  local state="missing"
  if [[ "$actual_bytes" == "$expected_bytes" ]]; then
    state="complete"
  elif ((actual_bytes > 0)); then
    state="partial"
  fi

  local extract_state="not-extracted"
  if [[ -d "$extract_dir/$expected_root" ]]; then
    extract_state="extracted"
  fi

  printf '%-18s %-9s %12s/%s %-13s %s\n' \
    "$key" "$state" "$actual_bytes" "$expected_bytes" "$extract_state" "$archive"
}

status_all() {
  mkdir -p "$raw_dir"
  local key url filename expected_bytes expected_root
  while IFS='|' read -r key url filename expected_bytes expected_root; do
    [[ -z "${key:-}" || "$key" == \#* ]] && continue
    status_one "$key" "$filename" "$expected_bytes" "$expected_root"
  done < <(dataset_manifest)

  local chunk_root="$raw_dir/chunks/M100_Band3_12m_CalibratedData.tgz"
  if [[ -d "$chunk_root" ]]; then
    local complete_chunks partial_chunks chunk_bytes
    complete_chunks="$(find "$chunk_root" -maxdepth 1 -type f -name '*.part' | wc -l | tr -d ' ')"
    partial_chunks="$(find "$chunk_root" -maxdepth 1 -type f -name '*.tmp' | wc -l | tr -d ' ')"
    chunk_bytes="$(find "$chunk_root" -maxdepth 1 -type f \( -name '*.part' -o -name '*.tmp' \) -exec stat -f '%z' {} \; | awk '{s += $1} END {print s + 0}')"
    printf '%-18s chunks    %12s/%s complete=%s partial=%s %s\n' \
      "m100-12m" "$chunk_bytes" "15580494468" "$complete_chunks" "$partial_chunks" "$chunk_root"
  fi
}

verify_selected() {
  mkdir -p "$raw_dir"
  : > "$manifest.tmp"
  local key url filename expected_bytes expected_root archive actual_bytes failed=0
  while IFS='|' read -r key url filename expected_bytes expected_root; do
    [[ -z "${key:-}" || "$key" == \#* ]] && continue
    selected "$key" "$@" || continue
    archive="$raw_dir/$filename"
    actual_bytes="$(file_size "$archive")"
    if [[ "$actual_bytes" != "$expected_bytes" ]]; then
      echo "incomplete: $key expected $expected_bytes actual $actual_bytes path=$archive" >&2
      failed=1
      continue
    fi
    shasum -a 256 "$archive" | tee -a "$manifest.tmp"
  done < <(dataset_manifest)
  mv "$manifest.tmp" "$manifest"
  if ((failed != 0)); then
    exit 1
  fi
  echo "Wrote $manifest"
}

download_selected() {
  mkdir -p "$raw_dir"
  local key url filename expected_bytes expected_root output actual_bytes failed=0
  while IFS='|' read -r key url filename expected_bytes expected_root; do
    [[ -z "${key:-}" || "$key" == \#* ]] && continue
    selected "$key" "$@" || continue
    output="$raw_dir/$filename"
    actual_bytes="$(file_size "$output")"
    if [[ "$actual_bytes" == "$expected_bytes" ]]; then
      echo "Already complete: $output"
      continue
    fi
    echo "Fetching $key"
    echo "  url: $url"
    echo "  output: $output"
    curl --fail --location --continue-at - --output "$output" "$url"
    actual_bytes="$(file_size "$output")"
    if [[ "$actual_bytes" != "$expected_bytes" ]]; then
      echo "Size mismatch for $output" >&2
      echo "expected $expected_bytes bytes" >&2
      echo "actual   $actual_bytes bytes" >&2
      failed=1
    fi
  done < <(dataset_manifest)
  if ((failed != 0)); then
    exit 1
  fi
}

extract_selected() {
  mkdir -p "$extract_dir"
  local key url filename expected_bytes expected_root archive actual_bytes target marker failed=0
  while IFS='|' read -r key url filename expected_bytes expected_root; do
    [[ -z "${key:-}" || "$key" == \#* ]] && continue
    selected "$key" "$@" || continue
    archive="$raw_dir/$filename"
    actual_bytes="$(file_size "$archive")"
    target="$extract_dir/$expected_root"
    marker="$target/.casa-rs-extract-complete"
    if [[ "$actual_bytes" != "$expected_bytes" ]]; then
      echo "Cannot extract incomplete archive: $key expected $expected_bytes actual $actual_bytes" >&2
      failed=1
      continue
    fi
    if [[ -f "$marker" ]]; then
      echo "Already extracted: $target"
      continue
    fi
    rm -rf "$target"
    mkdir -p "$target"
    echo "Extracting $archive into $target"
    tar -xzf "$archive" -C "$target"
    date -u '+%Y-%m-%dT%H:%M:%SZ' > "$marker"
  done < <(dataset_manifest)
  if ((failed != 0)); then
    exit 1
  fi
}

case "${1:-status}" in
  status)
    status_all
    ;;
  verify)
    shift || true
    verify_selected "$@"
    ;;
  download)
    shift || true
    download_selected "$@"
    ;;
  extract)
    shift || true
    extract_selected "$@"
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage >&2
    exit 64
    ;;
esac
