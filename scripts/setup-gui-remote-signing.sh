#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later

set -euo pipefail

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "remote GUI signing setup requires macOS" >&2
  exit 2
fi
if [[ -n "${SSH_CONNECTION:-}" ]]; then
  echo "stable GUI signing setup must run in Terminal in the worker's logged-in desktop session" >&2
  echo "open Terminal through Screen Sharing and run this script there once" >&2
  exit 2
fi

identity_name="${CASA_RS_GUI_TEST_SIGNING_NAME:-casa-rs GUI Test Worker}"
config_path="${CASA_RS_GUI_TEST_SIGNING_CONFIG:-$HOME/.config/casa-rs/gui-worker-signing.env}"
keychain_path="${CASA_RS_GUI_TEST_SIGNING_KEYCHAIN:-$HOME/Library/Keychains/casa-rs-gui-worker.keychain-db}"

verify_configured_identity() {
  # shellcheck disable=SC1090 -- this is the private config created below.
  source "$config_path"
  : "${CASA_RS_GUI_TEST_CODE_SIGN_IDENTITY:?missing code-sign identity in $config_path}"
  : "${CASA_RS_GUI_TEST_CODE_SIGN_KEYCHAIN:?missing keychain path in $config_path}"
  : "${CASA_RS_GUI_TEST_CODE_SIGN_KEYCHAIN_PASSWORD:?missing keychain password in $config_path}"
  /usr/bin/security unlock-keychain \
    -p "$CASA_RS_GUI_TEST_CODE_SIGN_KEYCHAIN_PASSWORD" \
    "$CASA_RS_GUI_TEST_CODE_SIGN_KEYCHAIN"
  /usr/bin/security find-identity -v -p codesigning \
    "$CASA_RS_GUI_TEST_CODE_SIGN_KEYCHAIN" |
    /usr/bin/grep -Fq "$CASA_RS_GUI_TEST_CODE_SIGN_IDENTITY"
}

if [[ -f "$config_path" ]]; then
  if verify_configured_identity; then
    echo "==> Stable GUI-worker signing identity is already configured"
    echo "==> Config: $config_path"
    echo "==> Identity: $CASA_RS_GUI_TEST_CODE_SIGN_IDENTITY"
    exit 0
  fi
  echo "existing GUI-worker signing config is invalid: $config_path" >&2
  exit 2
fi

if [[ -e "$keychain_path" ]]; then
  echo "refusing to replace an unconfigured keychain: $keychain_path" >&2
  exit 2
fi

command -v /usr/bin/openssl >/dev/null
command -v /usr/bin/security >/dev/null

umask 077
mkdir -p "$(dirname "$config_path")" "$(dirname "$keychain_path")"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/casa-rs-gui-signing.XXXXXX")"
created_keychain=0
setup_complete=0
cleanup() {
  rm -rf "$work_dir"
  if [[ "$created_keychain" == "1" && "$setup_complete" != "1" ]]; then
    /usr/bin/security delete-keychain "$keychain_path" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

keychain_password="$(/usr/bin/uuidgen | /usr/bin/tr -d '-')$(/usr/bin/uuidgen | /usr/bin/tr -d '-')"
archive_password="$(/usr/bin/uuidgen | /usr/bin/tr -d '-')"
private_key="$work_dir/worker.key"
certificate="$work_dir/worker.crt"
archive="$work_dir/worker.p12"

echo "==> Creating a dedicated stable GUI-worker signing identity"
/usr/bin/openssl req -new -newkey rsa:2048 -x509 -sha256 -days 3650 -nodes \
  -subj "/CN=$identity_name/O=casa-rs Development" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,digitalSignature,keyCertSign" \
  -addext "extendedKeyUsage=codeSigning" \
  -keyout "$private_key" \
  -out "$certificate" >/dev/null 2>&1
/usr/bin/openssl pkcs12 -export \
  -name "$identity_name" \
  -inkey "$private_key" \
  -in "$certificate" \
  -out "$archive" \
  -passout "pass:$archive_password"

/usr/bin/security create-keychain -p "$keychain_password" "$keychain_path"
created_keychain=1
/usr/bin/security set-keychain-settings -lut 21600 "$keychain_path"
/usr/bin/security unlock-keychain -p "$keychain_password" "$keychain_path"
/usr/bin/security import "$archive" \
  -k "$keychain_path" \
  -f pkcs12 \
  -P "$archive_password" \
  -T /usr/bin/codesign >/dev/null
/usr/bin/security set-key-partition-list \
  -S apple-tool:,apple:,codesign: \
  -s \
  -k "$keychain_password" \
  "$keychain_path" >/dev/null
/usr/bin/security add-trusted-cert \
  -r trustRoot \
  -k "$keychain_path" \
  "$certificate"

search_keychains=("$keychain_path")
while IFS= read -r listed_keychain; do
  listed_keychain="${listed_keychain#${listed_keychain%%[![:space:]]*}}"
  listed_keychain="${listed_keychain#\"}"
  listed_keychain="${listed_keychain%\"}"
  if [[ -n "$listed_keychain" && "$listed_keychain" != "$keychain_path" ]]; then
    search_keychains+=("$listed_keychain")
  fi
done < <(/usr/bin/security list-keychains -d user)
/usr/bin/security list-keychains -d user -s "${search_keychains[@]}"

identity_hash="$(
  /usr/bin/security find-identity -v -p codesigning "$keychain_path" |
    /usr/bin/awk -v name="$identity_name" 'index($0, "\"" name "\"") { print $2; exit }'
)"
if [[ -z "$identity_hash" ]]; then
  echo "the generated GUI-worker code-signing identity is not valid" >&2
  exit 1
fi

{
  printf 'CASA_RS_GUI_TEST_CODE_SIGN_CERTIFICATE_NAME=%q\n' "$identity_name"
  printf 'CASA_RS_GUI_TEST_CODE_SIGN_IDENTITY=%q\n' "$identity_hash"
  printf 'CASA_RS_GUI_TEST_CODE_SIGN_KEYCHAIN=%q\n' "$keychain_path"
  printf 'CASA_RS_GUI_TEST_CODE_SIGN_KEYCHAIN_PASSWORD=%q\n' "$keychain_password"
} >"$config_path"
chmod 600 "$config_path"
setup_complete=1

echo "==> Stable GUI-worker signing identity created"
echo "==> Config: $config_path"
echo "==> Identity: $identity_hash ($identity_name)"
echo "==> The stored password protects only this dedicated test keychain"
