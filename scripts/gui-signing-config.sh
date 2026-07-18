#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later

# Shared parser and validator for the private GUI-worker signing configuration.
gui_load_signing_config() {
  local config_path="$1"
  if [[ ! -f "$config_path" ]]; then
    echo "stable GUI-worker signing is not configured: $config_path" >&2
    return 2
  fi
  # shellcheck disable=SC1090 -- private config created by setup-gui-remote-signing.sh.
  source "$config_path"
  : "${CASA_RS_GUI_TEST_CODE_SIGN_IDENTITY:?missing code-sign identity in $config_path}"
  : "${CASA_RS_GUI_TEST_CODE_SIGN_KEYCHAIN:?missing keychain path in $config_path}"
  : "${CASA_RS_GUI_TEST_CODE_SIGN_KEYCHAIN_PASSWORD:?missing keychain password in $config_path}"
  if [[ "$CASA_RS_GUI_TEST_CODE_SIGN_KEYCHAIN" != /* ]]; then
    echo "GUI-worker keychain path must be absolute" >&2
    return 2
  fi
}

gui_verify_signing_identity() {
  /usr/bin/security unlock-keychain \
    -p "$CASA_RS_GUI_TEST_CODE_SIGN_KEYCHAIN_PASSWORD" \
    "$CASA_RS_GUI_TEST_CODE_SIGN_KEYCHAIN"
  /usr/bin/security find-identity -v -p codesigning \
    "$CASA_RS_GUI_TEST_CODE_SIGN_KEYCHAIN" |
    /usr/bin/grep -Fq "$CASA_RS_GUI_TEST_CODE_SIGN_IDENTITY"
}
