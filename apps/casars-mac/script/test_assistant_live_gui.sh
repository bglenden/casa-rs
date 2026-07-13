#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$ROOT_DIR/../.." && pwd)"
CODEX_COMMAND="${CASA_RS_CODEX_COMMAND:-codex}"
PYTHON_COMMAND="${CASA_RS_GUI_TEST_PYTHON:-$($REPO_ROOT/scripts/resolve-python.sh 3.10)}"

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "assistant live GUI acceptance requires macOS with an interactive GUI session" >&2
  exit 2
fi

command -v "$CODEX_COMMAND" >/dev/null
command -v "$PYTHON_COMMAND" >/dev/null

echo "==> CASA-RS revision: $(git -C "$REPO_ROOT" rev-parse HEAD)"
echo "==> Codex command: $(command -v "$CODEX_COMMAND")"
"$CODEX_COMMAND" --version
"$CODEX_COMMAND" login status
echo "==> Scientific Python: $PYTHON_COMMAND"
"$PYTHON_COMMAND" --version
echo "==> Authentication mode: existing ChatGPT subscription; metered API environment removed"

unset OPENAI_API_KEY AZURE_OPENAI_API_KEY OPENAI_BASE_URL
export CASA_RS_GUI_TEST_PYTHON="$PYTHON_COMMAND"
export CASA_RS_GUI_TEST_ONLY="CasarsMacUITests/CasarsMacUITests/testOptInProductionAssistantSubscriptionGUIResume"
export CASA_RS_GUI_TEST_RESULT_BUNDLE="${CASA_RS_GUI_TEST_ARTIFACT_ROOT:-$ROOT_DIR/.gui-test}/AssistantLiveGUI.xcresult"

LIVE_GATE="$ROOT_DIR/.gui-test/assistant-live-gui.enabled"
LIVE_PROJECT="$HOME/Library/Containers/org.casa-rs.casars-mac.uitests.xctrunner/Data/tmp/casars-wave5a-live-retained"
PASS_RECEIPT="$LIVE_PROJECT/.assistant-live-gui.passed"
mkdir -p "$ROOT_DIR/.gui-test"
rm -f "$LIVE_GATE" "$PASS_RECEIPT"
rm -rf "$LIVE_PROJECT"
/usr/bin/plutil -create xml1 "$LIVE_GATE"
/usr/bin/plutil -insert agentCommand -string "$(command -v "$CODEX_COMMAND")" "$LIVE_GATE"
/usr/bin/plutil -insert home -string "$HOME" "$LIVE_GATE"
/usr/bin/plutil -insert codexHome -string "${CODEX_HOME:-$HOME/.codex}" "$LIVE_GATE"
/usr/bin/plutil -insert path -string "$PATH" "$LIVE_GATE"
/usr/bin/plutil -insert pythonCommand -string "$PYTHON_COMMAND" "$LIVE_GATE"
/usr/bin/plutil -insert projectRoot -string "$LIVE_PROJECT" "$LIVE_GATE"
/usr/bin/plutil -insert passReceipt -string "$PASS_RECEIPT" "$LIVE_GATE"
trap 'rm -f "$LIVE_GATE" "$PASS_RECEIPT"' EXIT

if ! bash "$ROOT_DIR/script/test_gui.sh"; then
  echo "==> Failed live project retained for diagnosis: $LIVE_PROJECT" >&2
  CASA_RS_LIVE_TRANSCRIPT_PROJECT="$LIVE_PROJECT" swift test \
    --package-path "$ROOT_DIR" \
    --filter AssistantDiscussionTests/testOptInRetainedLiveTranscriptLoadsThroughProductionBoundary || true
  exit 1
fi
if [[ ! -f "$PASS_RECEIPT" ]]; then
  echo "live GUI acceptance did not write its success receipt" >&2
  exit 1
fi
rm -rf "$LIVE_PROJECT"
echo "==> Live GUI acceptance receipt: PASS"
