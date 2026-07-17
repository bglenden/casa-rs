#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$ROOT_DIR/../.." && pwd)"
CODEX_COMMAND="${CASA_RS_CODEX_COMMAND:-codex}"
PYTHON_COMMAND="${CASA_RS_GUI_TEST_PYTHON:-$("$REPO_ROOT/scripts/resolve-python.sh" 3.10)}"
TARGET_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}"
if [[ "$TARGET_DIR" != /* ]]; then
  TARGET_DIR="$REPO_ROOT/$TARGET_DIR"
fi

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "notebook production round-trip acceptance requires macOS with an interactive GUI session" >&2
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
"$PYTHON_COMMAND" -c 'import matplotlib, numpy; print(f"matplotlib={matplotlib.__version__} numpy={numpy.__version__}")'
echo "==> Building real simobserve and msexplore task helpers before the exclusive GUI window"
CARGO_INCREMENTAL=0 cargo build --manifest-path "$REPO_ROOT/Cargo.toml" \
  -p casa-ms --bin simobserve --bin msexplore
echo "==> Authentication mode: existing ChatGPT subscription; metered API environment removed"

unset OPENAI_API_KEY AZURE_OPENAI_API_KEY OPENAI_BASE_URL
export CASA_RS_GUI_TEST_PYTHON="$PYTHON_COMMAND"
export CASA_RS_GUI_TEST_ONLY="CasarsMacUITests/CasarsMacUITests/testOptInProductionNotebookTaskPythonPlotRoundTrip"
export CASA_RS_GUI_TEST_RESULT_BUNDLE="${CASA_RS_GUI_TEST_ARTIFACT_ROOT:-$ROOT_DIR/.gui-test}/NotebookRoundTripGUI.xcresult"

LIVE_GATE="$ROOT_DIR/.gui-test/notebook-roundtrip-gui.enabled"
LIVE_PROJECT_BASE="${CASA_RS_GUI_TEST_PROJECT_BASE:-$HOME/.casa-rs-gui-tests}"
LIVE_PROJECT="$LIVE_PROJECT_BASE/casars-wave5c-roundtrip-retained"
PASS_RECEIPT="$LIVE_PROJECT/.notebook-roundtrip-gui.passed"
EVIDENCE_REPORT="${CASA_RS_GUI_TEST_ARTIFACT_ROOT:-$ROOT_DIR/.gui-test}/NotebookRoundTripGUI.report.json"
TEST_EVIDENCE_REPORT="$LIVE_PROJECT/.notebook-roundtrip-gui.report.json"
RESUME_AFTER_TASK="${CASA_RS_NOTEBOOK_ROUNDTRIP_RESUME_AFTER_TASK:-0}"
mkdir -p "$ROOT_DIR/.gui-test"
mkdir -p "$LIVE_PROJECT_BASE"
rm -f "$LIVE_GATE" "$PASS_RECEIPT" "$EVIDENCE_REPORT" "$TEST_EVIDENCE_REPORT"
if [[ "$RESUME_AFTER_TASK" == "1" ]]; then
  [[ -d "$LIVE_PROJECT" ]] || {
    echo "cannot resume Wave 5C: retained project is missing at $LIVE_PROJECT" >&2
    exit 2
  }
else
  rm -rf "$LIVE_PROJECT"
fi
/usr/bin/plutil -create xml1 "$LIVE_GATE"
/usr/bin/plutil -insert agentCommand -string "$(command -v "$CODEX_COMMAND")" "$LIVE_GATE"
/usr/bin/plutil -insert home -string "$HOME" "$LIVE_GATE"
/usr/bin/plutil -insert codexHome -string "${CODEX_HOME:-$HOME/.codex}" "$LIVE_GATE"
/usr/bin/plutil -insert path -string "$PATH" "$LIVE_GATE"
/usr/bin/plutil -insert pythonCommand -string "$PYTHON_COMMAND" "$LIVE_GATE"
/usr/bin/plutil -insert projectRoot -string "$LIVE_PROJECT" "$LIVE_GATE"
/usr/bin/plutil -insert passReceipt -string "$PASS_RECEIPT" "$LIVE_GATE"
/usr/bin/plutil -insert evidenceReport -string "$TEST_EVIDENCE_REPORT" "$LIVE_GATE"
/usr/bin/plutil -insert repoRoot -string "$REPO_ROOT" "$LIVE_GATE"
/usr/bin/plutil -insert repoRevision -string "$(git -C "$REPO_ROOT" rev-parse HEAD)" "$LIVE_GATE"
/usr/bin/plutil -insert simobserveCommand -string "$TARGET_DIR/debug/simobserve" "$LIVE_GATE"
/usr/bin/plutil -insert msexploreCommand -string "$TARGET_DIR/debug/msexplore" "$LIVE_GATE"
/usr/bin/plutil -insert resumeAfterTask -string "$([[ "$RESUME_AFTER_TASK" == "1" ]] && echo true || echo false)" "$LIVE_GATE"
trap 'rm -f "$LIVE_GATE" "$PASS_RECEIPT"' EXIT

if ! bash "$ROOT_DIR/script/test_gui.sh"; then
  echo "==> Failed round-trip project retained for diagnosis: $LIVE_PROJECT" >&2
  exit 1
fi
if [[ ! -f "$PASS_RECEIPT" ]]; then
  echo "notebook production round-trip acceptance did not write its success receipt" >&2
  exit 1
fi
if [[ "$RESUME_AFTER_TASK" != "1" ]]; then
  [[ -f "$TEST_EVIDENCE_REPORT" ]] || {
    echo "notebook production round-trip acceptance did not write its evidence report" >&2
    exit 1
  }
  cp "$TEST_EVIDENCE_REPORT" "$EVIDENCE_REPORT"
  "$PYTHON_COMMAND" -m json.tool "$EVIDENCE_REPORT" >/dev/null
  echo "==> Durable sanitized evidence: $EVIDENCE_REPORT"
fi
rm -rf "$LIVE_PROJECT"
echo "==> Notebook production round-trip acceptance receipt: PASS"
