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
  echo "tutorial production journey acceptance requires macOS with an interactive GUI session" >&2
  exit 2
fi

command -v "$CODEX_COMMAND" >/dev/null
command -v "$PYTHON_COMMAND" >/dev/null
command -v curl >/dev/null

TEMPLATE_ROOT="$REPO_ROOT/resources/tutorials/tw-hya-first-look"
TUTORIAL_MANIFEST="$TEMPLATE_ROOT/tutorial.toml"
TUTORIAL_MARKDOWN="$TEMPLATE_ROOT/tutorial.md"
SOURCE_URI="https://bulk.cv.nrao.edu/almadata/public/casaguides/FirstLook_TWHya_Band7_6.6.1/twhya_calibrated.ms.tar"
EXPECTED_SIZE=435742720
EXPECTED_SHA256="f0cfeee5b9dec09ac9ed4d3e4e048d5eb28023c11cbc8295c09ddefe6b8a97b2"

[[ -f "$TUTORIAL_MANIFEST" && -f "$TUTORIAL_MARKDOWN" ]]
grep -Fq "$SOURCE_URI" "$TUTORIAL_MANIFEST"
grep -Fq "expected_size_bytes = $EXPECTED_SIZE" "$TUTORIAL_MANIFEST"
grep -Fq "sha256 = \"$EXPECTED_SHA256\"" "$TUTORIAL_MANIFEST"

echo "==> CASA-RS revision: $(git -C "$REPO_ROOT" rev-parse HEAD)"
echo "==> Tutorial template: $TEMPLATE_ROOT"
echo "==> Tutorial manifest SHA-256: $(shasum -a 256 "$TUTORIAL_MANIFEST" | awk '{print $1}')"
echo "==> Tutorial Markdown SHA-256: $(shasum -a 256 "$TUTORIAL_MARKDOWN" | awk '{print $1}')"
echo "==> Preflighting NRAO source without downloading the dataset"
headers="$(curl -fsSIL --max-time 30 "$SOURCE_URI")"
printf '%s\n' "$headers" | grep -Eiq '^HTTP/[^ ]+ 200'
printf '%s\n' "$headers" | grep -Eiq "^Content-Length: ${EXPECTED_SIZE}?$"
echo "==> Codex command: $(command -v "$CODEX_COMMAND")"
"$CODEX_COMMAND" --version
"$CODEX_COMMAND" login status
echo "==> Scientific Python: $PYTHON_COMMAND"
"$PYTHON_COMMAND" --version
"$PYTHON_COMMAND" -c 'import matplotlib, numpy; print(f"matplotlib={matplotlib.__version__} numpy={numpy.__version__}")'
echo "==> Building imager and MeasurementSet explorer helpers before the exclusive GUI window"
CARGO_INCREMENTAL=0 cargo build --manifest-path "$REPO_ROOT/Cargo.toml" \
  -p casars-imager --bin casars-imager \
  -p casa-ms --bin msexplore
echo "==> Authentication mode: existing ChatGPT subscription; metered API environment removed"

unset OPENAI_API_KEY AZURE_OPENAI_API_KEY OPENAI_BASE_URL
export CASA_RS_GUI_TEST_PYTHON="$PYTHON_COMMAND"
export CASA_RS_GUI_TEST_ONLY="CasarsMacUITests/CasarsMacUITests/testOptInProductionTWHyaTutorialJourney"
export CASA_RS_GUI_TEST_RESULT_BUNDLE="${CASA_RS_GUI_TEST_ARTIFACT_ROOT:-$ROOT_DIR/.gui-test}/TutorialJourneyGUI.xcresult"

LIVE_GATE="$ROOT_DIR/.gui-test/tutorial-journey-gui.enabled"
LIVE_PROJECT_BASE="${CASA_RS_GUI_TEST_PROJECT_BASE:-$HOME/.casa-rs-gui-tests}"
LIVE_PROJECT="$LIVE_PROJECT_BASE/casars-wave5d-tutorial-$(date -u +%Y%m%dT%H%M%SZ)-$$"
PASS_RECEIPT="$LIVE_PROJECT/.tutorial-journey-gui.passed"
EVIDENCE_REPORT="${CASA_RS_GUI_TEST_ARTIFACT_ROOT:-$ROOT_DIR/.gui-test}/TutorialJourneyGUI.report.json"
TEST_EVIDENCE_REPORT="$LIVE_PROJECT/.tutorial-journey-gui.report.json"
mkdir -p "$ROOT_DIR/.gui-test" "$LIVE_PROJECT_BASE"
rm -f "$LIVE_GATE" "$PASS_RECEIPT" "$EVIDENCE_REPORT" "$TEST_EVIDENCE_REPORT"
rm -rf "$LIVE_PROJECT"
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
/usr/bin/plutil -insert templateRoot -string "$TEMPLATE_ROOT" "$LIVE_GATE"
/usr/bin/plutil -insert templateManifestSha256 -string "$(shasum -a 256 "$TUTORIAL_MANIFEST" | awk '{print $1}')" "$LIVE_GATE"
/usr/bin/plutil -insert templateMarkdownSha256 -string "$(shasum -a 256 "$TUTORIAL_MARKDOWN" | awk '{print $1}')" "$LIVE_GATE"
/usr/bin/plutil -insert sourceUri -string "$SOURCE_URI" "$LIVE_GATE"
/usr/bin/plutil -insert expectedSize -string "$EXPECTED_SIZE" "$LIVE_GATE"
/usr/bin/plutil -insert expectedSha256 -string "$EXPECTED_SHA256" "$LIVE_GATE"
/usr/bin/plutil -insert imagerCommand -string "$TARGET_DIR/debug/casars-imager" "$LIVE_GATE"
/usr/bin/plutil -insert msexploreCommand -string "$TARGET_DIR/debug/msexplore" "$LIVE_GATE"
trap 'rm -f "$LIVE_GATE" "$PASS_RECEIPT"' EXIT

if ! bash "$ROOT_DIR/script/test_gui.sh"; then
  echo "==> Failed tutorial project retained for diagnosis: $LIVE_PROJECT" >&2
  exit 1
fi
[[ -f "$PASS_RECEIPT" ]] || {
  echo "tutorial production journey acceptance did not write its success receipt" >&2
  exit 1
}
[[ -f "$TEST_EVIDENCE_REPORT" ]] || {
  echo "tutorial production journey acceptance did not write its evidence report" >&2
  exit 1
}
cp "$TEST_EVIDENCE_REPORT" "$EVIDENCE_REPORT"
"$PYTHON_COMMAND" -m json.tool "$EVIDENCE_REPORT" >/dev/null
echo "==> Durable sanitized evidence: $EVIDENCE_REPORT"
rm -rf "$LIVE_PROJECT"
echo "==> TW Hya tutorial production journey receipt: PASS"
