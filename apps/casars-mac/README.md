# casars-mac

Truth class: current descriptive
Last reality check: 2026-07-10
Verification: swift test; just gui-test; swift run casars-mac --dump-debug-state --simulate-main-flow; swift run casars-mac --dump-debug-state --show-prototype notebook; ./script/build_and_run.sh --verify

`casars-mac` is the SwiftUI prototype for the native macOS `casa-rs`
workbench. The app keeps a synthetic demo fixture for layout and dry-run
coverage, but the normal interactive launcher opens a fresh temporary project
from the local TW Hya tutorial MeasurementSet when available so plot and
imaging paths exercise real dataset discovery with known tutorial parameters.

## Commands

From this directory:

```bash
swift test
swift build
swift run casars-mac --dump-debug-state --simulate-main-flow
swift run casars-mac --dump-debug-state --open-tutorial-pack /path/to/tutorial.pack
swift run casars-mac --dump-debug-state --open-imager-ms /path/to/input.ms
swift run casars-mac --dump-debug-state --show-prototype notebook --prototype-state happy-path
swift run casars-mac --capture-gui-evidence --capture-kind imager-progress-mockup --output /tmp/imager-progress.png
swift run casars-mac --capture-gui-evidence --capture-kind notebook-prototype --prototype-state external-conflict --output /tmp/notebook-conflict.png
./script/build_and_run.sh
./script/build_and_run.sh --verify
./script/install-local-gui.sh --force
./script/build_and_run.sh --project /path/to/project
./script/build_and_run.sh --imager-ms /path/to/input.ms --run-active-task
./script/build_and_run.sh --tutorial-pack /path/to/tutorial.pack
./script/build_and_run.sh --show-prototype notebook --prototype-state happy-path
./script/build_and_run.sh --empty
```

The `--dump-debug-state` path is intentionally non-interactive so local
automation can inspect the workbench model without screenshots.

## Executable GUI tests

From the repository root, run:

```bash
just gui-test
```

The command builds the Rust frontend dylib with incremental compilation
disabled, then uses the checked-in `CasarsMac.xcodeproj` and `CasarsMacGUI`
scheme to launch the existing app sources under the `CasarsMacUITests` macOS UI
Testing Bundle. The host is test infrastructure only: it reuses
`Sources/CasarsMacApp`, the local `CasarsMacCore` Swift package product, and the
same fixture launch arguments as the app. It owns no second UI, state model, or
persisted contract.

Run the gate from a logged-in macOS GUI session with Xcode installed. The first
local run may require macOS permission for Xcode's test runner to control the
app; clear any active system-authentication prompt before retrying. The tests
use the system pasteboard plus keyboard events for complete-document edits, so
the runner needs normal GUI focus and pasteboard access. No user project,
dataset, provider, task process, network service, or notebook file is opened or
written: every test launches `--show-prototype notebook` with deterministic
fixture state and asserts that the production-boundary audit remains zero.

Disposable build and test output lives under `apps/casars-mac/.gui-test/`.
The retained result bundle is
`apps/casars-mac/.gui-test/CasarsMacUITests.xcresult`; failing workflows attach
an app screenshot and accessibility hierarchy there. Delete `.gui-test/` to
force a clean Xcode build. The gate was established locally with Xcode 26.2
(17C52) on macOS 26.5.2 arm64. Pull-request CI runs the same command on the
supported `macos-15` runner, selects Xcode 26.2 explicitly so its compiler
matches the established local gate, and uploads the result bundle whether the
job passes or fails.

The app shell keeps left dock collapse/restore, inspector restore,
command/search routing, dock mode selection, Python ownership, and task/AI
actions in the testable core state. The runnable app also configures the
primary macOS window for normal resizable and full-screen Space behavior.

Use `./script/build_and_run.sh` for interactive GUI inspection. It stages a
local `.app` bundle under `dist/`, unpacks the tutorial-registry
`alma/first-look/twhya/calibrated-ms` archive from
`${CASA_RS_TUTORIAL_DATA_ROOT}` or `~/SoftwareProjects/casa-tutorial-data` into
a temporary project, and launches that project with `/usr/bin/open` so Dock
activation, menus, and native full-screen Spaces behave like a normal macOS app.
Imager defaults for the TW Hya tutorial sample pick field 5, SPW 0, 250 px,
and 0.1 arcsec cells, matching the documented ALMA First Look MFS
imaging slice. If the local tutorial archive is not staged, the launcher falls
back to the bundled `mssel_test_small_multifield_spw.ms.tgz` fixture and its
NGC4826-F3/SPW 5/raw YY defaults. Demo-mode temporary projects are removed
after the launched app exits; the default launcher stays attached until that
exit so cleanup is deterministic. Pass `--project` to inspect an existing
project directory, `--imager-ms` to open a MeasurementSet directly on the
imager task, `--run-active-task` to start the selected task after launch,
imager launch overrides such as `--image-size`, `--cell-arcsec`,
`--spectral-mode`, `--channel-count`, and `--niter` to tune the schema task,
`--tutorial-pack` to inspect a generated tutorial learning pack, or `--empty` to
start without opening a project. Tutorial packs open on the Tutorial tab
with input staging status, section checkpoints, learner docs, and an action
that applies each section's GUI parameters to the task panel.
Pass `--show-prototype notebook` to open the Wave 1 scientific-notebook
prototype without staging a project. `--prototype-state happy-path` selects the
normal fixture-only task-recording and annotation flow; `external-conflict` shows the
third-party-edit conflict state. The same state selector works with
`--capture-kind notebook-prototype` for deterministic GUI evidence.
`swift run casars-mac` is reserved for non-interactive debug-state commands and
low-level executable diagnosis.

Use `./script/install-local-gui.sh --force` to install the staged app under
`~/.local/opt/casa-rs/<version>/Applications/casars-mac.app` and update the
`~/.local/bin/casars-mac` launcher. From the repository root, `just
install-local` installs both the CLI/TUI/Python suite and this Swift GUI app on
macOS; `just install-local-gui` installs only the GUI.
