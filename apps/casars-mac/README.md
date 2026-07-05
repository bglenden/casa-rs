# casars-mac

Truth class: current descriptive
Last reality check: 2026-05-09
Verification: swift test; swift run casars-mac --dump-debug-state --simulate-main-flow; ./script/build_and_run.sh --verify

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
swift run casars-mac --capture-gui-evidence --capture-kind imager-progress-mockup --output /tmp/imager-progress.png
./script/build_and_run.sh
./script/build_and_run.sh --verify
./script/install-local-gui.sh --force
./script/build_and_run.sh --project /path/to/project
./script/build_and_run.sh --imager-ms /path/to/input.ms --run-active-task
./script/build_and_run.sh --tutorial-pack /path/to/tutorial.pack
./script/build_and_run.sh --empty
```

The `--dump-debug-state` path is intentionally non-interactive so local
automation can inspect the workbench model without screenshots.

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
`swift run casars-mac` is reserved for non-interactive debug-state commands and
low-level executable diagnosis.

Use `./script/install-local-gui.sh --force` to install the staged app under
`~/.local/opt/casa-rs/<version>/Applications/casars-mac.app` and update the
`~/.local/bin/casars-mac` launcher. From the repository root, `just
install-local` installs both the CLI/TUI/Python suite and this Swift GUI app on
macOS; `just install-local-gui` installs only the GUI.
