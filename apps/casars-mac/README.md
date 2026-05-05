# casars-mac

Truth class: current descriptive
Last reality check: 2026-05-05
Verification: swift test; swift run casars-mac --dump-debug-state --simulate-main-flow; ./script/build_and_run.sh --verify

`casars-mac` is the SwiftUI prototype for the native macOS `casa-rs`
workbench. The app keeps a synthetic demo fixture for layout and dry-run
coverage, but the normal interactive launcher opens a fresh temporary project
from a bundled real MeasurementSet fixture so plot and imaging paths exercise
real dataset discovery.

## Commands

From this directory:

```bash
swift test
swift build
swift run casars-mac --dump-debug-state --simulate-main-flow
./script/build_and_run.sh
./script/build_and_run.sh --verify
./script/build_and_run.sh --project /path/to/project
./script/build_and_run.sh --empty
```

The `--dump-debug-state` path is intentionally non-interactive so local
automation can inspect the workbench model without screenshots.

The app shell keeps left dock collapse/restore, inspector restore,
command/search routing, dock mode selection, Python ownership, and task/AI
actions in the testable core state. The runnable app also configures the
primary macOS window for normal resizable and full-screen Space behavior.

Use `./script/build_and_run.sh` for interactive GUI inspection. It stages a
local `.app` bundle under `dist/`, unpacks
`crates/casa-ms/tests/fixtures/mssel_test_small_multifield_spw.ms.tgz` into a
temporary project, and launches that project with `/usr/bin/open` so Dock
activation, menus, and native full-screen Spaces behave like a normal macOS app.
Dirty Imaging defaults for the bundled sample pick the NGC4826-F3 field, SPW 5
(a 64-channel line window near the NGC4826 rest frequency), and raw YY because
the sample has only one correlation plane. The temporary project is removed
after the launched app exits. Pass `--project` to inspect an existing project
directory, or `--empty` to start without opening a project. `swift run
casars-mac` is reserved for non-interactive debug-state commands and low-level
executable diagnosis.
