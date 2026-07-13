# casars-mac

Truth class: current descriptive
Last reality check: 2026-07-12
Verification: swift test; just gui-test; swift run casars-mac --dump-debug-state --simulate-main-flow; swift run casars-mac --dump-debug-state --show-prototype notebook; swift run casars-mac --dump-debug-state --show-prototype python; swift run casars-mac --dump-debug-state --show-prototype tutorial; swift run casars-mac --dump-debug-state --show-prototype ai; ./script/build_and_run.sh --verify

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
swift run casars-mac --dump-debug-state --project /path/to/project --open-tutorial-pack /path/to/tutorial-template
swift run casars-mac --dump-debug-state --open-imager-ms /path/to/input.ms
swift run casars-mac --dump-debug-state --show-prototype notebook --prototype-state happy-path
swift run casars-mac --dump-debug-state --show-prototype python --prototype-state happy-path
swift run casars-mac --dump-debug-state --show-prototype tutorial --prototype-state happy-path
swift run casars-mac --dump-debug-state --show-prototype ai --prototype-state happy-path
swift run casars-mac --capture-gui-evidence --capture-kind imager-progress-mockup --output /tmp/imager-progress.png
swift run casars-mac --capture-gui-evidence --capture-kind notebook-prototype --prototype-state external-conflict --output /tmp/notebook-conflict.png
swift run casars-mac --capture-gui-evidence --capture-kind python-prototype --prototype-state happy-path --output /tmp/python-notebook.png
swift run casars-mac --capture-gui-evidence --capture-kind tutorial-prototype --prototype-state happy-path --output /tmp/tutorial-notebook.png
swift run casars-mac --capture-gui-evidence --capture-kind ai-prototype --prototype-state happy-path --output /tmp/ai-discussion.png
./script/build_and_run.sh
./script/build_and_run.sh --verify
./script/install-local-gui.sh --force
./script/build_and_run.sh --project /path/to/project
./script/build_and_run.sh --imager-ms /path/to/input.ms --run-active-task
./script/build_and_run.sh --project /path/to/project --tutorial-pack /path/to/tutorial-template
./script/build_and_run.sh --show-prototype notebook --prototype-state happy-path
./script/build_and_run.sh --show-prototype python --prototype-state happy-path
./script/build_and_run.sh --show-prototype tutorial --prototype-state happy-path
./script/build_and_run.sh --show-prototype ai --prototype-state happy-path
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
dataset, provider, task process, network service, Python process, or notebook
file is opened or written: prototype tests launch `--show-prototype notebook`,
`--show-prototype python`, or `--show-prototype tutorial` with deterministic fixture state and assert that
the production-boundary audit remains zero. Production notebook tests use only
unique test-owned temporary projects and remove them after each test.

Local execution is deliberately batched into one exclusive foreground window.
The harness completes Rust and Xcode `build-for-testing` work first, then shows
a notification and ten-second countdown before running the whole suite with
`test-without-building`. Do not use the keyboard, mouse, or switch applications
until the completion notification. Set
`CASA_RS_GUI_TEST_COUNTDOWN_SECONDS=<seconds>` to change the countdown, or
`CASA_RS_GUI_TEST_EXCLUSIVE_NOTICE=0` only for an already isolated session.
Normal edit loops should use `swift test`, debug-state checks, and deterministic
evidence capture; accumulate interaction changes and run GUI tests together at
a prototype-review checkpoint rather than interleaving focused XCUITest runs
throughout development. A focused run remains appropriate when diagnosing a
failure from the consolidated gate.

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
`--tutorial-pack` together with `--project` to fork a portable tutorial template
into that project, or `--empty` to start without opening a project. A legacy
`pack.json` is accepted only as explicit one-shot migration input; the
resulting learner notebook and every subsequent reopen use v1
`tutorial.md`/`tutorial.toml`. The notebook keeps learner notes primary, shows
compact dataset state, requires exact acquisition approval, and loads typed
tutorial task cells directly into normal task tabs.
Pass `--show-prototype notebook` to open the Wave 1 scientific-notebook
prototype without staging a project. `--prototype-state happy-path` selects the
normal fixture-only task-recording and annotation flow; `external-conflict` shows the
third-party-edit conflict state. The same state selector works with
`--capture-kind notebook-prototype` for deterministic GUI evidence.
Pass `--show-prototype python` for the Wave 2 fixture-only Python notebook.
Its continuous notes-first document supports inline cell expansion, editing,
Run/Run All/Stop/Restart, ordered output, failure and retry, deterministic
PNG/SVG-style plot revisions, latest-first output with collapsed history,
regeneration, notebook insertion, explicit saved MeasurementSet/Image Explorer
snapshots, enlargement, parameter restoration, New plot/immutable Update
actions, and exact-code approval for AI-proposed cells. The `happy-path`, `failure`, and
`nonresponsive` states are also accepted by `--capture-kind python-prototype`.
The fixture launch touches no Python process, project file, task, provider, or
network; production Python and plotting adapters are tested separately.
Pass `--show-prototype tutorial` for the Wave 3 fixture-only learner notebook.
It keeps notes primary, shows compact section progress, requires an explicit
source/checksum/disk/extraction approval, and simulates Download, Verify,
Unpack, Ready, cancellation, resume, retry, offline, checksum, unsafe-archive,
and insufficient-disk states. A ready fixture dataset enables a typed
parameter block that opens the existing fixture task tab directly, where
tutorial overrides are visibly and accessibly identified. Accepted states are
`happy-path`, `checksum-failure`, `disk-failure`, `offline`, and
`unsafe-archive`. No file, network, archive, task, provider, or durable project
adapter is invoked by that prototype. The normal production runtime now forks
portable v1 templates into Rust-backed learner notebooks and uses the accepted
interaction for explicitly approved verified acquisition.
Pass `--show-prototype ai` for the revised Wave 4 fixture-only Codex discussion.
It presents a conventional notebook side chat opened by a purple lower-right
sparkle. Model, reasoning effort, and compact subscription usage remaining stay
visible below the composer; one settings popover contains agent, ChatGPT
subscription status, access preset, and scientific Python. The
fixture exercises typed CASA context inspection, citations, collapsed agent
activity, explicit Full-access confirmation, append-at-end notebook pins, and
direct loading of suggested parameters into the normal task tab. Return sends;
Shift-Return inserts a newline. The prototype does not launch Codex App Server,
authenticate, query a corpus, execute Python or tasks, access a project, or use
the network; the boundary counter remains zero. Accepted fixture states are
`happy-path`, `rate-limited`, and `nonresponsive`.
`swift run casars-mac` is reserved for non-interactive debug-state commands and
low-level executable diagnosis.

Use `./script/install-local-gui.sh --force` to install the staged app under
`~/.local/opt/casa-rs/<version>/Applications/casars-mac.app` and update the
`~/.local/bin/casars-mac` launcher. From the repository root, `just
install-local` installs both the CLI/TUI/Python suite and this Swift GUI app on
macOS; `just install-local-gui` installs only the GUI.
