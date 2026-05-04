# casars-mac

Truth class: current descriptive
Last reality check: 2026-05-04
Verification: swift test; swift run casars-mac --dump-debug-state --simulate-main-flow

`casars-mac` is the SwiftUI prototype for the native macOS `casa-rs`
workbench. GUI-Wave-0 is fixture-only: it demonstrates layout, actions,
headless model tests, and debug-state inspection before real provider, Python,
matplotlib, or AI execution is connected.

## Commands

From this directory:

```bash
swift test
swift build
swift run casars-mac --dump-debug-state --simulate-main-flow
./script/build_and_run.sh
./script/build_and_run.sh --verify
```

The `--dump-debug-state` path is intentionally non-interactive so local
automation can inspect the workbench model without screenshots.

The app shell keeps left dock collapse/restore, inspector restore,
command/search routing, dock mode selection, Python ownership, and task/AI
actions in the testable core state. The runnable app also configures the
primary macOS window for normal resizable and full-screen Space behavior.

Use `./script/build_and_run.sh` for interactive GUI inspection. It stages a
local `.app` bundle under `dist/` and launches that bundle with `/usr/bin/open`
so Dock activation, menus, and native full-screen Spaces behave like a normal
macOS app. `swift run casars-mac` is reserved for non-interactive debug-state
commands and low-level executable diagnosis.
