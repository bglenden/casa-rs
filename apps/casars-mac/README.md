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
swift run casars-mac
```

The `--dump-debug-state` path is intentionally non-interactive so local
automation can inspect the workbench model without screenshots.
