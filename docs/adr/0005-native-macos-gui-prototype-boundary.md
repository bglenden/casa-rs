# ADR-0005: Native macOS GUI prototype boundary

Status: accepted
Date: 2026-05-04
Truth class: normative
Supersedes:
Superseded by:

## Context

The repo needs a clickable macOS-native GUI prototype for iterating on the
AI-enhanced `casa-rs` radio astronomy workbench before real provider, Python,
matplotlib, and AI integration harden the product shape. The prototype also
needs headless tests and runtime-debug state from the beginning so GUI behavior
does not depend on manual screenshot review.

The existing architecture is Rust-first and provider-contract oriented. GUI
fixtures must not become a second source of truth for provider behavior.

## Decision

Create a SwiftPM package at `apps/casars-mac` for the native macOS prototype.

The package has two product surfaces:

- `CasarsMacCore`: SwiftPM-testable workbench state, actions, fixture data, and
  debug-state serialization.
- `casars-mac`: SwiftUI executable that projects `CasarsMacCore` state into the
  native workbench UI.

GUI-Wave-0 is fixture-only. It may model proposed provider capabilities, but
real provider integration must wait for canonical provider contracts, versioning,
and drift tests.

The primary shell behavior is part of the prototype boundary, not later visual
polish. The workbench must keep inspector collapse/restore, left-dock mode
selection and collapse/restore, central tab creation, command/search routing,
Python/AI ownership, and debug-state inspection in `CasarsMacCore` actions
where practical. The main toolbar owns left-dock collapse/restore; the left
dock header owns inspector collapse/restore while the dock is visible. Native
macOS window behavior, including full-screen Space support, may use a narrow
AppKit bridge when SwiftUI does not expose the needed window knob directly.
Interactive SwiftPM GUI runs should stage a local `.app` bundle rather than
launching the raw executable, because native Dock activation, menus, and
full-screen Spaces depend on bundle metadata.

## Consequences

Positive:
- the GUI can iterate as a native macOS app without waiting for provider work
- core workbench behavior is testable with `swift test`
- local automation can inspect state through `casars-mac --dump-debug-state`
- SwiftUI views project from a reusable state/action layer rather than owning
  durable behavior directly

Negative:
- the repo now has a second language/toolchain for the macOS prototype
- default Rust gates do not automatically compile the Swift package unless a
  future gate wires that in

Neutral / tradeoffs:
- the Swift fixture schema is intentionally separate from provider contracts
  until real integration starts
- AppKit remains available for future escape hatches, but GUI-Wave-0 should
  prefer SwiftUI APIs and keep any bridge narrow and window-specific

## Alternatives considered

1. Build the prototype in Electron, Tauri, or a webview. This was rejected for
   GUI-Wave-0 because the product direction is explicitly macOS-native.
2. Put SwiftUI directly on top of Rust in-process bindings. This was deferred
   because it would harden runtime and FFI choices before the UI is validated.
3. Keep the prototype as mockups only. This was rejected because the next
   required step is a clickable, testable workbench.

## Enforcement

This decision is enforced by:
- tests: `swift test` in `apps/casars-mac`
- lint/import/dependency rules: review blocks fixture schemas from becoming
  provider contracts without canonical contract work
- CI checks: `just docs-check`; Swift package checks are wave-local until wired
  into default gates
- review trigger: stop before adding real providers, Python execution, AI
  provider calls, persisted project formats, substantial dependencies, or direct
  Rust in-process bindings
- none / guidance only:

## Drift detection

Suspect drift if:
- SwiftUI views own durable behavior that is not represented in `CasarsMacCore`
- fixture schemas are used as real provider contracts
- GUI behavior cannot be tested with `swift test` or inspected through debug
  state
- shell controls such as inspector restore, command/search routing, or Python
  ownership become view-only behavior
- real provider, Python, AI, or persistence work enters the prototype without a
  shaped follow-up and contract decision
