# `casars` TUI Framework

This document is the architecture and authoring guide for the framework-owned
`casars` terminal UI.

It has two goals:

1. describe the current shell-family model clearly enough that maintainers can
   work on the framework without reverse-engineering it from `app.rs`
2. define the conventions that new applications must follow so `casars` stays
   coherent as more complex apps are added

## Scope

This document covers:

- shell-family architecture
- framework-owned versus app-owned responsibilities
- conventions for adding new apps
- expectations for workflow-style apps
- testing and documentation requirements for new apps

It does not try to document every line of `crates/casars/src/app.rs`. The code
remains the implementation source of truth.

## Design goal

`casars` is not meant to be a bag of unrelated TUIs. It is a small family of
framework-owned shells with a shared interaction model.

The required user-facing invariants are:

- one visual language
- one navigation model
- one keybinding model for common actions
- one picker model
- one result/history/output model
- a small number of shell types with stable meanings

Complex applications must not derive their primary UX from raw CLI schema
groups. CLI schemas remain important for scripting, tests, and subprocess
invocation, but the TUI should be organized around user workflow rather than
flag layout.

## Shell family

`casars` currently has three shell kinds.

### `InspectShell`

Use for applications where the user loads a dataset, chooses a view or preset,
and inspects results.

Examples:

- `msexplore`

Required shape:

- left pane sections:
  - `Context`
  - `Views`
  - `Controls`
- right pane tabs:
  - `Overview`
  - `Data`
  - `Plots`
  - `History`
  - `Stdout`
  - `Stderr`

### `BrowserShell`

Use for applications whose primary job is browsing persistent structure and
content.

Examples:

- `tablebrowser`
- `imexplore`

Required shape:

- left pane sections:
  - `Context`
  - `Views`
  - `Tools`
- right pane tabs:
  - `Overview`
  - `Structure`
  - `Content`
  - `Inspector`
  - `History`
  - `Stdout`
  - `Stderr`

### `WorkflowShell`

Use for applications with:

- stage ordering
- derived products or artifacts
- dependencies between stages
- diagnostics after each stage
- iteration or re-run behavior

Examples:

- `calibrate`
- future imaging, self-calibration, and VLBI workflow apps

Required shape:

- left pane sections:
  - `Context`
  - `Products`
  - `Stages`
  - `Stage Parameters`
- right pane tabs:
  - `Overview`
  - `Data`
  - `Products`
  - `Diagnostics`
  - `History`
  - `Stdout`
  - `Stderr`

## Framework-owned responsibilities

These are framework concerns and should not be reinvented per app:

- shell kind and top-level layout
- pane focus and tab focus behavior
- status line and help overlay
- scroll behavior
- popup picker rendering and keyboard behavior
- result, history, stdout, and stderr surfaces
- shared workflow graph presentation
- shared workflow display formatting helpers
- shell-specific overview presentation

Implementation anchors:

- `crates/casars/src/app.rs`
- `crates/casars/src/ui.rs`
- `crates/casars/src/shell.rs`
- `crates/casars/src/workflow.rs`

If a new app needs a new top-level screen arrangement, that is not an app-local
feature request. It is a framework change and should be treated as such.

## App-owned responsibilities

Apps are expected to provide domain state and actions, not bespoke shell chrome.

App responsibilities include:

- domain-specific subprocesses or browser protocols
- domain-specific context values
- domain-specific product or artifact meaning
- stage-specific parameters and validation
- diagnostics relevant to the domain
- mapping between native workflow rows and the underlying command model

Good app code supplies:

- state
- labels
- actions
- summaries

Bad app code duplicates:

- shell layout
- picker mechanics
- workflow row rendering
- history handling
- output-pane conventions

## Shell selection rules for new apps

When adding a new app, choose the shell by user task, not by implementation
convenience.

Choose `InspectShell` when:

- the user loads data
- chooses one of several views or presets
- inspects summaries, tables, or plots
- does not manage a long-lived chain of derived products

Choose `BrowserShell` when:

- the core activity is navigating a hierarchical or persistent object model
- structure/content inspection is primary
- the app behaves like a browser, not a guided procedure

Choose `WorkflowShell` when:

- the app has explicit stages
- stages create products or artifacts
- later stages depend on earlier outputs
- reruns can stale downstream products
- diagnostics are stage- or product-specific

If an app is “complicated” but still does not have products, dependencies, or
revisitable stages, it probably does not belong in `WorkflowShell`.

## WorkflowShell conventions

`WorkflowShell` is the most constrained shell because it is the one most likely
to sprawl if left unconstrained.

### Required model

Every workflow app should have these concepts:

- `Context`
  - dataset path or paths
  - selection
  - role assignments or scenario metadata where relevant
  - recommendations when available
- `Products`
  - typed derived artifacts
  - revisions
  - provenance
  - dependency and staleness status
- `Stages`
  - ordered display
  - revisitable execution
  - readiness/blocking state
  - recommended next stage
- `Diagnostics`
  - product-specific or latest-run-specific inspection views
- `History`
  - what ran
  - what it produced
  - what became stale

### Required UX behavior

Workflow apps should guide the user forward without hiding structure.

Required behaviors:

- the selected stage must be obvious
- `Overview` must tell the user the recommended next stage
- stage rows should communicate goal and status, not just task names
- products must be visible as first-class things, not implied by text fields
- diagnostics should be contextual to the selected product or latest run
- re-running upstream stages must stale downstream products instead of silently
  pretending everything is still current

### What not to do

Do not:

- expose raw CLI path lists as the main product model
- use free-form text where the framework can provide a picker
- treat workflow state as a one-pass wizard if re-run is meaningful
- mix unrelated stage fields into one giant parameter form
- make users infer the current chain from subprocess arguments alone

### Calibration-specific lessons that generalize

The `calibrate` migration established the following rules:

- field, SPW, and antenna choices should come from live summary data
- apply chains are products and policy, not just text inputs
- callibrary content should appear as structured workflow rows
- the first runnable stage must be safe and informative
- the walkthrough must succeed in the real TUI, not just in unit tests

These lessons should also hold for imaging and self-calibration apps.

## Pickers and editable rows

Pickers are framework features. New apps should use them whenever possible.

Use a picker when:

- the valid values come from summary data
- the valid values come from enum-like choices
- the user is selecting a product, artifact, field, SPW, antenna, or policy

Use free text only when:

- the value is genuinely open-ended
- the valid domain is too large or too dynamic for a practical picker

Machine values and display labels must be treated separately. A picker may show
human-readable labels, but it must write the value expected by the underlying
command model.

The bug class to avoid is exactly this:

- display label is a field name
- command expects numeric field ID
- picker writes the label instead of the machine value

When adding a new picker-backed row, verify both:

- display correctness
- written value correctness

## Result panes and diagnostics

Right-pane tabs are not arbitrary. The shell owns the meaning of each tab.

Conventions:

- `Overview` explains where the user is in the shell
- `Data` shows the current dataset-facing summaries or tables
- `Products` shows structured artifact state
- `Diagnostics` shows contextual plots, stats, and inspection output
- `History` shows run history and workflow progression
- `Stdout` and `Stderr` remain available for honesty and debugging

Apps should prefer putting structured content in the appropriate semantic tab
instead of telling users to read `Stdout`.

## App authoring checklist

When adding a new `casars` app:

1. choose the shell kind explicitly
2. define the app’s user task in one sentence
3. map that task to shell sections and tabs without inventing a new shell
4. identify what is framework-owned versus app-owned
5. add native rows for the domain concepts users actually think about
6. avoid exposing raw schema groups as the primary workflow
7. add guided defaults so the first action is safe and useful
8. add targeted unit tests for the shell behavior
9. add or update the ratatui smoke harness if the app is shipped
10. write user-facing documentation for how to operate the app

## Testing expectations for new apps

Every new app or major shell migration should have:

- unit tests for shell-state behavior
- unit tests for picker-backed edits
- unit tests for key workflow actions
- a smoke-harness path if the app is user-facing

For workflow apps, add tests for:

- initial recommended stage
- successful first-run behavior
- product creation
- product promotion or activation
- staleness after upstream rerun
- diagnostics changing with selected product or run

If a walkthrough is documented, at least one regression should exercise that
walkthrough on a real or realistic fixture.

## Documentation expectations for new apps

Every user-facing app should have:

- one architecture/app-authoring home in this framework document
- one user-facing guide for operating the app

Every workflow app should document:

- what the stages mean
- what products are created
- how the user inspects outputs
- what the recommended first walkthrough is on a known dataset

## Current limitations

The shell-family architecture is active and real, but not everything is yet a
clean protocol boundary. Some internal app-state plumbing still lives in
`crates/casars/src/app.rs`, especially around command-schema interop and
subprocess bridging.

That is acceptable for now. The important invariant is that new applications
must conform to the shell-family conventions above rather than extending the old
schema-first UI model.
