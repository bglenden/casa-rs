# Wave 0 Expansion Readiness

Truth class: current descriptive
Last reality check: 2026-04-26
Verification: just docs-check

Wave issue: #137

Wave 0 decides what must be cleaned up or mapped before tutorial feature
implementation starts. It implements the audit portions of:

- #131 Library expansion-readiness code-quality audit and cleanup map
- #115 Inventory current CASA Guide tutorials and datasets
- #116 Build tutorial capability matrix and CASA parity oracle harness

No Rust public API, persisted format, provider-contract bundle, dependency, or
runtime behavior is changed by this wave.

## Outputs

- [Code-quality map](code-quality-map.md)
  - classifies expansion-critical library hotspots and identifies whether
    existing or new shaped issues own the cleanup.
- [Current tutorial and dataset inventory](current-tutorial-inventory.md)
  - records current ALMA, VLA, and Simulation guide pages, versions, data
    artifacts, extracted task surfaces, and dataset registry keys.
- [Tutorial capability matrix](capability-matrix.md)
  - maps extracted CASA task/tool capabilities to current casa-rs support,
    library owners, app/Python exposure, correctness oracle, performance
    oracle, and owning waves/issues.

## Expansion-Readiness Decisions

- Keep active planning in GitHub Issues / Project. These docs are durable
  reference material, not the canonical backlog.
- Use numbered wave umbrellas for implementation order. Do not overload the
  project `Horizon` field as a wave sequence.
- Treat current CASA Guide pages as primary. Former/archived guide pages are
  regression candidates, not blockers for first-pass parity.
- Implement functionality in reusable `casa-*` libraries first, then expose it
  through `casars-*` applications and Python bindings following existing
  provider-contract patterns.
- Do not introduce CASA task-name aliases as the public API. Document mappings
  from CASA tasks/tools to casa-rs names instead.

## Follow-Up Waves

Wave 0 found no reason to stop for a new top-level crate, public persisted
format, provider-contract redesign, substantial dependency, or runtime model
change before the next wave. Those decisions may still arise in later
implementation waves and must follow the stop-and-ask process in `AGENTS.md`.

The immediate implementation queue is:

- Wave 1: #138 Core stabilization for tutorial work
- Wave 2: #139 Local seam stabilization
- Wave 3: #140 ALMA First Look / TW Hydra vertical
- Wave 4: #141 VLA IRC+10216 vertical
- Wave 5: #142 Simulation Protoplanetary Disk - VLA vertical
- Wave 6: #143 Breadth expansion
- Wave 7: #144 Performance parity closeout

