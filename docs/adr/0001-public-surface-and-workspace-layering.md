# ADR-0001: Public surface and workspace layering

Status: accepted
Date: 2026-04-19
Truth class: normative
Supersedes:
Superseded by:

## Context

`casa-rs` has grown into a multi-crate workspace with reusable libraries,
internal codecs/loaders, domain-specific astronomy crates, terminal apps, and a
Python package. Without an explicit layering rule, internal helper crates can
leak into the public surface and app/runtime crates can start owning domain or
persistence decisions that belong lower in the stack.

## Decision

The workspace uses a layered structure:

- internal core codecs and value model crates stay unpublished and feed higher layers
- public foundations and persistence crates own reusable on-disk behavior
- domain libraries build on those foundations for MeasurementSets, images, coordinates, imaging, and calibration
- provider/protocol crates expose boundary contracts rather than second domain models
- app/runtime crates orchestrate workflows on top of the libraries rather than replacing them

The durable public surface is centered on the published `casa-*` crates, with
`casars-*` reserved for app/runtime and protocol surfaces.

## Consequences

Positive:
- public vs internal ownership is easier to reason about
- lower layers can stay focused on persistence and domain behavior
- app/runtime work has a clearer place to stop before creating a second architecture

Negative:
- some cross-crate refactors require more up-front boundary review
- app features that want quick local shortcuts have less freedom to embed domain logic

Neutral / tradeoffs:
- current mechanical enforcement is lighter than the long-term ideal and still relies partly on review and docs

## Alternatives considered

1. Treat every crate as an equally public peer without strong layering.
2. Collapse more behavior into the app/runtime crates and let libraries thin out.

## Enforcement

This decision is enforced by:
- tests: cross-crate integration and parity tests continue to target library behavior rather than app-only copies
- lint/import/dependency rules: `just arch-check` validates the architecture surface and ADR index; deeper dependency checks can tighten later
- CI checks: `just verify`
- review trigger: stop for review when adding top-level crates or changing dependency direction
- none / guidance only:

## Drift detection

Suspect drift if:
- unpublished helper crates start appearing in user-facing examples or docs as stable APIs
- app/runtime crates begin to own persistence formats or duplicate lower-layer domain logic
- boundary/protocol crates grow independent behavior that disagrees with the library surface
