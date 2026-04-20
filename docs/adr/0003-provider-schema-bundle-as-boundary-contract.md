# ADR-0003: Provider schema bundle as boundary contract

Status: accepted
Date: 2026-04-19
Truth class: normative
Supersedes:
Superseded by:

## Context

The repo includes terminal shells, protocol crates, and Python/runtime surfaces
that need to exchange structured information with functionality providers. That
boundary can either be owned by versioned schema bundles or allowed to drift
into separate app-local UI models, ad hoc JSON shapes, and duplicated contract
definitions.

## Decision

Versioned provider schema bundles are the boundary contract between providers,
apps, and related runtime surfaces.

UI views and projections are derived from the contract bundle; they are not
separate sources of truth for behavior or shape. Changes to provider bundle
shape are contract changes and require the same level of review as other public
or persisted interfaces.

## Consequences

Positive:
- provider/app integration has one durable contract surface
- Python/runtime and TUI views can share the same underlying schema intent
- boundary changes become easier to reason about and version

Negative:
- ad hoc UI-only shortcuts become less attractive because the contract must stay authoritative
- contract changes need more deliberate review and compatibility thinking

Neutral / tradeoffs:
- projections can still vary by consumer, but they must remain derived from the same underlying contract bundle

## Alternatives considered

1. Let each UI/runtime surface define its own ad hoc provider shapes.
2. Treat the TUI schema and Python schema as separate authoritative models.

## Enforcement

This decision is enforced by:
- tests: protocol and contract tests in provider/protocol crates
- lint/import/dependency rules: review blocks app-local contract forks
- CI checks: `just verify`
- review trigger: stop for review when changing versioned provider bundles or deriving a new public projection surface
- none / guidance only:

## Drift detection

Suspect drift if:
- app or Python code starts hand-defining authoritative schema that does not round-trip through the provider bundle
- UI projection metadata begins to carry behavior that is not represented in the contract bundle
- contract changes land without explicit review of downstream consumers
