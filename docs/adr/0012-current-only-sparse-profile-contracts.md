# ADR-0012: Current-only sparse profile contracts

Status: accepted
Date: 2026-09-03
Truth class: normative
Supersedes: ADR-0006 profile-evolution rules
Superseded by:

## Context

ADR-0006 defined sparse human parameter profiles and assumed that surface
providers would retain aliases and ordered migrations for older contract
versions. The imaging architecture programme instead requires imager profiles
to expose only the current canonical request vocabulary. The repository is
still early, has no external compatibility obligation for these profiles, and
would otherwise carry fallback interpretation for every historical imaging
surface while omitted values continue to acquire current defaults.

Issue #551 records the approved T65 outcome: only the current profile schema is
accepted, compatibility aliases, migration readers, and fallback
interpretation are absent, and canonical sparse profiles contain only required
values and non-default overrides.

## Decision

A parameter surface may declare a **current-only profile contract** by
publishing an empty migration set. Such a surface:

- accepts exactly its current contract version;
- exposes and accepts only canonical parameter names, with no compatibility
  aliases;
- rejects stale and future contract versions instead of interpreting or
  rewriting them; and
- serializes only required values and current non-default overrides whose
  catalog persistence class is `profile`.

Omitted optional values adopt the current provider-owned defaults. They are not
materialized merely to preserve an older contract's behavior. Shipped profiles,
tutorials, and documentation examples must move to the new contract version
when a current-only surface advances.

The imager surface is current-only. Other surfaces may retain the ADR-0006
compatibility model by publishing a complete ordered migration chain; this
decision does not remove that mechanism.

This decision supersedes only ADR-0006's requirement that every parameterized
surface migrate older profiles and warn about historical default changes. The
rest of ADR-0006 remains accepted, including the catalog ownership, sparse TOML
format, managed lifecycle, and persistence boundaries.

## Consequences

Positive:

- imager profile meaning is defined by one current canonical request surface;
- compatibility aliases and fallback readers cannot become a second imaging
  vocabulary; and
- transient compiled state, plans, caches, provider resources, and runtime
  inventory remain outside human profiles.

Negative:

- an imager contract bump deliberately invalidates profiles that were not
  updated; and
- every shipped imager profile and example must advance with the contract.

Neutral / tradeoffs:

- additive defaults remain useful because omitted values adopt the current
  default, but a sparse profile is not a historical run-replay artifact; and
- surfaces with an explicit compatibility requirement may still own aliases
  and migrations.

## Alternatives considered

1. Retain all historical imager migrations and aliases. Rejected because it
   preserves the compatibility surface T65 removes.
2. Silently load stale profiles against current defaults. Rejected because the
   resulting request would look valid while changing meaning without a current
   contract declaration.
3. Remove contract versions from sparse profiles. Rejected because readers
   would no longer be able to fail closed across vocabulary changes.

## Enforcement

This decision is enforced by:

- provider-contract validation that forbids aliases when migrations are empty;
- profile resolution that rejects stale current-only contracts;
- T65 sparse-profile tests for canonical serialization, additive defaults,
  exact VLASS/continuum/cube semantics, and forbidden authority state; and
- generated parameter references plus architecture, documentation, and
  frontend-binding checks.

## Drift detection

Suspect drift if:

- a current-only surface publishes an alias or migration;
- an imager reader accepts a stale contract or fallback spelling;
- a shipped imager profile does not name the current surface contract; or
- a sparse profile contains compiled, plan, cache, resource-inventory, or
  other non-profile-owned state.
