# ADR-0011: Continuum-line coupling

Status: retired
Date: 2026-08-31
Truth class: normative
Supersedes:
Superseded by: ADR-0016 decision 11

ADR-0016 decision 11 removed the joint continuum-plus-line solver this ADR
introduced; only the sequential continuum transform below still applies,
and git history keeps the original text (#531).

## Context

CASA's `uvcontsub` fits and subtracts the continuum in the visibility domain
before line imaging (`casatasks/src/private/task_uvcontsub.py`,
`casatools/src/code/mstransform/TVI/UVContSubTVI.cc`). casa-rs ports that
transform. This ADR also accepted a joint continuum-plus-line
reconstruction, which no longer exists in the code.

## Decision

The sequential continuum transform is CASA's visibility-domain fit and
subtraction, composed before independent channel-local line imaging. It fits
each row and correlation over the fit channels with CASA's min/max frequency
basis, subtracts the fit on the application channels, keeps the input flags
and weights, and claims no propagated fit covariance. It is a visibility
transform, not a joint reconstruction, and is never described as one. It
lives in `casa-imaging-operator` (`fit_and_subtract_continuum`).

A joint continuum-plus-line solver would be a new capability with its own
scientific contract and a new ADR (ADR-0016 decision 11).

## Enforcement

This decision is enforced by:
- tests: `continuum_transform_*` and
  `t46_sequential_uvcontsub_anchor_v1_matches_frozen_casa_row` (a frozen
  CASA `UVContSubTVI` row) in `crates/casa-imaging-operator/tests/continuum.rs`
- none / guidance only: retired; kept as the record of the sequential
  transform
