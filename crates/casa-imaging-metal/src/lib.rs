// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]

//! Metal implementation of the imaging gridding backend.
//!
//! This crate is the only place a device API appears in the imaging stack. It
//! implements the `casa-imaging-operator` backend trait for both tap layouts
//! and all work kinds (grid, predict, fused predict-residual-grid), owns the
//! device, queue, buffer residency and fences, and accumulates grids in f32
//! with atomic adds. Unsupported geometry fails typed; there is no fallback.
//!
//! The design is `docs/imaging-architecture/imaging-foundation-plan-20261007.md`
//! sections 5.3 and 5.9 and ADR-0016. Ticket IF-4 (#653) fills this crate.
