// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]

//! Image-domain deconvolution: the minor-cycle driver, the CLEAN controller
//! and the solvers.
//!
//! One driver consumes an immutable minor-cycle view (residual planes, PSF
//! summary, support mask, controls, budget), runs a `Solver` (Hogbom, Clark,
//! multiscale, Taylor) under one controller that owns thresholds,
//! `cycleniter`, convergence and divergence rules, and returns a model delta.
//!
//! The design is `docs/imaging-architecture/imaging-foundation-plan-20261007.md`
//! section 5.5 and ADR-0016. Ticket IF-5 (#654) fills this crate.
