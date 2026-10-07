// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]

//! The shared radio-interferometric measurement operator.
//!
//! This crate owns the forward operator `A` (model image to predicted
//! visibilities) and its adjoint `A*` (weighted visibilities to unnormalised
//! image-domain normal images) as one implementation parameterised by a
//! convolution-function set (standard spheroidal, W-planes, AW catalog, mosaic
//! primary beam) and a gridding backend (CPU, Metal). It also owns the global
//! weighting generation (natural, uniform, Briggs, taper).
//!
//! The design is `docs/imaging-architecture/imaging-foundation-plan-20261007.md`
//! section 5.3 and ADR-0016. Ticket IF-1 (#650) fills this crate.
