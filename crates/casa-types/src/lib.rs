// SPDX-License-Identifier: LGPL-3.0-or-later
//! Public facade for CASA values, quanta, and measures.
//!
//! `casa-types` remains the stable public import path for:
//!
//! - the generic CASA scalar/array/record value model
//! - quanta and unit handling
//! - measures and frame conversion APIs
//!
//! The generic value layer is implemented in the lower internal
//! `casa-values` crate and re-exported here so downstream code does not need
//! to change imports.

pub mod measures;
pub mod quanta;

#[doc(inline)]
pub use casa_values::*;
