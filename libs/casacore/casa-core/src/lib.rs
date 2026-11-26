// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA data values, quanta, measures and reference-frame conversions.

pub mod correlation;
pub mod measures;
pub mod memory;
pub mod quanta;

pub use correlation::Correlation;

mod value;

pub use value::*;
