// SPDX-License-Identifier: LGPL-3.0-or-later
//! Unit system, dimensional analysis, and quantity conversions.
//!
//! This module implements the casacore unit/quanta system in Rust, providing:
//!
//! - **[`UnitDim`]** — SI dimension exponent vectors for dimensional analysis.
//! - **[`UnitVal`]** — scale factor + dimensions, the internal currency of the system.
//! - **[`Unit`]** — a validated unit string with cached [`UnitVal`].
//! - **[`Quantity`]** — a numeric value paired with a [`Unit`] (equivalent to
//!   C++ `Quantum<Double>`).
//! - **[`constants`]** — physical constants as [`Quantity`] values (equivalent
//!   to C++ `QC`).
//!
//! The unit parser understands compound expressions like `"km/s"`,
//! `"kg.m.s-2"`, `"mJy/beam"`, and all ~130 built-in casacore units plus
//! 24 SI prefixes.
//!
//! # Examples
//!
//! ```
//! use casacore_types::quanta::{Quantity, Unit};
//!
//! let distance = Quantity::new(1.5, "km").unwrap();
//! let time = Quantity::new(3.0, "s").unwrap();
//! let velocity = &distance / &time;
//!
//! let target = Unit::new("m/s").unwrap();
//! let v_ms = velocity.get_value_in(&target).unwrap();
//! assert!((v_ms - 500.0).abs() < 1e-10);
//! ```

pub mod constants;
pub mod dim;
pub mod error;
pub mod parser;
pub mod quantity;
pub mod registry;
pub mod registry_data;
pub mod unit;
pub mod unit_val;

// Re-exports for convenience.
pub use dim::{Dimension, UnitDim};
pub use error::UnitError;
pub use quantity::Quantity;
pub use unit::Unit;
pub use unit_val::UnitVal;
