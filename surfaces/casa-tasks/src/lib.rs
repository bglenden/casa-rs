// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]

//! Feature-gated task adapters, schemas, parameter resolution, and CASA return
//! projections shared between the CLI frontend and Python bindings.

mod runtime;

pub use runtime::*;

#[cfg(feature = "calibration")]
pub mod calibration;
#[cfg(feature = "images")]
pub mod images;
#[cfg(feature = "imaging")]
pub mod imaging;
#[cfg(feature = "ms")]
pub mod ms;
#[cfg(feature = "simulation")]
pub mod simulation;
#[cfg(feature = "vla")]
pub mod vla;
