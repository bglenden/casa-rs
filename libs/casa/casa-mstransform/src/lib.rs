// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

#[cfg(unix)]
pub mod concat;
#[cfg(all(feature = "msv4", unix))]
pub mod convert;
pub mod fits_idi;
pub mod tool_concatenate;
pub mod tool_iteration;
pub mod tool_lister;
pub mod tool_multims;
pub mod tool_range;
pub mod tool_sort;
pub mod tool_statistics;
pub mod tool_visibility;
mod transform;
pub use transform::*;
