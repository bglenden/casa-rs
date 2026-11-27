// SPDX-License-Identifier: LGPL-3.0-or-later
#![doc = include_str!("../README.md")]

pub mod correlation;
pub mod measures;
pub mod memory;
pub mod quanta;

pub use correlation::Correlation;

mod value;

pub use value::*;
