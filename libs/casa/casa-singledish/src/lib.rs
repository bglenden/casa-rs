// SPDX-License-Identifier: LGPL-3.0-or-later
#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

pub mod preparation;
mod single_dish_import;

pub use preparation::atmosphere::SingleDishAtmosphere;
pub use preparation::baseline::{
    SingleDishBaseline, SingleDishBaselineBasis, SingleDishBaselineDiagnostics,
    SingleDishBaselineMode, SingleDishLineFinder, SingleDishSinusoid,
};
pub use preparation::import::{import_asap, import_nro};
pub use preparation::line_fit::SingleDishLineFit;
pub use preparation::{SingleDishSession, SingleDishSmoothing};
