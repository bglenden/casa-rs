// SPDX-License-Identifier: LGPL-3.0-or-later
//! Renderer-neutral plot document, panel, layer, and binary-array payload types.

pub mod payload;
pub mod rich;
pub mod types;

pub use payload::{PlotPayloadWriter, plot_array_view};
pub use rich::*;
pub use types::*;
