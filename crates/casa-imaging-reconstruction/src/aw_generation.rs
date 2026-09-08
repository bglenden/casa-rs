// SPDX-License-Identifier: LGPL-3.0-or-later

//! Native paired A/W generation and its telescope aperture implementations.

mod evla;
mod paired;

pub use evla::{EvlaApertureGrid, EvlaApertureModel};
pub use paired::{NativeAwPair, NativeAwPlane, evla_aw_workspace_bytes, generate_evla_aw_pair};
use thiserror::Error;

/// A native A/W request or numerical calculation could not be completed.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum NativeAwGenerationError {
    /// Dish samples are not finite, regularly spaced, or physically valid.
    #[error("EVLA dish surface requires finite, uniformly spaced radius/height/slope samples")]
    InvalidSurface,
    /// The requested receiver band is outside the implemented EVLA proof.
    #[error("native EVLA aperture supports only L, S and C receiver bands (0.9 to 8 GHz)")]
    UnsupportedFrequency,
    /// Aperture geometry or numerical sampling is invalid.
    #[error(
        "native aperture grid requires a finite positive cell size, frequency, and even extent"
    )]
    InvalidGrid,
    /// The caller did not supply the declared numerical workspace.
    #[error("native A/W generation workspace does not fit the requested grid")]
    WorkspaceMismatch,
    /// A generated value or normalization is invalid.
    #[error("native A/W generation produced an invalid numerical value")]
    InvalidNumerics,
}
