// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! A native port of ATM, the atmospheric transmission model in CASA.
//!
//! ATM (Pardo, Cernicharo & Serabyn 2001, *IEEE Trans. Antennas Propag.*
//! 49, 1683) computes the opacity, phase delay and sky brightness of the
//! Earth's atmosphere from 1 GHz to a few THz. CASA uses it in the `at`
//! tool, in `sm.setnoise(mode='tsys-atm')` and in `sm.settrop`. This crate
//! ports the C++ library CASA ships (`casatools/src/code/atmosphere/ATM`,
//! version 1.0.0) so that casa-rs computes the same numbers natively.
//!
//! # The model
//!
//! 1. [`AtmosphereProfile`] divides the atmosphere above a site into
//!    horizontal layers, from the ground pressure, temperature and humidity
//!    and one of ATM's reference atmospheres ([`AtmosphereType`]). Each
//!    layer has a thickness and mean pressure, temperature, water vapour
//!    and minor-gas densities.
//! 2. [`refractivity`] gives each gas's complex refractivity at a layer's
//!    conditions: the imaginary part is the absorption coefficient, the
//!    real part the phase delay per unit path. Line species sum
//!    Van Vleck–Weisskopf profiles over ATM's line catalogues; the water
//!    and dry continua are empirical.
//! 3. A [`SpectralGrid`] lists the frequencies (spectral windows of
//!    channels) at which the refractivities are evaluated.
//!
//! Integrating the layer refractivities over the profile gives the zenith
//! opacity and path delay, and radiative transfer through the layers gives
//! the sky brightness; those build on this crate's types.
//!
//! # Agreement with the C++
//!
//! The port follows ATM's arithmetic, including its line-window tables and
//! its known data defects (listed in `docs/CASA (C++) bugs.md`), so that
//! results agree with CASA's `at` tool. The tests compare every layer
//! field and every species' refractivity with fixtures produced by
//! compiling CASA's ATM sources (`tools/atmosphere/build-atm-oracle.sh`).
//!
//! # Cost and parallelism
//!
//! The refractivity of every (layer, channel) pair is independent, and all
//! functions here are pure. A caller evaluating many channels can spread
//! them over threads; the crate itself starts none. ATM works in double
//! precision throughout, which rules out GPU backends without native
//! fp64 (Apple's).
//!
//! # Units
//!
//! Values are plain `f64`, with the unit in each name: pressures in hPa
//! (ATM's millibars), temperatures in kelvin, heights in metres unless the
//! name says kilometres, frequencies in hertz in the spectral grid and in
//! GHz in the refractivity functions (as in ATM).

mod profile;
pub mod refractivity;
mod spectral_grid;

pub use profile::{AtmosphereProfile, AtmosphereType, Layer, ProfileParameters};
pub use spectral_grid::{SpectralGrid, SpectralWindow};

/// Errors building atmospheric models.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum AtmosphereError {
    /// A parameter is outside the range the model accepts.
    #[error("invalid atmosphere parameter: {0}")]
    InvalidParameter(String),
}
