// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! A native port of ATM, the atmospheric transmission model in CASA.
//!
//! ATM (Pardo, Cernicharo & Serabyn 2001, *IEEE Trans. Antennas Propag.*
//! 49, 1683) computes the opacity, phase delay and sky brightness of the
//! Earth's atmosphere from 1 GHz to a few THz. CASA uses it in the `at`
//! tool, in `sm.setnoise(mode='tsys-atm')` and in `sm.settrop`. This crate
//! ports the C++ library CASA ships (`casatools/src/code/atmosphere/ATM`,
//! version 1.0.0) so that casa-rs computes the same numbers natively, less
//! four data errors it corrects (below).
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
//! 4. An [`AbsorptionProfile`] (ATM's `RefractiveIndexProfile`) holds every
//!    layer's refractivity in every channel and integrates them into zenith
//!    opacities, phase delays and path lengths, dry and wet, dispersive and
//!    not, scaled to any water column.
//! 5. Radiative transfer through the layers ([`Sky`], ATM's `SkyStatus`)
//!    gives the sky's equivalent blackbody and Rayleigh–Jeans temperatures
//!    for a water column, airmass, sky coupling and spillover.
//!
//! The simulator's `tsys-atm` noise needs the dry and wet opacity and the
//! sky brightness; its tropospheric phase screens (`settrop`) need the wet
//! phase delay per unit water column, dispersive and non-dispersive (CASA's
//! takes the dispersive part alone, `docs/CASA (C++) bugs.md`).
//!
//! # Agreement with the C++
//!
//! The port follows ATM's arithmetic, including its line-window tables, so
//! that results agree with CASA's `at` tool, except where it corrects four
//! of ATM's data errors (`docs/CASA (C++) bugs.md`): HCN's J=12–11 line is
//! at 1062.98 GHz, not 106.298 GHz; five reference atmospheres have the
//! leading digit of their sea-level air density back; and H2(17)O and HDO
//! take their own abundances. ATM's other defects, which do not change its
//! numbers or have no correct values to restore, are followed. The tests
//! compare every layer field, species refractivity, opacity and sky
//! temperature with fixtures produced by compiling CASA's ATM sources with
//! the same corrections (`tools/atmosphere/atm-corrections.patch`, applied
//! by `tools/atmosphere/build-atm-oracle.sh`), and
//! `tools/atmosphere/at_tool_check.py` links that C++ to the `at` tool.
//!
//! # Cost and parallelism
//!
//! Building an [`AbsorptionProfile`] costs about 0.155 ms per channel for a
//! 27-layer atmosphere on one core (an Apple M4), against 0.62 ms for ATM's
//! C++ `RefractiveIndexProfile`, which evaluates most species twice (once
//! for each total, once for its parts). Every channel is independent and
//! computed by a pure function ([`absorption::channel_absorption`],
//! [`absorption::channel_totals`]), so a caller can compute channels on its
//! own threads and assemble them with [`AbsorptionProfile::from_channels`];
//! the crate itself starts none. The compact form ([`ChannelTotals`]) keeps
//! 64 bytes a layer instead of 448, and [`AbsorptionProfile::bytes`] gives
//! a profile's memory before it is built. ATM works in double precision
//! throughout, which rules out GPU backends without native fp64 (Apple's).
//!
//! # Units
//!
//! Values are plain `f64`, with the unit in each name: pressures in hPa
//! (ATM's millibars), temperatures in kelvin, heights in metres unless the
//! name says kilometres, frequencies in hertz in the spectral grid and in
//! GHz in the refractivity functions (as in ATM).

pub mod absorption;
mod profile;
pub mod refractivity;
mod sky;
mod spectral_grid;

pub use absorption::{AbsorptionProfile, ChannelAbsorption, ChannelTotals};
pub use profile::{AtmosphereProfile, AtmosphereType, Layer, ProfileParameters};
pub use sky::{Sky, SkyColumn};
pub use spectral_grid::{SpectralGrid, SpectralWindow};

/// Errors building atmospheric models.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum AtmosphereError {
    /// A parameter is outside the range the model accepts.
    #[error("invalid atmosphere parameter: {0}")]
    InvalidParameter(String),
}
