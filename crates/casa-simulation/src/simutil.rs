// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's `simutil` helpers that `simobserve` uses (simulation Phase 5,
//! #841): CASA 6.7.6's `casatasks/src/private/simutil.py`, ported as its
//! specification (ADR-0018).
//!
//! | CASA | casa-rs |
//! |---|---|
//! | `direction_splitter`, `isdirection`, `dir_m2s`, `wrapang` | [`direction::split_direction`], [`direction::is_direction`], [`direction::format_direction`], [`direction::wrap_angle`] |
//! | `average_direction`, `median_direction` | [`direction::average_direction`], [`direction::median_direction`] |
//! | `calc_pointings2` (`hexagonal`, `square`, `ALMA`, `ALMA2012`) | [`pointings::calc_pointings`] |
//! | `read_pointings`, `write_pointings` | [`pointings::read_pointings`], [`pointings::write_pointings`] |
//! | `noisetemp`, `bandname`, `polsettings` | [`receivers::noise_temperature`], [`receivers::band_name`], [`receivers::polarization_settings`] |
//! | `itrf2loc`, `baselineLengths`, `approxBeam` | [`baselines::itrf_to_local`], [`baselines::baseline_lengths`], [`baselines::approximate_beam`] |
//! | `dir_s2m` | [`direction::direction_measure`] |
//! | `modifymodel`, `flatimage`, `ia.modify`, the component-only model | [`skymodel`] |
//!
//! **CASA's numbers to the last bit.** Pointing lists are compared with
//! CASA's as strings, so the arithmetic follows CASA's: every `qa` call goes
//! through casacore's `Quantum` arithmetic
//! ([`casa_types::quanta::casacore`]), which converts units as casacore
//! rounds and names them as casacore names them, and numbers that CASA
//! writes into strings and reads back (`str(x)+"arcsec"`,
//! `'%17.12fdeg' % x`) are written and read back the same way. The tests
//! compare a grid of CASA's outputs
//! (`tests/data/simutil_casa.json`, from `tools/simulation/simutil_oracle.py`).
//!
//! **Where casa-rs differs.** CASA raises Python exceptions where these
//! functions refuse an input; casa-rs returns [`SimutilError`]. It also
//! departs from CASA in a few places, each recorded in
//! `docs/CASA (C++) bugs.md`:
//! - `calc_pointings2`'s ALMA tiling takes its default beam from
//!   `spacing × pbcoeff × √3`, but CASA's `pbcoeff` is undefined there
//!   (`NameError`); casa-rs uses `simobserve`'s 1.13.
//! - `noisetemp`'s receiver lookup reads `f0[z]` before testing `z`, so
//!   above an unknown telescope's last tabulated frequency CASA raises
//!   `IndexError`; casa-rs keeps the last temperature. At a frequency equal
//!   to the first entry of the table, CASA's `t0[z-1]` with `z = 0` reads the
//!   last temperature (230 K for ALMA at 35 GHz); casa-rs reads the first.
//! - `direction_splitter` reads its second and third fields whatever the
//!   field count, so a direction without an epoch raises `IndexError` in
//!   CASA; casa-rs refuses it with a message.

pub mod baselines;
pub mod direction;
pub mod pointings;
pub(crate) mod qa;
pub mod receivers;
pub mod skymodel;

pub use qa::python_float;

use thiserror::Error;

/// An input a `simutil` helper refuses. Where CASA raises its own message,
/// the message is CASA's.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum SimutilError {
    /// A value that is not a quantity casacore can read.
    #[error("can't interpret '{0}' as a CASA quantity")]
    Quantity(String),
    /// A direction `direction_splitter` cannot split.
    #[error("can't interpret '{0}' as a direction")]
    Direction(String),
    /// A quantity of the wrong kind for its use, or casacore's arithmetic
    /// refusing it.
    #[error("{0}")]
    Arithmetic(String),
    /// A pointing file with no pointings in it.
    #[error("No valid lines found in pointing file")]
    NoPointings,
    /// Fewer pointing times than pointings.
    #[error("{times} pointing times for {pointings} pointings")]
    Times {
        /// The number of pointings.
        pointings: usize,
        /// The number of times.
        times: usize,
    },
    /// A frequency that is not positive and finite.
    #[error("frequency {0} GHz is not positive")]
    Frequency(f64),
}

impl From<casa_types::quanta::casacore::QuantumError> for SimutilError {
    fn from(error: casa_types::quanta::casacore::QuantumError) -> Self {
        Self::Arithmetic(error.0)
    }
}
