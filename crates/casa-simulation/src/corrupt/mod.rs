// SPDX-License-Identifier: LGPL-3.0-or-later
//! Visibility corruption: thermal noise, CASA's `ANoise` term
//! (`sm.setnoise(mode='simplenoise')` then `sm.corrupt()`), and the
//! `tsys-manual` noise that scales it per row ([`tsys`]).
//!
//! # What CASA adds
//!
//! Measured against CASA 6.7.6 (simulation Phase 1d probes, #841), each
//! `corrupt` adds, for every noise term, to every DATA sample:
//! - on a cross-correlation row, independent real and imaginary draws, each
//!   N(0, σ²), where σ is the term's `simplenoise` in Jy, or for a
//!   `tsys-manual` term the row's σ from its antennas, elevations, channel
//!   width and integration time ([`tsys`]);
//! - on an autocorrelation row, a real draw only: the imaginary part is
//!   unchanged;
//! - in every correlation, the cross-hands of four-correlation data
//!   included, each with its own draw;
//! - whatever the flags: flagged rows and samples get noise too.
//!
//! WEIGHT and SIGMA are left as observed, by both modes. CASA's
//! `ANoiseCorruptor::simPar`
//! draws in double precision and rounds `Complex(r·amp, i·amp)`, σ held as
//! a `Float`, to the single precision DATA is stored in. casa-rs draws in
//! single precision instead, and scales by σ in it too, for speed: a vector
//! register holds four lanes rather than two and the series are about half
//! as long. On an Apple M2 core that nearly halves the noise's cost, from
//! 6.2 to 3.2 ns per complex sample of two-correlation rows, the generator's
//! 1.2 ns included (#855). The difference is below what the stored `f32`
//! resolves: against noise of standard deviation σ it is of order 10⁻⁷σ
//! ([`casa_numerics::cell_noise`] gives the bounds and where they come
//! from).
//!
//! # Where the draws come from
//!
//! CASA draws from one `MLCG` stream per term, in the order its visibility
//! iterator visits rows, channels and correlations, and the stream carries on
//! across `corrupt` calls. casa-rs keys each draw instead: the draws come
//! from [`casa_numerics::cell_noise`] (Philox4x32-10 and Box–Muller in
//! `f32`), whose counter is the absolute MAIN row, the channel and the
//! correlation pair, and whose 64-bit key names the stream. This module
//! forms the key ([`NoiseApplication`]): its first word is the term's seed
//! (`sm.setseed`, as its 32 bits), and its second the purpose byte
//! [`PURPOSE_THERMAL_NOISE`] above the term's application ordinal (how many
//! `corrupt` calls the term had seen before). A `tsys-manual` term draws as
//! a `simplenoise` term of the same seed does, scaled by its row's σ, as
//! CASA's unit-amplitude `ANoise` is. The two schemes are
//! statistically equivalent, not bit-identical (plan, "Random numbers"), and
//! they share the structure scripts see:
//! - two terms with the same seed draw the same values in the same
//!   `corrupt`, so their noise adds coherently (variance 4σ² for two);
//! - one term applied twice draws independent values (variance 2σ²);
//! - a different seed draws different values.
//!
//! The result depends only on (seed, ordinal, row, channel, correlation) and
//! the row's σ, so it is bitwise the same for any batch size, worker count,
//! backend or platform; the tests hold a few cells to the bits they had
//! before the generator moved to casa-numerics (#855).

pub(crate) mod jones;
#[cfg(test)]
mod tests;
pub(crate) mod tsys;

use casa_numerics::cell_noise::CellNoise;
use num_complex::Complex32;

pub use tsys::TsysParameters;
pub(crate) use tsys::{RowNoise, TsysManual, WindowNoise, airmass};

/// The top byte of the key's second word names what the stream is for, so
/// later corruptions (gains, fBM screens) draw from streams disjoint from
/// thermal noise under the same seed.
const PURPOSE_THERMAL_NOISE: u32 = 0x01;

/// Application ordinals share the key word with the purpose byte, so a term
/// can be applied at most this many times.
pub(crate) const MAXIMUM_APPLICATIONS: u32 = 1 << 24;

/// How large a noise term's draws are.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum NoiseLevel {
    /// `simplenoise`: σ of each real and imaginary draw, Jy, held as CASA
    /// holds it.
    Simple(f32),
    /// `tsys-manual`: σ of each row from the model ([`tsys`]).
    TsysManual(TsysManual),
}

/// One application of a noise term: the noise one `corrupt` adds for one
/// `setnoise` term.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct NoiseApplication {
    /// The seed in force when the term was set (`sm.setseed`).
    pub(crate) seed: i32,
    /// How large its draws are.
    pub(crate) level: NoiseLevel,
    /// How many times the term had been applied before; below
    /// [`MAXIMUM_APPLICATIONS`].
    pub(crate) ordinal: u32,
}

impl NoiseApplication {
    /// The stream key: the seed's 32 bits (two's complement), then the
    /// thermal-noise purpose byte above the ordinal.
    fn key(&self) -> [u32; 2] {
        debug_assert!(self.ordinal < MAXIMUM_APPLICATIONS);
        [
            self.seed as u32,
            (PURPOSE_THERMAL_NOISE << 24) | self.ordinal,
        ]
    }

    /// This application's noise when every row has the same σ
    /// (`simplenoise`): σ under [`Self::key`]; `None` when σ depends on the
    /// row (`tsys-manual`).
    pub(crate) fn uniform(&self) -> Option<CellNoise> {
        match self.level {
            NoiseLevel::Simple(sigma_jy) => Some(CellNoise::new(self.key(), sigma_jy)),
            NoiseLevel::TsysManual(_) => None,
        }
    }

    /// Whether σ depends on the row (`tsys-manual`), which then needs each
    /// row's [`RowNoise`].
    pub(crate) fn varies_by_row(&self) -> bool {
        matches!(self.level, NoiseLevel::TsysManual(_))
    }

    /// This application for the rows of a scan in spectral window `window`.
    pub(crate) fn for_window(&self, window: &WindowNoise) -> ScanNoise {
        ScanNoise {
            key: self.key(),
            level: match &self.level {
                NoiseLevel::Simple(sigma_jy) => ScanLevel::Simple(*sigma_jy),
                NoiseLevel::TsysManual(model) => ScanLevel::TsysManual(model.for_window(window)),
            },
        }
    }
}

/// A noise application for the rows of one spectral window.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ScanNoise {
    key: [u32; 2],
    level: ScanLevel,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ScanLevel {
    Simple(f32),
    TsysManual(tsys::WindowTsys),
}

impl ScanNoise {
    /// σ of each real and imaginary draw of a row of `row`'s antennas,
    /// airmasses and integration time, Jy.
    pub(crate) fn sigma(&self, row: &RowNoise) -> f32 {
        match &self.level {
            ScanLevel::Simple(sigma_jy) => *sigma_jy,
            ScanLevel::TsysManual(model) => model.sigma(row),
        }
    }

    /// Add this application's noise to the cell of MAIN row `row`, whose
    /// antennas, airmasses and integration time are `inputs`:
    /// `correlations × channels` samples, correlation fastest. An
    /// autocorrelation row gets real draws only; samples past the last
    /// whole channel are left alone ([`CellNoise::add_to_row`]).
    pub(crate) fn add_to_row(
        &self,
        (row, inputs): (u64, &RowNoise),
        autocorrelation: bool,
        correlations: usize,
        cell: &mut [Complex32],
    ) {
        CellNoise::new(self.key, self.sigma(inputs)).add_to_row(
            row,
            autocorrelation,
            correlations,
            cell,
        );
    }
}
