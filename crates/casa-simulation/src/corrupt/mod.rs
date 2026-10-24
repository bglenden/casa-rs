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

mod batch_scales;
pub(crate) mod jones;
#[cfg(test)]
mod tests;
pub(crate) mod tsys;
pub(crate) mod tsys_atm;

use std::sync::Arc;

use casa_numerics::cell_noise::CellNoise;
use num_complex::Complex32;

pub(crate) use batch_scales::{BatchScales, BatchScalesBuilder};
pub use tsys::TsysParameters;
pub(crate) use tsys::{RowNoise, TsysManual, WindowNoise, airmass};
pub use tsys_atm::AtmosphereSpec;
pub(crate) use tsys_atm::TsysAtm;

/// The top byte of the key's second word names what the stream is for, so
/// later corruptions (gains, fBM screens) draw from streams disjoint from
/// thermal noise under the same seed.
const PURPOSE_THERMAL_NOISE: u32 = 0x01;

/// Application ordinals share the key word with the purpose byte, so a term
/// can be applied at most this many times.
pub(crate) const MAXIMUM_APPLICATIONS: u32 = 1 << 24;

/// How large a noise term's draws are.
#[derive(Debug, Clone)]
pub(crate) enum NoiseLevel {
    /// `simplenoise`: σ of each real and imaginary draw, Jy, held as CASA
    /// holds it.
    Simple(f32),
    /// `tsys-manual`: σ of each row from the model ([`tsys`]).
    TsysManual(TsysManual),
    /// `tsys-atm`: σ of each row and channel from the atmosphere
    /// ([`tsys_atm`]), shared by the term and the corruptions applying it.
    TsysAtm(Arc<TsysAtm>),
}

impl PartialEq for NoiseLevel {
    /// Equal levels; two `tsys-atm` levels are equal when they are the same
    /// term's.
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Simple(left), Self::Simple(right)) => left == right,
            (Self::TsysManual(left), Self::TsysManual(right)) => left == right,
            (Self::TsysAtm(left), Self::TsysAtm(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }
}

/// One application of a noise term: the noise one `corrupt` adds for one
/// `setnoise` term.
#[derive(Debug, Clone, PartialEq)]
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
    /// row (the Tsys modes).
    pub(crate) fn uniform(&self) -> Option<CellNoise> {
        match self.level {
            NoiseLevel::Simple(sigma_jy) => Some(CellNoise::new(self.key(), sigma_jy)),
            NoiseLevel::TsysManual(_) | NoiseLevel::TsysAtm(_) => None,
        }
    }

    /// Whether σ depends on the row (the Tsys modes), which then needs each
    /// row's [`RowNoise`].
    pub(crate) fn varies_by_row(&self) -> bool {
        !matches!(self.level, NoiseLevel::Simple(_))
    }

    /// The `tsys-atm` model, if the term is one.
    pub(crate) fn tsys_atm(&self) -> Option<&TsysAtm> {
        match &self.level {
            NoiseLevel::TsysAtm(model) => Some(model),
            NoiseLevel::Simple(_) | NoiseLevel::TsysManual(_) => None,
        }
    }

    /// This application for the rows of a scan in spectral window `window`.
    pub(crate) fn for_window(&self, window: &WindowNoise) -> ScanNoise<'_> {
        ScanNoise {
            key: self.key(),
            level: match &self.level {
                NoiseLevel::Simple(sigma_jy) => ScanLevel::Simple(*sigma_jy),
                NoiseLevel::TsysManual(model) => ScanLevel::TsysManual(model.for_window(window)),
                NoiseLevel::TsysAtm(model) => ScanLevel::TsysAtm {
                    model,
                    window_id: window.spectral_window_id,
                    window: model.window(window.spectral_window_id),
                },
            },
        }
    }

    /// Add to `scales` what a `tsys-atm` application needs for `rows` of a
    /// scan in spectral window `window`; nothing for the other modes.
    pub(crate) fn add_scales<'a>(
        &'a self,
        window: &WindowNoise,
        rows: impl Iterator<Item = RowNoise>,
        scales: &mut BatchScalesBuilder<'a>,
    ) {
        if let NoiseLevel::TsysAtm(model) = &self.level {
            let id = window.spectral_window_id;
            scales.add_rows(
                (model, id, model.window(id)),
                window.channel_bandwidth_hz,
                rows,
            );
        }
    }
}

/// A noise application for the rows of one spectral window.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ScanNoise<'a> {
    key: [u32; 2],
    level: ScanLevel<'a>,
}

#[derive(Debug, Clone, Copy)]
enum ScanLevel<'a> {
    Simple(f32),
    TsysManual(tsys::WindowTsys),
    TsysAtm {
        model: &'a TsysAtm,
        window_id: usize,
        window: &'a tsys_atm::WindowAtmosphere,
    },
}

/// What a noise job reads and keeps between rows for `tsys-atm`: its
/// batch's antenna scales, and a row's σ per channel, at most
/// [`NoiseScratch::bytes`].
#[derive(Debug)]
pub(crate) struct NoiseScratch<'s> {
    scales: &'s BatchScales,
    sigma: Vec<f32>,
}

impl<'s> NoiseScratch<'s> {
    /// A scratch reading `scales`.
    pub(crate) fn new(scales: &'s BatchScales) -> Self {
        Self {
            scales,
            sigma: Vec::new(),
        }
    }

    /// The most a scratch for windows of at most `channels` channels holds.
    pub(crate) fn bytes(channels: usize) -> u64 {
        (channels * size_of::<f32>()) as u64
    }

    /// σ per channel of a row of `row`'s antennas: s_i(c)·s_j(c).
    fn row_sigma(
        &mut self,
        noise: (&TsysAtm, usize, &tsys_atm::WindowAtmosphere),
        row: &RowNoise,
    ) -> &[f32] {
        let scales = self.scales;
        let (first, second) = (scales.get(noise, row, 0), scales.get(noise, row, 1));
        self.sigma.clear();
        self.sigma
            .extend(first.iter().zip(second).map(|(s_i, s_j)| s_i * s_j));
        &self.sigma
    }
}

impl ScanNoise<'_> {
    /// Add this application's noise to the cell of MAIN row `row`, whose
    /// antennas, airmasses and integration time are `inputs`:
    /// `correlations × channels` samples, correlation fastest. An
    /// autocorrelation row gets real draws only; samples past the last
    /// whole channel are left alone ([`CellNoise::add_to_row`]). A
    /// `tsys-atm` term's σ differs by channel: `scratch` reads it from the
    /// batch's antenna scales.
    pub(crate) fn add_to_row(
        &self,
        (row, inputs): (u64, &RowNoise),
        (autocorrelation, correlations): (bool, usize),
        cell: &mut [Complex32],
        scratch: &mut NoiseScratch<'_>,
    ) {
        let sigma = match &self.level {
            ScanLevel::Simple(sigma_jy) => *sigma_jy,
            ScanLevel::TsysManual(model) => model.sigma(inputs),
            ScanLevel::TsysAtm {
                model,
                window_id,
                window,
            } => {
                let sigma = scratch.row_sigma((model, *window_id, window), inputs);
                // The draws of a unit term, scaled channel by channel, as
                // CASA's `T` scales its unit `ANoise`.
                CellNoise::new(self.key, 1.0).add_to_row_scaled(
                    row,
                    autocorrelation,
                    correlations,
                    sigma,
                    cell,
                );
                return;
            }
        };
        CellNoise::new(self.key, sigma).add_to_row(row, autocorrelation, correlations, cell);
    }
}
