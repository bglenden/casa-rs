// SPDX-License-Identifier: LGPL-3.0-or-later
//! Spectral windows and correlations: `sm.setspwindow`.
//!
//! `NewMSSimulator::initSpWindows` adds one SPECTRAL_WINDOW, one
//! POLARIZATION and one DATA_DESCRIPTION row per call, all at the same index.

use casa_types::measures::frequency::FrequencyRef;

use super::error::{SimulatorError, SimulatorResult};
use crate::corrupt::WindowNoise;

/// The arguments of `sm.setspwindow`.
#[derive(Debug, Clone, PartialEq)]
pub struct SpectralWindowSpec {
    /// The window's name (`spwname`); `observe` finds windows by it.
    pub name: String,
    /// Centre frequency of the first channel (`freq`), Hz.
    pub start_frequency_hz: f64,
    /// Channel spacing (`deltafreq`), Hz; it may be negative.
    pub channel_width_hz: f64,
    /// `freqresolution`, Hz. CASA ignores it and writes the channel width.
    pub resolution_hz: f64,
    /// Frequency frame (`refcode`).
    pub frame: FrequencyRef,
    /// Number of channels (`nchannels`).
    pub channel_count: usize,
    /// Correlations (`stokes`), as `"RR LL"`, `"XX XY YX YY"` and so on.
    pub stokes: String,
}

/// One spectral window with its polarization setup.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SpectralWindow {
    pub(crate) name: String,
    pub(crate) channel_count: usize,
    pub(crate) start_frequency_hz: f64,
    pub(crate) channel_width_hz: f64,
    /// REF_FREQUENCY, Hz, which picks window 0's beams (`VPSkyJones`):
    /// `setspwindow` writes the start frequency, and `openfromms` reads it
    /// back, whatever the channels are.
    pub(crate) reference_frequency_hz: f64,
    /// TOTAL_BANDWIDTH, Hz: the channels' widths summed, as `setspwindow`
    /// writes it (negative for a decreasing axis), or as `openfromms` reads
    /// it. `tsys-manual` noise takes |TOTAL_BANDWIDTH| / NUM_CHAN as every
    /// channel's bandwidth.
    pub(crate) total_bandwidth_hz: f64,
    pub(crate) frame: FrequencyRef,
    /// casacore `Stokes::StokesTypes` codes, one per correlation.
    pub(crate) correlations: Vec<i32>,
}

impl SpectralWindow {
    /// The window `spec` describes, as CASA would write it.
    pub(crate) fn new(spec: &SpectralWindowSpec) -> SimulatorResult<Self> {
        if spec.channel_count == 0 {
            return Err(SimulatorError::refused("must provide nchannels"));
        }
        Ok(Self {
            name: spec.name.clone(),
            channel_count: spec.channel_count,
            start_frequency_hz: spec.start_frequency_hz,
            channel_width_hz: spec.channel_width_hz,
            reference_frequency_hz: spec.start_frequency_hz,
            total_bandwidth_hz: spec.channel_count as f64 * spec.channel_width_hz,
            frame: spec.frame,
            correlations: parse_stokes(&spec.stokes),
        })
    }

    /// CHAN_FREQ: the start plus a whole number of channel widths.
    pub(crate) fn channel_frequencies_hz(&self) -> Vec<f64> {
        (0..self.channel_count)
            .map(|channel| self.start_frequency_hz + channel as f64 * self.channel_width_hz)
            .collect()
    }

    /// What the window gives its rows' noise: REF_FREQUENCY, and
    /// |TOTAL_BANDWIDTH| / NUM_CHAN as each channel's bandwidth
    /// (`AtmosCorruptor::simPar`'s `deltaNu`).
    pub(crate) fn noise(&self) -> WindowNoise {
        WindowNoise {
            reference_frequency_hz: self.reference_frequency_hz,
            channel_bandwidth_hz: self.total_bandwidth_hz.abs() / self.channel_count.max(1) as f64,
        }
    }

    /// CORR_PRODUCT: for each correlation, the receptor pair
    /// (`Stokes::receptor1`, `Stokes::receptor2`).
    pub(crate) fn correlation_products(&self) -> Vec<[i32; 2]> {
        self.correlations
            .iter()
            .map(|&code| receptor_pair(code))
            .collect()
    }
}

/// casacore's `Stokes::StokesTypes` names, indexed by code from 1.
const STOKES_NAMES: [&str; 32] = [
    "I",
    "Q",
    "U",
    "V",
    "RR",
    "RL",
    "LR",
    "LL",
    "XX",
    "XY",
    "YX",
    "YY",
    "RX",
    "RY",
    "LX",
    "LY",
    "XR",
    "XL",
    "YR",
    "YL",
    "PP",
    "PQ",
    "QP",
    "QQ",
    "RCIRCULAR",
    "LCIRCULAR",
    "LINEAR",
    "PTOTAL",
    "PLINEAR",
    "PFTOTAL",
    "PFLINEAR",
    "PANGLE",
];

/// The correlations of a `stokes` string, as `initSpWindows` reads it: up to
/// four tokens of exactly two characters after leading blanks, each looked
/// up case-insensitively (0 when it names nothing, which still counts). A
/// one-letter name such as `I` is found only as the last token, since "I " is
/// not a name.
pub(crate) fn parse_stokes(stokes: &str) -> Vec<i32> {
    let mut rest = stokes;
    let mut codes = Vec::new();
    while codes.len() < 4 {
        rest = rest.trim_start_matches(' ');
        if rest.is_empty() {
            break;
        }
        let token: String = rest.chars().take(2).collect();
        rest = &rest[token.len()..];
        let upper = token.to_ascii_uppercase();
        let code = STOKES_NAMES
            .iter()
            .position(|name| *name == upper)
            .map_or(0, |index| index as i32 + 1);
        if code == 0 {
            tracing::warn!(token = %token, " Undefined polarization type in input");
        }
        codes.push(code);
    }
    codes
}

/// casacore's `Stokes::receptor1` and `receptor2` for the products RR to QQ
/// (codes 5 to 24); other codes have no receptors and give (0, 0).
fn receptor_pair(code: i32) -> [i32; 2] {
    if (5..=24).contains(&code) {
        let index = code - 5;
        [(index / 2) % 2, index % 2]
    } else {
        [0, 0]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stokes_strings_parse_as_casa_reads_them() {
        assert_eq!(parse_stokes("RR LL"), vec![5, 8]);
        assert_eq!(parse_stokes("RRLL"), vec![5, 8]);
        assert_eq!(parse_stokes("XX XY YX YY"), vec![9, 10, 11, 12]);
        assert_eq!(parse_stokes("  xx"), vec![9]);
        // "I " names nothing; a final "I" is Stokes I.
        assert_eq!(parse_stokes("I Q"), vec![0, 2]);
        assert_eq!(parse_stokes("RR RL LR LL XX"), vec![5, 6, 7, 8]);
    }

    #[test]
    fn receptor_products_follow_casacore() {
        assert_eq!(receptor_pair(5), [0, 0]);
        assert_eq!(receptor_pair(6), [0, 1]);
        assert_eq!(receptor_pair(7), [1, 0]);
        assert_eq!(receptor_pair(8), [1, 1]);
        assert_eq!(receptor_pair(12), [1, 1]);
        assert_eq!(receptor_pair(1), [0, 0]);
    }
}
