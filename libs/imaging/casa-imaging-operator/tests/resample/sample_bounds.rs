// SPDX-License-Identifier: LGPL-3.0-or-later
//! Counting law for resampling a native spectrum into an admitted row.

use super::*;
use casa_imaging_operator::RowSpectrum;

/// The spectrum of one row at `frequencies_ghz`, exactly.
fn spectrum_of(frequencies_ghz: &[f64]) -> RowSpectrum {
    let hz = |index: usize| frequencies_ghz[index] * 1.0e9;
    let last = frequencies_ghz.len() - 1;
    RowSpectrum {
        channels: frequencies_ghz.len(),
        first_spacing_hz: if last == 0 {
            0.0
        } else {
            (hz(1) - hz(0)).abs()
        },
        span_hz: (hz(last) - hz(0)).abs(),
    }
}

/// A row never places more samples than the resampler's per-row bound,
/// which sizes a pass's sample buffers before it reads any row
/// ([`SpectralResampler::samples_per_row`],
/// [`SpectralResampler::density_samples_per_row`]): for every sampling,
/// with output channels narrower than, as wide as and wider than the native
/// ones, over rows inside, across and beyond the output axis, ascending and
/// descending, evenly spaced, uneven and sparse (a selection like
/// `0:0~1;100`, whose first spacing sets CASA's fine grid), on unpadded and
/// padded density axes. Each row is within the bound of its own spectrum
/// and of the selection's: the smallest first spacing and widest span over
/// every row.
#[test]
fn a_row_places_at_most_its_per_row_bound() {
    let natives = |first_ghz: f64, width_ghz: f64, channels: usize| {
        (0..channels)
            .map(|channel| first_ghz + width_ghz * channel as f64)
            .collect::<Vec<_>>()
    };
    let rows = [
        natives(0.9, 0.1, 13),
        natives(1.0, 0.1, 8),
        natives(1.2, 0.1, 3),
        natives(1.0, 0.05, 20),
        natives(0.95, 0.2, 7),
        natives(1.7, -0.1, 9),
        natives(1.31, 0.013, 17),
        // Sparse and uneven: Astra's counterexample on #700 places 100
        // samples from three channels.
        vec![1.0, 1.01, 2.0],
        vec![2.0, 1.99, 1.0],
        vec![0.9, 0.95, 1.3, 1.31, 1.32, 1.9],
        vec![1.4, 1.45],
    ];
    let selection =
        rows.iter()
            .map(|row| spectrum_of(row))
            .fold(RowSpectrum::default(), |widest, row| RowSpectrum {
                channels: widest.channels.max(row.channels),
                first_spacing_hz: if widest.first_spacing_hz == 0.0 {
                    row.first_spacing_hz
                } else {
                    widest.first_spacing_hz.min(row.first_spacing_hz)
                },
                span_hz: widest.span_hz.max(row.span_hz),
            });
    let tabular_axis = |hz: [f64; 5], kernel| {
        SpectralResampler::channel_local(
            SpectralAxis::tabular(&hz, hz[1] - hz[0]).expect("tabular"),
            kernel,
        )
    };
    let resamplers = [
        (
            "direct",
            SpectralResampler::direct(Basis::Constant).expect("direct"),
        ),
        (
            "nearest",
            SpectralResampler::channel_local(axis(1.0, 0.1, 8), SpectralKernel::Nearest),
        ),
        (
            "linear, narrower outputs",
            SpectralResampler::channel_local(axis(1.0, 0.025, 32), SpectralKernel::Linear),
        ),
        (
            "linear, equal outputs",
            SpectralResampler::channel_local(axis(1.0, 0.1, 8), SpectralKernel::Linear),
        ),
        (
            "linear, wider outputs",
            SpectralResampler::channel_local(axis(1.05, 0.3, 3), SpectralKernel::Linear),
        ),
        (
            "linear, much wider outputs",
            SpectralResampler::channel_local(axis(1.05, 0.1, 10), SpectralKernel::Linear),
        ),
        (
            "tabular, widening",
            tabular_axis(
                [1.03e9, 1.08e9, 1.18e9, 1.57e9, 1.94e9],
                SpectralKernel::Linear,
            ),
        ),
    ];
    for (label, resampler) in &resamplers {
        let channel_local = matches!(resampler.basis(), Basis::ChannelLocal { .. });
        for row in &rows {
            let own = spectrum_of(row);
            let placed = planes_of(resampler, row, false).len();
            for (which, spectrum) in [("own", own), ("selection", selection)] {
                let bound = resampler.samples_per_row(spectrum);
                assert!(
                    placed <= bound,
                    "{label}, {row:?}, {which} bound: {placed} > {bound}"
                );
            }
            if !channel_local {
                continue;
            }
            for padding in [0, 3] {
                let shape = density_shape(resampler, padding);
                let placed = placed_with(resampler, row, Some(padding)).len();
                for (which, spectrum) in [("own", own), ("selection", selection)] {
                    let bound = resampler.density_samples_per_row(&shape, spectrum);
                    assert!(
                        placed <= bound,
                        "{label} density padded by {padding}, {row:?}, {which} bound: \
                         {placed} > {bound}"
                    );
                }
            }
        }
    }
    // The sparse row places far more samples than it has channels.
    let sparse = SpectralResampler::channel_local(axis(1.05, 0.1, 10), SpectralKernel::Linear);
    assert_eq!(planes_of(&sparse, &[1.0, 1.01, 2.0], false).len(), 100);
}
