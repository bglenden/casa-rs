// SPDX-License-Identifier: LGPL-3.0-or-later
//! The block layout of the selected observation's rows, and the budget
//! that bounds what the source holds before the problem quotes its own.

use casa_imaging_model::{CompiledProblem, SpectralWindowSelection};
use casa_imaging_operator::RowSpectrum;

use crate::pass::BlockShape;

/// Relative allowance for the Doppler factor between the frame of a
/// spectral window's stored `CHAN_FREQ` and the output frame rows reach the
/// passes in: 3,000 km/s.
pub const FRAME_MARGIN: f64 = 0.01;

/// One row of the blocks `problem`'s source delivers: the most channels any
/// selected spectral window gives a row and their frequencies' extent, the
/// most correlations any selection routes, and the image domains each row
/// is projected on; no rows.
#[must_use]
pub fn row_layout(problem: &CompiledProblem) -> BlockShape {
    let sources = problem.observation_transaction().read_set().sources();
    BlockShape {
        rows: 0,
        spectrum: row_spectrum(
            sources
                .iter()
                .flat_map(|source| source.selection().spectral_windows()),
        ),
        correlations: sources
            .iter()
            .flat_map(|source| source.selection().partitions())
            .map(|partition| partition.products().len())
            .max()
            .unwrap_or(0),
        // One projection per pass domain: each facet of a faceted image
        // domain is one (#664).
        domains: problem
            .geometry()
            .domains()
            .iter()
            .map(|domain| domain.facets().len())
            .sum(),
    }
}

/// What bounds the samples a row of `windows` places: its most selected
/// channels, and from the storage owner's `CHAN_FREQ` the smallest first
/// separation and the widest span of any window's selected channels,
/// widened by [`FRAME_MARGIN`]. A window without a catalog leaves its rows
/// evenly spaced ([`RowSpectrum::evenly_spaced`]); the storage owner binds
/// one to every MeasurementSet selection.
fn row_spectrum<'a>(windows: impl Iterator<Item = &'a SpectralWindowSelection>) -> RowSpectrum {
    let mut spectrum = RowSpectrum::evenly_spaced(0);
    let mut known = true;
    for window in windows {
        let channels = window.channel_indices();
        spectrum.channels = spectrum.channels.max(channels.len());
        let frequency = |index: usize| {
            let catalog = window.coordinate_catalog()?;
            catalog.channel_frequency_hz(*channels.get(index)? as usize)
        };
        if channels.len() < 2 {
            continue;
        }
        let (Some(first), Some(second), Some(last)) =
            (frequency(0), frequency(1), frequency(channels.len() - 1))
        else {
            known = false;
            continue;
        };
        let spacing = (second - first).abs() * (1.0 - FRAME_MARGIN);
        if spacing > 0.0
            && (spectrum.first_spacing_hz == 0.0 || spacing < spectrum.first_spacing_hz)
        {
            spectrum.first_spacing_hz = spacing;
        }
        spectrum.span_hz = spectrum
            .span_hz
            .max((last - first).abs() * (1.0 + FRAME_MARGIN));
    }
    if known {
        spectrum
    } else {
        RowSpectrum::evenly_spaced(spectrum.channels)
    }
}

/// The bytes that bound what the source reads before the compiled problem
/// quotes its own requirements: how far its batches grow beyond one time,
/// and what a phase centre's ephemeris table may hold.
pub const BOOTSTRAP_SOURCE_BYTES: usize = 64 << 20;
