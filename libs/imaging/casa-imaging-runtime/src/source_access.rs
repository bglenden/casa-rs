// SPDX-License-Identifier: LGPL-3.0-or-later
//! The block layout of the selected observation's rows, and the budget
//! that bounds what the source holds before the problem quotes its own.

use casa_imaging_model::{CompiledProblem, NativeSpacingSelection, SpectralWindowSelection};
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
/// channels, and from the storage owner's `CHAN_FREQ` the smallest adjacent
/// separation and the widest span of any window's selected channels,
/// widened by [`FRAME_MARGIN`]. A window without a catalog leaves its rows
/// evenly spaced ([`RowSpectrum::evenly_spaced`]); the storage owner binds
/// one to every MeasurementSet selection.
fn row_spectrum<'a>(windows: impl Iterator<Item = &'a SpectralWindowSelection>) -> RowSpectrum {
    let mut spectrum = RowSpectrum::evenly_spaced(0);
    let mut known = true;
    for window in windows {
        if let Some(selection) = window.native_spacing() {
            if let Some(catalog) = window.coordinate_catalog() {
                match selection {
                    NativeSpacingSelection::Pair([first, second]) => {
                        let hz = catalog.channel_frequencies_hz();
                        include_spacing(&mut spectrum, hz[*first as usize], hz[*second as usize]);
                    }
                    NativeSpacingSelection::Bands(_) => {
                        for pair in catalog.channel_frequencies_hz().windows(2) {
                            include_spacing(&mut spectrum, pair[0], pair[1]);
                        }
                    }
                }
            } else {
                known = false;
            }
        }
        for channels in
            std::iter::once(window.channel_indices()).chain(window.prediction_channels())
        {
            spectrum.channels = spectrum.channels.max(channels.len());
            let frequency = |index: usize| {
                let catalog = window.coordinate_catalog()?;
                catalog.channel_frequency_hz(*channels.get(index)? as usize)
            };
            if channels.len() < 2 {
                continue;
            }
            let (Some(first), Some(last)) = (frequency(0), frequency(channels.len() - 1)) else {
                known = false;
                continue;
            };
            // A per-time selection may omit leading channels, so any
            // adjacent gap can become the row's first gap. Native row
            // frequencies are strictly monotonic: removing intervening
            // channels cannot make a smaller gap.
            for index in 1..channels.len() {
                let (Some(left), Some(right)) = (frequency(index - 1), frequency(index)) else {
                    known = false;
                    break;
                };
                include_spacing(&mut spectrum, left, right);
            }
            spectrum.span_hz = spectrum
                .span_hz
                .max((last - first).abs() * (1.0 + FRAME_MARGIN));
        }
    }
    if known {
        spectrum
    } else {
        RowSpectrum::evenly_spaced(spectrum.channels)
    }
}

fn include_spacing(spectrum: &mut RowSpectrum, left: f64, right: f64) {
    let spacing = (right - left).abs() * (1.0 - FRAME_MARGIN);
    if spacing > 0.0 && (spectrum.first_spacing_hz == 0.0 || spacing < spectrum.first_spacing_hz) {
        spectrum.first_spacing_hz = spacing;
    }
}

/// The bytes that bound what the source reads before the compiled problem
/// quotes its own requirements: how far its batches grow beyond one time,
/// and what a phase centre's ephemeris table may hold.
pub const BOOTSTRAP_SOURCE_BYTES: usize = 64 << 20;

#[cfg(test)]
mod tests {
    use super::*;
    use casa_imaging_model::SpectralWindowCoordinateCatalog;

    /// A time can omit leading channels. Its first gap can be smaller than
    /// the full selection's, so it must not exceed the admitted row work.
    #[test]
    fn row_spectrum_bounds_the_first_gap_of_a_compacted_time() {
        let catalogue =
            SpectralWindowCoordinateCatalog::new(vec![1.0e9, 1.1e9, 1.101e9, 1.2e9], 1.0e6)
                .expect("coordinates");
        let window =
            SpectralWindowSelection::new(0, vec![0, 1, 2, 3]).with_coordinate_catalog(catalogue);
        let spectrum = row_spectrum(std::iter::once(&window));
        assert_eq!(spectrum.channels, 4);
        assert!((spectrum.first_spacing_hz - 0.99e6).abs() < 1e-6);
        assert!((spectrum.span_hz - 2.02e8).abs() < 1e-6);
    }

    #[test]
    fn row_spectrum_charges_pruned_spacing_without_reading_the_pair() {
        let catalogue = SpectralWindowCoordinateCatalog::new(
            vec![1.0e9, 1.001e9, 1.080e9, 1.085e9, 1.090e9],
            1.0e6,
        )
        .unwrap();
        let window = SpectralWindowSelection::new(0, vec![2, 3, 4])
            .with_coordinate_catalog(catalogue)
            .with_native_spacing(NativeSpacingSelection::Pair([0, 1]));
        let spectrum = row_spectrum(std::iter::once(&window));
        assert_eq!(spectrum.channels, 3);
        assert_eq!(spectrum.first_spacing_hz, 990_000.0);
        assert_eq!(spectrum.span_hz, 10_100_000.0);
    }

    /// The source and pass reserve for the wider final prediction as well
    /// as the channels selected for gridding.
    #[test]
    fn row_spectrum_bounds_the_prediction_selection() {
        let catalogue =
            SpectralWindowCoordinateCatalog::new(vec![1.0e9, 1.1e9, 1.2e9, 1.3e9, 1.4e9], 1.0e8)
                .expect("coordinates");
        let window = SpectralWindowSelection::new(0, vec![2, 3])
            .with_coordinate_catalog(catalogue)
            .with_prediction_read(vec![0, 1, 2, 3, 4], None);
        let spectrum = row_spectrum(std::iter::once(&window));
        assert_eq!(spectrum.channels, 5);
        assert!((spectrum.first_spacing_hz - 0.99e8).abs() < 1e-6);
        assert!((spectrum.span_hz - 4.04e8).abs() < 1e-6);
    }
}
