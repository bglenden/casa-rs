// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read original row identities for single-dish baseline fits and application.

use casa_core::ArrayValue;
use casa_tables::Table;
use ndarray::ArrayD;

use super::{MsTransformError, MsTransformRequest, scalar_f64, scalar_i32};

/// The original MeasurementSet coordinates of a selected baseline spectrum.
#[derive(Clone, Copy)]
pub struct SpectrumRow {
    /// Physical row index in MAIN.
    pub row: usize,
    /// Selected MAIN row index, or the row within a time-averaged iterator chunk.
    pub selection_row: usize,
    /// Original spectral-window identifier.
    pub window: i32,
    /// Parallel-hand positions after the correlation selector, when requested.
    pub parallel_hands: Option<[usize; 2]>,
    /// Scan number.
    pub scan: i32,
    /// Single-dish antenna identifier.
    pub antenna: i32,
    /// Feed or beam identifier.
    pub feed: i32,
    /// Integration time in MJD seconds.
    pub time: f64,
    /// Nominal integration length in seconds.
    pub interval: f64,
}

/// Weight cells supplied to a selected spectral operation.
pub struct SpectrumWeights<'a> {
    /// Row-level weights for the selected correlations.
    pub scalar: Option<&'a ArrayValue>,
    /// Optional per-channel weights for the selected correlations.
    pub spectrum: Option<&'a ArrayValue>,
}

/// Fit or apply a baseline within the selected MeasurementSet stream.
pub trait SpectrumBaseline {
    /// Whether to visit selected rows by observation, array, feed, and description.
    fn atmospheric_order(&self) -> bool {
        false
    }

    /// Whether to fill the output MeasurementSet with processed spectra.
    fn writes_rows(&self) -> bool;

    /// Whether the operation derives scalar weights and leaves spectral weights undefined.
    fn updates_weight(&self) -> bool;

    /// Whether the operation uses the two selected parallel-hand positions.
    fn reads_parallel_hands(&self) -> bool {
        false
    }

    /// Whether to read input channel weights for a spectrum that is not written.
    fn reads_spectral_weights(&self) -> bool {
        false
    }

    /// Whether the task removes its now-undefined spectral weight columns.
    fn omit_spectral_weights(&self) -> bool;

    /// Process one whole selected spectrum, optionally returning new scalar weights.
    fn process(
        &mut self,
        row: &SpectrumRow,
        data: &mut ArrayValue,
        flags: &mut ArrayD<bool>,
        weights: SpectrumWeights<'_>,
    ) -> Result<Option<ArrayValue>, String>;
}

pub(super) fn spectrum_rows(
    table: &Table,
    request: &MsTransformRequest,
    rows: &[usize],
    coordinates: impl Iterator<Item = (i32, Option<[usize; 2]>)>,
) -> Result<Vec<SpectrumRow>, MsTransformError> {
    let read = |name| {
        table
            .column_accessor(name)
            .and_then(|column| column.scalar_cells_owned_for_rows(rows))
            .map_err(|source| MsTransformError::MutateMeasurementSet {
                path: request.input_ms.display().to_string(),
                source: Box::new(source),
            })
    };
    let scan = read("SCAN_NUMBER")?;
    let antenna = read("ANTENNA1")?;
    let feed = read("FEED1")?;
    let times = read("TIME")?;
    let intervals = read("INTERVAL")?;
    let mut selection_rows = rows.to_vec();
    selection_rows.sort_unstable();
    rows.iter()
        .zip(coordinates)
        .enumerate()
        .map(|(index, (&row, (window, parallel_hands)))| {
            Ok(SpectrumRow {
                row,
                selection_row: selection_rows
                    .binary_search(&row)
                    .expect("selected MAIN row"),
                window,
                parallel_hands,
                scan: scalar_i32(scan[index].as_ref(), "SCAN_NUMBER", row)?,
                antenna: scalar_i32(antenna[index].as_ref(), "ANTENNA1", row)?,
                feed: scalar_i32(feed[index].as_ref(), "FEED1", row)?,
                time: scalar_f64(times[index].as_ref(), "TIME", row)?,
                interval: scalar_f64(intervals[index].as_ref(), "INTERVAL", row)?,
            })
        })
        .collect()
}
