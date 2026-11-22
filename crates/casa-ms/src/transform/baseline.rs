// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read original row identities for single-dish baseline fits and application.

use casa_tables::Table;
use casa_types::ArrayValue;
use ndarray::ArrayD;

use super::{MsTransformError, MsTransformRequest, scalar_f64, scalar_i32};

/// The original MeasurementSet coordinates of a selected baseline spectrum.
pub struct SpectrumRow {
    /// Physical row index in MAIN.
    pub row: usize,
    /// Row index within the selected MAIN view, before visibility iteration.
    pub selection_row: usize,
    /// Original spectral-window identifier.
    pub window: i32,
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

/// Fit or apply a baseline within the selected MeasurementSet stream.
pub trait SpectrumBaseline {
    /// Whether to fill the output MeasurementSet with processed spectra.
    fn writes_rows(&self) -> bool;

    /// Whether the operation derives scalar weights and leaves spectral weights undefined.
    fn updates_weight(&self) -> bool;

    /// Whether the task removes its now-undefined spectral weight columns.
    fn omit_spectral_weights(&self) -> bool;

    /// Process one whole selected spectrum, optionally returning new scalar weights.
    fn process(
        &mut self,
        row: &SpectrumRow,
        data: &mut ArrayValue,
        flags: &mut ArrayD<bool>,
        weights: Option<&ArrayValue>,
    ) -> Result<Option<ArrayValue>, String>;
}

pub(super) fn spectrum_rows(
    table: &Table,
    request: &MsTransformRequest,
    rows: &[usize],
    windows: impl Iterator<Item = i32>,
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
        .zip(windows)
        .enumerate()
        .map(|(index, (&row, window))| {
            Ok(SpectrumRow {
                row,
                selection_row: selection_rows
                    .binary_search(&row)
                    .expect("selected MAIN row"),
                window,
                scan: scalar_i32(scan[index].as_ref(), "SCAN_NUMBER", row)?,
                antenna: scalar_i32(antenna[index].as_ref(), "ANTENNA1", row)?,
                feed: scalar_i32(feed[index].as_ref(), "FEED1", row)?,
                time: scalar_f64(times[index].as_ref(), "TIME", row)?,
                interval: scalar_f64(intervals[index].as_ref(), "INTERVAL", row)?,
            })
        })
        .collect()
}
