// SPDX-License-Identifier: LGPL-3.0-or-later
//! Canonical calibration solve session boundary.

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use casa_ms::{
    MeasurementSet, MsError, MsSelection, MsSelectionError, MsSelectionIoBudget,
    ResolvedMsSelection, ResolvedMsSelectionRow,
};
use thiserror::Error;

use crate::ApplyCalibrationTableSpec;
use crate::bandpass::{
    BandpassSolveError, BandpassSolveReport, BandpassSolveRequest, solve_bandpass,
};
use crate::execute::EvaluatedApplyRow;
use crate::solve::grouping::{
    SelectedSolveRow, all_antenna_ids, collect_selected_rows, load_preapplied_rows, resolve_refant,
};
use crate::solve::{GainSolveError, GainSolveReport, GainSolveRequest, RefAntSelector, solve_gain};

/// Input ownership for one calibration solve.
pub enum CalibrationDataset<'a> {
    /// Open the MeasurementSet at this path for the duration of the solve.
    Path(PathBuf),
    /// Borrow an already-open MeasurementSet.
    Open(&'a MeasurementSet),
}

impl CalibrationDataset<'_> {
    /// Use an on-disk MeasurementSet path.
    pub fn path(path: impl AsRef<Path>) -> Self {
        Self::Path(path.as_ref().to_path_buf())
    }

    /// Use an already-open MeasurementSet.
    pub fn open(measurement_set: &MeasurementSet) -> CalibrationDataset<'_> {
        CalibrationDataset::Open(measurement_set)
    }
}

/// Family-specific numerical request behind the shared solve lifecycle.
#[derive(Debug, Clone)]
pub enum CalibrationSolveRequest {
    /// Solve gain calibration.
    Gain(GainSolveRequest),
    /// Solve channelized or polynomial bandpass calibration.
    Bandpass(BandpassSolveRequest),
}

/// Result of the canonical calibration solve entrypoint.
#[derive(Debug, Clone, PartialEq)]
pub enum CalibrationSolveResult {
    /// Gain-calibration report.
    Gain(GainSolveReport),
    /// Bandpass-calibration report.
    Bandpass(BandpassSolveReport),
}

/// Failure from the shared calibration solve lifecycle or a family kernel.
#[derive(Debug, Error)]
pub enum CalibrationError {
    /// Opening an on-disk MeasurementSet failed.
    #[error("failed to open MeasurementSet {path}: {source}")]
    OpenMeasurementSet {
        /// Requested MeasurementSet path.
        path: String,
        /// Underlying MeasurementSet error.
        #[source]
        source: MsError,
    },
    /// Gain solve failed.
    #[error(transparent)]
    Gain(Box<GainSolveError>),
    /// Bandpass solve failed.
    #[error(transparent)]
    Bandpass(Box<BandpassSolveError>),
}

impl From<GainSolveError> for CalibrationError {
    fn from(error: GainSolveError) -> Self {
        Self::Gain(Box::new(error))
    }
}

impl From<BandpassSolveError> for CalibrationError {
    fn from(error: BandpassSolveError) -> Self {
        Self::Bandpass(Box::new(error))
    }
}

/// Execute one gain or bandpass solve through the canonical dataset boundary.
pub fn solve_calibration(
    dataset: CalibrationDataset<'_>,
    request: CalibrationSolveRequest,
) -> Result<CalibrationSolveResult, CalibrationError> {
    match dataset {
        CalibrationDataset::Path(path) => {
            let measurement_set = MeasurementSet::open(&path).map_err(|source| {
                CalibrationError::OpenMeasurementSet {
                    path: path.display().to_string(),
                    source,
                }
            })?;
            solve_open_dataset(&measurement_set, request)
        }
        CalibrationDataset::Open(measurement_set) => solve_open_dataset(measurement_set, request),
    }
}

fn solve_open_dataset(
    measurement_set: &MeasurementSet,
    request: CalibrationSolveRequest,
) -> Result<CalibrationSolveResult, CalibrationError> {
    match request {
        CalibrationSolveRequest::Gain(request) => solve_gain(measurement_set, &request)
            .map(CalibrationSolveResult::Gain)
            .map_err(CalibrationError::from),
        CalibrationSolveRequest::Bandpass(request) => solve_bandpass(measurement_set, &request)
            .map(CalibrationSolveResult::Bandpass)
            .map_err(CalibrationError::from),
    }
}

/// Shared dataset-dependent inputs prepared once for every solve family.
pub(crate) struct CalibrationSolveContext {
    pub(crate) refant_id: i32,
    pub(crate) available_antennas: BTreeSet<i32>,
    pub(crate) rows: Vec<SelectedSolveRow>,
    pub(crate) preapplied_rows: Option<HashMap<usize, EvaluatedApplyRow>>,
}

/// Resolve the common selection, reference-antenna, and preapply lifecycle.
pub(crate) fn prepare_calibration_solve(
    measurement_set: &MeasurementSet,
    selection: &MsSelection,
    refant: &RefAntSelector,
    prior_calibration_tables: &[ApplyCalibrationTableSpec],
    parang: bool,
) -> Result<CalibrationSolveContext, GainSolveError> {
    Ok(CalibrationSolveContext {
        refant_id: resolve_refant(measurement_set, refant)?,
        available_antennas: all_antenna_ids(measurement_set)?,
        rows: collect_selected_rows(measurement_set, selection)?,
        preapplied_rows: load_preapplied_rows(
            measurement_set,
            selection,
            prior_calibration_tables,
            parang,
        )?,
    })
}

pub(crate) fn resolve_calibration_selection(
    measurement_set: &MeasurementSet,
    selection: &MsSelection,
) -> Result<ResolvedMsSelection, MsSelectionError> {
    measurement_set.resolve_selection(
        selection,
        MsSelectionIoBudget::from_system_memory(
            2,
            std::mem::size_of::<ResolvedMsSelectionRow>(),
            None,
        )?,
    )
}
