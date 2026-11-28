// SPDX-License-Identifier: LGPL-3.0-or-later
//! Canonical calibration solve session boundary.

pub(crate) mod scan_timing;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use casa_ms::{
    MeasurementSet, MsError, MsSelection, MsSelectionError, MsSelectionIoBudget,
    ResolvedMsSelection, ResolvedMsSelectionRow,
};
use thiserror::Error;

use crate::apply::ApplyCalibrationTableSpec;
use crate::bandpass::{
    BandpassSolveControls, BandpassSolveError, BandpassSolveReport, BandpassSolveRequest,
    solve_bandpass,
};
use crate::execute::PreappliedRows;
use crate::selected_data::SelectedData;
use crate::solve::grouping::{
    PriorCalibrationMode, SelectedSolveRow, all_antenna_ids, collect_selected_rows,
    load_preapplied_rows, resolve_refants,
};
use crate::solve::{
    GainSolveError, GainSolveReport, GainSolveRequest, GainSplineControls, GainType,
    RefAntSelector, accor, fringe, gspline, solve_gain,
};

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
    /// Solve legacy time-domain spline gains with explicit controls.
    GainSpline {
        /// Shared gain-selection, model and output parameters.
        gain: GainSolveRequest,
        /// Spline knot spacing and phase-wrap controls.
        controls: GainSplineControls,
    },
    /// Solve VLBI delay, rate and phase over selected visibility data.
    Fringe {
        /// Shared selection, prior and output settings.
        gain: GainSolveRequest,
        /// Fringe search and global refinement controls.
        controls: fringe::FringeControls,
    },
    /// Direct CASA fringe solve with an explicit phase/delay/rate/dispersion mask.
    FringeTool {
        /// Shared selection, prior and output settings.
        gain: GainSolveRequest,
        /// Fringe search controls and the delay, rate and dispersion masks.
        controls: fringe::FringeControls,
        /// Whether to include phase in the global refinement.
        active_phase: bool,
    },
    /// Normalize the sampler response from antenna autocorrelations.
    Accor(GainSolveRequest),
    /// Solve the additive Mueller response of each baseline from observed data.
    AdditiveMueller(GainSolveRequest),
    /// Fit a frequency polynomial for each baseline and correlation.
    AdditiveMuellerPolynomial {
        /// Shared selection, prior calibration and output controls.
        gain: GainSolveRequest,
        /// Polynomial degree along the selected frequency domain.
        fit_order: i32,
    },
    /// Solve one multiplicative response per baseline and parallel hand.
    Baseline {
        /// Shared selection, prior, model and output parameters.
        gain: GainSolveRequest,
        /// Keep a solution for each selected frequency bin.
        frequency_dependent: bool,
        /// Optional channel or bandwidth interval for frequency-dependent fits.
        frequency_interval: Option<crate::bandpass::BandpassFrequencyInterval>,
    },
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
    /// Legacy EP antenna pointing solution.
    LegacyEp {
        /// Output calibration table path.
        output_table: std::path::PathBuf,
        /// Number of antenna rows written.
        solution_row_count: usize,
    },
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
    solve_calibration_with_output_mode(
        dataset,
        request,
        crate::output::CalibrationOutputMode::Replace,
    )
}

/// Execute a solve with an explicit product publication policy. Append checks
/// CASA's table identity and SPW metadata before publishing; no solutions never
/// creates or changes a product, irrespective of this policy.
pub fn solve_calibration_with_output_mode(
    dataset: CalibrationDataset<'_>,
    request: CalibrationSolveRequest,
    mode: crate::output::CalibrationOutputMode,
) -> Result<CalibrationSolveResult, CalibrationError> {
    solve_calibration_with_activity(dataset, request, mode, None, ToolSolveOptions::default())
}

#[derive(Default)]
pub(crate) struct ToolSolveOptions<'a> {
    pub bandpass: BandpassSolveControls,
    pub spline_mode: Option<gspline::ToolOptions<'a>>,
    pub legacy_iterator: bool,
    pub invalid_mode: bool,
    pub tf: Option<crate::solve::TfSolveSettings>,
}

pub(crate) fn solve_calibration_with_activity(
    dataset: CalibrationDataset<'_>,
    request: CalibrationSolveRequest,
    mode: crate::output::CalibrationOutputMode,
    activity: Option<&mut crate::solve_stats::ToolSolveActivity>,
    options: ToolSolveOptions<'_>,
) -> Result<CalibrationSolveResult, CalibrationError> {
    if let CalibrationSolveRequest::Fringe { gain, .. }
    | CalibrationSolveRequest::FringeTool { gain, .. } = &request
        && gain.gain_type != GainType::K
    {
        return Err(GainSolveError::Delay(
            "fringe controls require the real delay parameter schema".into(),
        )
        .into());
    }
    match dataset {
        CalibrationDataset::Path(path) => {
            if matches!(request, CalibrationSolveRequest::GainSpline { .. })
                && casa_ms::observation_io::is_msv4_store(&path)
            {
                return Err(GainSolveError::from(
                    casa_observation::ObservationError::Unsupported {
                        what: "GSPLINE gaincal on an MSv4 MeasurementSet (#1055)",
                    },
                )
                .into());
            }
            #[cfg(unix)]
            if let CalibrationSolveRequest::Gain(gain) = &request
                && activity.is_none()
                && options.tf.is_none()
                && !options.legacy_iterator
                && !options.invalid_mode
            {
                if crate::solve::observation_gain::supports(gain) {
                    if let Some(report) = crate::solve::observation_gain::solve(&path, gain, mode)?
                    {
                        return Ok(CalibrationSolveResult::Gain(report));
                    }
                } else if casa_ms::observation_io::is_msv4_store(&path) {
                    return Err(GainSolveError::from(casa_observation::ObservationError::Unsupported {
                        what: "an MSv4 gain solve outside the time-binned G point-source modes (calmode='ap' or 'p') (#1055)",
                    }).into());
                }
            }
            let open_phase = crate::timing::Phase::start("open_ms");
            let measurement_set = MeasurementSet::open(&path).map_err(|source| {
                CalibrationError::OpenMeasurementSet {
                    path: path.display().to_string(),
                    source,
                }
            })?;
            drop(open_phase);
            solve_open_dataset(&measurement_set, request, mode, activity, options)
        }
        CalibrationDataset::Open(measurement_set) => {
            solve_open_dataset(measurement_set, request, mode, activity, options)
        }
    }
}

fn solve_open_dataset(
    measurement_set: &MeasurementSet,
    request: CalibrationSolveRequest,
    mode: crate::output::CalibrationOutputMode,
    activity: Option<&mut crate::solve_stats::ToolSolveActivity>,
    options: ToolSolveOptions<'_>,
) -> Result<CalibrationSolveResult, CalibrationError> {
    let ToolSolveOptions {
        bandpass,
        spline_mode,
        legacy_iterator,
        invalid_mode,
        tf,
    } = options;
    match request {
        CalibrationSolveRequest::Gain(request) => {
            let result = if let Some(settings) = tf {
                crate::solve::solve_tf(measurement_set, &request, settings, mode, activity)
            } else {
                solve_gain(measurement_set, &request, mode, activity, legacy_iterator)
            };
            result
                .map(CalibrationSolveResult::Gain)
                .map_err(CalibrationError::from)
        }
        CalibrationSolveRequest::GainSpline { gain, controls } => {
            gspline::solve(measurement_set, &gain, controls, mode, spline_mode)
                .map(CalibrationSolveResult::Gain)
                .map_err(CalibrationError::from)
        }
        CalibrationSolveRequest::Fringe { gain, controls } => {
            fringe::solve(measurement_set, &gain, controls, mode, None)
                .map(CalibrationSolveResult::Gain)
                .map_err(CalibrationError::from)
        }
        CalibrationSolveRequest::FringeTool {
            gain,
            controls,
            active_phase,
        } => fringe::solve(
            measurement_set,
            &gain,
            controls,
            mode,
            Some(fringe::ToolOptions {
                active_phase,
                legacy_iterator,
                activity,
            }),
        )
        .map(CalibrationSolveResult::Gain)
        .map_err(CalibrationError::from),
        CalibrationSolveRequest::Accor(gain) => accor::solve(
            measurement_set,
            &gain,
            mode,
            activity,
            legacy_iterator,
            invalid_mode,
        )
        .map(CalibrationSolveResult::Gain)
        .map_err(CalibrationError::from),
        CalibrationSolveRequest::Baseline {
            gain,
            frequency_dependent,
            frequency_interval,
        } => crate::solve::baseline::solve(
            measurement_set,
            &gain,
            crate::solve::baseline::Family::Multiplicative {
                frequency_dependent,
                frequency_interval,
            },
            mode,
            activity,
            legacy_iterator,
        )
        .map(CalibrationSolveResult::Gain)
        .map_err(|error| CalibrationError::Gain(Box::new(error))),
        CalibrationSolveRequest::AdditiveMueller(gain) => crate::solve::baseline::solve(
            measurement_set,
            &gain,
            crate::solve::baseline::Family::Additive,
            mode,
            activity,
            legacy_iterator,
        )
        .map(CalibrationSolveResult::Gain)
        .map_err(|error| CalibrationError::Gain(Box::new(error))),
        CalibrationSolveRequest::AdditiveMuellerPolynomial { gain, fit_order } => {
            crate::solve::additive_polynomial::solve(
                measurement_set,
                &gain,
                fit_order,
                mode,
                legacy_iterator,
            )
            .map(CalibrationSolveResult::Gain)
            .map_err(|error| CalibrationError::Gain(Box::new(error)))
        }
        CalibrationSolveRequest::Bandpass(request) => {
            solve_bandpass(measurement_set, &request, mode, activity, bandpass)
                .map(CalibrationSolveResult::Bandpass)
                .map_err(CalibrationError::from)
        }
    }
}

/// Shared dataset-dependent inputs prepared once for every solve family.
pub(crate) struct CalibrationSolveContext {
    pub(crate) refant_id: i32,
    pub(crate) refant_alternates: Vec<i32>,
    pub(crate) available_antennas: BTreeSet<i32>,
    pub(crate) rows: Vec<SelectedSolveRow>,
    /// What `rows` select, for the solve's report.
    pub(crate) selected_data: SelectedData,
    pub(crate) preapplied_rows: Option<PreappliedRows>,
}

/// Resolve the common selection, reference-antenna, and preapply lifecycle.
pub(crate) fn prepare_calibration_solve(
    measurement_set: &MeasurementSet,
    selection: &MsSelection,
    refant: &RefAntSelector,
    prior_calibration_tables: &[ApplyCalibrationTableSpec],
    parang: bool,
    full_selection: bool,
    prior_mode: PriorCalibrationMode,
) -> Result<CalibrationSolveContext, GainSolveError> {
    let refants = resolve_refants(measurement_set, refant)?;
    let refant_id = refants.first().copied().unwrap_or(-1);
    let available_antennas = all_antenna_ids(measurement_set)?;
    let rows = collect_selected_rows(measurement_set, selection, full_selection)?;
    if !full_selection {
        // The legacy bandpass setup still addresses a description by its
        // window. Gaincal can solve descriptions separately.
        refuse_shared_spectral_windows(&rows)?;
    }
    Ok(CalibrationSolveContext {
        refant_id,
        refant_alternates: refants.into_iter().skip(1).collect(),
        available_antennas,
        selected_data: SelectedData::of(measurement_set, &rows)?,
        rows,
        preapplied_rows: load_preapplied_rows(
            measurement_set,
            selection,
            prior_calibration_tables,
            parang,
            prior_mode,
        )?,
    })
}

pub(crate) fn refuse_shared_spectral_windows(
    rows: &[SelectedSolveRow],
) -> Result<(), GainSolveError> {
    let mut descriptions = BTreeMap::<i32, BTreeSet<i32>>::new();
    for row in rows {
        descriptions
            .entry(row.data_spw_id)
            .or_default()
            .insert(row.data_desc_id);
    }
    match descriptions.into_iter().find(|(_, ids)| ids.len() > 1) {
        Some((spw_id, ids)) => Err(GainSolveError::SharedSpectralWindow {
            spw_id,
            data_desc_ids: ids.into_iter().collect(),
        }),
        None => Ok(()),
    }
}

pub(crate) fn resolve_calibration_selection(
    measurement_set: &MeasurementSet,
    selection: &MsSelection,
) -> Result<ResolvedMsSelection, MsSelectionError> {
    measurement_set.resolve_selection(selection, calibration_selection_budget()?)
}

/// Resolve MS selection with calibration's CASA-grammar memory budget.
pub fn resolve_casa_calibration_selection(
    measurement_set: &MeasurementSet,
    selection: &MsSelection,
) -> Result<ResolvedMsSelection, MsSelectionError> {
    measurement_set.resolve_casa_selection(selection, calibration_selection_budget()?)
}

fn calibration_selection_budget() -> Result<MsSelectionIoBudget, MsSelectionError> {
    MsSelectionIoBudget::from_system_memory(2, std::mem::size_of::<ResolvedMsSelectionRow>(), None)
}
