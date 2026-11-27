// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native ALMA water-vapour radiometer calibration.

mod data;
mod dispersion;
mod model;
mod paths;
mod retrieval;
mod selectors;
mod series;
mod writer;

use std::io::Write;
use std::path::{Path, PathBuf};

use casa_core::{ArrayValue, ScalarValue};
use casa_ms::{MeasurementSet, MsError, derived::engine::MsCalEngine, schema::SubtableId};
use casa_tables::{RequiredScalarColumnValues, Table, TableOptions};
use serde::{Deserialize, Serialize};

use self::data::{Observation, RadiometerIntegration, RadiometerSeries};
use self::paths::{CoefficientTimeline, Retrieval, antenna_path_meters, path_weights};
use self::retrieval::Posterior;
use self::series::AntennaReplacement;
use self::writer::{AntennaPaths, PhaseWindow};

pub use selectors::{WvrSelectors, resolve_wvr_selectors};

/// Parameters of the native radiometer calibration operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WvrRequest {
    /// Input MeasurementSet.
    pub measurement_set: PathBuf,
    /// Destination T Jones table.
    pub output_table: PathBuf,
    /// Translate radiometer timestamps by this many seconds before fitting.
    pub time_offset_seconds: f64,
    /// Optional table of temperatures to subtract from raw measurements.
    pub temperature_offsets: Option<PathBuf>,
    /// Apply the frequency-dependent wet-path dispersion correction.
    pub disperse: bool,
    /// Restrict the four-channel windows used as radiometers.
    pub radiometer_windows: Vec<i32>,
    /// Restrict output windows. Empty means every window, including radiometers.
    pub output_windows: Vec<i32>,
    /// Reverse the phase sign on every selected spectral window.
    pub reverse: bool,
    /// Spectral windows whose phases are individually reversed.
    pub reversed_windows: Vec<i32>,
    /// Fit once for each contiguous interval of a source or tied group.
    pub segment_sources: bool,
    /// Number of fitted points in the fixed-interval retrieval.
    pub solution_count: usize,
    /// Source IDs grouped into a common fit when they occur consecutively.
    pub tied_sources: Vec<Vec<i32>>,
    /// Source IDs excluded from fitting and flagged in the output.
    pub flagged_sources: Vec<i32>,
    /// Source IDs included in RMS diagnostics. Empty means all sources.
    pub statistic_sources: Vec<i32>,
    /// Field IDs included in RMS diagnostics when no source selection is present.
    pub statistic_fields: Vec<i32>,
    /// Force use of the FIELD geometry in place of recorded antenna pointing.
    pub use_field_table: bool,
    /// Fit the optional continuum-opacity term.
    pub continuum: bool,
    /// Seed used by the ordinary atmospheric retrieval.
    pub seed: u32,
    /// Number of radiometer samples in the time smoother.
    pub smoothing_samples: usize,
    /// Factor applied to paths after computing the diagnostics.
    pub scale: f64,
    /// Antennas whose direct radiometer samples should be replaced.
    pub flagged_antennas: Vec<usize>,
    /// Candidate reference antennas in preference order.
    pub reference_antennas: Vec<usize>,
    /// Greatest distance to an eligible replacement antenna, in meters.
    pub max_distance_meters: f64,
    /// Number of available neighbors required to replace a radiometer.
    pub minimum_neighbors: usize,
    /// Minimum fraction of valid measurements for a direct radiometer.
    pub minimum_good_fraction: f64,
}

/// Per-antenna results from a WVR calibration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WvrAntennaReport {
    /// The MeasurementSet antenna name.
    pub name: String,
    /// Whether the antenna recorded radiometer measurements.
    pub has_radiometer: bool,
    /// Whether its direct measurements were excluded.
    pub flagged: bool,
    /// Good fraction of the original radiometer measurements.
    pub good_fraction: f64,
    /// RMS of the zenith wet path in microns, before scaling.
    pub rms_microns: f64,
    /// RMS of the path difference between the second and fourth channels.
    pub discrepancy_microns: f64,
}

/// WVR output and diagnostics shared by the task and direct tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WvrReport {
    /// Published calibration-table path.
    pub output_table: PathBuf,
    /// CASA's numerical completion status.
    pub status: i32,
    /// Per-antenna diagnostics in MeasurementSet order.
    pub antennas: Vec<WvrAntennaReport>,
    /// Text of the native WVR log.
    pub log: String,
}

/// Failure before publishing a radiometer calibration table.
#[derive(Debug, thiserror::Error)]
pub enum WvrError {
    /// Failed to read the MeasurementSet or its geometry.
    #[error(transparent)]
    Input(#[from] MsError),
    /// Input selection or retrieval could not furnish a useful path.
    #[error("{0}")]
    Invalid(String),
    /// No available measurements could furnish a solution.
    #[error("{0}")]
    NoSolution(String),
    /// The reference antenna produced zero evidence for every eligible fit.
    #[error("No finite WVR retrieval for antenna {0}")]
    ZeroEvidence(usize),
    /// The requested reference antennas could not provide or interpolate a solution.
    #[error("No usable WVR reference antenna")]
    ReferenceUnavailable,
    /// Requested input window is outside the four-channel radiometer collection.
    #[error("Spectral window {0} is not a WVR spectral window")]
    RadiometerWindow(i32),
    /// Requested output window does not exist in the MeasurementSet.
    #[error("Invalid output spectral window {0}")]
    OutputWindow(i32),
    /// Requested reversal has no selected output spectral window.
    #[error("Reversal requested for unselected spectral window {0}")]
    ReverseWindow(i32),
    /// The on-disk dispersion reference could not be read.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Failed to publish the calibration table.
    #[error(transparent)]
    Output(#[from] crate::table::CalibrationTableWriteError),
}

/// Generate atmospheric phase corrections from a MeasurementSet's radiometer data.
pub fn execute_wvr(request: &WvrRequest) -> Result<WvrReport, WvrError> {
    execute(request, crate::output::CalibrationOutputMode::Create, None)
}

fn execute(
    request: &WvrRequest,
    mode: crate::output::CalibrationOutputMode,
    logfile: Option<&Path>,
) -> Result<WvrReport, WvrError> {
    if request.solution_count == 0 {
        return Err(WvrError::Invalid("nsol must be at least one".into()));
    }
    if !request.scale.is_finite() || request.scale <= 0.0 {
        return Err(WvrError::Invalid(
            "scale must be positive and finite".into(),
        ));
    }
    if !request.time_offset_seconds.is_finite() {
        return Err(WvrError::Invalid("toffset must be finite".into()));
    }
    if request.smoothing_samples == 0 {
        return Err(WvrError::Invalid("smooth must be at least one".into()));
    }
    if request.max_distance_meters.is_nan() || request.max_distance_meters < 0.0 {
        return Err(WvrError::Invalid("maxdistm must be nonnegative".into()));
    }
    if request.continuum && (request.segment_sources || request.solution_count > 1) {
        return Err(WvrError::Invalid(
            "continuum requires a single fixed-interval fit".into(),
        ));
    }
    if !request.segment_sources
        && (!request.flagged_sources.is_empty() || !request.tied_sources.is_empty())
    {
        return Err(WvrError::Invalid(
            "sourceflag and tie require segsource".into(),
        ));
    }
    if mode == crate::output::CalibrationOutputMode::Create
        && request.output_table.symlink_metadata().is_ok()
    {
        return Err(WvrError::Invalid(format!(
            "Output calibration table {} already exists",
            request.output_table.display()
        )));
    }
    let ms = MeasurementSet::open(&request.measurement_set)?;
    let offsets = request
        .temperature_offsets
        .as_ref()
        .map(|path| Table::open(TableOptions::new(path)))
        .transpose()
        .map_err(MsError::from)?;
    let dispersion_path = request
        .disperse
        .then(dispersion::DispersionTable::reference_path)
        .transpose()?;
    let products = std::iter::once(request.output_table.as_path()).chain(logfile);
    for product in products {
        protect_inputs(&ms, product, offsets.as_ref(), dispersion_path.as_deref())?;
    }
    let windows = phase_windows(&ms, request, dispersion_path.as_deref())?;
    let fitted = match fit_observations(&ms, request, offsets.as_ref(), None) {
        Err(WvrError::ZeroEvidence(antenna)) if !request.continuum => {
            fit_observations(&ms, request, offsets.as_ref(), Some(antenna))?
        }
        other => other?,
    };
    let FittedObservations {
        data,
        elevations,
        replacement,
        antenna,
        coefficients,
        fits,
    } = fitted;
    let antenna_table = ms.antenna()?;
    let names = (0..antenna_table.row_count())
        .map(|id| antenna_table.name(id))
        .collect::<Result<Vec<_>, _>>()?;
    let statistic_intervals = series::statistics_intervals(&data, request);
    let mut diagnostics = series::PathDiagnostics::new(names, &replacement);
    let mut integrations = Vec::with_capacity(data.integrations.len());
    for (point, elevation) in data.integrations.iter().zip(elevations) {
        let derivative = coefficients
            .at(point.time_seconds)
            .temperature_per_path_k_per_meter;
        let weights = path_weights(derivative);
        let selected = statistic_intervals
            .iter()
            .any(|&[start, end]| (start..=end).contains(&point.time_seconds));
        let source_flagged = request.flagged_sources.contains(&point.source_id);
        let mut meters: Vec<_> = point
            .temperatures
            .iter()
            .enumerate()
            .map(|(id, &reading)| {
                let path = if source_flagged {
                    0.0
                } else {
                    antenna_path_meters(reading, weights)
                };
                if selected {
                    diagnostics.observe(id, path, elevation, reading, derivative);
                }
                path
            })
            .collect();
        if selected {
            diagnostics.baselines(&meters);
        }
        meters.iter_mut().for_each(|path| *path *= request.scale);
        integrations.push(AntennaPaths {
            time_seconds: point.time_seconds,
            field_id: point.field_id,
            meters,
        });
    }
    writer::publish(&ms, request, &integrations, &windows, mode)?;
    let (antennas, largest_baseline_meters) = diagnostics.finish();
    let log = series::render_log(
        &antennas,
        &fits,
        antenna,
        &statistic_intervals,
        data.observations[0].time_seconds,
        coefficients.representative(),
        largest_baseline_meters,
    );
    Ok(WvrReport {
        output_table: request.output_table.clone(),
        status: if request.continuum { -2 } else { 0 },
        antennas,
        log,
    })
}

/// Run the direct tool with its independently located diagnostic log.
pub fn execute_wvr_with_log(request: &WvrRequest, logfile: &Path) -> Result<WvrReport, WvrError> {
    let target = crate::paths::normalized(logfile)?;
    let paths = [&request.measurement_set, &request.output_table]
        .into_iter()
        .chain(request.temperature_offsets.iter());
    for path in paths {
        if crate::paths::overlap(&target, path)? {
            return Err(WvrError::Invalid(
                "WVR logfile path overlaps a scientific table".into(),
            ));
        }
    }
    if target.is_dir() {
        return Err(WvrError::Invalid("WVR logfile must be a file".into()));
    }
    let directory = target.parent().expect("an absolute file path has a parent");
    let mut file = tempfile::NamedTempFile::new_in(directory)?;
    let outcome = execute(
        request,
        crate::output::CalibrationOutputMode::Replace,
        Some(&target),
    );
    match &outcome {
        Ok(report) => file.write_all(report.log.as_bytes())?,
        Err(
            error @ (WvrError::NoSolution(_)
            | WvrError::ZeroEvidence(_)
            | WvrError::ReferenceUnavailable),
        ) => {
            writeln!(file, "WVR phase calibration\n{error}")?;
        }
        Err(_) => return outcome,
    }
    file.persist(target).map_err(|error| error.error)?;
    outcome
}

fn protect_inputs(
    ms: &MeasurementSet,
    product: &Path,
    offsets: Option<&Table>,
    dispersion: Option<&Path>,
) -> Result<(), WvrError> {
    let mut inputs = ms
        .path()
        .into_iter()
        .chain(dispersion)
        .map(Path::to_path_buf)
        .collect::<Vec<_>>();
    let subtables = SubtableId::ALL_REQUIRED
        .iter()
        .chain(SubtableId::ALL_OPTIONAL)
        .filter_map(|&subtable| ms.subtable(subtable));
    for table in std::iter::once(ms.main_table())
        .chain(subtables)
        .chain(offsets)
    {
        inputs.extend(table.path().map(Path::to_path_buf));
        if table.is_reference_table().map_err(MsError::from)? {
            inputs.extend(table.cache_plain_table_paths().map_err(MsError::from)?);
        }
    }
    for input in inputs {
        if crate::paths::overlap(product, &input)? {
            return Err(WvrError::Invalid(format!(
                "WVR product {} overlaps input {}",
                product.display(),
                input.display()
            )));
        }
    }
    Ok(())
}

fn sky_states(ms: &MeasurementSet) -> Result<Vec<i32>, WvrError> {
    let states = ms
        .subtable(SubtableId::State)
        .ok_or_else(|| MsError::MissingSubtable("STATE".into()))?;
    let mut ids = Vec::new();
    for row in 0..states.row_count() {
        let cell = states
            .cell_accessor(row, "OBS_MODE")
            .and_then(|cell| cell.scalar())
            .map_err(MsError::from)?;
        let ScalarValue::String(mode) = cell else {
            continue;
        };
        if !mode.contains("CALIBRATE_ATMOSPHERE")
            && ["ON_SOURCE", "REFERENCE", "SIGNAL"]
                .iter()
                .any(|intent| mode.contains(intent))
        {
            ids.push(row as i32);
        }
    }
    Ok(ids)
}

fn phase_windows(
    ms: &MeasurementSet,
    request: &WvrRequest,
    dispersion_path: Option<&Path>,
) -> Result<Vec<PhaseWindow>, WvrError> {
    let windows = ms.spectral_window()?;
    for &window in &request.radiometer_windows {
        if window < 0
            || window as usize >= windows.row_count()
            || windows.num_chan(window as usize)? != 4
        {
            return Err(WvrError::RadiometerWindow(window));
        }
    }
    for &window in &request.output_windows {
        if window < 0 || window as usize >= windows.row_count() {
            return Err(WvrError::OutputWindow(window));
        }
    }
    let dispersion = dispersion_path
        .map(dispersion::DispersionTable::load)
        .transpose()?;
    let indices: Vec<_> = if request.output_windows.is_empty() {
        (0..windows.row_count() as i32).collect()
    } else {
        request.output_windows.clone()
    };
    if !request.reverse {
        for &id in &request.reversed_windows {
            if !indices.contains(&id) {
                return Err(WvrError::ReverseWindow(id));
            }
        }
    }
    indices
        .into_iter()
        .map(|id| {
            let channels = windows.chan_freq(id as usize)?;
            let frequency = channels
                .get(channels.len() / 2)
                .copied()
                .filter(|frequency| frequency.is_finite() && *frequency > 0.0)
                .ok_or_else(|| {
                    WvrError::Invalid(format!(
                        "No valid middle-channel frequency in spectral window {id}"
                    ))
                })?;
            Ok(PhaseWindow {
                id,
                frequency_hz: frequency,
                reverse: request.reverse || request.reversed_windows.contains(&id),
                dispersion: dispersion
                    .as_ref()
                    .map_or(Ok(0.0), |table| table.at(frequency))?,
            })
        })
        .collect()
}

fn elevations(
    ms: &MeasurementSet,
    points: &[RadiometerIntegration],
    use_field_table: bool,
) -> Result<Vec<f64>, WvrError> {
    if !use_field_table && let Some(result) = pointing_elevations(ms, points) {
        return Ok(result);
    }
    let geometry = MsCalEngine::for_calibration(ms)?;
    points
        .iter()
        .map(|point| {
            let (_, elevation) = geometry.azel(point.time_seconds, point.field_id as usize, 0)?;
            Ok(elevation)
        })
        .collect()
}

fn pointing_elevations(ms: &MeasurementSet, points: &[RadiometerIntegration]) -> Option<Vec<f64>> {
    const BATCH_ROWS: usize = 32_768;
    let pointing = ms.subtable(SubtableId::Pointing)?;
    let columns = pointing.required_scalar_columns_owned(&["TIME"]).ok()?;
    let RequiredScalarColumnValues::Float64(times) = columns.get("TIME")? else {
        return None;
    };
    let last = times.len().checked_sub(1)?;
    let directions = pointing.column_accessor("DIRECTION").ok()?;
    let mut index = 0;
    let mut selected = Vec::with_capacity(points.len());
    for point in points {
        while index < last && times[index] < point.time_seconds {
            index += 1;
        }
        selected.push(index);
    }
    let mut selected = selected.into_iter().peekable();
    let mut elevations = Vec::with_capacity(points.len());
    // CASA falls back to FIELD when any POINTING direction cannot be read.
    // Validate in physical order without caching every direction in the table.
    for start in (0..times.len()).step_by(BATCH_ROWS) {
        let end = start.saturating_add(BATCH_ROWS).min(times.len());
        let rows = (start..end).collect::<Vec<_>>();
        let values = directions.array_cells_owned_uncached(&rows).ok()?;
        for (row, direction) in rows.into_iter().zip(values) {
            let Some(ArrayValue::Float64(direction)) = direction else {
                return None;
            };
            let elevation = *direction.get([1, 0])?;
            while selected.next_if_eq(&row).is_some() {
                elevations.push(elevation);
            }
        }
    }
    Some(elevations)
}

struct FittedObservations {
    data: RadiometerSeries,
    elevations: Vec<f64>,
    replacement: AntennaReplacement,
    antenna: usize,
    coefficients: CoefficientTimeline,
    fits: Vec<Posterior>,
}

fn fit_observations(
    ms: &MeasurementSet,
    request: &WvrRequest,
    offsets: Option<&Table>,
    failed_reference: Option<usize>,
) -> Result<FittedObservations, WvrError> {
    let mut data = RadiometerSeries::read(ms, &request.radiometer_windows, offsets)?;
    series::smooth(&mut data.integrations, request.smoothing_samples);
    let sky = sky_states(ms)?;
    data.integrations
        .retain(|point| sky.contains(&point.state_id));
    if data.integrations.len() < 2 {
        return Err(WvrError::NoSolution(
            "Fewer than two radiometer integrations point at the sky".into(),
        ));
    }
    let elevations = elevations(ms, &data.integrations, request.use_field_table)?;
    for point in &mut data.integrations {
        point.time_seconds += request.time_offset_seconds;
    }
    let replacement = series::replace_bad_antennas(ms, &mut data, request, failed_reference)?;
    let antenna = series::reference_antenna(&data, &replacement, request)?;
    let (coefficients, fits) = fit_timeline(
        &data.integrations,
        &data.observations,
        &elevations,
        antenna,
        request,
    )?;
    Ok(FittedObservations {
        data,
        elevations,
        replacement,
        antenna,
        coefficients,
        fits,
    })
}

fn fit_timeline(
    points: &[RadiometerIntegration],
    observations: &[Observation],
    elevations: &[f64],
    reference: usize,
    request: &WvrRequest,
) -> Result<(CoefficientTimeline, Vec<Posterior>), WvrError> {
    let mut knots = Vec::new();
    let mut fits = Vec::new();
    let mut attempted = false;
    for interval in series::fit_intervals(points, observations, request) {
        let point = &points[interval.reference];
        if request.flagged_sources.contains(&point.source_id) {
            continue;
        }
        let Some(temperatures) = point.temperatures[reference] else {
            continue;
        };
        if temperatures[0] <= 0.0 {
            continue;
        }
        attempted = true;
        let Some(fit) = retrieval::fit(
            temperatures,
            point.time_seconds,
            elevations[interval.reference],
            request.seed,
            request.continuum,
        ) else {
            continue;
        };
        for time_seconds in interval.boundaries {
            knots.push(Retrieval {
                time_seconds,
                ..fit.coefficients
            });
        }
        fits.push(fit);
    }
    let timeline = CoefficientTimeline::new(knots).ok_or_else(|| {
        if attempted {
            WvrError::ZeroEvidence(reference)
        } else {
            WvrError::NoSolution("No usable WVR retrievals".into())
        }
    })?;
    Ok((timeline, fits))
}
