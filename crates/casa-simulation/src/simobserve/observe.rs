// SPDX-License-Identifier: LGPL-3.0-or-later
//! The observation: the session's setters as CASA's task calls them, the
//! schedule's `observe` and `observemany`, `predict`, and the rest
//! frequencies CASA rewrites afterwards.

use std::path::{Path, PathBuf};

use casa_ms::MeasurementSet;
use casa_tables::{Table, TableOptions};
use casa_types::measures::direction::MDirection;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::quanta::Quantity;
use casa_types::quanta::casacore::tos;
use casa_types::{ArrayValue, ScalarValue, Value};

use super::antennas::Array;
use super::pointings::Pointings;
use super::schedule::{Calibrator, Schedule, Step, schedule};
use super::sky::Sky;
use super::{Run, SimobserveError, open_session, python};
use crate::session::Simulator;
use crate::simutil::direction::direction_measure;
use crate::simutil::qa;
use crate::simutil::receivers::band_name;
use crate::{
    ArrayConfig, CoordinateSystem, ObserveManySpec, ObserveSpec, Pointing, PredictSpec,
    PredictionOptions, SpectralWindowSpec, StateSpec, VoltagePatternSpec,
};

/// The calibrator's field and source name.
const CALIBRATOR: &str = "phase calibrator";

/// The MAIN rows read at once to find the longest baseline.
const UVW_ROWS: usize = 1 << 16;

/// Observe the pointings and predict the model into `<stem>.ms` (or
/// `<stem>.sd.ms`), then set its rest frequencies to the model's.
pub(super) fn observe(
    run: &mut Run<'_>,
    sky: &Sky,
    array: &Array,
    pointings: &Pointings,
    calibrator: Option<&Calibrator>,
) -> Result<(), SimobserveError> {
    let model = &sky.model;
    let ms = run.measurement_set();
    if run.parameters.verbose {
        tracing::warn!("preparing empty measurement set");
    } else {
        tracing::info!("preparing empty measurement set");
    }
    let band = band_name(qa::value_in(&model.specrefval, "GHz")?);
    let schedule = schedule(run, pointings, calibrator.is_some())?;
    if ms.exists() && !run.parameters.overwrite {
        return Err(SimobserveError::raised(format!(
            "measurement set {} already exists and user does not wish to overwrite",
            ms.display()
        )));
    }
    let mut sm = open_session(run, &ms);
    set_up(run, &mut sm, sky, array, &band)?;
    let integration_s = qa::value_in(&qa::read(&pointings.integration)?, "s")?;
    sm.set_times(integration_s, true, schedule.reference.clone())?;
    set_fields(run, &mut sm, pointings, calibrator)?;
    observe_schedule(run, &mut sm, &schedule, &band)?;
    // CASA selects the map's fields alone, which leaves the calibrator's
    // scans unpredicted; casa-rs predicts every scan.
    let fields = (0..pointings.pointings.len() + usize::from(calibrator.is_some()))
        .map(|field| field as i64)
        .collect::<Vec<_>>();
    sm.set_data(&[], &fields, "")?;
    set_prediction(run, &mut sm, sky, array, pointings.pointings.len())?;
    tracing::info!("done setting up observations (blank visibilities)");
    predict(run, &mut sm, sky, calibrator)?;
    sm.close()?;
    run.wrote(&ms);
    tracing::info!("generation of measurement set {} complete", ms.display());
    let hz = qa::value_in(&model.specrefval, "Hz")?;
    rewrite_column(&ms.join("SPECTRAL_WINDOW"), "REF_FREQUENCY", hz)?;
    rewrite_column(&ms.join("SOURCE"), "REST_FREQUENCY", hz)?;
    check_baselines(&ms, run.uvmode)
}

/// `setconfig`, `setspwindow`, `setfeed`, `setlimits` (only when
/// `verbose`) and `setauto`.
fn set_up(
    run: &Run<'_>,
    sm: &mut Simulator,
    sky: &Sky,
    array: &Array,
    band: &str,
) -> Result<(), SimobserveError> {
    let parameters = run.parameters;
    let model = &sky.model;
    let config = &array.config;
    let telescope = config.telescope.as_str();
    let mount = if matches!(telescope, "DRAO" | "WSRT") {
        "EQUATORIAL"
    } else {
        "alt-az"
    };
    sm.set_config(&ArrayConfig {
        telescope: telescope.to_string(),
        x: config.x.clone(),
        y: config.y.clone(),
        z: config.z.clone(),
        dish_diameter: config.dish_diameter.clone(),
        offset: vec![0.0],
        mount: vec![mount.to_string()],
        antenna_names: config.antenna_names.clone(),
        pad_names: config.pad_names.clone(),
        coordinate_system: CoordinateSystem::Global,
    })?;
    let (correlations, feed) = if telescope.to_uppercase().contains("VLA") {
        (["RR", "RL", "LR", "LL"], "perfect R L")
    } else {
        (["XX", "XY", "YX", "YY"], "perfect X Y")
    };
    let leakage = parameters.leakage;
    let stokes = if leakage > 0. && run.uvmode {
        tracing::info!(
            "Parameter 'leakage' is {}. Simulating FULL-STOKES.",
            python(leakage)
        );
        correlations.join(" ")
    } else if model.stokes == "IQUV" && run.uvmode {
        tracing::info!("Model Stokes is {}. Simulating FULL-STOKES.", model.stokes);
        correlations.join(" ")
    } else {
        if leakage > 0. {
            tracing::info!(
                "Parameter 'leakage' is {} but we are not in interferometric mode. Will ignore.",
                python(leakage)
            );
        }
        format!("{} {}", correlations[0], correlations[3])
    };
    tracing::info!(" Simulated correlation products: {stokes}");
    // The start is the centre of the first channel, and both it and the
    // width reach `setspwindow` as `qa.tos` writes them.
    let start = qa::sub(
        &model.specrefval,
        &qa::mul(&model.width, &qa::number(model.specrefpix))?,
    )?;
    let width_hz = written_hz(&model.width)?;
    let frame: FrequencyRef = parameters.outframe.parse().map_err(|_| {
        SimobserveError::raised(format!(
            "Unknown frequency frame outframe='{}'",
            parameters.outframe
        ))
    })?;
    sm.set_spectral_window(&SpectralWindowSpec {
        name: band.to_string(),
        start_frequency_hz: written_hz(&start)?,
        channel_width_hz: width_hz,
        resolution_hz: width_hz,
        frame,
        channel_count: model.nchan,
        stokes,
    })?;
    sm.set_feed(feed, &[0.0], &[0.0], &["R".to_string()])?;
    if parameters.verbose {
        tracing::info!(" spectral window set at {}", tos(&model.specrefval, 9));
        sm.set_limits(0.01, 10_f64.to_radians());
        tracing::info!(" Setting shadowlimit to 0.01 and elevationlimit to 10 deg");
    } else {
        tracing::info!(" shadowlimit and elevationlimit left at their default values");
    }
    // A single dish's autocorrelations are its data.
    sm.set_auto(if run.uvmode { 0.0 } else { 1.0 });
    Ok(())
}

/// A frequency as `setspwindow` reads `qa.tos` of it, Hz.
fn written_hz(frequency: &Quantity) -> Result<f64, SimobserveError> {
    Ok(qa::value_in(&qa::read(&tos(frequency, 9))?, "Hz")?)
}

/// A direction string as the session's measure.
fn direction(text: &str) -> Result<MDirection, SimobserveError> {
    let (refer, [longitude, latitude]) = direction_measure(text)?;
    Ok(MDirection::from_angles(longitude, latitude, refer))
}

/// `setfield` of each pointing, `<stem>_<k>`, and of the calibrator.
fn set_fields(
    run: &Run<'_>,
    sm: &mut Simulator,
    pointings: &Pointings,
    calibrator: Option<&Calibrator>,
) -> Result<(), SimobserveError> {
    for (field, pointing) in pointings.pointings.iter().enumerate() {
        sm.set_field(&field_name(run, field), &direction(pointing)?, "OBJ", 0.0)?;
    }
    if let Some(calibrator) = calibrator {
        sm.set_field(CALIBRATOR, &direction(&calibrator.direction)?, "C", 0.0)?;
    }
    Ok(())
}

/// The map's field `field`: `<stem>_<field>`.
fn field_name(run: &Run<'_>, field: usize) -> String {
    format!("{}_{field}", run.stem)
}

/// The schedule's steps as `observemany` and `observe` calls.
fn observe_schedule(
    run: &Run<'_>,
    sm: &mut Simulator,
    schedule: &Schedule,
    band: &str,
) -> Result<(), SimobserveError> {
    let state = |obs_mode: &str| StateSpec {
        sig: true,
        reference: false,
        cal: 0.0,
        load: 0.0,
        sub_scan: 0,
        obs_mode: obs_mode.to_string(),
    };
    for step in &schedule.steps {
        match step {
            Step::Targets(targets) => sm.observe_many(&ObserveManySpec {
                pointings: targets
                    .iter()
                    .map(|target| Pointing {
                        source: field_name(run, target.field),
                        start_s: target.start_s,
                        stop_s: target.stop_s,
                        direction: None,
                    })
                    .collect(),
                spectral_window: band.to_string(),
                add_observation: false,
                state: state("OBSERVE_TARGET#ON_SOURCE"),
                observer: "CASA simulator".to_string(),
                project: run.stem.clone(),
            })?,
            Step::Calibrator { start_s, stop_s } => sm.observe(&ObserveSpec {
                source: CALIBRATOR.to_string(),
                spectral_window: band.to_string(),
                start_s: *start_s,
                stop_s: *stop_s,
                add_observation: false,
                state: state("CALIBRATE_PHASE.ON_SOURCE"),
                observer: "CASA simulator".to_string(),
                project: run.stem.clone(),
            })?,
        }
    }
    Ok(())
}

/// `setvp` for an interferometer (or a component list), the mosaic
/// machine for a heterogeneous array, and `SDGrid` for a single dish.
fn set_prediction(
    run: &Run<'_>,
    sm: &mut Simulator,
    sky: &Sky,
    array: &Array,
    fields: usize,
) -> Result<(), SimobserveError> {
    if run.uvmode || sky.components_only {
        sm.set_vp(&VoltagePatternSpec {
            dovp: true,
            use_default_vp: false,
            ..VoltagePatternSpec::default()
        })?;
        let diameters = &array.config.dish_diameter;
        let telescope = array.config.telescope.as_str();
        if diameters.windows(2).any(|pair| pair[0] != pair[1]) {
            if fields <= 1 {
                return Err(SimobserveError::raised(
                    "Heterogeneous array only supported for mosaics (nfld>1), and make sure that \
                     your image is larger than the primary beam or results may be unstable",
                ));
            }
            sm.set_options(&PredictionOptions {
                ftmachine: "mosaic".to_string(),
                ..PredictionOptions::default()
            })?;
        } else {
            // CASA predicts with the telescope's beam whatever the
            // configuration's diameters, and says so.
            let diameter = diameters.first().copied().unwrap_or_default();
            match telescope {
                "ALMA" if !(10.0..=13.0).contains(&diameter) => tracing::warn!(
                    "Diameter = {diameter:.6} is inconsistent with telescope=ALMA in the \
                     configuration file. *12m ALMA PB will be used*. Use observatory='ACA' in the \
                     configuration file to use a 7m diameter."
                ),
                "ACA" if !(6.0..=7.5).contains(&diameter) => tracing::warn!(
                    "Diameter = {diameter:.6} is inconsistent with telescope=ALMA in the \
                     configuration file. *7m ACA PB will be used*"
                ),
                "ALMA" | "ACA" => {}
                _ => tracing::info!(
                    "Note: diameters in configuration file will not be used - PB for {telescope} \
                     will be used"
                ),
            }
        }
    }
    if !run.uvmode {
        sm.set_options(&PredictionOptions {
            ftmachine: "sd".to_string(),
            grid_function: "pb".to_string(),
            ..PredictionOptions::default()
        })?;
    }
    Ok(())
}

/// `predict` of the model image (none for a component list alone) with
/// `complist` and the calibrator's list.
fn predict(
    run: &Run<'_>,
    sm: &mut Simulator,
    sky: &Sky,
    calibrator: Option<&Calibrator>,
) -> Result<(), SimobserveError> {
    let mut lists: Vec<PathBuf> = Vec::new();
    if !run.parameters.complist.is_empty() {
        lists.push(PathBuf::from(&run.parameters.complist));
    }
    lists.extend(calibrator.map(|calibrator| calibrator.list.clone()));
    let named = lists
        .iter()
        .map(|list| list.display().to_string())
        .collect::<Vec<_>>()
        .join(",");
    let message = match &sky.image {
        Some(image) if named.is_empty() => format!("predicting from {}", image.display()),
        Some(image) => format!("predicting from {} and {named}", image.display()),
        None => format!("predicting from {named}"),
    };
    if run.parameters.verbose {
        tracing::warn!("{message}");
    } else {
        tracing::info!("{message}");
    }
    sm.predict(&PredictSpec {
        images: sky.image.iter().cloned().collect(),
        component_lists: lists,
        incremental: false,
    })?;
    Ok(())
}

/// Set every cell of the `Double` column `column` of `table` to `value`,
/// as CASA's task sets the rest frequencies, which the simulator writes as
/// the first channel's frequency, to the model's.
fn rewrite_column(table: &Path, column: &str, value: f64) -> Result<(), SimobserveError> {
    let failed = |reason: &dyn std::fmt::Display| SimobserveError::Table {
        table: table.to_path_buf(),
        reason: format!("{column}: {reason}"),
    };
    let mut handle = Table::open(TableOptions::new(table)).map_err(|error| failed(&error))?;
    for row in 0..handle.row_count() {
        let cell = handle
            .cell_accessor(row, column)
            .map_err(|error| failed(&error))?;
        let new = match cell.value().map_err(|error| failed(&error))? {
            Some(Value::Array(ArrayValue::Float64(values))) => {
                Value::Array(ArrayValue::Float64(values.mapv(|_| value)))
            }
            _ => Value::Scalar(ScalarValue::Float64(value)),
        };
        handle
            .cell_accessor_mut(row, column)
            .and_then(|mut cell| cell.set(new))
            .map_err(|error| failed(&error))?;
    }
    handle
        .prepare_write()
        .save_selected_columns(&[column])
        .map_err(|error| failed(&error))
}

/// CASA's check of the observation against the mode: an interferometer's
/// MeasurementSet must have a positive `u` or `v` (a baseline), a single
/// dish's none.
fn check_baselines(ms: &Path, uvmode: bool) -> Result<(), SimobserveError> {
    let failed = |reason: &dyn std::fmt::Display| SimobserveError::Table {
        table: ms.to_path_buf(),
        reason: format!("UVW: {reason}"),
    };
    let set = MeasurementSet::open(ms).map_err(|error| failed(&error))?;
    let main = set.main_table();
    let rows = main.row_count();
    if rows == 0 {
        return Err(SimobserveError::raised(format!(
            "no rows were observed in {}: is the source above the elevation limit?",
            ms.display()
        )));
    }
    let column = main
        .column_accessor("UVW")
        .map_err(|error| failed(&error))?;
    let mut longest = f64::NEG_INFINITY;
    let mut first = 0;
    while first < rows {
        let chunk: Vec<usize> = (first..rows.min(first + UVW_ROWS)).collect();
        for cell in column
            .array_cells_owned(&chunk)
            .map_err(|error| failed(&error))?
        {
            if let Some(ArrayValue::Float64(uvw)) = cell {
                longest = uvw.iter().take(2).copied().fold(longest, f64::max);
            }
        }
        first += UVW_ROWS;
    }
    if longest > 0. && !uvmode {
        return Err(SimobserveError::raised(format!(
            "An single observation is requested but CROSS-correlation in {}",
            ms.display()
        )));
    }
    if longest <= 0. && uvmode {
        return Err(SimobserveError::raised(format!(
            "An interferometer observation is requested but only AUTO-correlation in {}",
            ms.display()
        )));
    }
    Ok(())
}
