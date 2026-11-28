// SPDX-License-Identifier: LGPL-3.0-or-later
//! The observation: the session's setters as CASA's task calls them, the
//! schedule's `observe` and `observemany`, `predict`, and the persisted
//! model reference frequency.

use std::path::{Path, PathBuf};

use casa_core::measures::direction::MDirection;
use casa_core::measures::frequency::FrequencyRef;
use casa_core::quanta::Quantity;
use casa_core::quanta::casacore::tos;
use casa_ms::observation_io::creation::{Loss, Msv4Replacement, Stage};

use super::antennas::Array;
use super::pointings::Pointings;
use super::schedule::{Calibrator, Schedule, Step, schedule};
use super::sky::Sky;
use super::{
    Cancel, Run, SimobserveError, check_cancel, entry_exists, make_way, open_session, python,
};
use crate::session::Simulator;
use crate::simutil::direction::direction_measure;
use crate::simutil::qa;
use crate::simutil::receivers::band_name;
use crate::{
    ArrayConfig, CoordinateSystem, ObserveManySpec, ObserveSpec, Pointing, PredictSpec,
    PredictionOptions, SimulatorFormat, SpectralWindowSpec, StateSpec, VoltagePatternSpec,
};

/// The calibrator's field and source name.
const CALIBRATOR: &str = "phase calibrator";

/// Observe the pointings and predict the model into the requested store,
/// carrying the model's reference frequency into the written metadata.
pub(super) fn observe(
    run: &mut Run<'_>,
    sky: &Sky,
    array: &Array,
    pointings: &Pointings,
    calibrator: Option<&Calibrator>,
    cancel: &Cancel,
) -> Result<(), SimobserveError> {
    let ms = run.measurement_set();
    if entry_exists(&ms) && !run.parameters.overwrite {
        return Err(SimobserveError::raised(format!(
            "measurement set {} already exists and user does not wish to overwrite",
            ms.display()
        )));
    }
    let resources = &run.request.options.resources;
    let created = |path: &Path| -> Result<Vec<Loss>, SimobserveError> {
        let losses = render(run, sky, array, pointings, calibrator, path)?;
        check_baselines(path, &ms, run.uvmode, resources)?;
        Ok(losses)
    };
    let losses = if run.parameters.outformat == SimulatorFormat::Msv4 {
        publish_msv4(&ms, run.parameters.overwrite, |path| {
            let losses = created(path)?;
            check_cancel(cancel)?;
            Ok(losses)
        })?
    } else {
        created(&ms)?
    };
    for loss in losses {
        let warning = format!("MSv4: {loss}");
        tracing::warn!("{warning}");
        run.warnings.push(warning);
    }
    run.wrote(&ms);
    tracing::info!("generation of measurement set {} complete", ms.display());
    Ok(())
}

fn render(
    run: &Run<'_>,
    sky: &Sky,
    array: &Array,
    pointings: &Pointings,
    calibrator: Option<&Calibrator>,
    path: &Path,
) -> Result<Vec<Loss>, SimobserveError> {
    let model = &sky.model;
    if run.parameters.verbose {
        tracing::warn!("preparing empty measurement set");
    } else {
        tracing::info!("preparing empty measurement set");
    }
    let band = band_name(qa::value_in(&model.specrefval, "GHz")?);
    let schedule = schedule(run, pointings, calibrator.is_some())?;
    if run.parameters.outformat == SimulatorFormat::Msv2 {
        make_way(path)?;
    }
    let mut sm = open_session(run, path);
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
    sm.set_output_reference_frequency(qa::value_in(&model.specrefval, "Hz")?);
    sm.close_with_report().map_err(Into::into)
}

/// Keep an existing processing set in place until its replacement passes
/// the stored baseline check. A new destination uses the same no-clobber
/// publication as the common creation boundary.
fn publish_msv4(
    destination: &Path,
    overwrite: bool,
    create: impl FnOnce(&Path) -> Result<Vec<Loss>, SimobserveError>,
) -> Result<Vec<Loss>, SimobserveError> {
    let error = |value: casa_ms::observation_io::ObservationError| SimobserveError::Table {
        table: destination.to_path_buf(),
        reason: value.to_string(),
    };
    let replacement = if overwrite && entry_exists(destination) {
        Some(Msv4Replacement::hold(destination).map_err(error)?)
    } else {
        None
    };
    let stage = Stage::reserve(destination, "simobserve").map_err(error)?;
    let losses = match create(stage.path()) {
        Ok(losses) => losses,
        Err(error) => {
            stage.remove();
            return Err(error);
        }
    };
    if let Some(replacement) = replacement {
        if let Some(leftover) = replacement.publish(stage).map_err(error)? {
            tracing::warn!(path = %leftover.display(), "the replaced processing set could not be removed");
        }
    } else if let Err(cause) = stage.publish_new(destination) {
        stage.remove();
        return Err(error(cause));
    } else {
        stage.release();
    }
    Ok(losses)
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
    // CASA's simulator refuses these conversions, but its task ignores
    // predict=False and publishes an unpredicted MS. Report the same cause
    // before close writes DATA. REST image prediction remains supported.
    if run.uvmode {
        let frame = run
            .parameters
            .outframe
            .parse::<FrequencyRef>()
            .map_err(|error| SimobserveError::raised(error.to_string()))?;
        if frame == FrequencyRef::Undefined {
            return Err(SimobserveError::raised(
                "Transformations to/from frame \"Undefined\" are not possible.",
            ));
        }
        if frame == FrequencyRef::REST && !lists.is_empty() {
            return Err(SimobserveError::raised(
                "No MeasFrame specified for conversion of Frequency",
            ));
        }
    }
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

/// CASA's check of the observation against the mode: an interferometer's
/// MeasurementSet must have a positive `u` or `v` (a baseline), a single
/// dish's none.
fn check_baselines(
    ms: &Path,
    destination: &Path,
    uvmode: bool,
    resources: &crate::SessionResources,
) -> Result<(), SimobserveError> {
    let Some(longest) = super::uvw::largest_signed_uv(ms, resources)? else {
        return Err(SimobserveError::raised(format!(
            "no rows were observed in {}: is the source above the elevation limit?",
            destination.display()
        )));
    };
    if longest > 0. && !uvmode {
        return Err(SimobserveError::raised(format!(
            "An single observation is requested but CROSS-correlation in {}",
            destination.display()
        )));
    }
    if longest <= 0. && uvmode {
        return Err(SimobserveError::raised(format!(
            "An interferometer observation is requested but only AUTO-correlation in {}",
            destination.display()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};

    use casa_ms::observation_io::{is_measurement_set, is_msv4_store};

    use super::*;

    fn install_fixture(destination: &Path, name: &str) {
        let scratch = tempfile::tempdir().unwrap();
        let source = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../casacore/casa-ms/tests/fixtures/ms_trial")
            .join(format!("{name}.tgz"));
        tar::Archive::new(flate2::read::GzDecoder::new(File::open(source).unwrap()))
            .unpack(scratch.path())
            .unwrap();
        fs::rename(scratch.path().join(name), destination).unwrap();
        assert!(is_measurement_set(destination));
    }

    #[test]
    fn no_overwrite_keeps_a_store_arriving_after_the_early_check() {
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("output.ps.zarr");
        assert!(!entry_exists(&destination));
        install_fixture(&destination, "ms_trial.ps.zarr");
        fs::write(destination.join("sentinel"), "late MSv4").unwrap();
        let result = publish_msv4(&destination, false, |stage| {
            install_fixture(stage, "ms_trial.ps.zarr");
            Ok(Vec::new())
        });
        assert!(result.is_err());
        assert_eq!(
            fs::read_to_string(destination.join("sentinel")).unwrap(),
            "late MSv4"
        );
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
    }

    #[test]
    fn overwrite_keeps_an_msv2_that_replaces_the_held_msv4_during_render() {
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("output.ps.zarr");
        let original = root.path().join("original.ps.zarr");
        install_fixture(&destination, "ms_trial.ps.zarr");
        let result = publish_msv4(&destination, true, |stage| {
            fs::rename(&destination, &original).unwrap();
            install_fixture(&destination, "ms_trial.ms");
            fs::write(destination.join("sentinel"), "foreign MSv2").unwrap();
            install_fixture(stage, "ms_trial.ps.zarr");
            Ok(Vec::new())
        });
        assert!(result.is_err());
        assert!(is_measurement_set(&destination));
        assert!(!is_msv4_store(&destination));
        assert_eq!(
            fs::read_to_string(destination.join("sentinel")).unwrap(),
            "foreign MSv2"
        );
        assert!(is_measurement_set(&original));
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 2);
    }
}
