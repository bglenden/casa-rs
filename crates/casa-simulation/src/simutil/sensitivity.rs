// SPDX-License-Identifier: LGPL-3.0-or-later
//! `simutil.sensitivity`: the simulator's radiometer/noise-scale diagnostic.
//! Build the same bounded synthetic track and read its middle unflagged T
//! solution, rather than duplicate the already validated noise mathematics.

use casa_atmosphere::{AbsorptionProfile, AtmosphereProfile};
use casa_imaging_runtime::{Demand, admit};
use casa_tables::{Table, TableOptions};
use casa_types::ArrayValue;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{EpochRef, MEpoch};
use casa_types::quanta::Quantity;

use crate::corrupt::tsys_atm::{self, TsysAtm, WindowAtmosphere};
use crate::corrupt::{RowNoise, TsysManual, WindowNoise, airmass};
use crate::readantenna::AntennaConfigFile;
use crate::{
    ArrayConfig, AtmosphereSpec, CoordinateSystem, NoiseSpec, ObserveSpec, SessionResources,
    Simulator, SimulatorOptions, SpectralWindowSpec, StateSpec, TsysParameters,
};

use super::SimutilError;
use super::config::default_mount;
use super::receivers::{noise_temperature, polarization_settings};

/// Inputs of the numerical sensitivity estimate, in SI units.
#[derive(Debug, Clone)]
pub struct SensitivitySpec {
    /// Channel centre, Hz.
    pub frequency_hz: f64,
    /// Channel bandwidth, Hz.
    pub bandwidth_hz: f64,
    /// Total on-source time, seconds.
    pub time_s: f64,
    /// Requested transit elevation, radians.
    pub elevation_rad: f64,
    /// Atmospheric water column, mm (CASA's default is 2).
    pub pwv_mm: f64,
    /// Integration duration; absent means one percent of the total time.
    pub integration_s: Option<f64>,
    /// `tsys-atm` or `tsys-manual`.
    pub method: String,
    /// Manual zenith opacity.
    pub tau0: f64,
    /// Manual sky temperature, K (CASA's default is 200).
    pub sky_k: f64,
    /// Retain the diagnostic's own temporary MS and calibration table.
    pub debug: bool,
    /// Also measure the RMS of the simulated, lightly cleaned noise image.
    pub image_noise: bool,
}

/// The theoretical sensitivity and optional measured image noise, Jy.
#[derive(Debug, Clone, Copy)]
pub struct SensitivityOutcome {
    /// The simulator's two-polarization radiometer estimate.
    pub theoretical_jy: f64,
    /// RMS of the requested noise-only image.
    pub image_rms_jy: Option<f64>,
}

fn failed(error: impl std::fmt::Display) -> SimutilError {
    SimutilError::Arithmetic(error.to_string())
}

/// CASA's theoretical two-polarization point-source noise, Jy.
/// Native simulation retains its established UTC and per-row noise fixes.
///
/// # Errors
/// CASA's low-elevation or unknown-method refusal; invalid dimensions,
/// durations or an array without cross baselines; simulator/noise/table errors.
pub fn sensitivity(
    config: &AntennaConfigFile,
    spec: &SensitivitySpec,
    options: SimulatorOptions,
) -> Result<SensitivityOutcome, SimutilError> {
    let integration = validate_spec(spec)?;
    let antennas = config.x.len();
    if antennas < 2 {
        return Err(failed("sensitivity requires at least two antennas"));
    }
    configured_sensitivity(config, spec, options, integration)
}

fn validate_spec(spec: &SensitivitySpec) -> Result<f64, SimutilError> {
    if !spec.elevation_rad.is_finite() {
        return Err(failed("elevation must be finite"));
    }
    if spec.elevation_rad < 3.0_f64.to_radians() {
        return Err(failed("Elevation < ALMA limit of 3 deg"));
    }
    if !matches!(spec.method.as_str(), "tsys-atm" | "tsys-manual") {
        return Err(failed(format!(
            "Unknown calculation method {}",
            spec.method
        )));
    }
    let integration = spec.integration_s.unwrap_or(spec.time_s * 0.01);
    for (value, name) in [
        (spec.frequency_hz, "frequency"),
        (spec.bandwidth_hz.abs(), "bandwidth"),
        (spec.time_s, "observing time"),
        (integration, "integration time"),
    ] {
        if !value.is_finite() || value <= 0.0 {
            return Err(failed(format!("{name} must be positive and finite")));
        }
    }
    let used = if spec.method == "tsys-manual" {
        vec![(spec.tau0, "opacity"), (spec.sky_k, "sky temperature")]
    } else {
        vec![(spec.pwv_mm, "PWV")]
    };
    for (value, name) in used {
        if !value.is_finite() {
            return Err(failed(format!("{name} must be finite")));
        }
    }
    Ok(integration)
}

fn configured_sensitivity(
    config: &AntennaConfigFile,
    spec: &SensitivitySpec,
    options: SimulatorOptions,
    integration: f64,
) -> Result<SensitivityOutcome, SimutilError> {
    let mut temporary = tempfile::Builder::new();
    temporary.prefix("simulation-sensitivity-");
    let scratch = if spec.debug {
        temporary.tempdir_in(std::env::current_dir().map_err(failed)?)
    } else {
        temporary.tempdir()
    }
    .map_err(failed)?;
    let ms = scratch.path().join("track.ms");
    let table_root = scratch.path().join("scale");
    let resources = options.resources;
    let mut sm = Simulator::open(&ms, options);
    sm.set_config(&ArrayConfig {
        telescope: config.telescope.clone(),
        x: config.x.clone(),
        y: config.y.clone(),
        z: config.z.clone(),
        dish_diameter: config.dish_diameter.clone(),
        offset: vec![0.0],
        mount: vec![default_mount(&config.telescope).to_string()],
        antenna_names: config.antenna_names.clone(),
        pad_names: config.pad_names.clone(),
        coordinate_system: CoordinateSystem::Global,
    })
    .map_err(failed)?;
    let (stokes, feed) = polarization_settings(&config.telescope, None);
    sm.set_spectral_window(&SpectralWindowSpec {
        name: "band1".into(),
        start_frequency_hz: spec.frequency_hz,
        channel_width_hz: spec.bandwidth_hz,
        resolution_hz: spec.bandwidth_hz,
        frame: FrequencyRef::LSRK,
        channel_count: 1,
        stokes: stokes.into(),
    })
    .map_err(failed)?;
    sm.set_feed(feed, &[0.0], &[0.0], &[String::new()])
        .map_err(failed)?;
    sm.set_limits(0.01, 3.0_f64.to_radians());
    sm.set_auto(0.0);
    let declination =
        config.observatory.latitude_rad() + std::f64::consts::FRAC_PI_2 - spec.elevation_rad;
    let direction = MDirection::from_angles(0.0, declination, DirectionRef::J2000);
    sm.set_field("src1", &direction, "OBJ", 0.0)
        .map_err(failed)?;
    sm.set_times(integration, true, MEpoch::from_mjd(55_927.0, EpochRef::TAI))
        .map_err(failed)?;
    sm.observe(&ObserveSpec {
        source: "src1".into(),
        spectral_window: "band1".into(),
        start_s: 0.0,
        stop_s: spec.time_s,
        add_observation: true,
        state: StateSpec {
            sig: true,
            reference: false,
            cal: 0.0,
            load: 0.0,
            sub_scan: 0,
            obs_mode: String::new(),
        },
        observer: "CASA simulator".into(),
        project: String::new(),
    })
    .map_err(failed)?;
    sm.set_data(&[], &[], "").map_err(failed)?;
    let parameters = noise_parameters(&config.telescope, spec)?;
    let mut noise = if spec.method == "tsys-manual" {
        NoiseSpec::tsys_manual(parameters)
    } else {
        NoiseSpec::tsys_atm(parameters, spec.pwv_mm, AtmosphereSpec::default())
    };
    noise.table = table_root.display().to_string();
    sm.set_noise(&noise).map_err(failed)?;
    if spec.image_noise {
        sm.corrupt().map_err(failed)?;
    }
    sm.close().map_err(failed)?;
    let gain = middle_gain(&table_root.with_extension("T.cal"))?;
    let integrations = spec.time_s / integration;
    let antennas = config.x.len();
    let baselines = 0.5 * antennas as f64 * (antennas - 1) as f64;
    let theoretical = 1.0 / (f64::from(gain).powi(2) * (integrations * baselines * 2.0).sqrt());
    let image_rms_jy = spec
        .image_noise
        .then(|| {
            image_noise(
                &ms,
                &scratch.path().join("noise"),
                spec.frequency_hz,
                resources,
            )
        })
        .transpose()?;
    if spec.debug {
        tracing::info!(
            "sensitivity diagnostic retained at {}",
            scratch.path().display()
        );
        let _ = scratch.keep();
    }
    Ok(SensitivityOutcome {
        theoretical_jy: theoretical,
        image_rms_jy,
    })
}

/// Reuse the imaging application for CASA's small natural-weighted Clark
/// calculation. This completes the diagnostic, not the general imclean API.
fn image_noise(
    ms: &std::path::Path,
    prefix: &std::path::Path,
    frequency_hz: f64,
    resources: SessionResources,
) -> Result<f64, SimutilError> {
    use casa_images::PagedImage;
    use casa_imaging_application::{BackendRequest, Cancel, ImagingRequest, RunContext, execute};

    let cell = 6e3 / 250.0 / (frequency_hz / 1e9);
    let values = serde_json::json!({
        "vis": ms, "imagename": prefix, "imsize": [128,128], "cell": format!("{cell}arcsec"),
        "datacolumn": "data", "gridder": "standard", "deconvolver": "clark",
        "weighting": "natural", "stokes": "I", "niter": 100, "threshold": "0.01mJy", "pbcor": true,
    });
    let mut request =
        ImagingRequest::from_tclean_with_defaults(values.as_object().expect("imaging parameters"))
            .map_err(failed)?;
    request.backend = BackendRequest::Cpu;
    request.gridprecision = Some(casa_imaging_application::GridPrecision::F64);
    execute(
        &request,
        RunContext {
            host: *resources.host(),
            policy: *resources.policy(),
            cancel: Cancel::new(),
            summary: None,
        },
    )
    .map_err(failed)?;
    let image = PagedImage::<f32>::open(prefix.with_extension("image")).map_err(failed)?;
    let pixels = image
        .get_slice(&vec![0; image.shape().len()], image.shape())
        .map_err(failed)?;
    let squares = pixels.iter().try_fold(0.0, |sum, &value| {
        if value.is_finite() {
            Ok(sum + f64::from(value).powi(2))
        } else {
            Err(failed("noise image contains a nonfinite pixel"))
        }
    })?;
    Ok((squares / pixels.len() as f64).sqrt())
}

fn noise_parameters(
    telescope: &str,
    spec: &SensitivitySpec,
) -> Result<TsysParameters, SimutilError> {
    let frequency = Quantity::new(spec.frequency_hz, "Hz").map_err(failed)?;
    let receiver = noise_temperature(telescope, &frequency, None, None, None)?;
    Ok(TsysParameters {
        tau: spec.tau0,
        atmosphere_k: if spec.sky_k == 0.0 { 200.0 } else { spec.sky_k },
        antenna_efficiency: receiver.antenna(),
        spillover_efficiency: receiver.spillover,
        correlator_efficiency: receiver.correlator,
        receiver_k: receiver.receiver_temperature_k,
        ground_k: 270.0,
        cmb_k: 2.725,
        sensitivity: 0.0,
        receiver_type: i64::from(telescope == "ALMA" && spec.frequency_hz > 600e9),
    })
}

/// Radiometer sensitivity for identical dishes at the requested elevation,
/// without inventing array positions. Corrects CASA's undefined `padnames`
/// on its explicit telescope/diameter/count path.
///
/// # Errors
/// Nonphysical dish/count/durations, elevation below CASA's limit, an
/// unknown method, or failure building the existing atmosphere/noise model.
pub fn array_sensitivity(
    telescope: &str,
    diameter_m: f64,
    antennas: usize,
    spec: &SensitivitySpec,
    resources: SessionResources,
) -> Result<f64, SimutilError> {
    let integration = validate_spec(spec)?;
    if !diameter_m.is_finite() || diameter_m <= 0.0 {
        return Err(failed("diameter must be positive and finite"));
    }
    if antennas < 2 {
        return Err(failed("sensitivity requires at least two antennas"));
    }
    let parameters = noise_parameters(telescope, spec)?;
    let mass = airmass(spec.elevation_rad);
    let sigma = if spec.method == "tsys-manual" {
        let window = WindowNoise {
            spectral_window_id: 0,
            reference_frequency_hz: spec.frequency_hz,
            channel_bandwidth_hz: spec.bandwidth_hz.abs(),
        };
        let model = TsysManual::new(&parameters);
        check_zenith_tsys(model.zenith_tsys(spec.frequency_hz, spec.bandwidth_hz, 1))?;
        let model = model.for_window(&window);
        check_gain(model.antenna_gain(diameter_m, mass, integration))?;
        model.sigma(&RowNoise {
            time_s: 0.0,
            antennas: [0, 1],
            diameters_m: [diameter_m; 2],
            airmass: [mass; 2],
            exposure_s: integration,
        })
    } else {
        let atmosphere =
            AtmosphereProfile::new(&AtmosphereSpec::default().resolved(270.0)).map_err(failed)?;
        let memory = WindowAtmosphere::bytes(1, atmosphere.layers().len()) + 64 * 1024;
        let reservation = admit(
            resources.host(),
            resources.policy(),
            &Demand {
                phase: "simulation sensitivity atmosphere",
                memory,
            },
        )
        .map_err(failed)?;
        let frequencies = [spec.frequency_hz];
        let grid = tsys_atm::window_grid(&frequencies).map_err(failed)?;
        let totals = tsys_atm::channel_totals(&atmosphere, &frequencies);
        let profile = AbsorptionProfile::from_channels(atmosphere, grid, totals).map_err(failed)?;
        let pwv = if spec.pwv_mm > 0.0 { spec.pwv_mm } else { 1.0 };
        if pwv > 100.0 {
            return Err(failed("PWV must not exceed 100 mm"));
        }
        let window = WindowAtmosphere::new(
            &profile,
            tsys_atm::single(pwv) * 1e-3,
            (tsys_atm::single(parameters.spillover_efficiency), 270.0),
        )
        .map_err(failed)?;
        let model = TsysAtm::new(&parameters, vec![Some(window)], reservation);
        check_zenith_tsys(model.tsys(model.window(0), 0, 1.0))?;
        let mut scales = [0.0];
        model.antenna_gains(
            model.window(0),
            (diameter_m, mass),
            (spec.bandwidth_hz.abs(), integration),
            &mut scales,
        );
        check_gain(scales[0])?;
        let inverse = 1.0 / scales[0];
        inverse * inverse
    };
    let integrations = spec.time_s / integration;
    let baselines = 0.5 * antennas as f64 * (antennas - 1) as f64;
    Ok(f64::from(sigma) / (integrations * baselines * 2.0).sqrt())
}

fn check_zenith_tsys(value: f32) -> Result<(), SimutilError> {
    // Same 10^6 K admission as Simulator::set_noise, before its bad-gain
    // neutralization. A numerical estimate must not hide an invalid model.
    if !value.is_finite() || value <= 0.0 || value > 1e6 {
        return Err(failed(format!(
            "Tsys {value} poorly defined - check inputs"
        )));
    }
    Ok(())
}

fn check_gain(value: f32) -> Result<(), SimutilError> {
    if !value.is_finite() || value <= 0.0 {
        return Err(failed("Tsys noise scale is not positive and finite"));
    }
    Ok(())
}

fn middle_gain(path: &std::path::Path) -> Result<f32, SimutilError> {
    let table = Table::open(TableOptions::new(path)).map_err(failed)?;
    let mut unflagged = Vec::new();
    for row in 0..table.row_count() {
        let flag = table
            .cell_accessor(row, "FLAG")
            .and_then(|cell| cell.array())
            .map_err(failed)?;
        let ArrayValue::Bool(flags) = flag else {
            return Err(failed("Tsys FLAG is not boolean"));
        };
        if flags.iter().next().copied().unwrap_or(true) {
            continue;
        }
        let value = table
            .cell_accessor(row, "CPARAM")
            .and_then(|cell| cell.array())
            .map_err(failed)?;
        let ArrayValue::Complex32(gains) = value else {
            return Err(failed("Tsys CPARAM is not complex"));
        };
        if let Some(gain) = gains.iter().next() {
            unflagged.push(gain.re);
        }
    }
    unflagged
        .get(unflagged.len() / 2)
        .copied()
        .ok_or_else(|| failed("no unflagged sensitivity noise scale"))
}
