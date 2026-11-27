// SPDX-License-Identifier: LGPL-3.0-or-later
//! Apply differential atmospheric emission to calibrated single-dish spectra.

pub mod inputs;
mod model;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use casa_atmosphere::{AtmosphereType, ProfileParameters};
use casa_core::{ArrayValue, RecordValue, ScalarValue, Value};
use casa_ms::MeasurementSet;
use casa_mstransform::{
    MsTransformReport, MsTransformRequest, SpectrumBaseline, SpectrumRow, SpectrumWeights,
    transform_baseline_spectra,
};
use ndarray::{ArrayD, Axis};

use super::{SingleDishSession, data_column};
use casa_calibration::output::CalibrationOutput;
pub use inputs::MainRows;
use inputs::{Pointing, Weather};
use model::{GroundConditions, SpectralResponse, WindowModel};

/// Atmospheric correction on the attached MeasurementSet and selection.
pub struct SingleDishAtmosphere<'a> {
    /// CASA atmospheric configuration in model units and original spectral-window ids.
    pub config: &'a RecordValue,
    /// Observed, corrected, or float data column.
    pub datacolumn: &'a str,
    /// Destination for the selected output MeasurementSet.
    pub outfile: &'a Path,
    /// Replace an existing output after successful processing.
    pub overwrite: bool,
}

impl SingleDishSession {
    /// Correct the selected calibrated spectra and publish a MeasurementSet.
    pub fn atmcor(&self, request: SingleDishAtmosphere<'_>) -> Result<MsTransformReport, String> {
        let path = self.name()?;
        let ms = MeasurementSet::open(path).map_err(error_text)?;
        let rows = MainRows::read(&ms)?;
        self.atmcor_prepared(&ms, rows, request)
    }

    /// Correct spectra from an open MeasurementSet using the supplied MAIN rows.
    ///
    /// The rows must come from `ms`. The operation uses this session's selection
    /// and averaging state and publishes to the requested output path. An empty
    /// output path uses a temporary MeasurementSet.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid selection or configuration, overlapping input
    /// and output, failed correction, or publication errors.
    pub fn atmcor_prepared(
        &self,
        ms: &MeasurementSet,
        rows: MainRows,
        request: SingleDishAtmosphere<'_>,
    ) -> Result<MsTransformReport, String> {
        let path = self.name()?;
        let column = data_column(request.datacolumn)?;
        let scratch = request
            .outfile
            .as_os_str()
            .is_empty()
            .then(tempfile::tempdir)
            .transpose()
            .map_err(error_text)?;
        let hidden_output = scratch.as_ref().map(|path| path.path().join("atmcor.ms"));
        let output = hidden_output.as_deref().unwrap_or(request.outfile);
        if casa_calibration::paths::overlap(path, output).map_err(error_text)? {
            return Err("You are attempting to write the output on your input file.".into());
        }
        let destination =
            CalibrationOutput::prepare_atmosphere(output, request.overwrite).map_err(error_text)?;
        let options = self.line_averaging.operation_options()?;
        let spw = self
            .selection
            .spw
            .as_deref()
            .filter(|text| !text.is_empty())
            .unwrap_or("*");
        // The selected averaging iterator takes precedence over ATM in CASA.
        let mut worker: Box<dyn SpectrumBaseline> =
            if options.time_averaging.is_some() || options.single_dish_polarization.is_some() {
                Box::new(AveragedSpectra)
            } else {
                let config = Configuration::read(request.config)?;
                Box::new(Correction::prepare(path, ms, rows, config, spw)?)
            };
        let mut report = transform_baseline_spectra(
            &MsTransformRequest {
                input_ms: path.to_path_buf(),
                output_ms: destination.staging.clone(),
                spw: spw.into(),
                width: 1,
                data_column: column,
                selection: self.selection.clone(),
                keep_flags: true,
            },
            &options,
            self.reindex.unwrap_or(true),
            worker.as_mut(),
        )
        .map_err(error_text)?;
        destination.commit().map_err(error_text)?;
        report.output_ms = request.outfile.to_path_buf();
        Ok(report)
    }
}

struct AveragedSpectra;

impl SpectrumBaseline for AveragedSpectra {
    fn atmospheric_order(&self) -> bool {
        true
    }

    fn writes_rows(&self) -> bool {
        true
    }

    fn updates_weight(&self) -> bool {
        false
    }

    fn omit_spectral_weights(&self) -> bool {
        false
    }

    fn process(
        &mut self,
        _: &SpectrumRow,
        _: &mut ArrayValue,
        _: &mut ArrayD<bool>,
        _: SpectrumWeights<'_>,
    ) -> Result<Option<ArrayValue>, String> {
        Ok(None)
    }
}

struct Configuration {
    windows: BTreeSet<i32>,
    factors: Vec<f64>,
    reference: i32,
    profile: ProfileParameters,
    layers: Vec<(f64, f64)>,
    pressure: Option<f64>,
    temperature: Option<f64>,
    humidity: Option<f64>,
    water: Option<f64>,
    threads: usize,
}

impl Configuration {
    fn read(config: &RecordValue) -> Result<Self, String> {
        let windows = required_array(config, "processspw", "processing spw list (processspw)")?
            .into_iter()
            .map(|window| window as i32)
            .collect();
        let factors = required_array(config, "gainfactor", "gain factor list (gainfactor)")?;
        let reference = number(config, "refant")?
            .ok_or("ERROR in configuration: reference antenna (refant) must be given.")?
            as i32;
        let altitude = number(config, "siteAltitude")?
            .ok_or("ERROR in configuration: site altitude (in metre) must be given.")?;
        let atmosphere = number(config, "atmType")?.unwrap_or(2.) as u32;
        let atmosphere =
            AtmosphereType::from_number(atmosphere).ok_or("Invalid ATM atmosphere type.")?;
        let layers = temperature_layers(config)?;
        // The ATM layer loop uses its fixed step ratio even when the caller
        // supplied a different pressureStepFactor.
        number(config, "pressureStepFactor")?;
        let profile = ProfileParameters {
            altitude_m: altitude,
            lapse_rate_k_per_km: number(config, "lapseRate")?.unwrap_or(-5.6),
            water_vapour_scale_height_km: number(config, "scaleHeight")?.unwrap_or(2.),
            pressure_step_hpa: number(config, "pressureStep")?.unwrap_or(10.),
            top_altitude_km: number(config, "maxAltitude")?.unwrap_or(120.),
            atmosphere,
            ..ProfileParameters::default()
        };
        let threads = thread_count(number(config, "nthreads")?);
        Ok(Self {
            windows,
            factors,
            reference,
            profile,
            layers,
            pressure: number(config, "pressure")?,
            temperature: number(config, "temperature")?,
            humidity: number(config, "humidity")?,
            water: number(config, "pwv")?.map(|mm| mm * 1e-3),
            threads,
        })
    }

    fn conditions(&self, incoming: GroundConditions) -> GroundConditions {
        GroundConditions {
            pressure_hpa: self.pressure.unwrap_or(incoming.pressure_hpa),
            temperature_k: self.temperature.unwrap_or(incoming.temperature_k),
            humidity_percent: self.humidity.unwrap_or(incoming.humidity_percent),
            water_m: self.water.unwrap_or(incoming.water_m),
        }
    }
}

/// Resolve requested thread count into an OpenMP-compatible worker limit.
pub fn thread_count(requested: Option<f64>) -> usize {
    match requested {
        None => 1,
        Some(count) if count > 0. => count as usize,
        Some(_) => available_cpus().min(8),
    }
}

fn available_cpus() -> usize {
    #[cfg(target_os = "linux")]
    {
        // CASA's OpenMP default counts the process's affinity mask. The
        // standard library also applies container CPU quotas on Linux.
        // SAFETY: the mask is zeroed and writable for the length passed in.
        let mut mask = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
        if unsafe { libc::sched_getaffinity(0, std::mem::size_of_val(&mask), &mut mask) } == 0 {
            // SAFETY: sched_getaffinity initialized the mask.
            return (unsafe { libc::CPU_COUNT(&mask) } as usize).max(1);
        }
    }
    std::thread::available_parallelism().map_or(1, usize::from)
}

struct Correction {
    config: Configuration,
    rows: MainRows,
    weather: Weather,
    pointing: Pointing,
    windows: BTreeMap<i32, SelectedWindow>,
}

struct SelectedWindow {
    model: WindowModel,
    channels: Option<Vec<usize>>,
}

impl Correction {
    fn prepare(
        path: &Path,
        ms: &MeasurementSet,
        rows: MainRows,
        config: Configuration,
        spw: &str,
    ) -> Result<Self, String> {
        let pointing = Pointing::read(ms, config.reference)?;
        let weather = Weather::read(path)?;
        let windows = prepare_windows(ms, spw, &rows.science, &config)?;
        Ok(Self {
            config,
            rows,
            weather,
            pointing,
            windows,
        })
    }
}

impl SpectrumBaseline for Correction {
    fn atmospheric_order(&self) -> bool {
        true
    }
    fn writes_rows(&self) -> bool {
        true
    }
    fn updates_weight(&self) -> bool {
        false
    }
    fn omit_spectral_weights(&self) -> bool {
        false
    }

    fn process(
        &mut self,
        row: &SpectrumRow,
        data: &mut ArrayValue,
        _flags: &mut ArrayD<bool>,
        _weights: SpectrumWeights<'_>,
    ) -> Result<Option<ArrayValue>, String> {
        let Some(window) = self.windows.get_mut(&row.window) else {
            return Ok(None);
        };
        if !self.rows.on_source.get(row.row).copied().unwrap_or(false) {
            return Ok(None);
        }
        let Some(conditions) = self.weather.preceding(row.time) else {
            return Ok(None);
        };
        let elevations = self.rows.elevations(&self.pointing, row.time)?;
        let correction = window.model.correction(
            row.time,
            self.config.conditions(conditions),
            &self.config.profile,
            &self.config.layers,
            elevations,
        )?;
        match data {
            ArrayValue::Float32(data) => subtract(
                data,
                window.channels.as_deref(),
                correction,
                subtract_sample,
            )?,
            ArrayValue::Complex32(data) => {
                subtract(data, window.channels.as_deref(), correction, |v, c| {
                    subtract_sample(&mut v.re, c)
                })?
            }
            _ => return Err("Invalid atmospheric correction data cell.".into()),
        }
        Ok(None)
    }
}

fn subtract<T>(
    data: &mut ArrayD<T>,
    channels: Option<&[usize]>,
    correction: &[f64],
    apply: impl Fn(&mut T, f64) -> Result<(), String>,
) -> Result<(), String> {
    for (channel, mut values) in data.axis_iter_mut(Axis(1)).enumerate() {
        let index = channels.map_or(Some(channel), |indices| indices.get(channel).copied());
        let value = index
            .and_then(|index| correction.get(index))
            .copied()
            .ok_or("Atmospheric correction and selected channel axes disagree.")?;
        for sample in &mut values {
            apply(sample, value)?;
        }
    }
    Ok(())
}

fn subtract_sample(sample: &mut f32, correction: f64) -> Result<(), String> {
    let corrected = (f64::from(*sample) - correction) as f32;
    if sample.is_finite() && !corrected.is_finite() {
        return Err("Atmospheric correction overflows a Float32 spectrum.".into());
    }
    *sample = corrected;
    Ok(())
}

fn prepare_windows(
    ms: &MeasurementSet,
    expression: &str,
    science: &BTreeSet<i32>,
    config: &Configuration,
) -> Result<BTreeMap<i32, SelectedWindow>, String> {
    let selection = ms.resolve_spw_selector(expression).map_err(error_text)?;
    let spectral = ms.spectral_window().map_err(error_text)?;
    let basebands = Basebands::read(ms, science)?;
    let descriptions = ms.data_description().map_err(error_text)?;
    let polarization = ms.polarization().map_err(error_text)?;
    let mut receptors = BTreeMap::new();
    for row in 0..descriptions.row_count() {
        let window = descriptions.spectral_window_id(row).map_err(error_text)?;
        let id = descriptions.polarization_id(row).map_err(error_text)?;
        receptors
            .entry(window)
            .or_insert(polarization.num_corr(id as usize).map_err(error_text)? as usize);
    }
    let mut windows = BTreeMap::new();
    for &window in science.intersection(&config.windows) {
        let index = window as usize;
        if selection.spw_ids().contains(&index) {
            let weights = config
                .factors
                .get(index)
                .copied()
                .ok_or("Missing gain factor for selected window.")?;
            let band_count = basebands.channel_count(window);
            let response = match (
                band_count,
                band_count * receptors.get(&window).copied().unwrap_or(0),
            ) {
                (128 | 256, _) => SpectralResponse::Tdm,
                (_, 256 | 8192) => SpectralResponse::Triangle,
                _ => SpectralResponse::Channel,
            };
            let frequencies = spectral.chan_freq(index).map_err(error_text)?;
            let widths = spectral.chan_width(index).map_err(error_text)?;
            let model = WindowModel::new(
                &frequencies,
                widths.first().copied().unwrap_or(0.),
                response,
                weights,
                config.threads,
            )?;
            let channels = selection.ordered_channels_of(index).map_err(error_text)?;
            windows.insert(window, SelectedWindow { model, channels });
        }
    }
    Ok(windows)
}

struct Basebands {
    channels: BTreeMap<i32, usize>,
    ids: BTreeMap<i32, i32>,
}

impl Basebands {
    fn read(ms: &MeasurementSet, science: &BTreeSet<i32>) -> Result<Self, String> {
        let spectral = ms.spectral_window().map_err(error_text)?;
        let mut channels = BTreeMap::new();
        let mut ids = BTreeMap::new();
        for &window in science {
            let name = spectral.name(window as usize).map_err(error_text)?;
            if let Some((_, suffix)) = name.split_once("#BB_")
                && let Ok(band) = suffix.split('#').next().unwrap_or_default().parse::<i32>()
            {
                ids.insert(window, band);
                *channels.entry(band).or_insert(0) +=
                    spectral.num_chan(window as usize).map_err(error_text)? as usize;
            }
        }
        Ok(Self { channels, ids })
    }

    fn channel_count(&self, window: i32) -> usize {
        self.ids
            .get(&window)
            .and_then(|id| self.channels.get(id))
            .copied()
            .unwrap_or(0)
    }
}

fn number(config: &RecordValue, name: &str) -> Result<Option<f64>, String> {
    let Some(value) = config.get(name) else {
        return Ok(None);
    };
    let result = match value {
        Value::Scalar(ScalarValue::Float64(value)) => *value,
        Value::Scalar(ScalarValue::Float32(value)) => f64::from(*value),
        Value::Scalar(ScalarValue::Int32(value)) => f64::from(*value),
        Value::Scalar(ScalarValue::Int64(value)) => *value as f64,
        _ => return Err(format!("ATM configuration field {name} must be numeric.")),
    };
    Ok(Some(result))
}

fn required_array(config: &RecordValue, key: &str, display: &str) -> Result<Vec<f64>, String> {
    let field = config
        .get(key)
        .ok_or_else(|| format!("ERROR in configuration: {display} must be given."))?;
    numeric_array(field, key)
}

fn numeric_array(field: &Value, name: &str) -> Result<Vec<f64>, String> {
    match field {
        Value::Array(ArrayValue::Float64(values)) => Ok(values.iter().copied().collect()),
        Value::Array(ArrayValue::Float32(values)) => {
            Ok(values.iter().map(|&n| f64::from(n)).collect())
        }
        Value::Array(ArrayValue::Int32(values)) => {
            Ok(values.iter().map(|&n| f64::from(n)).collect())
        }
        Value::Array(ArrayValue::Int64(values)) => Ok(values.iter().map(|&n| n as f64).collect()),
        _ => Err(format!(
            "ATM configuration field {name} must be a numeric array."
        )),
    }
}

fn temperature_layers(config: &RecordValue) -> Result<Vec<(f64, f64)>, String> {
    let (Some(boundaries), Some(temperatures)) = (
        config.get("layerBoundaries"),
        config.get("layerTemperatures"),
    ) else {
        return Ok(Vec::new());
    };
    let boundaries = numeric_array(boundaries, "layerBoundaries")?;
    let temperatures = numeric_array(temperatures, "layerTemperatures")?;
    if boundaries.len() != temperatures.len() {
        return Err(
            "ERROR: list length of layerboundaries and layertemperature should be the same.".into(),
        );
    }
    Ok(boundaries.into_iter().zip(temperatures).collect())
}

fn error_text(error: impl std::fmt::Display) -> String {
    error.to_string()
}
