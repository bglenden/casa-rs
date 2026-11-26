// SPDX-License-Identifier: LGPL-3.0-or-later
//! Developer request planning; execution belongs to the native Simulator.
use super::family::*;
use super::spec::*;
use super::{Report, generate};
use casa_core::measures::{
    EpochRef, MEpoch, MeasFrame,
    direction::{DirectionRef, MDirection},
    position::MPosition,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Path, PathBuf},
    time::Instant,
};
/// One end-to-end synthetic-observation task request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RunRequest {
    /// FITS model image path.
    #[serde(default)]
    pub model_image: Option<PathBuf>,
    /// Typed sky model, or the FITS image in `model_image`.
    #[serde(default)]
    pub model: Option<SkyModel>,
    /// Optional peak brightness scaling in Jy/pixel.
    #[serde(default)]
    pub model_peak_jy_per_pixel: Option<f32>,
    /// Output MeasurementSet path.
    pub output_ms: PathBuf,
    /// Replace an existing output MeasurementSet directory.
    #[serde(default)]
    pub overwrite: bool,
    /// Telescope name written to `OBSERVATION`.
    #[serde(default)]
    pub telescope_name: Option<String>,
    /// Single-field name used when `fields` is empty.
    #[serde(default)]
    pub field_name: Option<String>,
    /// Antenna configuration. Defaults to the CASA Guide VLA A configuration.
    #[serde(default)]
    pub antennas: Vec<Antenna>,
    /// J2000 phase center `[right_ascension, declination]` in radians.
    #[serde(default)]
    pub phase_center_rad: Option<[f64; 2]>,
    /// Optional multi-field target list. When empty, use the single phase center.
    #[serde(default)]
    pub fields: Vec<Field>,
    /// Observation start time in MJD seconds UTC.
    #[serde(default)]
    pub start_time_mjd_seconds: Option<f64>,
    /// Requested on-source duration in seconds.
    #[serde(default)]
    pub duration_seconds: Option<f64>,
    /// Integration time in seconds.
    #[serde(default)]
    pub integration_seconds: Option<f64>,
    /// Minimum antenna elevation in radians for scheduled samples and flags.
    #[serde(default)]
    pub elevation_limit_rad: Option<f64>,
    /// Permit continuous tracks that include samples below the elevation limit.
    #[serde(default)]
    pub allow_below_elevation_limit: bool,
    /// Spectral windows. Empty selects the VLA ppdisk tutorial window.
    #[serde(default)]
    pub spectral_windows: Vec<SpectralSetup>,
    /// Optional polarization/correlation setup.
    #[serde(default)]
    pub polarization_setup: Option<PolarizationSetup>,
    /// Predict visibility samples from the model image into `MAIN.DATA`.
    #[serde(default = "default_predict_model")]
    pub predict_model: bool,
    /// Optional deterministic corruptions applied to generated visibility data.
    #[serde(default)]
    pub corruption: Option<CorruptionConfig>,
    /// Worker selection policy for native row/channel parallelism.
    #[serde(default)]
    pub worker_policy: WorkerPolicy,
    /// Observation row topology.
    #[serde(default)]
    pub observation_mode: ObservationMode,
    /// Explicit row worker count when `worker_policy` is `fixed`, or an upper
    /// bound when `worker_policy` is `auto`.
    #[serde(default)]
    pub row_workers: Option<usize>,
    /// Explicit channel prediction worker count when `worker_policy` is
    /// `fixed`, or an upper bound when `worker_policy` is `auto`.
    #[serde(default)]
    pub channel_workers: Option<usize>,
}

impl RunRequest {
    /// Build a reusable library request from the task projection.
    pub fn to_synthetic_request(&self) -> ObservationRequest {
        let antennas = if self.antennas.is_empty() {
            default_vla_antennas()
        } else {
            self.antennas.clone()
        };
        let model_image = self
            .model_image
            .clone()
            .or_else(|| {
                self.model
                    .as_ref()
                    .and_then(|model| model.path().map(Path::to_path_buf))
            })
            .unwrap_or_else(|| PathBuf::from("analytic-components.json"));
        let mut request = ObservationRequest::vla_ppdisk(model_image, &self.output_ms, antennas);
        request.model = self.model.clone();
        request.model_peak_jy_per_pixel = self.model_peak_jy_per_pixel;
        request.overwrite = self.overwrite;
        if let Some(telescope_name) = &self.telescope_name {
            request.telescope_name = telescope_name.clone();
        }
        if let Some(field_name) = &self.field_name {
            request.field_name = field_name.clone();
        }
        request.fields = self.fields.clone();
        if let Some(start_time_mjd_seconds) = self.start_time_mjd_seconds {
            request.start_time_mjd_seconds = start_time_mjd_seconds;
        }
        if let Some(duration_seconds) = self.duration_seconds {
            request.duration_seconds = duration_seconds;
        }
        if let Some(integration_seconds) = self.integration_seconds {
            request.integration_seconds = integration_seconds;
        }
        if let Some(elevation_limit_rad) = self.elevation_limit_rad {
            request.elevation_limit_rad = elevation_limit_rad;
        }
        request.allow_below_elevation_limit = self.allow_below_elevation_limit;
        if let Some(phase_center_rad) = self.phase_center_rad {
            request.phase_center_rad = phase_center_rad;
        } else if request.fields.is_empty() {
            request.phase_center_rad = zenith_transit_phase_center_rad(
                &request.telescope_name,
                &request.antennas,
                request.start_time_mjd_seconds,
            )
            .unwrap_or(request.phase_center_rad);
        }
        if !self.spectral_windows.is_empty() {
            request.spectral_windows = self.spectral_windows.clone();
        }
        if let Some(polarization_setup) = &self.polarization_setup {
            request.polarization_setup = polarization_setup.clone();
        }
        request.predict_model = self.predict_model;
        request.corruption = self.corruption.clone();
        request.worker_policy = self.worker_policy;
        request.observation_mode = self.observation_mode;
        request.row_workers = self.row_workers;
        request.channel_workers = self.channel_workers;
        request
    }

    /// Execute the request and return structured output metadata.
    pub fn execute(&self) -> Result<RunResult, String> {
        let request = self.to_synthetic_request();
        let started = Instant::now();
        tracing::info!(
            output_ms = %self.output_ms.display(),
            overwrite = self.overwrite,
            worker_policy = ?self.worker_policy,
            observation_mode = ?self.observation_mode,
            "simobserve generation started"
        );
        let report = generate(&request).map_err(|error| error.to_string())?;
        tracing::info!(
            output_ms = %self.output_ms.display(),
            main_rows = report.main_row_count,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "simobserve generation completed"
        );
        Ok(RunResult {
            report,
            elapsed_millis: started.elapsed().as_millis(),
        })
    }
}

/// Structured result for one `simobserve` task execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RunResult {
    /// Synthetic-observation writer report.
    pub report: Report,
    /// Wall-clock runtime for the Rust task.
    pub elapsed_millis: u128,
}

/// Dialog-friendly request for one synthetic MS family.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FamilyRequest {
    /// Source model used for this family.
    pub source_model: SkyModel,
    /// Telescope family, for example `VLA` or `ALMA`.
    pub telescope: String,
    /// Array configuration label, CASA `.cfg` path/name, or explicit synthetic layout label.
    pub array_config: String,
    /// Explicit antenna-configuration repositories supplied by the caller.
    #[serde(default)]
    pub data_roots: Vec<PathBuf>,
    /// Receiver band label.
    pub band: String,
    /// Target MeasurementSet size in GiB.
    pub target_ms_size_gib: f64,
    /// Number of polarization correlations in the generated MS.
    pub polarizations: usize,
    /// Fractional source Stokes `[Q/I, U/I, V/I]` used by synthetic
    /// visibility prediction. Non-zero values require four linear
    /// correlations.
    #[serde(default)]
    pub source_stokes_fractional_quv: [f64; 3],
    /// Number of frequency channels in the generated MS.
    pub ms_channels: usize,
    /// Number of output image channels expected for diagnostics.
    pub image_channels: usize,
    /// Number of pointings in the generated observing pattern.
    pub pointing_count: usize,
    /// Optional exact field phase centers for CASA-oracle mosaic regeneration.
    /// When provided, the length must match `pointing_count`.
    #[serde(default)]
    pub field_phase_centers_rad: Option<Vec<[f64; 2]>>,
    /// Optional phase center as `[right_ascension_rad, declination_rad]`.
    #[serde(default)]
    pub phase_center_rad: Option<[f64; 2]>,
    /// Optional first channel center frequency in Hz.
    #[serde(default)]
    pub start_frequency_hz: Option<f64>,
    /// Optional channel width in Hz.
    #[serde(default)]
    pub channel_width_hz: Option<f64>,
    /// Optional exact number of generated time samples.
    #[serde(default)]
    pub time_sample_count: Option<usize>,
    /// Optional integration time in seconds.
    #[serde(default)]
    pub integration_seconds: Option<f64>,
    /// Optional observation start time in MJD seconds UTC.
    #[serde(default)]
    pub start_time_mjd_seconds: Option<f64>,
    /// Imaging mode label, for example `single_field`, `mosaic`, `mfs`,
    /// `spectral_cube`, `cubedata`, or `mt_mfs`.
    pub imaging_mode: String,
    /// Optional output MeasurementSet path for generated family members.
    #[serde(default)]
    pub output_ms: Option<PathBuf>,
    /// Replace an existing family output MeasurementSet directory.
    #[serde(default)]
    pub overwrite: bool,
    /// Recursively measure the final MeasurementSet directory size. Disabled by
    /// default because it is expensive for large on-the-fly generation runs.
    #[serde(default)]
    pub measure_actual_size: bool,
    /// Worker selection policy for generated runs.
    #[serde(default)]
    pub worker_policy: WorkerPolicy,
    /// Observation row topology for generated runs.
    #[serde(default)]
    pub observation_mode: ObservationMode,
    /// Optional source antenna index for single-dish total-power family runs.
    #[serde(default)]
    pub total_power_antenna_index: Option<usize>,
    /// Optional row worker control for generated runs.
    #[serde(default)]
    pub row_workers: Option<usize>,
    /// Optional channel worker control for generated runs.
    #[serde(default)]
    pub channel_workers: Option<usize>,
}

impl FamilyRequest {
    /// Check the requested size, counts, phase centers, frequencies, times
    /// and imaging mode before any planning or I/O.
    fn validate_inputs(&self) -> Result<(), String> {
        if self.target_ms_size_gib <= 0.0 || !self.target_ms_size_gib.is_finite() {
            return Err("target_ms_size_gib must be positive".to_string());
        }
        if self.polarizations == 0 {
            return Err("polarizations must be positive".to_string());
        }
        if self.ms_channels == 0 {
            return Err("ms_channels must be positive".to_string());
        }
        if self.image_channels == 0 {
            return Err("image_channels must be positive".to_string());
        }
        if self.pointing_count == 0 {
            return Err("pointing_count must be positive".to_string());
        }
        if let Some(field_phase_centers_rad) = &self.field_phase_centers_rad {
            if field_phase_centers_rad.len() != self.pointing_count {
                return Err(
                    "field_phase_centers_rad length must match pointing_count when provided"
                        .to_string(),
                );
            }
            if field_phase_centers_rad.iter().any(|[ra, dec]| {
                !ra.is_finite()
                    || !dec.is_finite()
                    || *ra < -std::f64::consts::TAU
                    || *ra > std::f64::consts::TAU
                    || *dec < -std::f64::consts::FRAC_PI_2
                    || *dec > std::f64::consts::FRAC_PI_2
            }) {
                return Err("field_phase_centers_rad entries must be finite radians".to_string());
            }
        }
        if let Some([right_ascension_rad, declination_rad]) = self.phase_center_rad
            && (!right_ascension_rad.is_finite() || !declination_rad.is_finite())
        {
            return Err("phase_center_rad values must be finite when provided".to_string());
        }
        if let Some(start_frequency_hz) = self.start_frequency_hz
            && (start_frequency_hz <= 0.0 || !start_frequency_hz.is_finite())
        {
            return Err("start_frequency_hz must be positive when provided".to_string());
        }
        if let Some(channel_width_hz) = self.channel_width_hz
            && (channel_width_hz <= 0.0 || !channel_width_hz.is_finite())
        {
            return Err("channel_width_hz must be positive when provided".to_string());
        }
        if matches!(self.time_sample_count, Some(0)) {
            return Err("time_sample_count must be positive when provided".to_string());
        }
        if let Some(integration_seconds) = self.integration_seconds
            && (integration_seconds <= 0.0 || !integration_seconds.is_finite())
        {
            return Err("integration_seconds must be positive when provided".to_string());
        }
        if let Some(start_time_mjd_seconds) = self.start_time_mjd_seconds
            && !start_time_mjd_seconds.is_finite()
        {
            return Err("start_time_mjd_seconds must be finite when provided".to_string());
        }
        validate_family_imaging_mode(&self.imaging_mode)
    }

    /// Execute the family request by generating a concrete MeasurementSet and
    /// manifest from the persisted benchmark inputs.
    pub fn execute(&self) -> Result<FamilyResult, String> {
        self.validate_inputs()?;
        let output_ms = self
            .output_ms
            .clone()
            .ok_or_else(|| "family output_ms is required".to_string())?;
        let target_bytes = (self.target_ms_size_gib * 1024.0 * 1024.0 * 1024.0).round() as u64;
        let data_cell_bytes = self.polarizations as u64 * self.ms_channels as u64 * 8;
        let flag_cell_bytes = self.polarizations as u64 * self.ms_channels as u64;
        let row_payload_bytes = (data_cell_bytes + flag_cell_bytes).max(1);
        let estimated_main_rows = (target_bytes / row_payload_bytes).max(1);
        let preset = resolve_family_preset(self)?;
        let antennas = family_antennas_for_mode(
            preset.antennas.clone(),
            self.observation_mode,
            self.total_power_antenna_index,
        )?;
        let baseline_count = family_row_pair_count(antennas.len(), self.observation_mode);
        let time_sample_count = self.time_sample_count.unwrap_or_else(|| {
            estimated_main_rows
                .div_ceil(baseline_count.max(1) as u64)
                .max(1) as usize
        });
        let integration_seconds = self.integration_seconds.unwrap_or(2.0);
        let start_frequency_hz = self.start_frequency_hz.unwrap_or(preset.start_frequency_hz);
        let channel_width_hz = self
            .channel_width_hz
            .unwrap_or_else(|| family_channel_width_hz(&self.imaging_mode, &preset));
        let phase_center_rad = self.phase_center_rad.unwrap_or(preset.phase_center_rad);
        let spectral_setup = SpectralSetup {
            name: format!("{}-{}", self.band, self.ms_channels),
            start_frequency_hz,
            channel_width_hz,
            channel_count: self.ms_channels,
        };
        let fields = family_fields(
            self.pointing_count,
            phase_center_rad,
            self.field_phase_centers_rad.as_deref(),
            &self.imaging_mode,
            spectral_setup.reference_frequency_hz(),
            preset.dish_diameter_m,
        );
        let mut polarization_setup =
            PolarizationSetup::new(preset.polarization_basis, self.polarizations)
                .map_err(|error| error.to_string())?;
        polarization_setup.source_stokes_fractional_quv = self.source_stokes_fractional_quv;
        polarization_setup
            .validate()
            .map_err(|error| error.to_string())?;
        let telescope_name = if self.observation_mode == ObservationMode::TotalPower
            && matches!(preset.telescope_name.as_str(), "ALMA" | "ACA")
        {
            "ALMASD".to_string()
        } else {
            preset.telescope_name.clone()
        };
        let run_request = RunRequest {
            model_image: self
                .source_model
                .path()
                .map(Path::to_path_buf)
                .or_else(|| Some(PathBuf::from("analytic-components.json"))),
            model: Some(self.source_model.clone()),
            model_peak_jy_per_pixel: None,
            output_ms: output_ms.clone(),
            overwrite: self.overwrite,
            telescope_name: Some(telescope_name),
            field_name: Some(preset.field_name.clone()),
            antennas: antennas.clone(),
            phase_center_rad: Some(phase_center_rad),
            fields,
            start_time_mjd_seconds: Some(
                self.start_time_mjd_seconds
                    .unwrap_or(preset.start_time_mjd_seconds),
            ),
            duration_seconds: Some(time_sample_count as f64 * integration_seconds),
            integration_seconds: Some(integration_seconds),
            elevation_limit_rad: Some((-89.0_f64).to_radians()),
            allow_below_elevation_limit: true,
            spectral_windows: vec![spectral_setup],
            polarization_setup: Some(polarization_setup),
            predict_model: true,
            corruption: None,
            worker_policy: self.worker_policy,
            observation_mode: self.observation_mode,
            row_workers: self.row_workers,
            channel_workers: self.channel_workers,
        };
        let run_result = run_request.execute()?;
        let (actual_ms_bytes, actual_ms_bytes_source) = if self.measure_actual_size {
            (
                directory_size_bytes(&output_ms).map_err(|error| {
                    format!(
                        "failed to measure generated MeasurementSet {}: {error}",
                        output_ms.display()
                    )
                })?,
                "directory_walk".to_string(),
            )
        } else {
            (
                directory_size_bytes(&output_ms).map_err(|e| e.to_string())?,
                "directory_walk".to_string(),
            )
        };
        let manifest_path = family_manifest_path(&output_ms);
        let manifest = FamilyManifest {
            source_model: self.source_model.clone(),
            source_model_kind: self.source_model.kind_name().to_string(),
            telescope: self.telescope.clone(),
            array_config: self.array_config.clone(),
            array_config_source: preset.array_config_source.clone(),
            band: self.band.clone(),
            imaging_mode: self.imaging_mode.clone(),
            output_ms: output_ms.clone(),
            target_bytes,
            actual_ms_bytes,
            actual_ms_bytes_source: actual_ms_bytes_source.clone(),
            requested_polarizations: self.polarizations,
            requested_ms_channels: self.ms_channels,
            requested_image_channels: self.image_channels,
            requested_pointing_count: self.pointing_count,
            requested_field_phase_centers_rad: self.field_phase_centers_rad.clone(),
            estimated_main_rows,
            generated_main_rows: run_result.report.main_row_count as u64,
            worker_policy: self.worker_policy,
            observation_mode: self.observation_mode,
            total_power_antenna_index: self.total_power_antenna_index,
            row_workers: self.row_workers,
            channel_workers: self.channel_workers,
            run_request: run_request.clone(),
            run_result: run_result.clone(),
        };
        fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest).map_err(|error| error.to_string())?,
        )
        .map_err(|error| {
            format!(
                "failed to write family manifest {}: {error}",
                manifest_path.display()
            )
        })?;
        Ok(FamilyResult {
            source_model_kind: self.source_model.kind_name().to_string(),
            target_bytes,
            actual_ms_bytes,
            actual_ms_bytes_source,
            estimated_main_rows,
            estimated_visibility_payload_bytes: estimated_main_rows * row_payload_bytes,
            requested_ms_channels: self.ms_channels,
            requested_image_channels: self.image_channels,
            requested_pointing_count: self.pointing_count,
            requested_field_phase_centers_rad: self.field_phase_centers_rad.clone(),
            generated_main_rows: run_result.report.main_row_count as u64,
            output_ms,
            manifest_path,
            run_result: Box::new(run_result),
            worker_policy: self.worker_policy,
            observation_mode: self.observation_mode,
            total_power_antenna_index: self.total_power_antenna_index,
            row_workers: self.row_workers,
            channel_workers: self.channel_workers,
        })
    }
}

/// Structured manifest persisted next to a generated synthetic MS family member.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FamilyManifest {
    /// Source model used for this family member.
    pub source_model: SkyModel,
    /// Active source-model kind.
    pub source_model_kind: String,
    /// Requested telescope label.
    pub telescope: String,
    /// Requested array configuration label.
    pub array_config: String,
    /// Resolved source of the antenna coordinates.
    pub array_config_source: String,
    /// Requested receiver band label.
    pub band: String,
    /// Requested imaging mode label.
    pub imaging_mode: String,
    /// Generated MeasurementSet path.
    pub output_ms: PathBuf,
    /// Requested target size in bytes.
    pub target_bytes: u64,
    /// Actual generated size in bytes from `actual_ms_bytes_source`.
    pub actual_ms_bytes: u64,
    /// Source used for `actual_ms_bytes`.
    pub actual_ms_bytes_source: String,
    /// Requested number of polarization correlations.
    pub requested_polarizations: usize,
    /// Requested MS channel count.
    pub requested_ms_channels: usize,
    /// Requested image channel count.
    pub requested_image_channels: usize,
    /// Requested pointing count.
    pub requested_pointing_count: usize,
    /// Requested exact field phase centers, when supplied by an oracle harness.
    #[serde(default)]
    pub requested_field_phase_centers_rad: Option<Vec<[f64; 2]>>,
    /// Estimated MAIN rows from the sizing planner.
    pub estimated_main_rows: u64,
    /// MAIN rows actually generated.
    pub generated_main_rows: u64,
    /// Worker selection policy.
    pub worker_policy: WorkerPolicy,
    /// Observation row topology.
    #[serde(default)]
    pub observation_mode: ObservationMode,
    /// Source antenna index for total-power family runs.
    #[serde(default)]
    pub total_power_antenna_index: Option<usize>,
    /// Optional row worker count.
    #[serde(default)]
    pub row_workers: Option<usize>,
    /// Optional channel worker count.
    #[serde(default)]
    pub channel_workers: Option<usize>,
    /// Concrete run request generated from the family request.
    pub run_request: RunRequest,
    /// Concrete run result.
    pub run_result: RunResult,
}

/// Result for a generated synthetic MS family request.
/// Developer generator request envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "request", rename_all = "snake_case")]
pub enum Request {
    /// Execute one developer fixture request.
    Run(Box<RunRequest>),
    /// Plan one synthetic MeasurementSet family from persisted benchmark inputs.
    Family(Box<FamilyRequest>),
}

impl Request {
    /// Execute the request and return the canonical task result envelope.
    pub fn execute(&self) -> Result<Outcome, String> {
        match self {
            Self::Run(request) => Ok(Outcome::Run(Box::new(request.execute()?))),
            Self::Family(request) => Ok(Outcome::Family(Box::new(request.execute()?))),
        }
    }
}

/// Developer generator result envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "result", rename_all = "snake_case")]
pub enum Outcome {
    /// Completed synthetic-observation run.
    Run(Box<RunResult>),
    /// Planned synthetic MeasurementSet family.
    Family(Box<FamilyResult>),
}

fn default_predict_model() -> bool {
    true
}

fn default_vla_antennas() -> Vec<Antenna> {
    tutorial_vla_a_antennas()
}

fn zenith_transit_phase_center_rad(
    _telescope: &str,
    antennas: &[Antenna],
    seconds: f64,
) -> Option<[f64; 2]> {
    if antennas.is_empty() {
        return None;
    }
    let center = std::array::from_fn::<_, 3, _>(|axis| {
        antennas.iter().map(|a| a.position_m[axis]).sum::<f64>() / antennas.len() as f64
    });
    let frame = MeasFrame::new()
        .with_position(MPosition::new_itrf(center[0], center[1], center[2]))
        .with_epoch(MEpoch::from_mjd(seconds / 86_400., EpochRef::UTC))
        .with_measures(casa_ms::open_measures_runtime().ok()?);
    let direction = MDirection::from_angles(0., std::f64::consts::FRAC_PI_2, DirectionRef::AZEL)
        .convert_to(DirectionRef::J2000, &frame)
        .ok()?;
    Some([direction.longitude_rad(), direction.latitude_rad()])
}
