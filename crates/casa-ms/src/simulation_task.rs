// SPDX-License-Identifier: LGPL-3.0-or-later
//! Canonical `simobserve` task request/result contracts shared by CLI and Python.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Instant;

use casa_provider_contracts::{
    ParameterValue, ProviderCliMachineActions, ProviderCliProjection, ProviderComponentSchemas,
    ProviderInvocation, ProviderInvocationAdaptation, ProviderProjectionMetadata,
    ProviderSurfaceKind, TaskOperationDescriptor, TaskSemanticContract, builtin_surface_bundle,
    derived_ui_schema_annotations, merged_components, project_ui_schema,
};
use schemars::{JsonSchema, schema::RootSchema, schema_for};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use casa_types::measures::position::MPosition;

use crate::simulation::{
    SyntheticAntenna, SyntheticBandpassCorruption, SyntheticBandpassMode,
    SyntheticCorruptionConfig, SyntheticField, SyntheticGainCorruption, SyntheticGainMode,
    SyntheticNoiseCorruption, SyntheticNoiseMode, SyntheticObservationMode,
    SyntheticObservationReport, SyntheticObservationRequest, SyntheticPointingCorruption,
    SyntheticPolarizationBasis, SyntheticPolarizationLeakageCorruption,
    SyntheticPolarizationLeakageMode, SyntheticPolarizationSetup, SyntheticSkyModel,
    SyntheticSpectralSetup, SyntheticWorkerPolicy, generate_synthetic_observation_ms,
    tutorial_vla_a_antennas, zenith_transit_phase_center_rad,
};
use crate::ui_schema::UiCommandSchema;

/// Stable protocol name advertised by `simobserve --protocol-info`.
pub const SIMOBSERVE_TASK_PROTOCOL_NAME: &str = "casa_simobserve_task";
/// Stable protocol version advertised by `simobserve --protocol-info`.
pub const SIMOBSERVE_TASK_PROTOCOL_VERSION: u32 = 2;

/// Version/compatibility information for the JSON task protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SimobserveProtocolInfo {
    /// Stable protocol identifier.
    pub protocol_name: String,
    /// Monotonic protocol version for compatibility checks.
    pub protocol_version: u32,
    /// Provider surface kind defined by the shared architecture contract.
    pub surface_kind: ProviderSurfaceKind,
    /// Binary version implementing the protocol.
    pub binary_version: String,
}

impl SimobserveProtocolInfo {
    /// Build the current `simobserve` protocol descriptor.
    pub fn current() -> Self {
        Self {
            protocol_name: SIMOBSERVE_TASK_PROTOCOL_NAME.to_string(),
            protocol_version: SIMOBSERVE_TASK_PROTOCOL_VERSION,
            surface_kind: ProviderSurfaceKind::Task,
            binary_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

/// One end-to-end synthetic-observation task request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SimobserveRunTaskRequest {
    /// Existing FITS model image path. Kept for compatibility with protocol v1.
    #[serde(default)]
    pub model_image: Option<PathBuf>,
    /// Preferred sky model. When absent, `model_image` keeps the legacy FITS
    /// image behavior.
    #[serde(default)]
    pub model: Option<SyntheticSkyModel>,
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
    pub antennas: Vec<SyntheticAntenna>,
    /// J2000 phase center `[right_ascension, declination]` in radians.
    #[serde(default)]
    pub phase_center_rad: Option<[f64; 2]>,
    /// Optional multi-field target list. When empty, use the single phase center.
    #[serde(default)]
    pub fields: Vec<SyntheticField>,
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
    /// Spectral-window setup. Defaults to the VLA ppdisk tutorial frequency.
    #[serde(default)]
    pub spectral_setup: Option<SyntheticSpectralSetup>,
    /// Optional polarization/correlation setup.
    #[serde(default)]
    pub polarization_setup: Option<SyntheticPolarizationSetup>,
    /// Predict visibility samples from the model image into `MAIN.DATA`.
    #[serde(default = "default_predict_model")]
    pub predict_model: bool,
    /// Optional deterministic corruptions applied to generated visibility data.
    #[serde(default)]
    pub corruption: Option<SyntheticCorruptionConfig>,
    /// Worker selection policy for native row/channel parallelism.
    #[serde(default)]
    pub worker_policy: SyntheticWorkerPolicy,
    /// Observation row topology.
    #[serde(default)]
    pub observation_mode: SyntheticObservationMode,
    /// Explicit row worker count when `worker_policy` is `fixed`, or an upper
    /// bound when `worker_policy` is `auto`.
    #[serde(default)]
    pub row_workers: Option<usize>,
    /// Explicit channel prediction worker count when `worker_policy` is
    /// `fixed`, or an upper bound when `worker_policy` is `auto`.
    #[serde(default)]
    pub channel_workers: Option<usize>,
}

impl SimobserveRunTaskRequest {
    /// Build a reusable library request from the task projection.
    pub fn to_synthetic_request(&self) -> SyntheticObservationRequest {
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
        let mut request =
            SyntheticObservationRequest::vla_ppdisk(model_image, &self.output_ms, antennas);
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
        if let Some(spectral_setup) = &self.spectral_setup {
            request.spectral_setup = spectral_setup.clone();
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
    pub fn execute(&self) -> Result<SimobserveRunTaskResult, String> {
        let request = self.to_synthetic_request();
        let started = Instant::now();
        tracing::info!(
            output_ms = %self.output_ms.display(),
            overwrite = self.overwrite,
            worker_policy = ?self.worker_policy,
            observation_mode = ?self.observation_mode,
            "simobserve generation started"
        );
        let report =
            generate_synthetic_observation_ms(&request).map_err(|error| error.to_string())?;
        tracing::info!(
            output_ms = %self.output_ms.display(),
            main_rows = report.main_row_count,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "simobserve generation completed"
        );
        Ok(SimobserveRunTaskResult {
            report,
            elapsed_millis: started.elapsed().as_millis(),
        })
    }
}

/// Structured result for one `simobserve` task execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SimobserveRunTaskResult {
    /// Synthetic-observation writer report.
    pub report: SyntheticObservationReport,
    /// Wall-clock runtime for the Rust task.
    pub elapsed_millis: u128,
}

/// Dialog-friendly request for one synthetic MS family.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SimobserveFamilyTaskRequest {
    /// Source model used for this family.
    pub source_model: SyntheticSkyModel,
    /// Telescope family, for example `VLA` or `ALMA`.
    pub telescope: String,
    /// Array configuration label, CASA `.cfg` path/name, or explicit synthetic layout label.
    pub array_config: String,
    /// Receiver band label.
    pub band: String,
    /// Target MeasurementSet size in GiB.
    pub target_ms_size_gib: f64,
    /// Number of polarization correlations in the generated MS.
    pub polarizations: usize,
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
    pub worker_policy: SyntheticWorkerPolicy,
    /// Observation row topology for generated runs.
    #[serde(default)]
    pub observation_mode: SyntheticObservationMode,
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

impl SimobserveFamilyTaskRequest {
    /// Execute the family request by generating a concrete MeasurementSet and
    /// manifest from the persisted dialog inputs.
    pub fn execute(&self) -> Result<SimobserveFamilyTaskResult, String> {
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
        if let Some([right_ascension_rad, declination_rad]) = self.phase_center_rad {
            if !right_ascension_rad.is_finite() || !declination_rad.is_finite() {
                return Err("phase_center_rad values must be finite when provided".to_string());
            }
        }
        if let Some(start_frequency_hz) = self.start_frequency_hz {
            if start_frequency_hz <= 0.0 || !start_frequency_hz.is_finite() {
                return Err("start_frequency_hz must be positive when provided".to_string());
            }
        }
        if let Some(channel_width_hz) = self.channel_width_hz {
            if channel_width_hz <= 0.0 || !channel_width_hz.is_finite() {
                return Err("channel_width_hz must be positive when provided".to_string());
            }
        }
        if matches!(self.time_sample_count, Some(0)) {
            return Err("time_sample_count must be positive when provided".to_string());
        }
        if let Some(integration_seconds) = self.integration_seconds {
            if integration_seconds <= 0.0 || !integration_seconds.is_finite() {
                return Err("integration_seconds must be positive when provided".to_string());
            }
        }
        if let Some(start_time_mjd_seconds) = self.start_time_mjd_seconds {
            if !start_time_mjd_seconds.is_finite() {
                return Err("start_time_mjd_seconds must be finite when provided".to_string());
            }
        }
        validate_family_imaging_mode(&self.imaging_mode)?;
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
        let spectral_setup = SyntheticSpectralSetup {
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
        let polarization_setup =
            SyntheticPolarizationSetup::new(preset.polarization_basis, self.polarizations)
                .map_err(|error| error.to_string())?;
        let telescope_name = if self.observation_mode == SyntheticObservationMode::TotalPower
            && matches!(preset.telescope_name.as_str(), "ALMA" | "ACA")
        {
            "ALMASD".to_string()
        } else {
            preset.telescope_name.clone()
        };
        let run_request = SimobserveRunTaskRequest {
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
            spectral_setup: Some(spectral_setup),
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
                run_result.report.timing.main_rows.data_io_bytes,
                "streamed_columns".to_string(),
            )
        };
        let manifest_path = family_manifest_path(&output_ms);
        let manifest = SimobserveFamilyManifest {
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
        Ok(SimobserveFamilyTaskResult {
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
pub struct SimobserveFamilyManifest {
    /// Source model used for this family member.
    pub source_model: SyntheticSkyModel,
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
    pub worker_policy: SyntheticWorkerPolicy,
    /// Observation row topology.
    #[serde(default)]
    pub observation_mode: SyntheticObservationMode,
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
    pub run_request: SimobserveRunTaskRequest,
    /// Concrete run result.
    pub run_result: SimobserveRunTaskResult,
}

/// Result for a generated synthetic MS family request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SimobserveFamilyTaskResult {
    /// Active source-model kind.
    pub source_model_kind: String,
    /// Requested target size in bytes.
    pub target_bytes: u64,
    /// Actual generated size in bytes from `actual_ms_bytes_source`.
    pub actual_ms_bytes: u64,
    /// Source used for `actual_ms_bytes`.
    pub actual_ms_bytes_source: String,
    /// Estimated MAIN row count needed to approach the target.
    pub estimated_main_rows: u64,
    /// Estimated DATA+FLAG payload bytes represented by the row budget.
    pub estimated_visibility_payload_bytes: u64,
    /// Requested MS channel count.
    pub requested_ms_channels: usize,
    /// Requested image channel count.
    pub requested_image_channels: usize,
    /// Requested pointing count.
    pub requested_pointing_count: usize,
    /// Requested exact field phase centers, when supplied by an oracle harness.
    #[serde(default)]
    pub requested_field_phase_centers_rad: Option<Vec<[f64; 2]>>,
    /// MAIN rows actually generated.
    pub generated_main_rows: u64,
    /// Generated MeasurementSet path.
    pub output_ms: PathBuf,
    /// Persisted manifest path.
    pub manifest_path: PathBuf,
    /// Concrete run result for the generated family member.
    pub run_result: Box<SimobserveRunTaskResult>,
    /// Worker selection policy for generated runs.
    pub worker_policy: SyntheticWorkerPolicy,
    /// Observation row topology for generated runs.
    #[serde(default)]
    pub observation_mode: SyntheticObservationMode,
    /// Source antenna index for total-power family runs.
    #[serde(default)]
    pub total_power_antenna_index: Option<usize>,
    /// Optional row worker count.
    #[serde(default)]
    pub row_workers: Option<usize>,
    /// Optional channel worker count.
    #[serde(default)]
    pub channel_workers: Option<usize>,
}

#[derive(Debug, Clone)]
struct FamilyPreset {
    telescope_name: String,
    field_name: String,
    antennas: Vec<SyntheticAntenna>,
    array_config_source: String,
    phase_center_rad: [f64; 2],
    start_time_mjd_seconds: f64,
    start_frequency_hz: f64,
    channel_width_hz: f64,
    dish_diameter_m: f64,
    polarization_basis: SyntheticPolarizationBasis,
}

fn resolve_family_preset(request: &SimobserveFamilyTaskRequest) -> Result<FamilyPreset, String> {
    let telescope = request.telescope.trim().to_ascii_lowercase();
    let array_config = request.array_config.trim().to_ascii_lowercase();
    let start_frequency_hz = resolve_band_frequency_hz(&telescope, &request.band)?;
    let phase_center_rad = [4.712_391_234_768_306, -0.401_423_788_703_971_4];
    let start_time_mjd_seconds = 4_895_229_577.784_943;

    if telescope == "vla" {
        let (antennas, array_config_source) = match array_config.as_str() {
            "a" | "vla.a" | "vla.a.cfg" => load_real_family_config("VLA", "vla.a.cfg")?
                .unwrap_or_else(|| {
                    (
                        tutorial_vla_a_antennas(),
                        "embedded CASA vla.a.cfg coordinates".to_string(),
                    )
                }),
            "b" | "vla.b" | "vla.b.cfg" => require_real_family_config("VLA", "vla.b.cfg")?,
            "c" | "vla.c" | "vla.c.cfg" => require_real_family_config("VLA", "vla.c.cfg")?,
            "d" | "vla.d" | "vla.d.cfg" => require_real_family_config("VLA", "vla.d.cfg")?,
            "synthetic-vla-b" | "synthetic-b" => (
                scaled_vla_antennas(0.35),
                "synthetic scaled VLA A".to_string(),
            ),
            "synthetic-vla-c" | "synthetic-c" => (
                scaled_vla_antennas(0.11),
                "synthetic scaled VLA A".to_string(),
            ),
            "synthetic-vla-d" | "synthetic-d" => (
                scaled_vla_antennas(0.035),
                "synthetic scaled VLA A".to_string(),
            ),
            _ => match load_real_family_config("VLA", request.array_config.trim())? {
                Some(loaded) => loaded,
                None => {
                    return Err(format!(
                        "unsupported VLA array_config {:?}; expected A, B, C, D, a CASA .cfg path/name, or synthetic-vla-b/c/d",
                        request.array_config
                    ));
                }
            },
        };
        return Ok(FamilyPreset {
            telescope_name: "VLA".to_string(),
            field_name: "synthetic-vla".to_string(),
            antennas,
            array_config_source,
            phase_center_rad,
            start_time_mjd_seconds,
            start_frequency_hz,
            channel_width_hz: 1.0e6,
            dish_diameter_m: 25.0,
            polarization_basis: SyntheticPolarizationBasis::Circular,
        });
    }

    if telescope == "alma" || telescope == "aca" {
        let (antennas, array_config_source, aca_only, simalma) = match array_config.as_str() {
            "aca" | "aca.cycle10" | "aca.cycle10.cfg" | "aca.cycle10.named.cfg"
                if telescope == "aca" =>
            {
                let loaded = load_real_family_config("ACA", "aca.cycle10.cfg")?
                    .or(load_real_family_config("ACA", "aca.cycle10.named.cfg")?);
                match loaded {
                    Some((antennas, source)) => (antennas, source, true, false),
                    None => {
                        return Err(real_config_not_found_message("ACA", "aca.cycle10.cfg"));
                    }
                }
            }
            "alma.cycle8.5.cfg" | "alma.cycle10.5" | "alma.cycle10.5.cfg" => {
                let config_name = if array_config == "alma.cycle8.5.cfg" {
                    "alma.cycle8.5.cfg"
                } else {
                    "alma.cycle10.5.cfg"
                };
                let (antennas, source) = require_real_family_config("ALMA", config_name)?;
                (antennas, source, false, false)
            }
            "synthetic-alma-compact" | "synthetic-12m" | "synthetic-compact" => (
                synthetic_alma_antennas("DA", 16, 12.0, 450.0),
                "synthetic ALMA 12m compact".to_string(),
                false,
                false,
            ),
            "synthetic-aca" => (
                synthetic_alma_antennas("CM", 10, 7.0, 120.0),
                "synthetic ACA 7m compact".to_string(),
                true,
                false,
            ),
            "synthetic-simalma" => {
                let mut antennas = synthetic_alma_antennas("DA", 12, 12.0, 450.0);
                antennas.extend(synthetic_alma_antennas("CM", 10, 7.0, 120.0));
                (
                    antennas,
                    "synthetic ALMA 12m plus ACA 7m".to_string(),
                    false,
                    true,
                )
            }
            _ => match load_real_family_config(&request.telescope, request.array_config.trim())? {
                Some((antennas, source)) => (
                    antennas,
                    source,
                    telescope == "aca" || array_config.contains("aca"),
                    false,
                ),
                None => {
                    return Err(format!(
                        "unsupported ALMA/ACA array_config {:?}; expected a CASA .cfg path/name such as alma.cycle10.5.cfg or aca.cycle10.cfg, or synthetic-alma-compact/synthetic-aca/synthetic-simalma",
                        request.array_config
                    ));
                }
            },
        };
        let dish_diameter_m = if aca_only { 7.0 } else { 12.0 };
        return Ok(FamilyPreset {
            telescope_name: if aca_only {
                "ACA".to_string()
            } else {
                "ALMA".to_string()
            },
            field_name: if simalma {
                "synthetic-simalma".to_string()
            } else if aca_only {
                "synthetic-aca".to_string()
            } else {
                "synthetic-alma".to_string()
            },
            antennas,
            array_config_source,
            phase_center_rad,
            start_time_mjd_seconds,
            start_frequency_hz,
            channel_width_hz: 1.0e6,
            dish_diameter_m,
            polarization_basis: SyntheticPolarizationBasis::Linear,
        });
    }

    Err(format!(
        "unsupported telescope {:?}; expected VLA, ALMA, or ACA",
        request.telescope
    ))
}

fn require_real_family_config(
    telescope: &str,
    config_name: &str,
) -> Result<(Vec<SyntheticAntenna>, String), String> {
    load_real_family_config(telescope, config_name)?
        .ok_or_else(|| real_config_not_found_message(telescope, config_name))
}

fn real_config_not_found_message(telescope: &str, config_name: &str) -> String {
    format!(
        "real {telescope} array_config {config_name:?} was not found; set CASA_RS_SIMOBSERVE_CONFIG_ROOT to a directory containing CASA simmos .cfg files, pass an explicit .cfg path, or use an explicit synthetic-* array_config"
    )
}

fn load_real_family_config(
    telescope: &str,
    config_label: &str,
) -> Result<Option<(Vec<SyntheticAntenna>, String)>, String> {
    let label = config_label.trim();
    if label.is_empty() {
        return Ok(None);
    }
    let looks_like_cfg = label.ends_with(".cfg") || Path::new(label).is_file();
    if !looks_like_cfg {
        return Ok(None);
    }
    for candidate in family_config_candidates(label) {
        if candidate.is_file() {
            let antennas = parse_casa_array_config(telescope, &candidate)?;
            return Ok(Some((antennas, candidate.display().to_string())));
        }
    }
    Ok(None)
}

fn family_config_candidates(label: &str) -> Vec<PathBuf> {
    let path = Path::new(label);
    if path.is_absolute() || path.components().count() > 1 {
        return vec![path.to_path_buf()];
    }

    let mut candidates = Vec::new();
    for env_name in ["CASA_RS_SIMOBSERVE_CONFIG_ROOT", "CASADATA"] {
        if let Some(paths) = std::env::var_os(env_name) {
            for root in std::env::split_paths(&paths) {
                push_family_config_candidates(&mut candidates, &root, label);
            }
        }
    }
    if let Some(casapath) = std::env::var_os("CASAPATH") {
        if let Some(first_root) = casapath.to_string_lossy().split_whitespace().next() {
            push_family_config_candidates(
                &mut candidates,
                &Path::new(first_root).join("data"),
                label,
            );
        }
    }
    candidates
}

fn push_family_config_candidates(candidates: &mut Vec<PathBuf>, root: &Path, label: &str) {
    candidates.push(root.join(label));
    candidates.push(root.join("alma").join("simmos").join(label));
    candidates.push(
        root.join("__data__")
            .join("alma")
            .join("simmos")
            .join(label),
    );
}

fn parse_casa_array_config(telescope: &str, path: &Path) -> Result<Vec<SyntheticAntenna>, String> {
    let text = fs::read_to_string(path)
        .map_err(|error| format!("failed to read array_config {}: {error}", path.display()))?;
    let mut observatory = telescope.trim().to_string();
    let mut coordsys = "XYZ".to_string();
    let mut datum = "WGS84".to_string();
    let mut utm_zone: Option<i32> = None;
    let mut utm_south: Option<bool> = None;
    let mut antennas = Vec::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(comment) = line.strip_prefix('#') {
            let comment = comment.trim();
            if let Some((key, value)) = comment.split_once('=') {
                match key.trim().to_ascii_lowercase().as_str() {
                    "observatory" => observatory = value.trim().to_string(),
                    "coordsys" => coordsys = value.trim().to_ascii_uppercase(),
                    "datum" => datum = value.trim().to_ascii_uppercase(),
                    "zone" => {
                        utm_zone = Some(value.trim().parse::<i32>().map_err(|error| {
                            format!(
                                "array_config {} has invalid UTM zone {value:?}: {error}",
                                path.display()
                            )
                        })?)
                    }
                    "hemisphere" => {
                        let hemisphere = value.trim();
                        utm_south = Some(
                            if hemisphere.eq_ignore_ascii_case("S")
                                || hemisphere.eq_ignore_ascii_case("SOUTH")
                            {
                                true
                            } else if hemisphere.eq_ignore_ascii_case("N")
                                || hemisphere.eq_ignore_ascii_case("NORTH")
                            {
                                false
                            } else {
                                return Err(format!(
                                    "array_config {} has invalid UTM hemisphere {hemisphere:?}",
                                    path.display()
                                ));
                            },
                        );
                    }
                    _ => {}
                }
            }
            continue;
        }

        let fields = line.split_whitespace().collect::<Vec<_>>();
        if fields.len() < 5 {
            return Err(format!(
                "array_config {} has malformed antenna row {line:?}",
                path.display()
            ));
        }
        let x_m = parse_config_f64(fields[0], path)?;
        let y_m = parse_config_f64(fields[1], path)?;
        let z_m = parse_config_f64(fields[2], path)?;
        let dish_diameter_m = parse_config_f64(fields[3], path)?;
        let station = fields[4].to_string();
        let position_m = if coordsys.starts_with("LOC") {
            let observatory_position = config_observatory_position(&observatory).ok_or_else(|| {
                format!(
                    "array_config {} uses local coordinates but observatory {observatory:?} is unknown",
                    path.display()
                )
            })?;
            offset_itrf_position_geodetic_enu(&observatory_position, x_m, y_m, z_m).ok_or_else(
                || {
                    format!(
                        "array_config {} uses local coordinates but observatory {observatory:?} has invalid geodetic basis",
                        path.display()
                    )
                },
            )?
        } else if coordsys.starts_with("UTM") {
            utm_to_itrf_position(&datum, utm_zone, utm_south, x_m, y_m, z_m, path)?
        } else {
            [x_m, y_m, z_m]
        };
        antennas.push(SyntheticAntenna {
            name: station.clone(),
            station,
            position_m,
            dish_diameter_m,
        });
    }

    if antennas.is_empty() {
        return Err(format!("array_config {} has no antennas", path.display()));
    }
    Ok(antennas)
}

#[derive(Debug, Clone, Copy)]
struct UtmDatum {
    semi_major_axis_m: f64,
    inverse_flattening: f64,
    translation_to_itrf_m: [f64; 3],
}

fn utm_to_itrf_position(
    datum: &str,
    zone: Option<i32>,
    south: Option<bool>,
    easting_m: f64,
    northing_m: f64,
    height_m: f64,
    path: &Path,
) -> Result<[f64; 3], String> {
    let zone = zone.ok_or_else(|| {
        format!(
            "array_config {} uses UTM coordinates but has no # zone=<n> comment",
            path.display()
        )
    })?;
    let south = south.ok_or_else(|| {
        format!(
            "array_config {} uses UTM coordinates but has no # hemisphere=N|S comment",
            path.display()
        )
    })?;
    if !(1..=60).contains(&zone) {
        return Err(format!(
            "array_config {} has invalid UTM zone {zone}; expected 1..60",
            path.display()
        ));
    }
    let datum = utm_datum(datum).ok_or_else(|| {
        format!(
            "array_config {} uses unsupported UTM datum {datum:?}",
            path.display()
        )
    })?;
    let [longitude_rad, latitude_rad] =
        inverse_utm_to_geodetic(easting_m, northing_m, zone, south, datum);
    let mut position = geodetic_to_ecef(
        longitude_rad,
        latitude_rad,
        height_m,
        datum.semi_major_axis_m,
        1.0 / datum.inverse_flattening,
    );
    position[0] += datum.translation_to_itrf_m[0];
    position[1] += datum.translation_to_itrf_m[1];
    position[2] += datum.translation_to_itrf_m[2];
    Ok(position)
}

fn utm_datum(name: &str) -> Option<UtmDatum> {
    match name.trim().to_ascii_uppercase().as_str() {
        "WGS84" | "WGS_84" => Some(UtmDatum {
            semi_major_axis_m: 6_378_137.0,
            inverse_flattening: 298.257_223_563,
            translation_to_itrf_m: [0.0, 0.0, 0.0],
        }),
        "SAM56" | "PSAD56" => Some(UtmDatum {
            semi_major_axis_m: 6_378_388.0,
            inverse_flattening: 297.0,
            translation_to_itrf_m: [-288.0, 175.0, -376.0],
        }),
        _ => None,
    }
}

fn inverse_utm_to_geodetic(
    easting_m: f64,
    northing_m: f64,
    zone: i32,
    south: bool,
    datum: UtmDatum,
) -> [f64; 2] {
    let flattening = 1.0 / datum.inverse_flattening;
    let eccentricity_squared = flattening * (2.0 - flattening);
    let second_eccentricity_squared = eccentricity_squared / (1.0 - eccentricity_squared);
    let scale = 0.9996;
    let x = easting_m - 500_000.0;
    let y = northing_m - if south { 10_000_000.0 } else { 0.0 };
    let central_meridian_rad = ((zone as f64 * 6.0) - 183.0).to_radians();
    let meridional_arc = y / scale;
    let mu = meridional_arc
        / (datum.semi_major_axis_m
            * (1.0
                - eccentricity_squared / 4.0
                - 3.0 * eccentricity_squared.powi(2) / 64.0
                - 5.0 * eccentricity_squared.powi(3) / 256.0));
    let e1 =
        (1.0 - (1.0 - eccentricity_squared).sqrt()) / (1.0 + (1.0 - eccentricity_squared).sqrt());
    let footprint_latitude_rad = mu
        + (3.0 * e1 / 2.0 - 27.0 * e1.powi(3) / 32.0) * (2.0 * mu).sin()
        + (21.0 * e1.powi(2) / 16.0 - 55.0 * e1.powi(4) / 32.0) * (4.0 * mu).sin()
        + (151.0 * e1.powi(3) / 96.0) * (6.0 * mu).sin()
        + (1097.0 * e1.powi(4) / 512.0) * (8.0 * mu).sin();
    let sin_footprint = footprint_latitude_rad.sin();
    let cos_footprint = footprint_latitude_rad.cos();
    let tan_footprint = footprint_latitude_rad.tan();
    let c1 = second_eccentricity_squared * cos_footprint.powi(2);
    let t1 = tan_footprint.powi(2);
    let n1 = datum.semi_major_axis_m / (1.0 - eccentricity_squared * sin_footprint.powi(2)).sqrt();
    let r1 = datum.semi_major_axis_m * (1.0 - eccentricity_squared)
        / (1.0 - eccentricity_squared * sin_footprint.powi(2)).powf(1.5);
    let d = x / (n1 * scale);
    let latitude_rad = footprint_latitude_rad
        - (n1 * tan_footprint / r1)
            * (d.powi(2) / 2.0
                - (5.0 + 3.0 * t1 + 10.0 * c1
                    - 4.0 * c1.powi(2)
                    - 9.0 * second_eccentricity_squared)
                    * d.powi(4)
                    / 24.0
                + (61.0 + 90.0 * t1 + 298.0 * c1 + 45.0 * t1.powi(2)
                    - 252.0 * second_eccentricity_squared
                    - 3.0 * c1.powi(2))
                    * d.powi(6)
                    / 720.0);
    let longitude_rad = central_meridian_rad
        + (d - (1.0 + 2.0 * t1 + c1) * d.powi(3) / 6.0
            + (5.0 - 2.0 * c1 + 28.0 * t1 - 3.0 * c1.powi(2)
                + 8.0 * second_eccentricity_squared
                + 24.0 * t1.powi(2))
                * d.powi(5)
                / 120.0)
            / cos_footprint;
    [longitude_rad, latitude_rad]
}

fn geodetic_to_ecef(
    longitude_rad: f64,
    latitude_rad: f64,
    height_m: f64,
    semi_major_axis_m: f64,
    flattening: f64,
) -> [f64; 3] {
    let eccentricity_squared = flattening * (2.0 - flattening);
    let sin_latitude = latitude_rad.sin();
    let cos_latitude = latitude_rad.cos();
    let prime_vertical_radius =
        semi_major_axis_m / (1.0 - eccentricity_squared * sin_latitude.powi(2)).sqrt();
    [
        (prime_vertical_radius + height_m) * cos_latitude * longitude_rad.cos(),
        (prime_vertical_radius + height_m) * cos_latitude * longitude_rad.sin(),
        (prime_vertical_radius * (1.0 - eccentricity_squared) + height_m) * sin_latitude,
    ]
}

fn config_observatory_position(name: &str) -> Option<MPosition> {
    MPosition::from_observatory_name(name).or_else(|| {
        if name.eq_ignore_ascii_case("ALMASD") {
            MPosition::from_observatory_name("ALMA")
        } else {
            None
        }
    })
}

fn parse_config_f64(value: &str, path: &Path) -> Result<f64, String> {
    value.parse::<f64>().map_err(|error| {
        format!(
            "array_config {} has invalid floating-point value {value:?}: {error}",
            path.display()
        )
    })
}

fn resolve_band_frequency_hz(telescope: &str, band: &str) -> Result<f64, String> {
    let band = band.trim().to_ascii_lowercase().replace(' ', "");
    match (telescope, band.as_str()) {
        ("vla", "l") => Ok(1.5e9),
        ("vla", "s") => Ok(3.0e9),
        ("vla", "c") => Ok(6.0e9),
        ("vla", "x") => Ok(10.0e9),
        ("vla", "ku") => Ok(15.0e9),
        ("vla", "k") => Ok(22.0e9),
        ("vla", "ka") => Ok(33.0e9),
        ("vla", "q") => Ok(44.0e9),
        ("alma" | "aca", "3" | "band3" | "band_3") => Ok(100.0e9),
        ("alma" | "aca", "6" | "band6" | "band_6") => Ok(230.0e9),
        ("alma" | "aca", "7" | "band7" | "band_7") => Ok(345.0e9),
        ("alma" | "aca", "9" | "band9" | "band_9") => Ok(690.0e9),
        ("vla", other) => Err(format!(
            "unsupported VLA band {other:?}; expected L, S, C, X, Ku, K, Ka, or Q"
        )),
        (_, other) => Err(format!(
            "unsupported ALMA/ACA band {other:?}; expected Band 3, 6, 7, or 9"
        )),
    }
}

fn family_channel_width_hz(imaging_mode: &str, preset: &FamilyPreset) -> f64 {
    let mode = imaging_mode.trim().to_ascii_lowercase();
    if matches!(
        mode.as_str(),
        "mfs" | "continuum" | "continuum_mfs" | "single_field" | "mt_mfs" | "mtmfs"
    ) {
        preset.channel_width_hz.max(32.0e6)
    } else {
        preset.channel_width_hz
    }
}

fn family_row_pair_count(
    antenna_count: usize,
    observation_mode: SyntheticObservationMode,
) -> usize {
    match observation_mode {
        SyntheticObservationMode::Interferometric => {
            antenna_count * antenna_count.saturating_sub(1) / 2
        }
        SyntheticObservationMode::TotalPower => antenna_count,
    }
    .max(1)
}

fn family_antennas_for_mode(
    antennas: Vec<SyntheticAntenna>,
    observation_mode: SyntheticObservationMode,
    total_power_antenna_index: Option<usize>,
) -> Result<Vec<SyntheticAntenna>, String> {
    match observation_mode {
        SyntheticObservationMode::Interferometric => Ok(antennas),
        SyntheticObservationMode::TotalPower => {
            let selected_index = total_power_antenna_index.unwrap_or(0);
            antennas
                .into_iter()
                .nth(selected_index)
                .map(|antenna| vec![antenna])
                .ok_or_else(|| {
                    format!(
                        "total_power_antenna_index {selected_index} is out of range for array_config"
                    )
                })
        }
    }
}

fn validate_family_imaging_mode(imaging_mode: &str) -> Result<(), String> {
    let mode = imaging_mode.trim().to_ascii_lowercase();
    if matches!(
        mode.as_str(),
        "single_field"
            | "mfs"
            | "continuum"
            | "continuum_mfs"
            | "mosaic"
            | "mosaic_mfs"
            | "spectral_cube"
            | "cube"
            | "cubedata"
            | "mt_mfs"
            | "mtmfs"
            | "simalma"
            | "aca"
    ) {
        return Ok(());
    }
    Err(format!(
        "unsupported imaging_mode {imaging_mode:?}; expected single_field, mfs, mosaic, spectral_cube, cubedata, mt_mfs, simalma, or aca"
    ))
}

fn scaled_vla_antennas(scale: f64) -> Vec<SyntheticAntenna> {
    let antennas = tutorial_vla_a_antennas();
    if (scale - 1.0).abs() < f64::EPSILON {
        return antennas;
    }
    let center = antenna_centroid(&antennas);
    antennas
        .into_iter()
        .map(|antenna| SyntheticAntenna {
            position_m: [
                center[0] + (antenna.position_m[0] - center[0]) * scale,
                center[1] + (antenna.position_m[1] - center[1]) * scale,
                center[2] + (antenna.position_m[2] - center[2]) * scale,
            ],
            ..antenna
        })
        .collect()
}

fn antenna_centroid(antennas: &[SyntheticAntenna]) -> [f64; 3] {
    let mut sum = [0.0; 3];
    for antenna in antennas {
        sum[0] += antenna.position_m[0];
        sum[1] += antenna.position_m[1];
        sum[2] += antenna.position_m[2];
    }
    let count = antennas.len().max(1) as f64;
    [sum[0] / count, sum[1] / count, sum[2] / count]
}

fn synthetic_alma_antennas(
    name_prefix: &str,
    count: usize,
    dish_diameter_m: f64,
    max_radius_m: f64,
) -> Vec<SyntheticAntenna> {
    let center = MPosition::from_observatory_name("ALMA")
        .map(|position| position.as_itrf())
        .unwrap_or([-2_223_990.194, -5_440_045.461, -2_481_682.086]);
    (0..count)
        .map(|index| {
            let radius = if count <= 1 {
                0.0
            } else {
                max_radius_m * ((index + 1) as f64 / count as f64).sqrt()
            };
            let angle = index as f64 * 2.399_963_229_728_653;
            let east_m = radius * angle.cos();
            let north_m = radius * angle.sin();
            let name = format!("{name_prefix}{:02}", index + 1);
            SyntheticAntenna {
                name: name.clone(),
                station: name,
                position_m: offset_itrf_position(center, east_m, north_m),
                dish_diameter_m,
            }
        })
        .collect()
}

fn offset_itrf_position(center: [f64; 3], east_m: f64, north_m: f64) -> [f64; 3] {
    offset_itrf_position_enu(center, east_m, north_m, 0.0)
}

fn offset_itrf_position_enu(center: [f64; 3], east_m: f64, north_m: f64, up_m: f64) -> [f64; 3] {
    let up = normalized(center);
    let east = normalized([-center[1], center[0], 0.0]);
    let north = [
        up[1] * east[2] - up[2] * east[1],
        up[2] * east[0] - up[0] * east[2],
        up[0] * east[1] - up[1] * east[0],
    ];
    [
        center[0] + east[0] * east_m + north[0] * north_m + up[0] * up_m,
        center[1] + east[1] * east_m + north[1] * north_m + up[1] * up_m,
        center[2] + east[2] * east_m + north[2] * north_m + up[2] * up_m,
    ]
}

fn offset_itrf_position_geodetic_enu(
    position: &MPosition,
    east_m: f64,
    north_m: f64,
    up_m: f64,
) -> Option<[f64; 3]> {
    let center = position.as_itrf();
    let longitude_rad = position.longitude_rad();
    let latitude_rad = position.latitude_rad();
    if !longitude_rad.is_finite()
        || !latitude_rad.is_finite()
        || center.iter().any(|value| !value.is_finite())
    {
        return None;
    }
    let (sin_lon, cos_lon) = longitude_rad.sin_cos();
    let (sin_lat, cos_lat) = latitude_rad.sin_cos();
    let east = [-sin_lon, cos_lon, 0.0];
    let north = [-sin_lat * cos_lon, -sin_lat * sin_lon, cos_lat];
    let up = [cos_lat * cos_lon, cos_lat * sin_lon, sin_lat];
    Some([
        center[0] + east[0] * east_m + north[0] * north_m + up[0] * up_m,
        center[1] + east[1] * east_m + north[1] * north_m + up[1] * up_m,
        center[2] + east[2] * east_m + north[2] * north_m + up[2] * up_m,
    ])
}

fn normalized(vector: [f64; 3]) -> [f64; 3] {
    let norm = (vector[0] * vector[0] + vector[1] * vector[1] + vector[2] * vector[2]).sqrt();
    if norm == 0.0 {
        return [0.0, 0.0, 0.0];
    }
    [vector[0] / norm, vector[1] / norm, vector[2] / norm]
}

fn family_fields(
    pointing_count: usize,
    phase_center_rad: [f64; 2],
    field_phase_centers_rad: Option<&[[f64; 2]]>,
    imaging_mode: &str,
    reference_frequency_hz: f64,
    dish_diameter_m: f64,
) -> Vec<SyntheticField> {
    if let Some(field_phase_centers_rad) = field_phase_centers_rad {
        if field_phase_centers_rad.len() == 1 {
            return Vec::new();
        }
        return field_phase_centers_rad
            .iter()
            .enumerate()
            .map(|(index, phase_center_rad)| SyntheticField {
                name: format!("P{:03}", index),
                phase_center_rad: *phase_center_rad,
            })
            .collect();
    }
    if pointing_count <= 1 || !mode_uses_mosaic(imaging_mode) {
        return Vec::new();
    }
    let wavelength_m = 299_792_458.0 / reference_frequency_hz;
    let primary_beam_fwhm_rad = 1.13 * wavelength_m / dish_diameter_m;
    let spacing_rad = 0.45 * primary_beam_fwhm_rad;
    mosaic_offsets(pointing_count, spacing_rad)
        .into_iter()
        .enumerate()
        .map(|(index, [l_rad, m_rad])| SyntheticField {
            name: format!("P{:03}", index),
            phase_center_rad: [
                phase_center_rad[0] + l_rad / phase_center_rad[1].cos().max(1.0e-6),
                phase_center_rad[1] + m_rad,
            ],
        })
        .collect()
}

fn mode_uses_mosaic(imaging_mode: &str) -> bool {
    let mode = imaging_mode.trim().to_ascii_lowercase();
    matches!(
        mode.as_str(),
        "mosaic" | "mosaic_mfs" | "mosaic_cube" | "simalma" | "aca"
    )
}

fn mosaic_offsets(pointing_count: usize, spacing_rad: f64) -> Vec<[f64; 2]> {
    let mut offsets = vec![[0.0, 0.0]];
    let mut ring = 1usize;
    while offsets.len() < pointing_count {
        let samples = ring * 6;
        for sample in 0..samples {
            if offsets.len() == pointing_count {
                break;
            }
            let angle = std::f64::consts::TAU * sample as f64 / samples as f64;
            offsets.push([
                ring as f64 * spacing_rad * angle.cos(),
                ring as f64 * spacing_rad * angle.sin(),
            ]);
        }
        ring += 1;
    }
    offsets
}

fn family_manifest_path(output_ms: &Path) -> PathBuf {
    let mut path = output_ms.to_path_buf();
    path.set_extension("synthetic-family.json");
    path
}

fn directory_size_bytes(path: &Path) -> std::io::Result<u64> {
    let metadata = fs::metadata(path)?;
    if metadata.is_file() {
        return Ok(metadata.len());
    }
    let mut total = 0;
    for entry in fs::read_dir(path)? {
        total += directory_size_bytes(&entry?.path())?;
    }
    Ok(total)
}

/// Canonical `simobserve` task request envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "request", rename_all = "snake_case")]
pub enum SimobserveTaskRequest {
    /// Execute one `simobserve` request.
    Run(Box<SimobserveRunTaskRequest>),
    /// Plan one synthetic MeasurementSet family from persisted dialog inputs.
    Family(Box<SimobserveFamilyTaskRequest>),
}

/// Project resolved canonical parameters into the provider's invocation
/// transport. Run requests retain their direct CLI arguments; family requests
/// use the typed JSON request envelope over stdin without creating a saved
/// request file.
pub fn simobserve_provider_invocation(
    values: &BTreeMap<String, ParameterValue>,
    direct_args: Vec<String>,
) -> Result<ProviderInvocationAdaptation, String> {
    let request_kind = optional_parameter_string(values, "request_kind")?.unwrap_or("run");
    match request_kind {
        "run" => Ok(ProviderInvocationAdaptation {
            invocation: ProviderInvocation::direct(direct_args),
            consumed_parameters: BTreeSet::from(["request_kind".to_string()]),
        }),
        "family" => {
            let request = SimobserveTaskRequest::Family(Box::new(
                simobserve_family_request_from_parameters(values)?,
            ));
            let mut stdin = serde_json::to_string(&request)
                .map_err(|error| format!("serialize simobserve family request: {error}"))?;
            stdin.push('\n');
            Ok(ProviderInvocationAdaptation {
                invocation: ProviderInvocation {
                    args: vec!["--json-run".to_string(), "-".to_string()],
                    stdin: Some(stdin),
                },
                consumed_parameters: values.keys().cloned().collect(),
            })
        }
        value => Err(format!(
            "unsupported simobserve request_kind {value:?}; expected run or family"
        )),
    }
}

fn simobserve_family_request_from_parameters(
    values: &BTreeMap<String, ParameterValue>,
) -> Result<SimobserveFamilyTaskRequest, String> {
    let source_model = required_parameter_string(values, "source_model")?;
    let source_model = serde_json::from_str::<SyntheticSkyModel>(source_model)
        .map_err(|error| format!("parse source_model JSON: {error}"))?;
    let worker_policy = optional_parameter_string(values, "worker_policy")?
        .map(parse_worker_policy)
        .transpose()?
        .unwrap_or_default();
    let observation_mode = match optional_parameter_string(values, "observation_mode")?
        .unwrap_or("interferometric")
    {
        "interferometric" => SyntheticObservationMode::Interferometric,
        "total_power" => SyntheticObservationMode::TotalPower,
        value => {
            return Err(format!(
                "unsupported observation_mode {value:?}; expected interferometric or total_power"
            ));
        }
    };
    Ok(SimobserveFamilyTaskRequest {
        source_model,
        telescope: required_parameter_string(values, "telescope")?.to_string(),
        array_config: required_parameter_string(values, "array_config")?.to_string(),
        band: required_parameter_string(values, "band")?.to_string(),
        target_ms_size_gib: required_parameter_f64(values, "target_ms_size_gib")?,
        polarizations: required_parameter_usize(values, "polarizations")?,
        ms_channels: required_parameter_usize(values, "ms_channels")?,
        image_channels: required_parameter_usize(values, "image_channels")?,
        pointing_count: required_parameter_usize(values, "pointing_count")?,
        field_phase_centers_rad: None,
        phase_center_rad: None,
        start_frequency_hz: None,
        channel_width_hz: None,
        time_sample_count: optional_parameter_usize(values, "time_sample_count")?,
        integration_seconds: optional_parameter_f64(values, "integration_seconds")?,
        start_time_mjd_seconds: optional_parameter_f64(values, "start_time_mjd_seconds")?,
        imaging_mode: required_parameter_string(values, "imaging_mode")?.to_string(),
        output_ms: Some(PathBuf::from(required_parameter_string(
            values,
            "output_ms",
        )?)),
        overwrite: optional_parameter_bool(values, "overwrite")?.unwrap_or(false),
        measure_actual_size: optional_parameter_bool(values, "measure_actual_size")?
            .unwrap_or(false),
        worker_policy,
        observation_mode,
        total_power_antenna_index: None,
        row_workers: optional_parameter_usize(values, "row_workers")?,
        channel_workers: optional_parameter_usize(values, "channel_workers")?,
    })
}

fn parameter_value<'a>(
    values: &'a BTreeMap<String, ParameterValue>,
    name: &str,
) -> Result<&'a ParameterValue, String> {
    values
        .get(name)
        .ok_or_else(|| format!("simobserve family parameters are missing {name:?}"))
}

fn required_parameter_string<'a>(
    values: &'a BTreeMap<String, ParameterValue>,
    name: &str,
) -> Result<&'a str, String> {
    match parameter_value(values, name)? {
        ParameterValue::String(value) => Ok(value),
        value => Err(format!(
            "simobserve family parameter {name:?} must be a string, got {value:?}"
        )),
    }
}

fn optional_parameter_string<'a>(
    values: &'a BTreeMap<String, ParameterValue>,
    name: &str,
) -> Result<Option<&'a str>, String> {
    match values.get(name) {
        None => Ok(None),
        Some(ParameterValue::String(value)) if value == "none" => Ok(None),
        Some(ParameterValue::String(value)) => Ok(Some(value)),
        Some(value) => Err(format!(
            "simobserve parameter {name:?} must be a string, got {value:?}"
        )),
    }
}

fn required_parameter_f64(
    values: &BTreeMap<String, ParameterValue>,
    name: &str,
) -> Result<f64, String> {
    match parameter_value(values, name)? {
        ParameterValue::Integer(value) => Ok(*value as f64),
        ParameterValue::Float(value) if value.is_finite() => Ok(*value),
        value => Err(format!(
            "simobserve family parameter {name:?} must be finite numeric, got {value:?}"
        )),
    }
}

fn optional_parameter_f64(
    values: &BTreeMap<String, ParameterValue>,
    name: &str,
) -> Result<Option<f64>, String> {
    match values.get(name) {
        None => Ok(None),
        Some(ParameterValue::String(value)) if value == "none" => Ok(None),
        Some(ParameterValue::Integer(value)) => Ok(Some(*value as f64)),
        Some(ParameterValue::Float(value)) if value.is_finite() => Ok(Some(*value)),
        Some(value) => Err(format!(
            "simobserve family parameter {name:?} must be none or finite numeric, got {value:?}"
        )),
    }
}

fn required_parameter_usize(
    values: &BTreeMap<String, ParameterValue>,
    name: &str,
) -> Result<usize, String> {
    match parameter_value(values, name)? {
        ParameterValue::Integer(value) if *value >= 0 => usize::try_from(*value)
            .map_err(|_| format!("simobserve family parameter {name:?} is too large")),
        ParameterValue::String(value) => value
            .parse::<usize>()
            .map_err(|error| format!("parse simobserve family parameter {name:?}: {error}")),
        value => Err(format!(
            "simobserve family parameter {name:?} must be a non-negative integer, got {value:?}"
        )),
    }
}

fn optional_parameter_usize(
    values: &BTreeMap<String, ParameterValue>,
    name: &str,
) -> Result<Option<usize>, String> {
    match values.get(name) {
        None => Ok(None),
        Some(ParameterValue::String(value)) if value == "none" => Ok(None),
        Some(ParameterValue::Integer(value)) if *value >= 0 => usize::try_from(*value)
            .map(Some)
            .map_err(|_| format!("simobserve family parameter {name:?} is too large")),
        Some(ParameterValue::String(value)) => value
            .parse::<usize>()
            .map(Some)
            .map_err(|error| format!("parse simobserve family parameter {name:?}: {error}")),
        Some(value) => Err(format!(
            "simobserve family parameter {name:?} must be none or a non-negative integer, got {value:?}"
        )),
    }
}

fn optional_parameter_bool(
    values: &BTreeMap<String, ParameterValue>,
    name: &str,
) -> Result<Option<bool>, String> {
    match values.get(name) {
        None => Ok(None),
        Some(ParameterValue::Bool(value)) => Ok(Some(*value)),
        Some(value) => Err(format!(
            "simobserve family parameter {name:?} must be boolean, got {value:?}"
        )),
    }
}

impl SimobserveTaskRequest {
    /// Execute the request and return the canonical task result envelope.
    pub fn execute(&self) -> Result<SimobserveTaskResult, String> {
        match self {
            Self::Run(request) => Ok(SimobserveTaskResult::Run(Box::new(request.execute()?))),
            Self::Family(request) => Ok(SimobserveTaskResult::Family(Box::new(request.execute()?))),
        }
    }

    /// Read one task request from a file path or `-` for stdin.
    pub fn read_from_source(source: &str) -> Result<Self, String> {
        let payload = if source == "-" {
            let mut payload = String::new();
            std::io::stdin()
                .read_to_string(&mut payload)
                .map_err(|error| format!("failed to read JSON request from stdin: {error}"))?;
            payload
        } else {
            fs::read_to_string(source).map_err(|error| {
                format!(
                    "failed to read JSON request from {}: {error}",
                    Path::new(source).display()
                )
            })?
        };
        serde_json::from_str(&payload)
            .map_err(|error| format!("failed to parse simobserve task request: {error}"))
    }
}

/// Canonical `simobserve` task result envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "result", rename_all = "snake_case")]
pub enum SimobserveTaskResult {
    /// Completed synthetic-observation run.
    Run(Box<SimobserveRunTaskResult>),
    /// Planned synthetic MeasurementSet family.
    Family(Box<SimobserveFamilyTaskResult>),
}

/// JSON-schema bundle for the public `simobserve` task protocol.
#[derive(Debug, Clone, Serialize)]
pub struct SimobserveTaskSchemaBundle {
    /// Compatibility descriptor for the request/result schemas.
    pub protocol: SimobserveProtocolInfo,
    /// Canonical semantic task contract.
    pub semantic: TaskSemanticContract,
    /// Shared component schemas reusable across projections.
    pub components: ProviderComponentSchemas,
    /// Presentation annotations carried with the canonical bundle.
    pub annotations: JsonValue,
    /// Derived projection metadata for UI and CLI consumers.
    pub projections: ProviderProjectionMetadata,
    /// Canonical parameter contract embedded for self-contained consumers.
    pub parameter_surfaces: Vec<casa_provider_contracts::SurfaceContractBundle>,
    /// JSON schema for [`SimobserveTaskRequest`].
    pub request_schema: RootSchema,
    /// JSON schema for [`SimobserveTaskResult`].
    pub result_schema: RootSchema,
}

impl SimobserveTaskSchemaBundle {
    /// Build the current request/result schema bundle.
    pub fn current() -> Self {
        let request_schema = schema_for!(SimobserveTaskRequest);
        let result_schema = schema_for!(SimobserveTaskResult);
        let parameter_surface = builtin_surface_bundle("simobserve")
            .expect("built-in simobserve parameter surface must remain valid");
        let ui_schema = project_ui_schema(&parameter_surface);
        Self {
            protocol: SimobserveProtocolInfo::current(),
            semantic: TaskSemanticContract {
                request_schema: request_schema.clone(),
                result_schema: result_schema.clone(),
                operations: vec![
                    TaskOperationDescriptor {
                        name: "run".to_string(),
                        request_kind: "run".to_string(),
                        result_kind: Some("run".to_string()),
                    },
                    TaskOperationDescriptor {
                        name: "family".to_string(),
                        request_kind: "family".to_string(),
                        result_kind: Some("family".to_string()),
                    },
                ],
            },
            components: merged_components([&request_schema, &result_schema]),
            annotations: derived_ui_schema_annotations(),
            projections: ProviderProjectionMetadata {
                cli: Some(ProviderCliProjection {
                    machine_actions: ProviderCliMachineActions {
                        ui_schema: Some("--ui-schema".to_string()),
                        json_schema: Some("--json-schema".to_string()),
                        protocol_info: Some("--protocol-info".to_string()),
                        json_run: Some("--json-run <SOURCE>".to_string()),
                        session: None,
                    },
                }),
                ui_schema: Some(ui_schema),
                python: None,
            },
            parameter_surfaces: vec![parameter_surface],
            request_schema,
            result_schema,
        }
    }

    /// Return the launcher/TUI compatibility view projected from the bundle.
    pub fn ui_schema_projection(&self) -> Result<UiCommandSchema, String> {
        let value = self
            .projections
            .ui_schema
            .clone()
            .ok_or_else(|| "missing ui_schema projection".to_string())?;
        serde_json::from_value(value)
            .map_err(|error| format!("parse simobserve ui schema: {error}"))
    }
}

/// Return the launcher/TUI compatibility schema.
/// Return the launcher/TUI compatibility schema projected from the canonical
/// parameter surface. The executable spelling may be overridden by launchers
/// without changing parameter identity or semantics.
pub fn command_schema(program_name: &str) -> UiCommandSchema {
    let bundle = builtin_surface_bundle("simobserve")
        .expect("built-in simobserve parameter surface must remain valid");
    let mut projection = project_ui_schema(&bundle);
    projection["invocation_name"] = JsonValue::String(program_name.to_string());
    projection["usage"] = JsonValue::String(format!("{program_name} [parameters]"));
    serde_json::from_value(projection)
        .expect("canonical simobserve UI projection must remain valid")
}

/// Execute CLI-style arguments for the `simobserve` binary.
pub fn run_with_cli_args(args: impl IntoIterator<Item = std::ffi::OsString>) -> Result<(), String> {
    let args = args.into_iter().collect::<Vec<_>>();
    if args
        .iter()
        .any(|arg| matches!(arg.to_str(), Some("-h" | "--help")))
    {
        print!("{}", command_schema("simobserve").render_help());
        return Ok(());
    }
    if args
        .iter()
        .any(|arg| matches!(arg.to_str(), Some("--ui-schema")))
    {
        println!(
            "{}",
            command_schema("simobserve")
                .render_json_pretty()
                .map_err(|error| error.to_string())?
        );
        return Ok(());
    }
    if args
        .iter()
        .any(|arg| matches!(arg.to_str(), Some("--json-schema")))
    {
        println!(
            "{}",
            serde_json::to_string_pretty(&SimobserveTaskSchemaBundle::current())
                .map_err(|error| error.to_string())?
        );
        return Ok(());
    }
    if args
        .iter()
        .any(|arg| matches!(arg.to_str(), Some("--protocol-info")))
    {
        println!(
            "{}",
            serde_json::to_string_pretty(&SimobserveProtocolInfo::current())
                .map_err(|error| error.to_string())?
        );
        return Ok(());
    }
    let (json_run, args) = extract_string_option(&args, "--json-run")?;
    if let Some(source) = json_run {
        let result = SimobserveTaskRequest::read_from_source(&source)?.execute()?;
        println!(
            "{}",
            serde_json::to_string_pretty(&result).map_err(|error| error.to_string())?
        );
        return Ok(());
    }
    let request = request_from_cli_args(&args)?;
    let result = request.execute()?;
    println!(
        "{}",
        serde_json::to_string_pretty(&SimobserveTaskResult::Run(Box::new(result)))
            .map_err(|error| error.to_string())?
    );
    Ok(())
}

fn request_from_cli_args(args: &[std::ffi::OsString]) -> Result<SimobserveRunTaskRequest, String> {
    let model_image = required_option(args, "--model")?;
    let output_ms = required_option(args, "--out")?;
    let model_peak_jy_per_pixel = optional_f32(args, "--inbright-jy-per-pixel")?.or(Some(3.0e-5));
    let duration_seconds = optional_f64(args, "--duration")?;
    let integration_seconds = optional_f64(args, "--integration")?;
    let elevation_limit_rad = optional_f64(args, "--elevation-limit-deg")?.map(f64::to_radians);
    let start_frequency_hz = optional_f64(args, "--start-frequency-hz")?.unwrap_or(44.0e9);
    let channel_width_hz = optional_f64(args, "--channel-width-hz")?.unwrap_or(128.0e6);
    let channel_count = optional_usize(args, "--channels")?.unwrap_or(1);
    let polarization_basis = optional_string(args, "--polarization-basis")
        .as_deref()
        .map(parse_polarization_basis)
        .transpose()?
        .unwrap_or_default();
    let polarization_count = optional_usize(args, "--polarizations")?.unwrap_or(2);
    let polarization_setup =
        SyntheticPolarizationSetup::new(polarization_basis, polarization_count)
            .map_err(|error| error.to_string())?;
    let worker_policy = optional_string(args, "--worker-policy")
        .as_deref()
        .map(parse_worker_policy)
        .transpose()?
        .unwrap_or_default();
    let row_workers = optional_usize(args, "--row-workers")?;
    let channel_workers = optional_usize(args, "--channel-workers")?;
    let corruption = corruption_from_cli_args(args)?;
    Ok(SimobserveRunTaskRequest {
        model_image: Some(model_image),
        model: None,
        model_peak_jy_per_pixel,
        output_ms,
        overwrite: has_flag(args, "--overwrite"),
        telescope_name: optional_string(args, "--telescope"),
        field_name: optional_string(args, "--field-name"),
        antennas: Vec::new(),
        phase_center_rad: None,
        fields: Vec::new(),
        start_time_mjd_seconds: None,
        duration_seconds,
        integration_seconds,
        elevation_limit_rad,
        allow_below_elevation_limit: has_flag(args, "--allow-below-elevation-limit"),
        spectral_setup: Some(SyntheticSpectralSetup {
            name: "band1".to_string(),
            start_frequency_hz,
            channel_width_hz,
            channel_count,
        }),
        polarization_setup: Some(polarization_setup),
        predict_model: !has_flag(args, "--no-predict-model"),
        corruption,
        worker_policy,
        observation_mode: SyntheticObservationMode::Interferometric,
        row_workers,
        channel_workers,
    })
}

fn corruption_from_cli_args(
    args: &[std::ffi::OsString],
) -> Result<Option<SyntheticCorruptionConfig>, String> {
    let seed = optional_u64(args, "--corruption-seed")?.unwrap_or(1);
    let noise_simplenoise_jy = optional_f32(args, "--noise-simplenoise-jy")?;
    let gain_mode = optional_string(args, "--gain-mode")
        .as_deref()
        .map(parse_gain_mode)
        .transpose()?
        .unwrap_or(SyntheticGainMode::Fbm);
    let gain_interval_seconds = optional_f64(args, "--gain-interval-seconds")?.unwrap_or(10.0);
    let gain_amplitude = optional_f32_vector2(args, "--gain-amplitude")?;
    let bandpass_mode = optional_string(args, "--bandpass-mode")
        .as_deref()
        .map(parse_bandpass_mode)
        .transpose()?
        .unwrap_or(SyntheticBandpassMode::Calculate);
    let bandpass_interval_seconds =
        optional_f64(args, "--bandpass-interval-seconds")?.unwrap_or(3600.0);
    let bandpass_amplitude = optional_f32_vector2(args, "--bandpass-amplitude")?;
    let leakage_amplitude = optional_f32_vector2(args, "--leakage-amplitude")?;
    let leakage_offset = optional_f32_vector2(args, "--leakage-offset")?;
    let pointing_offset_ra_arcsec = optional_f64(args, "--pointing-offset-ra-arcsec")?;
    let pointing_offset_dec_arcsec = optional_f64(args, "--pointing-offset-dec-arcsec")?;

    let noise = noise_simplenoise_jy.map(|simplenoise_jy| SyntheticNoiseCorruption {
        mode: SyntheticNoiseMode::SimpleNoise,
        simplenoise_jy,
    });
    let gain = gain_amplitude.map(|amplitude| SyntheticGainCorruption {
        mode: gain_mode,
        interval_seconds: gain_interval_seconds,
        amplitude,
    });
    let bandpass = bandpass_amplitude.map(|amplitude| SyntheticBandpassCorruption {
        mode: bandpass_mode,
        interval_seconds: bandpass_interval_seconds,
        amplitude,
    });
    let leakage = if leakage_amplitude.is_some() || leakage_offset.is_some() {
        Some(SyntheticPolarizationLeakageCorruption {
            mode: SyntheticPolarizationLeakageMode::Constant,
            amplitude: leakage_amplitude.unwrap_or([0.01, 0.01]),
            offset: leakage_offset.unwrap_or([0.0, 0.0]),
        })
    } else {
        None
    };
    let pointing = if pointing_offset_ra_arcsec.is_some() || pointing_offset_dec_arcsec.is_some() {
        let arcsec_to_rad = std::f64::consts::PI / 180.0 / 3600.0;
        Some(SyntheticPointingCorruption {
            epjtablename: None,
            apply_pointing_offsets: true,
            do_pb_correction: false,
            offset_rad: [
                pointing_offset_ra_arcsec.unwrap_or(0.0) * arcsec_to_rad,
                pointing_offset_dec_arcsec.unwrap_or(0.0) * arcsec_to_rad,
            ],
        })
    } else {
        None
    };

    if noise.is_none()
        && gain.is_none()
        && bandpass.is_none()
        && leakage.is_none()
        && pointing.is_none()
    {
        return Ok(None);
    }
    Ok(Some(SyntheticCorruptionConfig {
        seed,
        noise,
        gain,
        bandpass,
        leakage,
        pointing,
    }))
}

fn parse_gain_mode(value: &str) -> Result<SyntheticGainMode, String> {
    match value {
        "fbm" => Ok(SyntheticGainMode::Fbm),
        "random" => Ok(SyntheticGainMode::Random),
        other => Err(format!(
            "unsupported --gain-mode {other:?}; expected fbm or random"
        )),
    }
}

fn parse_bandpass_mode(value: &str) -> Result<SyntheticBandpassMode, String> {
    match value {
        "calculate" => Ok(SyntheticBandpassMode::Calculate),
        "table" => Ok(SyntheticBandpassMode::Table),
        other => Err(format!(
            "unsupported --bandpass-mode {other:?}; expected calculate or table"
        )),
    }
}

fn parse_worker_policy(value: &str) -> Result<SyntheticWorkerPolicy, String> {
    match value {
        "auto" => Ok(SyntheticWorkerPolicy::Auto),
        "fixed" => Ok(SyntheticWorkerPolicy::Fixed),
        other => Err(format!(
            "unsupported --worker-policy {other:?}; expected auto or fixed"
        )),
    }
}

fn parse_polarization_basis(value: &str) -> Result<SyntheticPolarizationBasis, String> {
    match value {
        "circular" => Ok(SyntheticPolarizationBasis::Circular),
        "linear" => Ok(SyntheticPolarizationBasis::Linear),
        other => Err(format!(
            "unsupported --polarization-basis {other:?}; expected circular or linear"
        )),
    }
}

fn default_predict_model() -> bool {
    true
}

fn default_vla_antennas() -> Vec<SyntheticAntenna> {
    tutorial_vla_a_antennas()
}

fn extract_string_option(
    args: &[std::ffi::OsString],
    flag: &str,
) -> Result<(Option<String>, Vec<std::ffi::OsString>), String> {
    let mut output = Vec::new();
    let mut found = None;
    let mut index = 0usize;
    while index < args.len() {
        if args[index].to_str() == Some(flag) {
            index += 1;
            let value = args
                .get(index)
                .and_then(|value| value.to_str())
                .ok_or_else(|| format!("{flag} requires a value"))?;
            found = Some(value.to_string());
        } else {
            output.push(args[index].clone());
        }
        index += 1;
    }
    Ok((found, output))
}

fn required_option(args: &[std::ffi::OsString], flag: &str) -> Result<PathBuf, String> {
    option_value(args, flag)
        .map(PathBuf::from)
        .ok_or_else(|| format!("missing required {flag} option"))
}

fn optional_f64(args: &[std::ffi::OsString], flag: &str) -> Result<Option<f64>, String> {
    option_value(args, flag)
        .map(|value| {
            value
                .parse::<f64>()
                .map_err(|error| format!("parse {flag}: {error}"))
        })
        .transpose()
}

fn optional_f32(args: &[std::ffi::OsString], flag: &str) -> Result<Option<f32>, String> {
    option_value(args, flag)
        .map(|value| {
            value
                .parse::<f32>()
                .map_err(|error| format!("parse {flag}: {error}"))
        })
        .transpose()
}

fn optional_string(args: &[std::ffi::OsString], flag: &str) -> Option<String> {
    option_value(args, flag)
}

fn optional_f32_vector2(
    args: &[std::ffi::OsString],
    flag: &str,
) -> Result<Option<[f32; 2]>, String> {
    option_value(args, flag)
        .map(|value| parse_f32_vector2(flag, &value))
        .transpose()
}

fn parse_f32_vector2(flag: &str, value: &str) -> Result<[f32; 2], String> {
    let values = value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| {
            part.parse::<f32>()
                .map_err(|error| format!("parse {flag}: {error}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    match values.as_slice() {
        [single] => Ok([*single, *single]),
        [first, second] => Ok([*first, *second]),
        _ => Err(format!("{flag} expects a scalar or real,imag vector")),
    }
}

fn optional_usize(args: &[std::ffi::OsString], flag: &str) -> Result<Option<usize>, String> {
    option_value(args, flag)
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|error| format!("parse {flag}: {error}"))
        })
        .transpose()
}

fn optional_u64(args: &[std::ffi::OsString], flag: &str) -> Result<Option<u64>, String> {
    option_value(args, flag)
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|error| format!("parse {flag}: {error}"))
        })
        .transpose()
}

fn option_value(args: &[std::ffi::OsString], flag: &str) -> Option<String> {
    args.windows(2).find_map(|pair| {
        if pair[0].to_str() == Some(flag) {
            pair[1].to_str().map(str::to_string)
        } else {
            None
        }
    })
}

fn has_flag(args: &[std::ffi::OsString], flag: &str) -> bool {
    args.iter().any(|arg| arg.to_str() == Some(flag))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use casa_provider_contracts::{ParameterValue, ProviderSurfaceKind};

    use super::{
        SIMOBSERVE_TASK_PROTOCOL_NAME, SIMOBSERVE_TASK_PROTOCOL_VERSION, SimobserveFamilyManifest,
        SimobserveProtocolInfo, SimobserveTaskRequest, SimobserveTaskSchemaBundle,
        SyntheticObservationMode, SyntheticPolarizationBasis, SyntheticWorkerPolicy,
        command_schema, load_real_family_config, request_from_cli_args,
        simobserve_provider_invocation,
    };
    use crate::columns::main_ids;
    use crate::ui_schema::{UiArgumentParser, UiValueKind};

    #[test]
    fn schema_bundle_uses_current_protocol_and_projection() {
        let bundle = SimobserveTaskSchemaBundle::current();
        assert_eq!(bundle.protocol.protocol_name, SIMOBSERVE_TASK_PROTOCOL_NAME);
        assert_eq!(
            bundle.protocol.protocol_version,
            SIMOBSERVE_TASK_PROTOCOL_VERSION
        );
        assert_eq!(bundle.protocol.surface_kind, ProviderSurfaceKind::Task);
        assert_eq!(bundle.semantic.operations[0].request_kind, "run");
        assert_eq!(bundle.semantic.operations[1].request_kind, "family");
        assert!(bundle.components.contains_key("SimobserveRunTaskRequest"));
        assert_eq!(bundle.parameter_surfaces.len(), 1);
        assert_eq!(bundle.parameter_surfaces[0].surface.id(), "simobserve");
        bundle.parameter_surfaces[0]
            .validate()
            .expect("embedded simobserve parameter surface");
        assert_eq!(
            serde_json::to_value(&bundle).unwrap()["parameter_surfaces"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert!(
            bundle
                .components
                .contains_key("SimobserveFamilyTaskRequest")
        );
        let ui_schema = command_schema("simobserve");
        assert_eq!(ui_schema.command_id, "simobserve");
        let request_kind = ui_schema.argument("request_kind").expect("request_kind");
        assert_eq!(request_kind.value_kind, UiValueKind::Choice);
        let UiArgumentParser::Option { choices, .. } = &request_kind.parser else {
            panic!("request_kind should be a choice option");
        };
        assert_eq!(choices, &["family".to_string(), "run".to_string()]);
        assert!(ui_schema.argument("source_model").is_some());
        assert!(ui_schema.argument("target_ms_size_gib").is_some());
        assert!(ui_schema.argument("observation_mode").is_some());
        assert!(ui_schema.argument("request_json").is_none());
        let projected = bundle.projections.ui_schema.as_ref().unwrap();
        assert!(
            projected["arguments"]
                .as_array()
                .unwrap()
                .iter()
                .any(|argument| argument["id"] == "request_kind"
                    && argument["concept_id"] == "parameter.request_kind")
        );
    }

    #[test]
    fn protocol_info_matches_public_constants() {
        let info = SimobserveProtocolInfo::current();
        assert_eq!(info.protocol_name, SIMOBSERVE_TASK_PROTOCOL_NAME);
        assert_eq!(info.protocol_version, SIMOBSERVE_TASK_PROTOCOL_VERSION);
        assert_eq!(info.surface_kind, ProviderSurfaceKind::Task);
    }

    #[test]
    fn family_parameter_projection_reaches_the_typed_stdin_request() {
        let source_model = r#"{"kind":"analytic_components","components":[{"kind":"point","l_rad":0.0,"m_rad":0.0,"spectrum":{"flux_jy":2.5}}]}"#;
        let values = BTreeMap::from([
            (
                "request_kind".into(),
                ParameterValue::String("family".into()),
            ),
            (
                "source_model".into(),
                ParameterValue::String(source_model.into()),
            ),
            ("telescope".into(), ParameterValue::String("ALMA".into())),
            (
                "array_config".into(),
                ParameterValue::String("synthetic-alma-compact".into()),
            ),
            ("band".into(), ParameterValue::String("Band 6".into())),
            ("target_ms_size_gib".into(), ParameterValue::Float(0.025)),
            (
                "output_ms".into(),
                ParameterValue::String("products/family.ms".into()),
            ),
            ("overwrite".into(), ParameterValue::Bool(true)),
            ("polarizations".into(), ParameterValue::String("4".into())),
            ("ms_channels".into(), ParameterValue::Integer(16)),
            ("image_channels".into(), ParameterValue::Integer(8)),
            ("pointing_count".into(), ParameterValue::Integer(7)),
            ("time_sample_count".into(), ParameterValue::Integer(9)),
            ("integration_seconds".into(), ParameterValue::Float(3.5)),
            (
                "start_time_mjd_seconds".into(),
                ParameterValue::Float(4_895_229_000.0),
            ),
            (
                "imaging_mode".into(),
                ParameterValue::String("spectral_cube".into()),
            ),
            (
                "observation_mode".into(),
                ParameterValue::String("total_power".into()),
            ),
            ("measure_actual_size".into(), ParameterValue::Bool(true)),
            (
                "worker_policy".into(),
                ParameterValue::String("fixed".into()),
            ),
            ("row_workers".into(), ParameterValue::Integer(2)),
            ("channel_workers".into(), ParameterValue::Integer(3)),
        ]);

        let invocation = simobserve_provider_invocation(&values, vec!["ignored".into()])
            .expect("family invocation");
        assert_eq!(invocation.invocation.args, ["--json-run", "-"]);
        let stdin = invocation.invocation.stdin.expect("family JSON stdin");
        assert!(stdin.ends_with('\n'));
        let SimobserveTaskRequest::Family(request) =
            serde_json::from_str(&stdin).expect("typed family request")
        else {
            panic!("expected family request")
        };
        assert_eq!(
            request.source_model,
            serde_json::from_str(source_model).expect("source model")
        );
        assert_eq!(request.telescope, "ALMA");
        assert_eq!(request.array_config, "synthetic-alma-compact");
        assert_eq!(request.band, "Band 6");
        assert_eq!(request.target_ms_size_gib, 0.025);
        assert_eq!(
            request.output_ms.as_deref(),
            Some(std::path::Path::new("products/family.ms"))
        );
        assert!(request.overwrite);
        assert_eq!(request.polarizations, 4);
        assert_eq!(request.ms_channels, 16);
        assert_eq!(request.image_channels, 8);
        assert_eq!(request.pointing_count, 7);
        assert_eq!(request.time_sample_count, Some(9));
        assert_eq!(request.integration_seconds, Some(3.5));
        assert_eq!(request.start_time_mjd_seconds, Some(4_895_229_000.0));
        assert_eq!(request.imaging_mode, "spectral_cube");
        assert_eq!(
            request.observation_mode,
            SyntheticObservationMode::TotalPower
        );
        assert!(request.measure_actual_size);
        assert_eq!(request.worker_policy, SyntheticWorkerPolicy::Fixed);
        assert_eq!(request.row_workers, Some(2));
        assert_eq!(request.channel_workers, Some(3));
    }

    #[test]
    fn run_parameter_projection_keeps_direct_cli_and_has_no_stdin() {
        let values =
            BTreeMap::from([("request_kind".into(), ParameterValue::String("run".into()))]);
        let direct = vec![
            "--model".into(),
            "model.fits".into(),
            "--out".into(),
            "out.ms".into(),
        ];
        let invocation = simobserve_provider_invocation(&values, direct.clone()).unwrap();
        assert_eq!(invocation.invocation.args, direct);
        assert_eq!(invocation.invocation.stdin, None);
        assert_eq!(
            invocation.consumed_parameters,
            BTreeSet::from(["request_kind".to_string()])
        );
    }

    #[test]
    fn cli_parses_common_corruption_controls() {
        let request = request_from_cli_args(
            &[
                "--model",
                "model.fits",
                "--out",
                "out.ms",
                "--corruption-seed",
                "123",
                "--noise-simplenoise-jy",
                "0.001",
                "--gain-mode",
                "fbm",
                "--gain-interval-seconds",
                "10",
                "--gain-amplitude",
                "0.05,0.02",
                "--bandpass-mode",
                "calculate",
                "--bandpass-interval-seconds",
                "3600",
                "--bandpass-amplitude",
                "0.03,0.04",
                "--leakage-amplitude",
                "0.01,0.0",
                "--leakage-offset",
                "0.0",
                "--pointing-offset-ra-arcsec",
                "2.5",
                "--pointing-offset-dec-arcsec",
                "-1.5",
                "--elevation-limit-deg",
                "25",
            ]
            .iter()
            .map(std::ffi::OsString::from)
            .collect::<Vec<_>>(),
        )
        .expect("parse simobserve cli request");
        let corruption = request.corruption.expect("corruption config");
        assert_eq!(corruption.seed, 123);
        assert_eq!(
            corruption.noise.as_ref().expect("noise").simplenoise_jy,
            0.001
        );
        assert_eq!(
            corruption.gain.as_ref().expect("gain").amplitude,
            [0.05, 0.02]
        );
        assert_eq!(
            corruption.bandpass.as_ref().expect("bandpass").amplitude,
            [0.03, 0.04]
        );
        assert_eq!(
            corruption.leakage.as_ref().expect("leakage").amplitude,
            [0.01, 0.0]
        );
        let pointing = corruption.pointing.expect("pointing");
        assert!(pointing.apply_pointing_offsets);
        assert!(pointing.offset_rad[0] > 0.0);
        assert!(pointing.offset_rad[1] < 0.0);
        assert!((request.elevation_limit_rad.unwrap().to_degrees() - 25.0).abs() < 1.0e-12);
    }

    #[test]
    fn cli_parses_below_elevation_override() {
        let request = request_from_cli_args(
            &[
                "--model",
                "model.fits",
                "--out",
                "out.ms",
                "--allow-below-elevation-limit",
            ]
            .iter()
            .map(std::ffi::OsString::from)
            .collect::<Vec<_>>(),
        )
        .expect("parse simobserve cli request");

        assert!(request.allow_below_elevation_limit);
    }

    #[test]
    fn cli_parses_explicit_worker_controls() {
        let request = request_from_cli_args(
            &[
                "--model",
                "model.fits",
                "--out",
                "out.ms",
                "--worker-policy",
                "fixed",
                "--row-workers",
                "3",
                "--channel-workers",
                "5",
            ]
            .iter()
            .map(std::ffi::OsString::from)
            .collect::<Vec<_>>(),
        )
        .expect("parse simobserve cli request");

        assert_eq!(request.worker_policy, SyntheticWorkerPolicy::Fixed);
        assert_eq!(request.row_workers, Some(3));
        assert_eq!(request.channel_workers, Some(5));
    }

    #[test]
    fn cli_parses_polarization_controls() {
        let request = request_from_cli_args(
            &[
                "--model",
                "model.fits",
                "--out",
                "out.ms",
                "--polarizations",
                "4",
                "--polarization-basis",
                "linear",
            ]
            .iter()
            .map(std::ffi::OsString::from)
            .collect::<Vec<_>>(),
        )
        .expect("parse simobserve cli request");
        let polarization_setup = request.polarization_setup.expect("polarization setup");

        assert_eq!(polarization_setup.basis, SyntheticPolarizationBasis::Linear);
        assert_eq!(polarization_setup.correlation_count, 4);
    }

    #[test]
    fn run_request_accepts_analytic_model_without_legacy_model_image() {
        let request: SimobserveTaskRequest = serde_json::from_str(
            r#"{
                "kind": "run",
                "request": {
                    "model": {
                        "kind": "analytic_components",
                        "components": [{
                            "kind": "point",
                            "l_rad": 0.0,
                            "m_rad": 0.0,
                            "spectrum": { "flux_jy": 1.0 }
                        }]
                    },
                    "output_ms": "out.ms"
                }
            }"#,
        )
        .expect("parse analytic run request");
        let SimobserveTaskRequest::Run(request) = request else {
            panic!("expected run request");
        };

        let synthetic = request.to_synthetic_request();

        assert!(synthetic.model.is_some());
        assert_eq!(
            synthetic.model_image,
            std::path::PathBuf::from("analytic-components.json")
        );
    }

    #[test]
    fn real_array_config_loader_accepts_casa_xyz_cfg_path() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("vla.test.cfg");
        std::fs::write(
            &config_path,
            "# observatory=VLA\n# coordsys=XYZ\n1.0 2.0 3.0 25.0 VLA01\n4.0 5.0 6.0 25.0 VLA02\n",
        )
        .unwrap();

        let loaded = load_real_family_config("VLA", config_path.to_str().unwrap())
            .expect("load config")
            .expect("config found");

        assert_eq!(loaded.0.len(), 2);
        assert_eq!(loaded.0[0].station, "VLA01");
        assert_eq!(loaded.0[1].position_m, [4.0, 5.0, 6.0]);
        assert_eq!(loaded.1, config_path.display().to_string());
    }

    #[test]
    fn real_array_config_loader_uses_casa_geodetic_loc_basis() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("alma.test.cfg");
        std::fs::write(
            &config_path,
            "# observatory=ALMA\n# coordsys=LOC (local tangent plane)\n-33.89412596 -712.7516484 -2.330089496 12.0 A001\n",
        )
        .unwrap();

        let loaded = load_real_family_config("ALMA", config_path.to_str().unwrap())
            .expect("load config")
            .expect("config found");
        let position = loaded.0[0].position_m;

        assert!((position[0] - 2_225_004.468_449_519_5).abs() < 1.0e-3);
        assert!((position[1] + 5_440_060.207_407_018).abs() < 1.0e-3);
        assert!((position[2] + 2_481_684.920_604_503).abs() < 1.0e-3);
    }

    #[test]
    fn real_array_config_loader_converts_casa_utm_sam56_basis() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("alma.out07.test.cfg");
        std::fs::write(
            &config_path,
            "# observatory=ALMA\n# coordsys=UTM\n# datum=SAM56\n# zone=19\n# hemisphere=S\n627789.81 7453079.62 5029.4 12.0 1\n",
        )
        .unwrap();

        let loaded = load_real_family_config("ALMA", config_path.to_str().unwrap())
            .expect("load config")
            .expect("config found");
        let position = loaded.0[0].position_m;

        assert!((position[0] - 2_225_083.645_110_78).abs() < 1.0e-3);
        assert!((position[1] + 5_440_152.112_940_93).abs() < 1.0e-3);
        assert!((position[2] + 2_481_694.493_444_05).abs() < 1.0e-3);
    }

    #[test]
    fn family_total_power_accepts_almasd_cfg_and_uses_first_antenna() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("aca.tp.cfg");
        std::fs::write(
            &config_path,
            "# observatory=ALMASD\n# coordsys=LOC\n-47.7 178.2 -2.0 12.0 T701\n-42.1 118.7 -2.0 12.0 T702\n",
        )
        .unwrap();
        let output_ms = temp.path().join("tp-family.ms");
        let request_json = serde_json::json!({
            "kind": "family",
            "request": {
                "source_model": {
                    "kind": "analytic_components",
                    "components": [{
                        "kind": "point",
                        "l_rad": 0.0,
                        "m_rad": 0.0,
                        "spectrum": { "flux_jy": 1.0 }
                    }]
                },
                "telescope": "ACA",
                "array_config": config_path,
                "band": "Band 7",
                "target_ms_size_gib": 0.00001,
                "polarizations": 2,
                "ms_channels": 1,
                "image_channels": 1,
                "pointing_count": 3,
                "imaging_mode": "mosaic",
                "observation_mode": "total_power",
                "output_ms": output_ms
            }
        });

        let request: SimobserveTaskRequest =
            serde_json::from_value(request_json).expect("parse family request");
        let result = request.execute().expect("execute family request");
        let super::SimobserveTaskResult::Family(result) = result else {
            panic!("expected family result");
        };

        assert_eq!(
            result.run_result.report.observation_mode,
            SyntheticObservationMode::TotalPower
        );
        assert_eq!(result.run_result.report.antenna_count, 1);
        assert_eq!(result.run_result.report.baseline_count, 1);
        assert_eq!(
            result.run_result.report.main_row_count,
            result.run_result.report.time_sample_count
        );
        let manifest: SimobserveFamilyManifest =
            serde_json::from_slice(&std::fs::read(&result.manifest_path).expect("read manifest"))
                .expect("parse manifest");
        assert_eq!(
            manifest.observation_mode,
            SyntheticObservationMode::TotalPower
        );
        assert_eq!(manifest.run_request.antennas.len(), 1);
        assert_eq!(manifest.run_request.antennas[0].station, "T701");
        assert_eq!(
            manifest.run_request.telescope_name.as_deref(),
            Some("ALMASD")
        );
    }

    #[test]
    fn family_request_generates_ms_and_manifest_from_dialog_inputs() {
        let temp = tempfile::tempdir().unwrap();
        let output_ms = temp.path().join("family.ms");
        let request_json = serde_json::json!({
            "kind": "family",
            "request": {
                "source_model": {
                    "kind": "analytic_components",
                    "components": [{
                        "kind": "point",
                        "l_rad": 0.0,
                        "m_rad": 0.0,
                        "spectrum": { "flux_jy": 1.0 }
                    }]
                },
                "telescope": "ALMA",
                "array_config": "synthetic-aca",
                "band": "Band 3",
                "target_ms_size_gib": 0.0001,
                "polarizations": 4,
                "ms_channels": 4,
                "image_channels": 2,
                "pointing_count": 3,
                "imaging_mode": "mosaic",
                "output_ms": output_ms
            }
        });
        let request: SimobserveTaskRequest =
            serde_json::from_value(request_json).expect("parse family request");
        let result = request.execute().expect("execute family request");
        let super::SimobserveTaskResult::Family(result) = result else {
            panic!("expected family result");
        };

        assert_eq!(result.source_model_kind, "analytic_components");
        assert_eq!(result.requested_ms_channels, 4);
        assert_eq!(result.requested_image_channels, 2);
        assert_eq!(result.requested_pointing_count, 3);
        assert!(result.estimated_main_rows > 0);
        assert!(result.output_ms.exists());
        assert!(result.manifest_path.exists());
        assert_eq!(result.run_result.report.correlation_count, 4);
        assert_eq!(result.run_result.report.channel_count, 4);

        let ms = crate::MeasurementSet::open(&result.output_ms).expect("open family MS");
        assert!(ms.validate().unwrap().is_empty());
        assert_eq!(ms.polarization().unwrap().num_corr(0).unwrap(), 4);
        assert_eq!(ms.field().unwrap().row_count(), 3);
        let antenna_count = result.run_result.report.antenna_count;
        assert_eq!(
            ms.pointing().unwrap().row_count(),
            antenna_count * result.run_result.report.time_sample_count
        );
        let baseline_count = antenna_count * (antenna_count - 1) / 2;
        assert_eq!(main_ids::field_id(ms.main_table()).get(0).unwrap(), 0);
        assert_eq!(
            main_ids::field_id(ms.main_table())
                .get(baseline_count)
                .unwrap(),
            1
        );
        assert_eq!(
            main_ids::field_id(ms.main_table())
                .get(2 * baseline_count)
                .unwrap(),
            2
        );

        let manifest: SimobserveFamilyManifest =
            serde_json::from_slice(&std::fs::read(&result.manifest_path).expect("read manifest"))
                .expect("parse manifest");
        assert_eq!(manifest.requested_image_channels, 2);
        assert_eq!(manifest.array_config_source, "synthetic ACA 7m compact");
        assert_eq!(manifest.run_request.fields.len(), 3);
    }

    #[test]
    fn family_request_generates_expected_mode_matrix_members() {
        let temp = tempfile::tempdir().unwrap();
        let cases = [
            ("single_field", "VLA", "A", "Q", 1usize),
            ("mfs", "VLA", "A", "Q", 1usize),
            ("mosaic", "VLA", "A", "Q", 3usize),
            ("spectral_cube", "VLA", "A", "Q", 1usize),
            ("cubedata", "VLA", "A", "Q", 1usize),
            ("mt_mfs", "VLA", "A", "Q", 1usize),
            ("simalma", "ALMA", "synthetic-simalma", "Band 3", 3usize),
            ("aca", "ACA", "synthetic-aca", "Band 3", 3usize),
        ];

        for (mode, telescope, array_config, band, expected_fields) in cases {
            let output_ms = temp.path().join(format!("{mode}.ms"));
            let request_json = serde_json::json!({
                "kind": "family",
                "request": {
                    "source_model": {
                        "kind": "analytic_components",
                        "components": [{
                            "kind": "point",
                            "l_rad": 0.0,
                            "m_rad": 0.0,
                            "spectrum": {
                                "flux_jy": 1.0,
                                "line_peak_jy": 0.25,
                                "line_center_fraction": 0.5,
                                "line_sigma_fraction": 0.2
                            }
                        }]
                    },
                    "telescope": telescope,
                    "array_config": array_config,
                    "band": band,
                    "target_ms_size_gib": 0.000001,
                    "polarizations": 2,
                    "ms_channels": 4,
                    "image_channels": 2,
                    "pointing_count": 3,
                    "imaging_mode": mode,
                    "output_ms": output_ms
                }
            });
            let request: SimobserveTaskRequest =
                serde_json::from_value(request_json).expect("parse family request");
            let result = request.execute().expect("execute family request");
            let super::SimobserveTaskResult::Family(result) = result else {
                panic!("expected family result");
            };
            let ms = crate::MeasurementSet::open(&result.output_ms).expect("open family MS");
            assert_eq!(
                ms.field().unwrap().row_count(),
                expected_fields,
                "mode {mode}"
            );
            assert_eq!(ms.spectral_window().unwrap().num_chan(0).unwrap(), 4);
        }
    }
}
