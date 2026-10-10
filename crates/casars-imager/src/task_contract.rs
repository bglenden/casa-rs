// SPDX-License-Identifier: LGPL-3.0-or-later
//! The imager's task contract over the shared provider envelope: the
//! catalog-named parameters of one run in, its report and products out.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use casa_imaging_application::{
    CleanStop, ImagingOutcome, ImagingRequest, NativeMinorCycleOutcome, NativeMinorCycleStopReason,
};
use casa_provider_contracts::{
    NoAdditionalProviderSchemas, ParameterValue, ProviderCliMachineActions, ProviderCliProjection,
    ProviderInvocation, ProviderInvocationAdaptation, ProviderProjectionMetadata,
    ProviderProtocolDescriptor, ProviderSurfaceKind, TaskOperationDescriptor, TaskProviderContract,
    TaskProviderSchemas, TaskSemanticContract, builtin_surface_bundle, merged_components,
};
use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};

/// Stable protocol name advertised by `casars-imager --protocol-info`.
pub const IMAGER_TASK_PROTOCOL_NAME: &str = "casa_imager_task";
/// Protocol version advertised by `casars-imager --protocol-info`.
pub const IMAGER_TASK_PROTOCOL_VERSION: u32 = 12;

/// The imager's protocol descriptor.
pub fn imager_protocol_descriptor() -> ProviderProtocolDescriptor {
    ProviderProtocolDescriptor::new(
        IMAGER_TASK_PROTOCOL_NAME,
        IMAGER_TASK_PROTOCOL_VERSION,
        ProviderSurfaceKind::Task,
        env!("CARGO_PKG_VERSION"),
    )
}

/// The imager's schema bundle in the shared envelope; the catalog's
/// `imager` surface defines the request's parameters.
pub fn imager_task_schema_bundle() -> TaskProviderContract {
    let request_schema = schema_for!(ImagerTaskRequest);
    let result_schema = schema_for!(ImagerTaskResult);
    TaskProviderContract {
        protocol: imager_protocol_descriptor(),
        semantic: TaskSemanticContract {
            request_schema: request_schema.clone(),
            result_schema: result_schema.clone(),
            operations: vec![TaskOperationDescriptor {
                name: "run".to_string(),
                request_kind: "run".to_string(),
                result_kind: Some("run".to_string()),
            }],
        },
        components: merged_components([&request_schema, &result_schema]),
        annotations: serde_json::json!({}),
        projections: ProviderProjectionMetadata {
            cli: Some(ProviderCliProjection {
                machine_actions: ProviderCliMachineActions {
                    json_schema: Some("--json-schema".to_string()),
                    protocol_info: Some("--protocol-info".to_string()),
                    json_run: Some("--json-run <SOURCE>".to_string()),
                    session: None,
                },
            }),
            python: None,
        },
        parameter_surfaces: vec![
            builtin_surface_bundle("imager")
                .expect("built-in imager parameter surface must remain valid"),
        ],
        domain_schemas: TaskProviderSchemas {
            request_schema,
            result_schema,
            additional: NoAdditionalProviderSchemas {},
        },
    }
}

/// Project one resolved parameter set into the imager's invocation: the
/// parameters, as JSON on stdin, run with `--json-run -`.
pub fn imager_provider_invocation(
    values: &BTreeMap<String, ParameterValue>,
    direct_args: Vec<String>,
) -> Result<ProviderInvocationAdaptation, String> {
    let mut args = managed_output_args(direct_args)?;
    args.extend(["--json-run".to_string(), "-".to_string()]);
    let request = ImagerTaskRequest::Run(ImagingParameters(
        values
            .iter()
            .map(|(name, value)| (name.clone(), value.to_plain_json()))
            .collect(),
    ));
    let mut stdin = serde_json::to_string(&request)
        .map_err(|error| format!("serialize the imager request: {error}"))?;
    stdin.push('\n');
    Ok(ProviderInvocationAdaptation {
        invocation: ProviderInvocation {
            args,
            stdin: Some(stdin),
        },
        consumed_parameters: values.keys().cloned().collect(),
    })
}

/// The `--managed-output <bool>` pair of the direct arguments; the task's
/// own parameters travel on stdin.
fn managed_output_args(args: Vec<String>) -> Result<Vec<String>, String> {
    let mut managed = Vec::new();
    let mut args = args.into_iter();
    while let Some(argument) = args.next() {
        if argument != "--managed-output" {
            continue;
        }
        let value = args
            .next()
            .ok_or_else(|| "--managed-output requires its projected boolean value".to_string())?;
        if !matches!(value.as_str(), "true" | "false") {
            return Err(format!(
                "--managed-output expects true or false, found {value:?}"
            ));
        }
        managed.extend([argument, value]);
    }
    Ok(managed)
}

/// Catalog-named imager parameters as plain JSON: sparse in a request,
/// every active parameter once resolved (the run's request echo).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct ImagingParameters(pub serde_json::Map<String, serde_json::Value>);

/// The imager's request envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "request", rename_all = "snake_case")]
pub enum ImagerTaskRequest {
    /// Image one MeasurementSet; the parameters are resolved over the
    /// catalog's defaults exactly as the command line's are.
    Run(ImagingParameters),
}

impl ImagerTaskRequest {
    /// Resolve and run the request.
    pub fn execute(&self) -> Result<ImagerTaskResult, String> {
        let Self::Run(parameters) = self;
        let (parameters, request) = crate::resolve_request(parameters)?;
        crate::run(parameters, &request).map(ImagerTaskResult::Run)
    }
}

/// The imager's result envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "result", rename_all = "snake_case")]
pub enum ImagerTaskResult {
    /// A completed run.
    Run(ImagerRunTaskResult),
}

/// The result of one run: the resolved parameters, the run's report and
/// its products.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImagerRunTaskResult {
    /// The resolved parameters the run imaged with.
    pub request: ImagingParameters,
    /// The run's report.
    pub run: ImagerRunReport,
    /// The products, each with whether it exists after the run.
    pub artifacts: Vec<ImagerArtifact>,
}

impl ImagerRunTaskResult {
    /// The result of a completed run of `request`, resolved from
    /// `parameters`, that took `elapsed`.
    pub fn from_outcome(
        parameters: ImagingParameters,
        request: &ImagingRequest,
        outcome: &ImagingOutcome,
        elapsed: Duration,
    ) -> Self {
        Self {
            artifacts: artifacts(&request.imagename, &outcome.product_names()),
            request: parameters,
            run: ImagerRunReport {
                gridded_samples: outcome.scientific.normal_state().sample_count(),
                major_cycles: outcome.major_cycle_count,
                minor_iterations: outcome.total_minor_iterations,
                actual_minor_iterations: outcome.total_actual_minor_iterations,
                clean_stop_reason: outcome.stop.map(ImagerCleanStopReason::from),
                minor_cycles: outcome
                    .minor_cycles
                    .iter()
                    .map(project_minor_cycle)
                    .collect(),
                visibility_products: outcome.visibility_products.as_ref().map(|completion| {
                    ImagerVisibilityProductDiagnostic {
                        final_model_generation: hex(completion.final_model().as_bytes()),
                        sample_count: completion.sample_count(),
                    }
                }),
                elapsed_ns: u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
            },
        }
    }
}

/// The report of one completed run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImagerRunReport {
    /// Selected samples gridded by each major cycle.
    pub gridded_samples: u64,
    /// Major cycles run, the initial one included.
    pub major_cycles: usize,
    /// Minor-cycle iterations charged to the `niter` budget.
    pub minor_iterations: usize,
    /// Minor-cycle components actually applied.
    pub actual_minor_iterations: usize,
    /// Why cleaning stopped, when it ran.
    pub clean_stop_reason: Option<ImagerCleanStopReason>,
    /// Each minor cycle, in order.
    pub minor_cycles: Vec<ImagerMinorCycleDiagnostic>,
    /// The final prediction of a run that writes visibilities.
    pub visibility_products: Option<ImagerVisibilityProductDiagnostic>,
    /// Wall time of the run.
    pub elapsed_ns: u64,
}

/// The final model a run predicted written visibilities from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ImagerVisibilityProductDiagnostic {
    /// The final model generation.
    pub final_model_generation: String,
    /// Selected visibility samples predicted.
    pub sample_count: u64,
}

fn hex(bytes: [u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Why cleaning stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ImagerCleanStopReason {
    /// The absolute threshold was reached.
    GlobalThresholdReached,
    /// The `nsigma` threshold was reached.
    NsigmaThresholdReached,
    /// The `niter` budget was spent.
    IterationLimitReached,
    /// The `nmajor` budget was spent.
    MajorCycleLimitReached,
    /// No pixel could be cleaned.
    NoCleanablePixels,
    /// The residual peak rose after earlier progress.
    DivergenceDetected,
}

impl From<CleanStop> for ImagerCleanStopReason {
    fn from(stop: CleanStop) -> Self {
        match stop {
            CleanStop::Iterations => Self::IterationLimitReached,
            CleanStop::Threshold => Self::GlobalThresholdReached,
            CleanStop::NSigma => Self::NsigmaThresholdReached,
            CleanStop::ZeroMask => Self::NoCleanablePixels,
            CleanStop::MajorCycles => Self::MajorCycleLimitReached,
            CleanStop::NoChange
            | CleanStop::DivergedFromPrevious
            | CleanStop::DivergedFromMinimum => Self::DivergenceDetected,
        }
    }
}

/// Why one minor cycle ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ImagerMinorCycleStopReason {
    /// The cycle threshold was reached.
    ThresholdReached,
    /// The cycle's iteration bound was reached.
    IterationBound,
    /// A plane's peak residual rose more than 10% above its minimum.
    Diverged,
}

/// One component a minor cycle applied.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImagerMinorCycleComponent {
    /// Image domain.
    pub domain: usize,
    /// Spectral-basis coefficient.
    pub coefficient: usize,
    /// Polarization coordinate.
    pub polarization: usize,
    /// Pixel x.
    pub x: usize,
    /// Pixel y.
    pub y: usize,
    /// Signed flux in model units.
    pub flux: f64,
    /// Scale in pixels; 0 for a point.
    pub scale_px: f64,
}

/// One auto-multithresh update.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImagerAutoMaskDiagnostic {
    /// Robust residual median.
    pub median: f64,
    /// MAD-derived robust RMS.
    pub robust_rms: f64,
    /// Positive detection threshold.
    pub positive_threshold: f64,
    /// Low-noise growth threshold.
    pub low_noise_threshold: f64,
    /// Negative detection threshold, when enabled.
    pub negative_threshold: Option<f64>,
    /// Support pixels changed.
    pub changed_pixels: usize,
    /// Whether later cycles keep this support.
    pub channel_stopped: bool,
}

/// One minor cycle.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImagerMinorCycleDiagnostic {
    /// One-based cycle.
    pub cycle: usize,
    /// Charged iterations before the cycle.
    pub iterations_entering: usize,
    /// Iterations charged to the cycle.
    pub iterations: usize,
    /// Charged iterations after the cycle.
    pub total_iterations: usize,
    /// Applied components before the cycle.
    pub actual_iterations_entering: usize,
    /// Components applied in the cycle.
    pub actual_iterations: usize,
    /// Applied components after the cycle.
    pub total_actual_iterations: usize,
    /// Absolute component flux accepted in the cycle.
    pub total_flux: f64,
    /// Normalized residual peak at the cycle's start.
    pub initial_peak_flux: f64,
    /// Normalized residual peak at its end.
    pub final_peak_flux: f64,
    /// Robust RMS of `nsigma`, when enabled.
    pub noise_rms: Option<f64>,
    /// The threshold the cycle cleaned to.
    pub effective_threshold: f64,
    /// The absolute or noise threshold before the cycle threshold.
    pub global_threshold: f64,
    /// The PSF-derived cycle threshold, when enabled.
    pub cycle_threshold: Option<f64>,
    /// Why the cycle ended.
    pub stop_reason: ImagerMinorCycleStopReason,
    /// Clark's whole-plane residual refreshes.
    pub clark_refreshes: usize,
    /// One-based major cycle that followed the cycle.
    pub associated_replay_ordinal: usize,
    /// The cycle's first components.
    pub components: Vec<ImagerMinorCycleComponent>,
    /// The x-major support the components were placed in.
    pub mask_support: Vec<bool>,
    /// The auto-multithresh update, when it ran.
    pub auto_mask: Option<ImagerAutoMaskDiagnostic>,
}

fn project_minor_cycle(cycle: &NativeMinorCycleOutcome) -> ImagerMinorCycleDiagnostic {
    ImagerMinorCycleDiagnostic {
        cycle: cycle.cycle,
        iterations_entering: cycle.iterations_entering,
        iterations: cycle.iterations,
        total_iterations: cycle.total_iterations,
        actual_iterations_entering: cycle.actual_iterations_entering,
        actual_iterations: cycle.actual_iterations,
        total_actual_iterations: cycle.total_actual_iterations,
        total_flux: cycle.total_flux,
        initial_peak_flux: cycle.initial_peak_flux,
        final_peak_flux: cycle.final_peak_flux,
        noise_rms: cycle.noise_rms,
        effective_threshold: cycle.effective_threshold,
        global_threshold: cycle.global_threshold,
        cycle_threshold: cycle.cycle_threshold,
        stop_reason: match cycle.stop_reason {
            NativeMinorCycleStopReason::ThresholdReached => {
                ImagerMinorCycleStopReason::ThresholdReached
            }
            NativeMinorCycleStopReason::IterationBound => {
                ImagerMinorCycleStopReason::IterationBound
            }
            NativeMinorCycleStopReason::Diverged => ImagerMinorCycleStopReason::Diverged,
        },
        clark_refreshes: cycle.clark_refreshes,
        associated_replay_ordinal: cycle.associated_replay_ordinal,
        components: cycle
            .recorded_components
            .iter()
            .map(|component| {
                let cell = component.cell;
                let [x, y] = cell.pixel();
                ImagerMinorCycleComponent {
                    domain: cell.domain(),
                    coefficient: cell.coefficient(),
                    polarization: cell.polarization(),
                    x,
                    y,
                    flux: component.flux,
                    scale_px: component.scale_px,
                }
            })
            .collect(),
        mask_support: cycle.mask_support.clone(),
        auto_mask: cycle.auto_mask.map(|evidence| ImagerAutoMaskDiagnostic {
            median: evidence.median,
            robust_rms: evidence.robust_rms,
            positive_threshold: evidence.positive_threshold,
            low_noise_threshold: evidence.low_noise_threshold,
            negative_threshold: evidence.negative_threshold,
            changed_pixels: evidence.changed_pixels,
            channel_stopped: evidence.channel_stopped,
        }),
    }
}

/// Kind of a written product, spelled as its CASA suffix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ImagerArtifactKind {
    /// `.psf`.
    #[serde(rename = "psf")]
    Psf,
    /// `.residual`.
    #[serde(rename = "residual")]
    Residual,
    /// `.model`.
    #[serde(rename = "model")]
    Model,
    /// `.image`, the restored image.
    #[serde(rename = "image")]
    Image,
    /// `.mask`.
    #[serde(rename = "mask")]
    Mask,
    /// `.weight`.
    #[serde(rename = "weight")]
    Weight,
    /// `.sumwt`.
    #[serde(rename = "sumwt")]
    Sumwt,
    /// `.pb`.
    #[serde(rename = "pb")]
    PrimaryBeam,
    /// `.image.pbcor`.
    #[serde(rename = "image.pbcor")]
    ImagePbcor,
    /// `.alpha`.
    #[serde(rename = "alpha")]
    Alpha,
    /// `.alpha.error`.
    #[serde(rename = "alpha.error")]
    AlphaError,
    /// `.alpha.pbcor`.
    #[serde(rename = "alpha.pbcor")]
    AlphaPbcor,
}

impl ImagerArtifactKind {
    /// The kind of the product with CASA suffix `suffix` (`.image.tt0`).
    fn of_suffix(suffix: &str) -> Self {
        match suffix {
            ".alpha.error" => Self::AlphaError,
            ".alpha.pbcor" => Self::AlphaPbcor,
            ".alpha" => Self::Alpha,
            _ if suffix.starts_with(".psf") => Self::Psf,
            _ if suffix.starts_with(".residual") => Self::Residual,
            _ if suffix.starts_with(".model") => Self::Model,
            _ if suffix.starts_with(".sumwt") => Self::Sumwt,
            _ if suffix.starts_with(".weight") => Self::Weight,
            _ if suffix.starts_with(".pb") => Self::PrimaryBeam,
            _ if suffix.contains(".pbcor") => Self::ImagePbcor,
            _ if suffix.starts_with(".mask") => Self::Mask,
            _ => Self::Image,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Psf => "PSF",
            Self::Residual => "Residual",
            Self::Model => "Model",
            Self::Image => "Restored Image",
            Self::Mask => "Clean Mask",
            Self::Weight => "Weight",
            Self::Sumwt => "Sum of Weights",
            Self::PrimaryBeam => "Primary Beam",
            Self::ImagePbcor => "PB-corrected Image",
            Self::Alpha => "Spectral Index",
            Self::AlphaError => "Spectral Index Error",
            Self::AlphaPbcor => "PB-corrected Spectral Index",
        }
    }
}

/// One product of a run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ImagerArtifact {
    /// The product's kind.
    pub kind: ImagerArtifactKind,
    /// Its label, with its Taylor term (`Restored Image tt0`).
    pub label: String,
    /// Its path.
    pub path: String,
    /// Whether it exists after the run.
    pub exists: bool,
}

/// The products of a run with prefix `imagename` and CASA suffixes
/// `products`.
fn artifacts(imagename: &std::path::Path, products: &[String]) -> Vec<ImagerArtifact> {
    let base = imagename.to_string_lossy();
    products
        .iter()
        .map(|suffix| {
            let kind = ImagerArtifactKind::of_suffix(suffix);
            let label = suffix
                .split(".tt")
                .nth(1)
                .and_then(|term| term.split('.').next())
                .filter(|term| !term.is_empty())
                .map_or_else(
                    || kind.label().to_string(),
                    |term| format!("{} tt{term}", kind.label()),
                );
            let path = PathBuf::from(format!("{base}{suffix}"));
            ImagerArtifact {
                kind,
                label,
                exists: path.exists(),
                path: path.display().to_string(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use casa_provider_contracts::ProviderSurfaceKind;

    use super::*;

    #[test]
    fn the_schema_bundle_carries_the_catalog_surface() {
        let bundle = imager_task_schema_bundle();
        bundle.validate().expect("shared provider envelope");
        assert_eq!(bundle.protocol.protocol_name, IMAGER_TASK_PROTOCOL_NAME);
        assert_eq!(
            bundle.protocol.protocol_version,
            IMAGER_TASK_PROTOCOL_VERSION
        );
        assert_eq!(bundle.protocol.surface_kind, ProviderSurfaceKind::Task);
        assert_eq!(bundle.semantic.operations[0].request_kind, "run");
        assert_eq!(bundle.parameter_surfaces.len(), 1);
    }

    #[test]
    fn the_invocation_carries_every_resolved_parameter_on_stdin() {
        let values = BTreeMap::from([
            (
                "vis".to_string(),
                ParameterValue::Array(vec![ParameterValue::String("in.ms".to_string())]),
            ),
            ("niter".to_string(), ParameterValue::Integer(100)),
            ("gain".to_string(), ParameterValue::Float(0.1)),
        ]);
        let adaptation = imager_provider_invocation(
            &values,
            vec!["--managed-output".to_string(), "true".to_string()],
        )
        .expect("invocation");
        assert_eq!(
            adaptation.invocation.args,
            ["--managed-output", "true", "--json-run", "-"]
        );
        let request: ImagerTaskRequest =
            serde_json::from_str(adaptation.invocation.stdin.as_deref().expect("stdin"))
                .expect("request");
        let ImagerTaskRequest::Run(parameters) = request;
        assert_eq!(
            serde_json::Value::Object(parameters.0),
            serde_json::json!({ "vis": ["in.ms"], "niter": 100, "gain": 0.1 })
        );
        assert_eq!(
            adaptation.consumed_parameters,
            values.keys().cloned().collect()
        );
    }

    #[test]
    fn products_are_labelled_by_kind_and_taylor_term() {
        let artifacts = artifacts(
            std::path::Path::new("/nowhere/run"),
            &[
                ".image.tt0".to_string(),
                ".alpha.error".to_string(),
                ".pb.tt0".to_string(),
                ".image.tt0.pbcor".to_string(),
            ],
        );
        let kinds = artifacts
            .iter()
            .map(|artifact| (artifact.kind, artifact.label.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            [
                (ImagerArtifactKind::Image, "Restored Image tt0"),
                (ImagerArtifactKind::AlphaError, "Spectral Index Error"),
                (ImagerArtifactKind::PrimaryBeam, "Primary Beam tt0"),
                (ImagerArtifactKind::ImagePbcor, "PB-corrected Image tt0"),
            ]
        );
        assert_eq!(
            serde_json::to_value(ImagerArtifactKind::ImagePbcor).expect("kind"),
            "image.pbcor"
        );
        assert!(artifacts.iter().all(|artifact| !artifact.exists));
    }
}
