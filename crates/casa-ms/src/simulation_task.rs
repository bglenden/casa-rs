// SPDX-License-Identifier: LGPL-3.0-or-later
//! Canonical `simobserve` task request/result contracts shared by CLI and Python.

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Instant;

use casa_provider_contracts::{
    ProviderCliMachineActions, ProviderCliProjection, ProviderComponentSchemas,
    ProviderProjectionMetadata, ProviderSurfaceKind, TaskOperationDescriptor, TaskSemanticContract,
    derived_ui_schema_annotations, merged_components,
};
use schemars::{JsonSchema, schema::RootSchema, schema_for};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::simulation::{
    SyntheticAntenna, SyntheticObservationReport, SyntheticObservationRequest,
    SyntheticSpectralSetup, generate_synthetic_observation_ms,
};
use crate::ui_schema::{
    UiActionKind, UiArgumentParser, UiArgumentSchema, UiCommandSchema, UiValueKind,
};

/// Stable protocol name advertised by `simobserve --protocol-info`.
pub const SIMOBSERVE_TASK_PROTOCOL_NAME: &str = "casa_simobserve_task";
/// Stable protocol version advertised by `simobserve --protocol-info`.
pub const SIMOBSERVE_TASK_PROTOCOL_VERSION: u32 = 1;

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
    /// Existing FITS model image path.
    pub model_image: PathBuf,
    /// Output MeasurementSet path.
    pub output_ms: PathBuf,
    /// Replace an existing output MeasurementSet directory.
    #[serde(default)]
    pub overwrite: bool,
    /// Antenna configuration. Defaults to a compact three-antenna VLA layout.
    #[serde(default)]
    pub antennas: Vec<SyntheticAntenna>,
    /// J2000 phase center `[right_ascension, declination]` in radians.
    #[serde(default)]
    pub phase_center_rad: Option<[f64; 2]>,
    /// Observation start time in MJD seconds UTC.
    #[serde(default)]
    pub start_time_mjd_seconds: Option<f64>,
    /// Requested on-source duration in seconds.
    #[serde(default)]
    pub duration_seconds: Option<f64>,
    /// Integration time in seconds.
    #[serde(default)]
    pub integration_seconds: Option<f64>,
    /// Spectral-window setup. Defaults to the VLA ppdisk tutorial frequency.
    #[serde(default)]
    pub spectral_setup: Option<SyntheticSpectralSetup>,
    /// Predict visibility samples from the model image into `MAIN.DATA`.
    #[serde(default = "default_predict_model")]
    pub predict_model: bool,
}

impl SimobserveRunTaskRequest {
    /// Build a reusable library request from the task projection.
    pub fn to_synthetic_request(&self) -> SyntheticObservationRequest {
        let antennas = if self.antennas.is_empty() {
            default_vla_antennas()
        } else {
            self.antennas.clone()
        };
        let mut request =
            SyntheticObservationRequest::vla_ppdisk(&self.model_image, &self.output_ms, antennas);
        request.overwrite = self.overwrite;
        if let Some(phase_center_rad) = self.phase_center_rad {
            request.phase_center_rad = phase_center_rad;
        }
        if let Some(start_time_mjd_seconds) = self.start_time_mjd_seconds {
            request.start_time_mjd_seconds = start_time_mjd_seconds;
        }
        if let Some(duration_seconds) = self.duration_seconds {
            request.duration_seconds = duration_seconds;
        }
        if let Some(integration_seconds) = self.integration_seconds {
            request.integration_seconds = integration_seconds;
        }
        if let Some(spectral_setup) = &self.spectral_setup {
            request.spectral_setup = spectral_setup.clone();
        }
        request.predict_model = self.predict_model;
        request
    }

    /// Execute the request and return structured output metadata.
    pub fn execute(&self) -> Result<SimobserveRunTaskResult, String> {
        let request = self.to_synthetic_request();
        let started = Instant::now();
        let report =
            generate_synthetic_observation_ms(&request).map_err(|error| error.to_string())?;
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

/// Canonical `simobserve` task request envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "request", rename_all = "snake_case")]
pub enum SimobserveTaskRequest {
    /// Execute one `simobserve` request.
    Run(SimobserveRunTaskRequest),
}

impl SimobserveTaskRequest {
    /// Execute the request and return the canonical task result envelope.
    pub fn execute(&self) -> Result<SimobserveTaskResult, String> {
        match self {
            Self::Run(request) => Ok(SimobserveTaskResult::Run(request.execute()?)),
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
    Run(SimobserveRunTaskResult),
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
        let ui_schema = serde_json::to_value(command_schema("simobserve"))
            .expect("serialize simobserve ui schema projection");
        Self {
            protocol: SimobserveProtocolInfo::current(),
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
            request_schema,
            result_schema,
        }
    }
}

/// Return the launcher/TUI compatibility schema.
pub fn command_schema(program_name: &str) -> UiCommandSchema {
    UiCommandSchema {
        schema_version: 1,
        command_id: "simobserve".to_string(),
        invocation_name: program_name.to_string(),
        display_name: "SimObserve".to_string(),
        category: "Simulation".to_string(),
        summary: "Generate a CASA-compatible synthetic VLA MeasurementSet".to_string(),
        usage: format!("{program_name} --model PATH --out PATH [options]"),
        arguments: vec![
            option_argument(OptionArgumentConfig {
                id: "model",
                label: "Model FITS",
                order: 0,
                flags: &["--model"],
                metavar: "PATH",
                value_kind: UiValueKind::Path,
                default: None,
                required: true,
                help: "Input FITS model image",
            }),
            option_argument(OptionArgumentConfig {
                id: "out",
                label: "Output MS",
                order: 1,
                flags: &["--out"],
                metavar: "PATH",
                value_kind: UiValueKind::Path,
                default: None,
                required: true,
                help: "Output MeasurementSet path",
            }),
            option_argument(OptionArgumentConfig {
                id: "duration",
                label: "Duration (s)",
                order: 2,
                flags: &["--duration"],
                metavar: "SECONDS",
                value_kind: UiValueKind::Float,
                default: Some("60"),
                required: false,
                help: "On-source duration in seconds",
            }),
            option_argument(OptionArgumentConfig {
                id: "integration",
                label: "Integration (s)",
                order: 3,
                flags: &["--integration"],
                metavar: "SECONDS",
                value_kind: UiValueKind::Float,
                default: Some("10"),
                required: false,
                help: "Integration time in seconds",
            }),
            option_argument(OptionArgumentConfig {
                id: "start_frequency_hz",
                label: "Start Frequency (Hz)",
                order: 4,
                flags: &["--start-frequency-hz"],
                metavar: "HZ",
                value_kind: UiValueKind::Float,
                default: Some("672000000000"),
                required: false,
                help: "First channel center frequency",
            }),
            option_argument(OptionArgumentConfig {
                id: "channel_width_hz",
                label: "Channel Width (Hz)",
                order: 5,
                flags: &["--channel-width-hz"],
                metavar: "HZ",
                value_kind: UiValueKind::Float,
                default: Some("1000000"),
                required: false,
                help: "Channel width",
            }),
            option_argument(OptionArgumentConfig {
                id: "channel_count",
                label: "Channels",
                order: 6,
                flags: &["--channels"],
                metavar: "N",
                value_kind: UiValueKind::String,
                default: Some("1"),
                required: false,
                help: "Number of channels",
            }),
            toggle_argument(
                "overwrite",
                "Overwrite",
                7,
                &["--overwrite"],
                &["--no-overwrite"],
                Some("false"),
                "Replace an existing output MeasurementSet",
            ),
            toggle_argument(
                "predict_model",
                "Predict Model",
                8,
                &["--predict-model"],
                &["--no-predict-model"],
                Some("true"),
                "Predict visibilities from the model image",
            ),
            action_argument(
                "help",
                "Help",
                9,
                &["-h", "--help"],
                UiActionKind::Help,
                "Render help text",
            ),
            action_argument(
                "ui_schema",
                "UI Schema",
                10,
                &["--ui-schema"],
                UiActionKind::UiSchema,
                "Emit the launcher/TUI schema",
            ),
        ],
        managed_output: None,
    }
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
        serde_json::to_string_pretty(&SimobserveTaskResult::Run(result))
            .map_err(|error| error.to_string())?
    );
    Ok(())
}

fn request_from_cli_args(args: &[std::ffi::OsString]) -> Result<SimobserveRunTaskRequest, String> {
    let model_image = required_option(args, "--model")?;
    let output_ms = required_option(args, "--out")?;
    let duration_seconds = optional_f64(args, "--duration")?;
    let integration_seconds = optional_f64(args, "--integration")?;
    let start_frequency_hz = optional_f64(args, "--start-frequency-hz")?.unwrap_or(672.0e9);
    let channel_width_hz = optional_f64(args, "--channel-width-hz")?.unwrap_or(1.0e6);
    let channel_count = optional_usize(args, "--channels")?.unwrap_or(1);
    Ok(SimobserveRunTaskRequest {
        model_image,
        output_ms,
        overwrite: has_flag(args, "--overwrite"),
        antennas: Vec::new(),
        phase_center_rad: None,
        start_time_mjd_seconds: None,
        duration_seconds,
        integration_seconds,
        spectral_setup: Some(SyntheticSpectralSetup {
            name: "band1".to_string(),
            start_frequency_hz,
            channel_width_hz,
            channel_count,
        }),
        predict_model: !has_flag(args, "--no-predict-model"),
    })
}

fn default_predict_model() -> bool {
    true
}

fn default_vla_antennas() -> Vec<SyntheticAntenna> {
    vec![
        SyntheticAntenna::vla("VLA01", "N01", [-1_601_185.4, -5_041_977.5, 3_554_875.9]),
        SyntheticAntenna::vla("VLA02", "N02", [-1_601_085.4, -5_041_977.5, 3_554_875.9]),
        SyntheticAntenna::vla("VLA03", "N03", [-1_601_185.4, -5_041_877.5, 3_554_875.9]),
    ]
}

struct OptionArgumentConfig<'a> {
    id: &'a str,
    label: &'a str,
    order: usize,
    flags: &'a [&'a str],
    metavar: &'a str,
    value_kind: UiValueKind,
    default: Option<&'a str>,
    required: bool,
    help: &'a str,
}

fn option_argument(config: OptionArgumentConfig<'_>) -> UiArgumentSchema {
    UiArgumentSchema {
        id: config.id.to_string(),
        label: config.label.to_string(),
        order: config.order,
        parser: UiArgumentParser::Option {
            flags: config
                .flags
                .iter()
                .map(|flag| (*flag).to_string())
                .collect(),
            metavar: config.metavar.to_string(),
            choices: Vec::new(),
        },
        value_kind: config.value_kind,
        required: config.required,
        default: config.default.map(str::to_string),
        help: config.help.to_string(),
        group: "Synthetic Observation".to_string(),
        advanced: false,
        hidden_in_tui: false,
    }
}

fn toggle_argument(
    id: &str,
    label: &str,
    order: usize,
    true_flags: &[&str],
    false_flags: &[&str],
    default: Option<&str>,
    help: &str,
) -> UiArgumentSchema {
    UiArgumentSchema {
        id: id.to_string(),
        label: label.to_string(),
        order,
        parser: UiArgumentParser::Toggle {
            true_flags: true_flags.iter().map(|flag| (*flag).to_string()).collect(),
            false_flags: false_flags.iter().map(|flag| (*flag).to_string()).collect(),
        },
        value_kind: UiValueKind::Bool,
        required: false,
        default: default.map(str::to_string),
        help: help.to_string(),
        group: "Synthetic Observation".to_string(),
        advanced: false,
        hidden_in_tui: false,
    }
}

fn action_argument(
    id: &str,
    label: &str,
    order: usize,
    flags: &[&str],
    action: UiActionKind,
    help: &str,
) -> UiArgumentSchema {
    UiArgumentSchema {
        id: id.to_string(),
        label: label.to_string(),
        order,
        parser: UiArgumentParser::Action {
            flags: flags.iter().map(|flag| (*flag).to_string()).collect(),
            action,
        },
        value_kind: UiValueKind::None,
        required: false,
        default: None,
        help: help.to_string(),
        group: "Machine".to_string(),
        advanced: true,
        hidden_in_tui: true,
    }
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

fn optional_usize(args: &[std::ffi::OsString], flag: &str) -> Result<Option<usize>, String> {
    option_value(args, flag)
        .map(|value| {
            value
                .parse::<usize>()
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
    use casa_provider_contracts::ProviderSurfaceKind;

    use super::{
        SIMOBSERVE_TASK_PROTOCOL_NAME, SIMOBSERVE_TASK_PROTOCOL_VERSION, SimobserveProtocolInfo,
        SimobserveTaskSchemaBundle, command_schema,
    };

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
        assert!(bundle.components.contains_key("SimobserveRunTaskRequest"));
        let ui_schema = command_schema("simobserve");
        assert_eq!(ui_schema.command_id, "simobserve");
    }

    #[test]
    fn protocol_info_matches_public_constants() {
        let info = SimobserveProtocolInfo::current();
        assert_eq!(info.protocol_name, SIMOBSERVE_TASK_PROTOCOL_NAME);
        assert_eq!(info.protocol_version, SIMOBSERVE_TASK_PROTOCOL_VERSION);
        assert_eq!(info.surface_kind, ProviderSurfaceKind::Task);
    }
}
