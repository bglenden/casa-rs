// SPDX-License-Identifier: LGPL-3.0-or-later
//! Canonical one-shot provider contracts for native MeasurementSet tasks.

use std::path::PathBuf;

use casa_provider_contracts::{
    NoAdditionalProviderSchemas, ProviderCliMachineActions, ProviderCliProjection,
    ProviderProjectionMetadata, ProviderProtocolDescriptor, ProviderSurfaceKind,
    TaskOperationDescriptor, TaskProviderContract, TaskProviderSchemas, TaskSemanticContract,
    builtin_surface_bundle, merged_components,
};
use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};

use crate::{FlagMerge, FlagVersionEntry};

/// JSON request accepted by the native `mstransform` provider.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MsTransformTaskRequest {
    /// Input MeasurementSet.
    pub vis: PathBuf,
    /// Output MeasurementSet.
    pub outputvis: PathBuf,
    /// CASA spectral-window selector.
    #[serde(default)]
    pub spw: String,
    /// Adjacent channels to average.
    #[serde(default = "default_width")]
    pub width: usize,
    /// CASA field selector.
    #[serde(default)]
    pub field: String,
    /// CASA scan selector.
    #[serde(default)]
    pub scan: String,
    /// CASA antenna selector.
    #[serde(default)]
    pub antenna: String,
    /// MJD-seconds range in `start~end` form.
    #[serde(default)]
    pub timerange: String,
    /// Additional TaQL row selection.
    #[serde(default)]
    pub msselect: String,
    /// Input visibility column.
    #[serde(default = "default_corrected_data")]
    pub datacolumn: String,
    /// Preserve fully flagged rows.
    #[serde(default = "default_true")]
    pub keepflags: bool,
}

/// JSON request accepted by the native `flagdata` provider.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FlagDataTaskRequest {
    /// Input MeasurementSet.
    pub vis: PathBuf,
    /// Flagging mode.
    #[serde(default = "default_manual")]
    pub mode: String,
    /// CASA spectral-window selector.
    #[serde(default)]
    pub spw: String,
    /// CASA field selector.
    #[serde(default)]
    pub field: String,
    /// CASA scan selector.
    #[serde(default)]
    pub scan: String,
    /// CASA antenna selector.
    #[serde(default)]
    pub antenna: String,
    /// Visibility column used by automatic modes.
    #[serde(default = "default_data")]
    pub datacolumn: String,
    /// Manual flagging action.
    #[serde(default = "default_flag")]
    pub action: String,
    /// Save the current flags before mutation.
    #[serde(default = "default_true")]
    pub flagbackup: bool,
    /// Clip exact zeros.
    #[serde(default)]
    pub clipzeros: bool,
    /// Quack interval in seconds.
    #[serde(default)]
    pub quackinterval: f64,
    /// Quack scan edge.
    #[serde(default = "default_beg")]
    pub quackmode: String,
    /// TFCrop time cutoff.
    #[serde(default = "default_timecutoff")]
    pub timecutoff: f64,
    /// TFCrop frequency cutoff.
    #[serde(default = "default_freqcutoff")]
    pub freqcutoff: f64,
    /// Optional RFlag time threshold.
    #[serde(default)]
    pub timedev: Option<f64>,
    /// Optional RFlag spectral threshold.
    #[serde(default)]
    pub freqdev: Option<f64>,
    /// RFlag time threshold scale.
    #[serde(default = "default_rflag_scale")]
    pub timedevscale: f64,
    /// RFlag spectral threshold scale.
    #[serde(default = "default_rflag_scale")]
    pub freqdevscale: f64,
    /// RFlag maximum spectral standard deviation.
    #[serde(default = "default_spectralmax")]
    pub spectralmax: f64,
    /// RFlag minimum spectral standard deviation.
    #[serde(default)]
    pub spectralmin: f64,
    /// Run automatic flag extension.
    #[serde(default = "default_true")]
    pub extendflags: bool,
    /// Extend flags across correlations.
    #[serde(default)]
    pub extendpols: bool,
    /// Flag time columns above this percentage.
    #[serde(default)]
    pub growtime: f64,
    /// Flag spectra above this percentage.
    #[serde(default)]
    pub growfreq: f64,
}

/// JSON request accepted by the native `flagmanager` provider.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FlagManagerTaskRequest {
    /// Input MeasurementSet.
    pub vis: PathBuf,
    /// Version operation.
    #[serde(default = "default_list")]
    pub mode: String,
    /// Destination version name.
    #[serde(default)]
    pub versionname: Option<String>,
    /// Source version name for rename.
    #[serde(default)]
    pub oldname: Option<String>,
    /// Version comment.
    #[serde(default)]
    pub comment: Option<String>,
    /// Restore/save merge behavior.
    #[serde(default)]
    pub merge: FlagMerge,
}

/// JSON result returned by the native `flagmanager` provider.
#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum FlagManagerTaskResult {
    /// Existing flag versions returned by list mode.
    Versions(Vec<FlagVersionEntry>),
    /// Mutation acknowledgement.
    Mutation(FlagManagerMutationResult),
}

/// Mutation acknowledgement returned by `flagmanager`.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct FlagManagerMutationResult {
    /// Completed operation.
    pub mode: String,
    /// Destination version, when applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub versionname: Option<String>,
    /// Source version, for rename.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldname: Option<String>,
}

/// Canonical provider bundle for `mstransform` and `split`.
pub fn mstransform_task_schema_bundle() -> TaskProviderContract {
    one_shot_contract::<MsTransformTaskRequest, crate::MsTransformReport>(
        "casa_ms_transform_task",
        &["mstransform", "split"],
    )
}

/// Canonical provider bundle for `flagdata`.
pub fn flagdata_task_schema_bundle() -> TaskProviderContract {
    one_shot_contract::<FlagDataTaskRequest, crate::FlagDataReport>(
        "casa_ms_flagdata_task",
        &["flagdata"],
    )
}

/// Canonical provider bundle for `flagmanager`.
pub fn flagmanager_task_schema_bundle() -> TaskProviderContract {
    one_shot_contract::<FlagManagerTaskRequest, FlagManagerTaskResult>(
        "casa_ms_flagmanager_task",
        &["flagmanager"],
    )
}

fn one_shot_contract<R: JsonSchema, O: JsonSchema>(
    protocol_name: &str,
    surface_ids: &[&str],
) -> TaskProviderContract {
    let request_schema = schema_for!(R);
    let result_schema = schema_for!(O);
    TaskProviderContract {
        protocol: ProviderProtocolDescriptor::new(
            protocol_name,
            1,
            ProviderSurfaceKind::Task,
            env!("CARGO_PKG_VERSION"),
        ),
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
        parameter_surfaces: surface_ids
            .iter()
            .map(|surface| {
                builtin_surface_bundle(surface)
                    .unwrap_or_else(|error| panic!("built-in task surface {surface:?}: {error}"))
            })
            .collect(),
        domain_schemas: TaskProviderSchemas {
            request_schema,
            result_schema,
            additional: NoAdditionalProviderSchemas {},
        },
    }
}

const fn default_width() -> usize {
    1
}

fn default_corrected_data() -> String {
    "corrected".to_string()
}

const fn default_true() -> bool {
    true
}

fn default_manual() -> String {
    "manual".to_string()
}

fn default_data() -> String {
    "data".to_string()
}

fn default_flag() -> String {
    "flag".to_string()
}

fn default_beg() -> String {
    "beg".to_string()
}

const fn default_timecutoff() -> f64 {
    4.0
}

const fn default_freqcutoff() -> f64 {
    3.0
}

const fn default_rflag_scale() -> f64 {
    5.0
}

const fn default_spectralmax() -> f64 {
    1.0e6
}

fn default_list() -> String {
    "list".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_ms_task_contracts_use_the_common_envelope() {
        for bundle in [
            mstransform_task_schema_bundle(),
            flagdata_task_schema_bundle(),
            flagmanager_task_schema_bundle(),
        ] {
            bundle.validate().expect("valid native MS task provider");
        }
    }
}
