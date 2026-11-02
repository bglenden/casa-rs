// SPDX-License-Identifier: LGPL-3.0-or-later
//! msconvert's task contract over the shared provider envelope: the
//! catalog's `msconvert` parameters in, the conversion's report out.

use std::path::PathBuf;

use casa_provider_contracts::{
    NoAdditionalProviderSchemas, ProviderCliMachineActions, ProviderCliProjection,
    ProviderProjectionMetadata, ProviderProtocolDescriptor, ProviderSurfaceKind,
    TaskOperationDescriptor, TaskProviderContract, TaskProviderSchemas, TaskSemanticContract,
    builtin_surface_bundle, merged_components,
};
use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};

/// Stable protocol name advertised by `msconvert --protocol-info`.
pub const MSCONVERT_TASK_PROTOCOL_NAME: &str = "casars_msconvert_task";
/// Protocol version advertised by `msconvert --protocol-info`.
pub const MSCONVERT_TASK_PROTOCOL_VERSION: u32 = 1;

/// msconvert's protocol descriptor.
pub fn msconvert_protocol_descriptor() -> ProviderProtocolDescriptor {
    ProviderProtocolDescriptor::new(
        MSCONVERT_TASK_PROTOCOL_NAME,
        MSCONVERT_TASK_PROTOCOL_VERSION,
        ProviderSurfaceKind::Task,
        env!("CARGO_PKG_VERSION"),
    )
}

/// msconvert's schema bundle in the shared envelope; the catalog's
/// `msconvert` surface defines the request's parameters.
pub fn msconvert_task_schema_bundle() -> TaskProviderContract {
    let request_schema = schema_for!(MsconvertTaskRequest);
    let result_schema = schema_for!(MsconvertTaskResult);
    TaskProviderContract {
        protocol: msconvert_protocol_descriptor(),
        semantic: TaskSemanticContract {
            request_schema: request_schema.clone(),
            result_schema: result_schema.clone(),
            operations: vec![TaskOperationDescriptor {
                name: "run".to_string(),
                request_kind: "run".to_string(),
                result_kind: Some("run".to_string()),
            }],
            front_end: None,
        },
        components: merged_components([&request_schema, &result_schema]),
        annotations: serde_json::json!({}),
        projections: ProviderProjectionMetadata {
            cli: Some(ProviderCliProjection {
                machine_actions: ProviderCliMachineActions {
                    json_schema: Some("--json-schema".to_string()),
                    protocol_info: Some("--protocol-info".to_string()),
                    json_run: Some("--json-run <SOURCE>".to_string()),
                },
            }),
            python: None,
        },
        parameter_surfaces: vec![
            builtin_surface_bundle("msconvert")
                .expect("built-in msconvert parameter surface must remain valid"),
        ],
        domain_schemas: TaskProviderSchemas {
            request_schema,
            result_schema,
            additional: NoAdditionalProviderSchemas {},
        },
    }
}

/// msconvert's request: the catalog's parameters of one run, as the front
/// end resolves them over their defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MsconvertTaskRequest {
    /// The store to convert: a MeasurementSet v2 or an MSv4 processing set.
    pub vis: PathBuf,
    /// The new store; empty for one beside `vis`, named after it as XRADIO
    /// names stores: `.ps.zarr` for an MSv4, `.ms` for an MSv2. An MSv4
    /// output not ending in `ps.zarr` gains `.ps.zarr`, as XRADIO's
    /// converter names it.
    #[serde(default)]
    pub outputvis: PathBuf,
    /// `msv2` or `msv4`; empty for the other format than `vis`.
    #[serde(default)]
    pub outformat: String,
    /// The data groups to convert, the first carrying flags, weights and
    /// UVW; empty for every group, `base` first.
    #[serde(default)]
    pub datagroups: Vec<String>,
    /// MSv2 into MSv4: further keys the MSv2 is partitioned by, XRADIO's
    /// `partition_scheme`: `FIELD_ID`, `SCAN_NUMBER`.
    #[serde(default)]
    pub partitionscheme: Vec<String>,
    /// MSv4 output: convert the pointing, XRADIO's `with_pointing`.
    #[serde(default = "default_true")]
    pub withpointing: bool,
    /// MSv2 output: what a padded grid cell becomes, `flagged` or
    /// `omitted`.
    #[serde(default = "default_padding")]
    pub padding: String,
    /// MSv4 output: the chunk shape `[time, baseline, frequency,
    /// polarization]`, 0 for a whole axis; empty for XRADIO's default.
    #[serde(default)]
    pub chunks: Vec<i64>,
    /// MSv4 output: the payload codec, `blosc`, `zstd` or `none`.
    #[serde(default = "default_codec")]
    pub codec: String,
    /// MSv4 output: partitions converted at once; 0 for as many as the
    /// cores and the memory allow.
    #[serde(default)]
    pub nthreads: i64,
    /// Replace an existing MeasurementSet at `outputvis`.
    #[serde(default)]
    pub overwrite: bool,
}

fn default_true() -> bool {
    true
}

fn default_padding() -> String {
    "flagged".to_string()
}

fn default_codec() -> String {
    "blosc".to_string()
}

/// What one conversion did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MsconvertTaskResult {
    /// The store converted.
    pub vis: String,
    /// The store written.
    pub outputvis: String,
    /// The format of `vis`: `msv2` or `msv4`.
    pub informat: String,
    /// The format of `outputvis`: `msv2` or `msv4`.
    pub outformat: String,
    /// MSv4 output: the chunk shape asked of every dataset, each axis then
    /// clipped to the dataset's extent (0 for a whole axis); `None` for
    /// XRADIO's, sized for each dataset.
    pub chunks: Option<[u64; 4]>,
    /// What the output does not carry or states by default, as sentences.
    pub losses: Vec<String>,
    /// Partitions converted (MSv4 datasets).
    pub partitions: usize,
    /// Samples read, padding included.
    pub samples: usize,
    /// MSv2 output: rows written.
    pub rows: usize,
    /// MSv4 output: payload elements encoded.
    pub elements: usize,
    /// Wall time of the conversion.
    pub elapsed_ns: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_schema_bundle_carries_the_msconvert_surface() {
        let bundle = msconvert_task_schema_bundle();
        assert_eq!(bundle.protocol.protocol_name, MSCONVERT_TASK_PROTOCOL_NAME);
        assert_eq!(bundle.parameter_surfaces.len(), 1);
        assert_eq!(bundle.parameter_surfaces[0].surface.id(), "msconvert");
        bundle.validate().expect("valid msconvert provider");
    }
}
