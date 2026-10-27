// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_provider_contracts::{
    ManagedResultDecoder, RunProductDescriptor, RunProductKind, RunProductRole, RunProductSource,
    SurfaceContractBundle,
};
use serde::Deserialize;
use thiserror::Error;

/// One resolved scalar value made available to product projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskOutputValue {
    pub name: String,
    pub value: String,
}

/// A declared product from a successfully completed task.
///
/// Dataset metadata is intentionally absent. A domain probe is the only valid
/// source of that metadata, and consumers retain this reference with a
/// diagnostic when probing fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunProductReference {
    pub id: String,
    pub role: RunProductRole,
    pub resource_kind: RunProductKind,
    pub label: String,
    pub path: PathBuf,
    pub exists: bool,
    pub preview_path: Option<PathBuf>,
    pub preview_exists: bool,
    pub diagnostic: Option<String>,
}

/// Rust-decoded successful task completion projected to application surfaces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskCompletion {
    pub surface_id: String,
    pub summary: String,
    pub products: Vec<RunProductReference>,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TaskCompletionError {
    #[error("unknown managed result for {surface:?}: {detail}")]
    MalformedResult { surface: String, detail: String },
    #[error("surface {surface:?} declares decoded products without a managed decoder")]
    MissingDecoder { surface: String },
    #[error("surface {surface:?} is missing required product value {parameter:?}")]
    MissingProductValue { surface: String, parameter: String },
}

/// Decode a successful provider result and project only contract-declared products.
///
/// The caller supplies resolved parameter strings because several providers
/// report success without repeating their output path in stdout. The canonical
/// product descriptor selects exact parameter names; arbitrary-key searches are
/// never performed.
pub fn decode_task_completion(
    bundle: &SurfaceContractBundle,
    stdout: &str,
    workspace: &Path,
    values: &[TaskOutputValue],
) -> Result<TaskCompletion, TaskCompletionError> {
    let decoded = bundle
        .surface
        .execution()
        .managed_output
        .as_ref()
        .map(|managed| decode_managed_result(bundle.surface.id(), managed.decoder, stdout))
        .transpose()?;
    let values = values
        .iter()
        .map(|entry| (entry.name.as_str(), entry.value.as_str()))
        .collect::<BTreeMap<_, _>>();
    let mut products = Vec::new();
    for descriptor in bundle.surface.execution().products.descriptors() {
        match &descriptor.source {
            RunProductSource::Parameter { parameter } => {
                let value = values
                    .get(parameter.as_str())
                    .map(|value| value.trim())
                    .filter(|value| !value.is_empty() && *value != "none");
                let Some(value) = value else {
                    if descriptor.optional {
                        continue;
                    }
                    return Err(TaskCompletionError::MissingProductValue {
                        surface: bundle.surface.id().to_string(),
                        parameter: parameter.clone(),
                    });
                };
                products.push(product_from_parameter(descriptor, value, workspace));
            }
            RunProductSource::DecodedArtifacts => {
                let Some(artifacts) = decoded.as_ref().and_then(DecodedManagedResult::artifacts)
                else {
                    return Err(TaskCompletionError::MissingDecoder {
                        surface: bundle.surface.id().to_string(),
                    });
                };
                products.extend(artifacts.iter().map(|artifact| {
                    product_from_artifact(bundle.surface.id(), descriptor, artifact, workspace)
                }));
            }
        }
    }
    let (summary, diagnostics) = decoded.map_or_else(
        || (format!("{} completed", bundle.surface.id()), Vec::new()),
        DecodedManagedResult::summary_and_diagnostics,
    );
    Ok(TaskCompletion {
        surface_id: bundle.surface.id().to_string(),
        summary,
        products,
        diagnostics,
    })
}

fn product_from_parameter(
    descriptor: &RunProductDescriptor,
    value: &str,
    workspace: &Path,
) -> RunProductReference {
    let path = resolve_product_path(workspace, value);
    let exists = path.exists();
    RunProductReference {
        id: descriptor.id.clone(),
        role: descriptor.role,
        resource_kind: descriptor.resource_kind,
        label: path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(&descriptor.id)
            .to_string(),
        path: path.clone(),
        exists,
        preview_path: None,
        preview_exists: false,
        diagnostic: (!exists).then(|| {
            format!(
                "declared product {} does not exist after successful completion",
                path.display()
            )
        }),
    }
}

/// A product a managed result names; its own resource kind, when it gives
/// one, over the descriptor's.
fn product_from_artifact(
    surface: &str,
    descriptor: &RunProductDescriptor,
    artifact: &ArtifactWire,
    workspace: &Path,
) -> RunProductReference {
    let path = resolve_product_path(workspace, &artifact.path);
    let exists = artifact.exists && path.exists();
    RunProductReference {
        id: format!("{}:{}", descriptor.id, artifact.kind),
        role: descriptor.role,
        resource_kind: artifact.resource_kind.unwrap_or(descriptor.resource_kind),
        label: artifact.label.clone(),
        path: path.clone(),
        exists,
        preview_path: None,
        preview_exists: false,
        diagnostic: (!exists).then(|| {
            format!(
                "{surface} declared product {} but an exact path check did not find it",
                path.display()
            )
        }),
    }
}

fn resolve_product_path(workspace: &Path, value: &str) -> PathBuf {
    let path = Path::new(value);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        workspace.join(path)
    }
}

enum DecodedManagedResult {
    Calibration(CalibrationResultWire),
    Imager(ImagerOutputWire),
    Simobserve(SimobserveOutputWire),
}

impl DecodedManagedResult {
    /// The products the result names, for a decoder that names them.
    fn artifacts(&self) -> Option<&[ArtifactWire]> {
        match self {
            Self::Calibration(_) => None,
            Self::Imager(result) => Some(&result.artifacts),
            Self::Simobserve(result) => Some(&result.artifacts),
        }
    }

    fn summary_and_diagnostics(self) -> (String, Vec<String>) {
        match self {
            Self::Calibration(result) => {
                (format!("Calibration result: {}", result.kind), Vec::new())
            }
            Self::Imager(result) => (
                format!(
                    "{} gridded samples, {} major cycles, {} minor iterations",
                    result.run.gridded_samples,
                    result.run.major_cycles,
                    result.run.minor_iterations
                ),
                result.run.warnings,
            ),
            Self::Simobserve(result) => (
                format!("{} products of {}", result.artifacts.len(), result.run.stem),
                result.run.warnings,
            ),
        }
    }
}

fn decode_managed_result(
    surface: &str,
    decoder: ManagedResultDecoder,
    stdout: &str,
) -> Result<DecodedManagedResult, TaskCompletionError> {
    let malformed = |error: serde_json::Error| TaskCompletionError::MalformedResult {
        surface: surface.to_string(),
        detail: error.to_string(),
    };
    match decoder {
        ManagedResultDecoder::CalibrationReportV1 => serde_json::from_str(stdout)
            .map(DecodedManagedResult::Calibration)
            .map_err(malformed),
        ManagedResultDecoder::ImagerRunV1 => serde_json::from_str(stdout)
            .map(DecodedManagedResult::Imager)
            .map_err(malformed),
        ManagedResultDecoder::SimobserveRunV1 => serde_json::from_str(stdout)
            .map(DecodedManagedResult::Simobserve)
            .map_err(malformed),
    }
}

#[derive(Deserialize)]
struct CalibrationResultWire {
    kind: String,
}

#[derive(Deserialize)]
struct ImagerOutputWire {
    run: ImagerRunWire,
    artifacts: Vec<ArtifactWire>,
}

#[derive(Deserialize)]
struct SimobserveOutputWire {
    run: SimobserveRunWire,
    artifacts: Vec<ArtifactWire>,
}

#[derive(Deserialize)]
struct SimobserveRunWire {
    stem: String,
    #[serde(default)]
    warnings: Vec<String>,
}

#[derive(Deserialize)]
struct ImagerRunWire {
    #[serde(default)]
    warnings: Vec<String>,
    gridded_samples: u64,
    major_cycles: u64,
    minor_iterations: u64,
}

/// One product a managed result names.
#[derive(Deserialize)]
struct ArtifactWire {
    kind: String,
    label: String,
    path: String,
    exists: bool,
    /// Its kind when it differs from the descriptor's (simobserve writes
    /// images, tables and files beside its MeasurementSets).
    #[serde(default)]
    resource_kind: Option<RunProductKind>,
}

#[cfg(test)]
mod tests {
    use std::fs;

    use casa_provider_contracts::builtin_surface_bundle;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn imager_result_is_typed_and_missing_products_remain_references() {
        let root = tempdir().unwrap();
        let image = root.path().join("target.image");
        fs::create_dir(&image).unwrap();
        let stdout = serde_json::json!({
            "request": {},
            "run": {
                "warnings": ["science warning"],
                "gridded_samples": 42,
                "major_cycles": 2,
                "minor_iterations": 3
            },
            "artifacts": [
                {
                    "kind": "image",
                    "label": "Image",
                    "path": "target.image",
                    "exists": true
                },
                {
                    "kind": "model",
                    "label": "Model",
                    "path": "missing.model",
                    "exists": true
                }
            ]
        })
        .to_string();
        let completion = decode_task_completion(
            &builtin_surface_bundle("imager").unwrap(),
            &stdout,
            root.path(),
            &[],
        )
        .unwrap();
        assert_eq!(completion.products.len(), 2);
        assert!(completion.products[0].exists);
        assert!(!completion.products[1].exists);
        assert!(completion.products[1].diagnostic.is_some());
        assert_eq!(completion.diagnostics, ["science warning"]);
    }

    #[test]
    fn simobserve_products_keep_their_own_kinds() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join("p/p.alma.out01.ms")).unwrap();
        fs::write(root.path().join("p/p.alma.out01.ptg.txt"), "").unwrap();
        let stdout = serde_json::json!({
            "request": {},
            "run": {"stem": "p.alma.out01", "elapsed_ns": 1},
            "artifacts": [
                {"kind": "ms", "label": "MeasurementSet", "path": "p/p.alma.out01.ms",
                 "exists": true, "resource_kind": "measurement_set"},
                {"kind": "ptg.txt", "label": "Pointings", "path": "p/p.alma.out01.ptg.txt",
                 "exists": true, "resource_kind": "file"}
            ]
        })
        .to_string();
        let completion = decode_task_completion(
            &builtin_surface_bundle("simobserve").unwrap(),
            &stdout,
            root.path(),
            &[],
        )
        .unwrap();
        assert_eq!(completion.summary, "2 products of p.alma.out01");
        let kinds: Vec<_> = completion
            .products
            .iter()
            .map(|product| (product.resource_kind, product.exists))
            .collect();
        assert_eq!(
            kinds,
            [
                (RunProductKind::MeasurementSet, true),
                (RunProductKind::File, true)
            ]
        );
    }

    #[test]
    fn malformed_managed_result_is_not_silently_discarded() {
        let error = decode_task_completion(
            &builtin_surface_bundle("imager").unwrap(),
            "{not-json}",
            Path::new("."),
            &[],
        )
        .unwrap_err();
        assert!(matches!(error, TaskCompletionError::MalformedResult { .. }));
    }

    #[test]
    fn exact_parameter_binding_replaces_arbitrary_result_key_search() {
        let root = tempdir().unwrap();
        let fits = root.path().join("target.fits");
        fs::write(&fits, b"fits").unwrap();
        let completion = decode_task_completion(
            &builtin_surface_bundle("exportfits").unwrap(),
            "arbitrary human output",
            root.path(),
            &[TaskOutputValue {
                name: "fitsimage".into(),
                value: "target.fits".into(),
            }],
        )
        .unwrap();
        assert_eq!(completion.products.len(), 1);
        assert_eq!(completion.products[0].path, fits);
        assert!(completion.products[0].exists);
    }

    #[test]
    fn absent_optional_product_parameter_does_not_fabricate_a_product() {
        let completion = decode_task_completion(
            &builtin_surface_bundle("gaincal").unwrap(),
            r#"{"kind":"solve_gain","report":{}}"#,
            Path::new("."),
            &[TaskOutputValue {
                name: "output".into(),
                value: "none".into(),
            }],
        )
        .unwrap();

        assert!(completion.products.is_empty());
    }

    #[test]
    fn msexplore_stdout_is_left_in_the_format_the_user_chose() {
        let root = tempdir().unwrap();
        fs::write(root.path().join("preview.json"), b"{}").unwrap();
        for stdout in ["MeasurementSet Name: target.ms\n", ""] {
            let completion = decode_task_completion(
                &builtin_surface_bundle("msexplore").unwrap(),
                stdout,
                root.path(),
                &[TaskOutputValue {
                    name: "flag_output".into(),
                    value: "preview.json".into(),
                }],
            )
            .unwrap();

            assert_eq!(completion.summary, "msexplore completed");
            assert_eq!(completion.products.len(), 1);
            assert_eq!(completion.products[0].resource_kind, RunProductKind::File);
            assert!(completion.products[0].exists);
        }
    }
}
