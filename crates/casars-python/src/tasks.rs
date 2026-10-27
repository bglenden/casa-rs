// SPDX-License-Identifier: LGPL-3.0-or-later
//! Task launch metadata and results for `casars.tasks`: the application
//! catalog's launch entries, and a successful task's declared products.

use std::path::Path;

use casa_provider_contracts::{
    RunProductKind, RunProductRole, builtin_application_catalog, builtin_surface_bundle,
};
use casa_task_runtime::{TaskOutputValue, decode_task_completion};
use pyo3::prelude::*;
use serde::Serialize;

use crate::parameters::{reason_error, to_python};

/// One application-catalog entry: its identity and how to launch it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ApplicationView {
    pub(crate) id: String,
    pub(crate) kind: String,
    pub(crate) category: String,
    pub(crate) display_name: String,
    pub(crate) executable: String,
    pub(crate) cargo_package: String,
    pub(crate) override_env: String,
    pub(crate) dataset_kinds: Vec<String>,
    pub(crate) include_in_suite: bool,
}

/// Every application in the catalog, with the surface names of its task.
pub(crate) fn applications() -> Result<Vec<ApplicationView>, String> {
    let catalog = builtin_application_catalog()?;
    catalog
        .applications
        .iter()
        .map(|application| {
            let surface = application
                .surface_bundle()
                .map_err(|reason| format!("resolve application {}: {reason}", application.id))?;
            let kind = serde_json::to_value(application.kind)
                .map_err(|error| format!("project application kind: {error}"))?
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| {
                    format!(
                        "application {} has a non-string enum projection",
                        application.id
                    )
                })?;
            Ok(ApplicationView {
                id: application.id.clone(),
                kind,
                display_name: surface
                    .as_ref()
                    .map_or("CASA-RS", |bundle| bundle.surface.display_name())
                    .to_string(),
                category: surface
                    .as_ref()
                    .map_or("Launcher", |bundle| bundle.surface.category())
                    .to_string(),
                executable: application.launch.executable.clone(),
                cargo_package: application.launch.cargo_package.clone(),
                override_env: application.launch.override_env.clone(),
                dataset_kinds: application.dataset_kinds.clone(),
                include_in_suite: application.include_in_suite,
            })
        })
        .collect()
}

/// One product a completed task declared.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ProductView {
    pub(crate) id: String,
    pub(crate) role: &'static str,
    pub(crate) resource_kind: &'static str,
    pub(crate) label: String,
    pub(crate) path: String,
    pub(crate) exists: bool,
    pub(crate) preview_path: Option<String>,
    pub(crate) preview_exists: bool,
    pub(crate) diagnostic: Option<String>,
}

/// A successful task's summary, products and diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct CompletionView {
    pub(crate) surface_id: String,
    pub(crate) summary: String,
    pub(crate) products: Vec<ProductView>,
    pub(crate) diagnostics: Vec<String>,
}

/// Decode one successful provider result into its contract-declared
/// products. `values` are the task's parameters as display text.
pub(crate) fn completion(
    surface_id: &str,
    stdout: &str,
    workspace: &Path,
    values: Vec<(String, String)>,
) -> Result<CompletionView, String> {
    let bundle = builtin_surface_bundle(surface_id)?;
    let values = values
        .into_iter()
        .map(|(name, value)| TaskOutputValue { name, value })
        .collect::<Vec<_>>();
    let completion = decode_task_completion(&bundle, stdout, workspace, &values)
        .map_err(|error| error.to_string())?;
    let products = completion
        .products
        .into_iter()
        .map(|product| ProductView {
            id: product.id,
            role: match product.role {
                RunProductRole::Primary => "PRIMARY",
                RunProductRole::Auxiliary => "AUXILIARY",
                RunProductRole::Preview => "PREVIEW",
            },
            resource_kind: match product.resource_kind {
                RunProductKind::MeasurementSet => "MEASUREMENT_SET",
                RunProductKind::CasaImage => "CASA_IMAGE",
                RunProductKind::CasaTable => "CASA_TABLE",
                RunProductKind::FitsImage => "FITS_IMAGE",
                RunProductKind::File => "FILE",
            },
            label: product.label,
            path: product.path.to_string_lossy().into_owned(),
            exists: product.exists,
            preview_path: product
                .preview_path
                .map(|path| path.to_string_lossy().into_owned()),
            preview_exists: product.preview_exists,
            diagnostic: product.diagnostic,
        })
        .collect();
    Ok(CompletionView {
        surface_id: completion.surface_id,
        summary: completion.summary,
        products,
        diagnostics: completion.diagnostics,
    })
}

/// The application catalog's launch entries.
#[pyfunction]
pub(crate) fn application_catalog(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    let applications = applications().map_err(|reason| reason_error(py, reason))?;
    to_python(py, &applications)
}

/// A successful task's products. Releases the GIL while it checks them.
#[pyfunction]
pub(crate) fn task_completion<'py>(
    py: Python<'py>,
    surface_id: &str,
    stdout: &str,
    workspace: std::path::PathBuf,
    values: Vec<(String, String)>,
) -> PyResult<Bound<'py, PyAny>> {
    let completion = py
        .detach(|| completion(surface_id, stdout, &workspace, values))
        .map_err(|reason| reason_error(py, reason))?;
    to_python(py, &completion)
}

#[cfg(test)]
#[path = "tasks_tests.rs"]
mod tests;
