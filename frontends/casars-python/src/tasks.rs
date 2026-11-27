// SPDX-License-Identifier: LGPL-3.0-or-later
//! Product projection, plot validation and table reservations for Python tasks.

use std::path::{Path, PathBuf};

use casa_contracts::{RunProductKind, RunProductRole, builtin_surface_bundle};
use casa_tasks::{TaskOutputValue, decode_task_completion};
use pyo3::prelude::*;
use serde::Serialize;

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

/// Yield caller-owned automatic locks and keep local threads from taking
/// data locks until the clearcal provider returns.
#[pyfunction]
pub(crate) fn prepare_task_table_write(py: Python<'_>, path: PathBuf) -> PyResult<TaskTableWrite> {
    #[cfg(unix)]
    {
        let reservations = py.detach(|| {
            let targets = clearcal_lock_targets(&path)?;
            Ok::<_, PyErr>(crate::tools::table::prepare_provider_write(&targets)?.into())
        })?;
        Ok(TaskTableWrite {
            reservations: Some(reservations),
        })
    }
    #[cfg(not(unix))]
    {
        let _ = (py, path);
        Ok(TaskTableWrite {})
    }
}

#[pyclass]
pub(crate) struct TaskTableWrite {
    #[cfg(unix)]
    pub(crate) reservations: Option<std::sync::Arc<[casa_tables::DelegatedTableWrite]>>,
}

#[pymethods]
impl TaskTableWrite {
    fn release(&mut self, py: Python<'_>) {
        #[cfg(unix)]
        {
            let reservations = std::mem::take(&mut self.reservations);
            py.detach(|| drop(reservations));
        }
        #[cfg(not(unix))]
        let _ = py;
    }
}

#[cfg(unix)]
fn clearcal_lock_targets(path: &Path) -> PyResult<Vec<PathBuf>> {
    use casa_tables::{Table, TableInfo};
    use pyo3::exceptions::PyRuntimeError;

    let concatenated = std::fs::read_to_string(path.join("table.info"))
        .is_ok_and(|text| TableInfo::parse(&text).sub_type == "CONCATENATED");
    let partitions = if concatenated {
        Table::concatenated_source_paths(path)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?
            .unwrap_or_default()
    } else {
        vec![path.to_path_buf()]
    };
    let mut targets = std::collections::BTreeSet::new();
    for table in std::iter::once(path.to_path_buf()).chain(
        partitions
            .iter()
            .flat_map(|part| [part.clone(), part.join("HISTORY"), part.join("SOURCE")]),
    ) {
        if table.join("table.dat").is_file() {
            targets.insert(table.canonicalize().map_err(|error| {
                PyRuntimeError::new_err(format!("{}: {error}", table.display()))
            })?);
        }
    }
    Ok(targets.into_iter().collect())
}

pub(crate) fn decode_plot_documents(
    surface_id: &str,
    result: serde_json::Value,
    payload_bytes: u64,
) -> Result<Vec<casa_plot_data::plot_document::PlotFigure>, String> {
    #[derive(serde::Deserialize)]
    struct PlotReply {
        #[serde(default)]
        plots: Vec<casa_plot_data::plot_document::PlotFigure>,
        #[serde(default)]
        artifacts: Vec<casa_tasks::simulation::SimobserveArtifact>,
    }
    let reply: PlotReply =
        serde_json::from_value(result).map_err(|e| format!("{surface_id} plot reply: {e}"))?;
    for plot in &reply.plots {
        plot.document
            .validate_payload(payload_bytes)
            .map_err(|e| e.to_string())?;
        if surface_id != "plotms"
            && let Some(path) = &plot.output_file
            && reply
                .artifacts
                .iter()
                .filter(|a| {
                    a.render_pending
                        && a.resource_kind == RunProductKind::File
                        && Path::new(&a.path) == path
                })
                .count()
                != 1
        {
            return Err("a simulation figure needs one declared pending file product".into());
        }
    }
    for artifact in reply.artifacts.iter().filter(|a| a.render_pending) {
        let figures = reply
            .plots
            .iter()
            .filter(|plot| plot.output_file.as_deref() == Some(Path::new(&artifact.path)))
            .count();
        if artifact.resource_kind != RunProductKind::File || figures != 1 {
            return Err("a pending plot artifact needs exactly one matching figure".into());
        }
    }
    Ok(reply.plots)
}

#[cfg(test)]
#[path = "tasks_tests.rs"]
mod tests;
