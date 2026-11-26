// SPDX-License-Identifier: LGPL-3.0-or-later

mod convert;
mod native_tasks;
mod object_contract;
mod parameters;
mod plot;
mod tasks;
mod tools;

use std::path::PathBuf;

use casa_images::ImageError;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::object_contract::{DataObjectProtocolInfo, DataObjectSchemaBundle};

fn image_err(error: ImageError) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

#[pyfunction]
fn linearmosaic(
    py: Python<'_>,
    pointings: Vec<(PathBuf, PathBuf)>,
    template: PathBuf,
    output: PathBuf,
    output_weight: PathBuf,
) -> PyResult<()> {
    py.detach(move || casa_images::linearmosaic(&pointings, &template, &output, &output_weight))
        .map_err(image_err)
}

#[pyfunction]
fn data_protocol_info_json() -> PyResult<String> {
    serde_json::to_string_pretty(&DataObjectProtocolInfo::current())
        .map_err(|error| PyRuntimeError::new_err(format!("serialize data protocol info: {error}")))
}

#[pyfunction]
fn data_schema_bundle_json() -> PyResult<String> {
    DataObjectSchemaBundle::current()
        .to_json_string()
        .map_err(|error| PyRuntimeError::new_err(format!("serialize data schema bundle: {error}")))
}

/// The CASA tools and the native parameter, plot, data and task functions.
#[pymodule]
fn _core(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<tools::simulator::PySimulator>()?;
    module.add_class::<tools::measures::PyMeasures>()?;
    module.add_class::<tools::quanta::PyQuanta>()?;
    module.add_class::<tools::componentlist::PyComponentList>()?;
    module.add_class::<tools::image::PyImageTool>()?;
    module.add_class::<tools::coordsys::PyCoordsys>()?;
    module.add_class::<tools::table::PyTableTool>()?;
    module.add_class::<tools::table::PyTableColIter>()?;
    module.add_class::<tools::table::PyTableRowTool>()?;
    module.add_class::<tools::msmetadata::PyMsMetadata>()?;
    module.add_class::<tools::ms::PyMsTool>()?;
    module.add_class::<tools::calibrater::PyCalibrater>()?;
    module.add_class::<tools::calanalysis::PyCalAnalysis>()?;
    module.add_class::<tools::atmosphere::PyAtmosphere>()?;
    module.add_class::<tools::agentflagger::PyAgentFlagger>()?;
    module.add_class::<tools::imager::PyImager>()?;
    module.add_class::<tools::simutil::PySimutil>()?;
    module.add_class::<tools::wvr::PyWvr>()?;
    module.add_class::<tools::singledishms::PySingleDishMs>()?;
    module.add_function(wrap_pyfunction!(linearmosaic, module)?)?;
    module.add_function(wrap_pyfunction!(data_protocol_info_json, module)?)?;
    module.add_function(wrap_pyfunction!(data_schema_bundle_json, module)?)?;
    module.add_function(wrap_pyfunction!(parameters::parameter_catalog, module)?)?;
    module.add_function(wrap_pyfunction!(
        parameters::parameter_surface_bundle,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        parameters::parameter_profile_surface,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(
        parameters::parameter_template_toml,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(parameters::parameter_last, module)?)?;
    module.add_function(wrap_pyfunction!(parameters::parameter_resolve, module)?)?;
    module.add_function(wrap_pyfunction!(parameters::parameter_render_toml, module)?)?;
    module.add_function(wrap_pyfunction!(
        parameters::parameter_provider_invocation,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(parameters::parameter_save, module)?)?;
    module.add_function(wrap_pyfunction!(
        parameters::parameter_write_managed,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(tasks::application_catalog, module)?)?;
    module.add_function(wrap_pyfunction!(native_tasks::run_native_task, module)?)?;
    module.add_function(wrap_pyfunction!(tasks::task_executable, module)?)?;
    module.add_function(wrap_pyfunction!(tasks::prepare_task_table_write, module)?)?;
    module.add_function(wrap_pyfunction!(tasks::task_outcome, module)?)?;
    module.add_function(wrap_pyfunction!(tasks::task_plot_documents, module)?)?;
    module.add_function(wrap_pyfunction!(plot::build_measurement_set_plot, module)?)?;
    Ok(())
}
