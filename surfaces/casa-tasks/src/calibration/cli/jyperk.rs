// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA tasks that produce a Jy/K gain table.

use std::path::PathBuf;

use casa_contracts::CasaValue;
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

use crate::calibration::CalibrationTaskResult;
use casa_calibration::gencal::jyperk::{self, Endpoint, JyperkError, ServiceOptions};
use casa_calibration::{CalibrationOutputMode, GencalRequest, GencalType};

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let standalone = task.surface() == "getjyperkalma";
    let caltable = task.required_text("caltable")?;
    if caltable.is_empty() {
        return Err(TaskFailure::validation("A caltable name must be specified").at("caltable"));
    }
    let endpoint = Endpoint::parse(&task.required_text("endpoint")?).ok_or_else(|| {
        let message = if standalone {
            "endpoint must be 'asdm', 'interpolation', or 'model-fit'"
        } else {
            "When the caltype is jyperk, endpoint must be one of asdm, interpolation or model-fit"
        };
        TaskFailure::validation(message).at("endpoint")
    })?;
    let measurement_set = task.path("vis")?;
    if !measurement_set.exists() {
        if standalone {
            return Err(TaskFailure::execution(format!(
                "Visibility data not found: {}",
                measurement_set.display()
            ))
            .at("jyperk.vis"));
        }
        return Err(TaskFailure::validation(
            "Visibility data set not found - please verify the name",
        )
        .at("vis"));
    }
    let request = GencalRequest {
        measurement_set,
        output_table: caltable.into(),
        caltype: GencalType::Jyperk,
        antenna: String::new(),
        spw: task.text("spw")?.unwrap_or_default(),
        parameter: Vec::new(),
        gaincurve_table: None,
    };
    let infile = task
        .text("infile")?
        .filter(|name| !name.is_empty())
        .map(PathBuf::from);
    let report = super::read::report(task)?;
    if let Some(path) = report.output.as_deref() {
        super::refuse_existing_report(path, report.overwrite)?;
        let products = vec![
            request.measurement_set.clone(),
            request.output_table.clone(),
        ];
        super::refuse_report_in_products(
            path,
            products.into_iter().chain(infile.clone()).collect(),
            task,
        )?;
    }
    let options = ServiceOptions {
        endpoint,
        timeout: task.integer("timeout")?,
        attempts: task.integer("retry")?,
        delay: task.integer("retry_wait_time")?,
    };
    let backup_hosts = standalone.then(|| task.texts("backup_hosts")).transpose()?;
    let calibration = jyperk::calibrate(
        &request,
        infile.as_deref(),
        &options,
        backup_hosts.as_deref(),
        CalibrationOutputMode::Update,
    )
    .map_err(|error| input_failure(error, standalone))?;
    super::report_success(
        &report,
        &CalibrationTaskResult::Gencal(calibration),
        CasaValue::None,
    )
}

fn input_failure(error: JyperkError, standalone: bool) -> TaskFailure {
    match error {
        JyperkError::NoFactors if !standalone => {
            TaskFailure::execution("There is no factor.").at("jyperk.no_factors")
        }
        JyperkError::MissingFile(_) | JyperkError::ReadFile { .. } => {
            TaskFailure::execution(error.to_string()).at("jyperk.os")
        }
        JyperkError::InvalidNumber(_) | JyperkError::InvalidFactor { .. } => {
            TaskFailure::validation(error.to_string()).at("infile")
        }
        _ => TaskFailure::execution(error.to_string()),
    }
}
