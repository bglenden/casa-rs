// SPDX-License-Identifier: LGPL-3.0-or-later
//! The CASA task applies each referenced Multi-MS partition independently.

use std::path::{Path, PathBuf};

use crate::{TaskFailure, TaskInvocation};
use casa_contracts::ParameterValue;
use casa_ms::{MeasurementSet, save_next_partition_flag_backup};
use casa_tables::{Table, TableInfo, table_cache_name};

use crate::calibration::managed_output::{ApplyMultiMsReport, ApplyPartitionReport};
use crate::calibration::{CalibrationTaskRequest, ExecuteApplyTaskRequest};
use casa_calibration::ApplyExecutionReport;

pub(super) fn execute(
    task: &TaskInvocation<'_>,
    request: &ExecuteApplyTaskRequest,
) -> Result<Option<ApplyMultiMsReport>, TaskFailure> {
    let path = &request.measurement_set;
    let Some(parts) = partition_paths(path)? else {
        return Ok(None);
    };
    let backup = if request.flagbackup && request.apply_mode.writes_flags() {
        Some(
            save_next_partition_flag_backup(path, &parts, "applycal")
                .map_err(|error| TaskFailure::execution(error.to_string()))?,
        )
    } else {
        None
    };

    let mut session = task.session().clone();
    let gain_paths = task
        .texts("gaintable")?
        .iter()
        .map(|path| {
            let path = if path.is_empty() {
                Path::new(".")
            } else {
                Path::new(path)
            };
            table_cache_name(path)
                .map(|absolute| ParameterValue::String(absolute.to_string_lossy().into_owned()))
                .map_err(|error| TaskFailure::execution(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    session
        .set("gaintable", ParameterValue::Array(gain_paths))
        .map_err(|error| TaskFailure::execution(error.to_string()))?;
    if backup.is_some() {
        session
            .set("flagbackup", ParameterValue::Bool(false))
            .map_err(|error| TaskFailure::execution(error.to_string()))?;
    }
    let mut partitions = Vec::with_capacity(parts.len());
    for path in parts {
        session
            .set(
                "vis",
                ParameterValue::String(path.to_string_lossy().into_owned()),
            )
            .map_err(|error| TaskFailure::execution(error.to_string()))?;
        let result = run_partition(&task.with_session(&session), &path);
        if let Err(error) = &result {
            tracing::warn!(partition = %path.display(), %error, "applycal failed for Multi-MS partition");
        }
        partitions.push(ApplyPartitionReport {
            measurement_set: path,
            result,
        });
    }
    Ok(Some(ApplyMultiMsReport {
        measurement_set: path.clone(),
        flag_backup_version: backup,
        partitions,
    }))
}

pub(super) fn partition_paths(path: &Path) -> Result<Option<Vec<PathBuf>>, TaskFailure> {
    let Ok(text) = std::fs::read_to_string(path.join("table.info")) else {
        return Ok(None);
    };
    if TableInfo::parse(&text).sub_type != "CONCATENATED" {
        return Ok(None);
    }
    let parts = Table::concatenated_source_paths(path)
        .map_err(|error| TaskFailure::execution(error.to_string()))?
        .ok_or_else(|| TaskFailure::execution("Multi-MS has no concatenation"))?;
    // CASA opens the aggregate before dispatch, so a broken reference
    // fails before any partition receives changes.
    for part in &parts {
        MeasurementSet::open(part).map_err(|error| {
            TaskFailure::execution(format!(
                "open Multi-MS partition {}: {error}",
                part.display()
            ))
        })?;
    }
    Ok(Some(parts))
}

fn run_partition(task: &TaskInvocation<'_>, path: &Path) -> Result<ApplyExecutionReport, String> {
    let CalibrationTaskRequest::ExecuteApply(request) =
        super::casa_read::task_request(task).map_err(|failure| failure.message)?
    else {
        unreachable!("Multi-MS applycal uses an apply request");
    };
    let report = request.execute_apply().map_err(|error| error.to_string())?;
    super::history::record_applycal(task, path);
    Ok(report)
}
