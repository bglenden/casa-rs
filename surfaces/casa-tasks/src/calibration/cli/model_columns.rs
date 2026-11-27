// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read the CASA scratch-column task surfaces.

use std::path::Path;

use crate::{TaskFailure, TaskInvocation, TaskSuccess};
use casa_contracts::{CasaValue, NumpyScalarType, ParameterValue};
use casa_ms::{MeasurementSet, MsSelection};
use casa_tables::TableWriteLock;

use casa_calibration::model_columns::delete_calibration_model;
use casa_calibration::model_columns::reset_scratch_columns;

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let path = task.input_path("vis")?;
    if task.surface() == "uvsub" {
        casa_calibration::uvsub::subtract_visibility_model(&path, task.flag("reverse")?)
            .map_err(|error| TaskFailure::execution(error.to_string()))?;
        super::history::record_task(task, &path);
        return TaskSuccess::new(CasaValue::None, &());
    }
    if let Some(parts) = super::multi_ms::partition_paths(&path)? {
        let mut session = task.session().clone();
        for part in parts {
            session
                .set(
                    "vis",
                    ParameterValue::String(part.to_string_lossy().into_owned()),
                )
                .map_err(|error| TaskFailure::execution(error.to_string()))?;
            let partition = task.with_session(&session);
            if let Err(error) = run_one(&partition, &part, true) {
                if error.kind == casa_contracts::TaskFailureKind::Interrupted {
                    return Err(error);
                }
                tracing::warn!(task = task.surface(), partition = %part.display(), error = %error.message, "Multi-MS partition failed");
            }
        }
    } else {
        run_one(task, &path, false)?;
    }
    TaskSuccess::new(CasaValue::None, &())
}

fn run_one(task: &TaskInvocation<'_>, path: &Path, partition: bool) -> Result<(), TaskFailure> {
    let mut lock = acquire_write(task, path).map_err(|error| {
        if task
            .native_options()
            .is_some_and(crate::NativeTaskOptions::is_cancelled)
        {
            TaskFailure::interrupted(error.to_string()).at("vis")
        } else {
            TaskFailure::execution(error.to_string()).at("vis")
        }
    })?;
    let mut ms = MeasurementSet::open(path)
        .map_err(|error| TaskFailure::execution(error.to_string()).at("vis"))?;
    if !partition {
        // MMS sends native booleans to its workers. The direct tool call
        // rejects a NumPy Boolean before changing its MeasurementSet.
        let flags: &[(&str, &str, u8)] = if task.surface() == "clearcal" {
            &[("addmodel", "calibrater_open", 5)]
        } else {
            &[
                ("otf", "calibrater_delmod", 2),
                ("scr", "calibrater_delmod", 5),
            ]
        };
        for &(name, method, index) in flags {
            if matches!(
                task.session().given(name),
                Some(ParameterValue::NumpyScalar {
                    dtype: NumpyScalarType::Bool,
                    ..
                })
            ) {
                return Err(TaskFailure::execution(format!(
                    "in method '{method}', argument {index} of type 'bool'"
                ))
                .at(name));
            }
        }
    }
    let field = task.text("field")?;
    if task.surface() == "clearcal" {
        let selection = MsSelection {
            selectdata: true,
            field: selectvis_text(field),
            spw: selectvis_text(task.text("spw")?),
            intent: selectvis_text(task.text("intent")?),
            ..MsSelection::default()
        };
        lock = reset_scratch_columns(&mut ms, &selection, task.flag("addmodel")?, lock)
            .map_err(|error| TaskFailure::execution(error.to_string()))?;
    } else {
        delete_calibration_model(
            &mut ms,
            selectvis_text(field).as_deref(),
            None,
            task.flag("otf")?,
            task.flag("scr")?,
            &mut lock,
        )
        .map_err(|error| TaskFailure::execution(error.to_string()))?;
    }
    drop(ms);
    lock.release()
        .map_err(|error| TaskFailure::execution(error.to_string()).at("vis"))?;
    super::history::record_task(task, path);
    Ok(())
}

fn acquire_write(
    task: &TaskInvocation<'_>,
    path: &Path,
) -> Result<TableWriteLock, casa_tables::TableError> {
    #[cfg(unix)]
    if let Some(options) = task.native_options()
        && let Some(reservations) = options.table_writes.as_ref()
    {
        return TableWriteLock::acquire_reserved_interruptibly(path, reservations, &|| {
            options.is_cancelled()
        });
    }
    let _ = task;
    TableWriteLock::acquire_to_reread(path, 0)
}

fn selectvis_text(value: Option<String>) -> Option<String> {
    let text = value?;
    let selector = text
        .strip_prefix('[')
        .and_then(|inner| inner.strip_suffix(']'))
        .unwrap_or(&text);
    (!selector.is_empty()).then(|| selector.to_owned())
}
