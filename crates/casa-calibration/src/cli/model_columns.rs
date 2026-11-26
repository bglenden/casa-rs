// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read the CASA scratch-column task surfaces.

use std::path::Path;

use casa_contracts::{CasaValue, NumpyScalarType, ParameterValue};
use casa_ms::{MeasurementSet, MsSelection};
use casa_tables::TableWriteLock;
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

use crate::delete_calibration_model;
use crate::model_columns::reset_scratch_columns;

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let path = task.input_path("vis")?;
    if task.surface() == "uvsub" {
        crate::subtract_visibility_model(&path, task.flag("reverse")?)
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
            let partition = TaskInvocation::new(&session, task.workspace());
            if let Err(error) = run_one(&partition, &part, true) {
                tracing::warn!(task = task.surface(), partition = %part.display(), error = %error.message, "Multi-MS partition failed");
            }
        }
    } else {
        run_one(task, &path, false)?;
    }
    TaskSuccess::new(CasaValue::None, &())
}

fn run_one(task: &TaskInvocation<'_>, path: &Path, partition: bool) -> Result<(), TaskFailure> {
    let mut lock = TableWriteLock::acquire_to_reread(path, 0)
        .map_err(|error| TaskFailure::execution(error.to_string()).at("vis"))?;
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

fn selectvis_text(value: Option<String>) -> Option<String> {
    let text = value?;
    let selector = text
        .strip_prefix('[')
        .and_then(|inner| inner.strip_suffix(']'))
        .unwrap_or(&text);
    (!selector.is_empty()).then(|| selector.to_owned())
}
