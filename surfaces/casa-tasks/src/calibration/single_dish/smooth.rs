// SPDX-License-Identifier: LGPL-3.0-or-later
//! Task adapter for `sdsmooth`.

use std::path::Path;

use crate::{TaskFailure, TaskInvocation, TaskSuccess};
use casa_contracts::CasaValue;
use casa_ms::MsSelection;
use casa_mstransform::TransformTaskHistory;
use casa_singledish::{SingleDishSession, SingleDishSmoothing};

use super::selector;

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let mut session = SingleDishSession::default();
    session.open(task.input_path("infile")?);
    session
        .set_selection(
            MsSelection {
                antenna: selector(task, "antenna")?,
                field: selector(task, "field")?,
                spw: selector(task, "spw")?,
                timerange: selector(task, "timerange")?,
                scan: selector(task, "scan")?,
                correlation: selector(task, "pol")?,
                intent: selector(task, "intent")?,
                ..MsSelection::default()
            },
            task.flag("reindex")?,
        )
        .map_err(TaskFailure::execution)?;
    let outfile = task.file_text("outfile")?.unwrap_or_default();
    let overwrite = task.flag("overwrite")?;
    if outfile.is_empty() {
        return Err(TaskFailure::validation("outfile is empty.").at("outfile"));
    }
    let output = Path::new(&outfile);
    if output.exists() && !overwrite {
        return Err(TaskFailure::execution(format!("{outfile} exists.")).at("outfile"));
    }
    let report = session
        .smooth(SingleDishSmoothing {
            kernel: &task.text("kernel")?.unwrap_or_else(|| "gaussian".into()),
            width: task.integer("kwidth")? as f32,
            datacolumn: &task.text("datacolumn")?.unwrap_or_else(|| "data".into()),
            output,
            overwrite,
            history: Some(TransformTaskHistory {
                task: "sdsmooth".into(),
                parameters: Vec::new(),
            }),
        })
        .map_err(TaskFailure::execution)?;
    TaskSuccess::new(CasaValue::None, &report)
}
