// SPDX-License-Identifier: LGPL-3.0-or-later
//! Direct and composite single-dish calibration application.

use std::path::{Path, PathBuf};

use casa_contracts::{CasaValue, ParameterValue};
use casa_ms::{MeasurementSet, MsSelection, ResolvedMsSelection};
use casa_tables::{Table, TableOptions};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};
use casa_types::{ScalarValue, Value};

use super::{generate_edges, generate_position_switch, resolve_selection, selector, tsys};
use crate::{
    ApplyCalibrationTableSpec, ApplyInterpolationMode, ApplyMode, ApplyPlanRequest,
    CalibrationOutputMode, execute::execute_single_dish_apply,
};

pub(super) fn run(
    task: &TaskInvocation<'_>,
    mode: &str,
    ms: MeasurementSet,
    selection: MsSelection,
    selected: ResolvedMsSelection,
) -> Result<TaskSuccess, TaskFailure> {
    let infile = ms
        .path()
        .expect("sdcal opened an on-disk MeasurementSet")
        .to_path_buf();
    let mut paths = supplied_tables(task.value("applytable"))?;
    let staging = (mode != "apply")
        .then(|| tempfile::Builder::new().prefix("sdcal-").tempdir())
        .transpose()
        .map_err(|error| TaskFailure::execution(error.to_string()))?;
    if let Some(staging) = &staging {
        let position_switch = mode.starts_with("ps,");
        let map = mode.starts_with("otf");
        if position_switch || map {
            let output = staging.path().join("sky.cal");
            let intent = if map && mode.contains("tsys,") {
                Some("OBSERVE_TARGET#ON_SOURCE".into())
            } else {
                selector(task, "intent")?
            };
            let sky_selection = if intent.is_some() {
                let selection = MsSelection {
                    intent,
                    ..selection.clone()
                };
                resolve_selection(&ms, &selection)?
            } else {
                selected
            };
            let generate_sky = if position_switch {
                generate_position_switch
            } else {
                generate_edges
            };
            generate_sky(
                task,
                &ms,
                sky_selection,
                &output,
                CalibrationOutputMode::Create,
            )?;
            paths.push(output);
        }
        if mode.contains("tsys,") {
            let output = staging.path().join("tsys.cal");
            tsys(
                infile.clone(),
                output.clone(),
                CalibrationOutputMode::Create,
            )?;
            paths.push(output);
        }
    }
    if paths.is_empty() {
        return Err(TaskFailure::execution("Applytable name must be specified.").at("applytable"));
    }
    let windows = ms
        .spectral_window()
        .map_err(|error| TaskFailure::execution(error.to_string()))?;
    let mapping = spectral_mapping(task.value("spwmap"), windows.row_count())?;
    let interpolation = task.text("interp")?.unwrap_or_default();
    let calibration_tables = paths
        .into_iter()
        .map(|path| table_spec(path, &mapping, &interpolation))
        .collect::<Result<Vec<_>, _>>()?;
    if task.text("outfile")?.is_some_and(|path| !path.is_empty()) {
        tracing::warn!(
            "Calibrated data will be stored in CORRECTED_DATA in the input MeasurementSet."
        );
    }
    drop(ms);
    let report = execute_single_dish_apply(
        &infile,
        &ApplyPlanRequest {
            selection,
            apply_mode: ApplyMode::CalFlag,
            parang: false,
            calibration_tables,
        },
    )
    .map_err(|error| TaskFailure::execution(error.to_string()))?;
    crate::cli::history::record_sdcal(task, &infile);
    TaskSuccess::new(CasaValue::None, &report)
}

fn supplied_tables(value: Option<&ParameterValue>) -> Result<Vec<PathBuf>, TaskFailure> {
    match value {
        None => Ok(Vec::new()),
        Some(ParameterValue::String(path)) if path.is_empty() => Ok(Vec::new()),
        Some(ParameterValue::String(path)) => Ok(vec![PathBuf::from(path)]),
        Some(ParameterValue::Array(values)) => {
            values
                .iter()
                .map(|value| match value {
                    ParameterValue::String(path) if !path.is_empty() => Ok(PathBuf::from(path)),
                    _ => Err(TaskFailure::execution("Applytable name must be specified.")
                        .at("applytable")),
                })
                .collect()
        }
        _ => Err(
            TaskFailure::execution("Applytable must be a path or a list of paths.")
                .at("applytable"),
        ),
    }
}

fn spectral_mapping(
    value: Option<&ParameterValue>,
    windows: usize,
) -> Result<Vec<i32>, TaskFailure> {
    let invalid = || TaskFailure::execution("Spwmap type must be list or dictionary.").at("spwmap");
    let id = |value: &ParameterValue| match value {
        ParameterValue::Integer(id) => i32::try_from(*id).map_err(|_| invalid()),
        _ => Err(invalid()),
    };
    match value {
        None => Ok(Vec::new()),
        Some(ParameterValue::Array(values)) => values.iter().map(id).collect(),
        Some(ParameterValue::Table(values)) => {
            let mut mapping = (0..windows as i32).collect::<Vec<_>>();
            let mut replace = |source: &str, targets: &ParameterValue| {
                let source = source.trim().parse::<i32>().map_err(|_| invalid())?;
                let ParameterValue::Array(targets) = targets else {
                    return Err(invalid());
                };
                for target in targets {
                    let target = id(target)?;
                    if let Some(slot) = mapping.iter_mut().find(|entry| **entry == target) {
                        *slot = source;
                    }
                }
                Ok(())
            };
            if let Some(ParameterValue::Array(entries)) = values.get("entries") {
                for entry in entries {
                    let ParameterValue::Array(pair) = entry else {
                        return Err(invalid());
                    };
                    let [ParameterValue::String(source), targets] = pair.as_slice() else {
                        return Err(invalid());
                    };
                    replace(source, targets)?;
                }
            } else {
                for (source, targets) in values {
                    replace(source, targets)?;
                }
            }
            Ok(mapping)
        }
        _ => Err(invalid()),
    }
}

fn table_spec(
    path: PathBuf,
    tsys_mapping: &[i32],
    interpolation: &str,
) -> Result<ApplyCalibrationTableSpec, TaskFailure> {
    if !path.exists() {
        return Err(
            TaskFailure::execution(format!("Table doesn't exist: {}", path.display()))
                .at("applytable"),
        );
    }
    let invalid = |path: &Path| {
        TaskFailure::execution(format!(
            "Applytable '{}' is not a caltable format",
            path.display()
        ))
        .at("applytable")
    };
    let table = Table::open(TableOptions::new(&path)).map_err(|_| invalid(&path))?;
    let Some(Value::Scalar(ScalarValue::String(subtype))) =
        table.keywords().get(crate::constants::KEY_VIS_CAL)
    else {
        return Err(invalid(&path));
    };
    let mut spec = ApplyCalibrationTableSpec::new(path);
    spec.calwt = true;
    spec.interp = ApplyInterpolationMode::parse_casa(interpolation)
        .map_err(|_| TaskFailure::execution("Error in Calibrater::setapply.").at("interp"))?;
    if subtype == "B TSYS" {
        spec.spwmap = tsys_mapping.to_vec();
    }
    Ok(spec)
}
