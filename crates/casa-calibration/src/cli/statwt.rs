// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read the CASA statistical-weighting task surface.

use casa_ms::{MeasurementSet, MsSelection, save_next_flag_backup};
use casa_provider_contracts::{CasaValue, ParameterValue};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};
use casa_types::quanta::{Quantity, Unit};

use crate::{
    ClassicScalarStatWt, StatWtCombine, StatWtDataColumn, StatWtTimeBin, classical_scalar_statwt,
};

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let path = task.input_path("vis")?;
    let options = options(task)?;
    let selectdata = task.flag("selectdata")?;
    let selector = |name: &str| {
        task.text(name)
            .map(|value| value.filter(|text| selectdata && !text.is_empty()))
    };
    let selection = MsSelection {
        selectdata,
        field: selector("field")?,
        spw: selector("spw")?,
        intent: selector("intent")?,
        array: selector("array")?,
        observation: selector("observation")?,
        scan: selector("scan")?,
        ..MsSelection::default()
    };
    let result = if task.flag("preview")? {
        #[cfg(unix)]
        let mut ms = MeasurementSet::open_retained_read(&path).map_err(runtime_error)?;
        #[cfg(not(unix))]
        let mut ms = MeasurementSet::open(&path).map_err(runtime_error)?;
        require_scalar_weights(&ms)?;
        let selection = crate::session::resolve_casa_calibration_selection(&ms, &selection)
            .map_err(runtime_error)?;
        classical_scalar_statwt(&mut ms, &selection, options, None).map_err(runtime_error)?
    } else {
        let mut opened = super::multi_ms::OpenedMs::open(&path)?;
        require_scalar_weights(&opened.measurement_set)?;
        let selection =
            crate::session::resolve_casa_calibration_selection(&opened.measurement_set, &selection)
                .map_err(runtime_error)?;
        if task.flag("flagbackup")? {
            save_next_flag_backup(&opened.measurement_set, "statwt").map_err(runtime_error)?;
        }
        let result = classical_scalar_statwt(
            &mut opened.measurement_set,
            &selection,
            options,
            Some(&mut opened.lock),
        )
        .map_err(runtime_error)?;
        opened.release()?;
        super::history::record_task(task, &path);
        result
    };
    let value = CasaValue::dict([
        ("mean", CasaValue::float(result.mean)),
        ("variance", CasaValue::float(result.variance)),
    ]);
    TaskSuccess::new(value, &result)
}

fn require_scalar_weights(ms: &MeasurementSet) -> Result<(), TaskFailure> {
    if let Some(schema) = ms.main_table().schema()
        && ["WEIGHT_SPECTRUM", "SIGMA_SPECTRUM"]
            .iter()
            .any(|name| schema.contains_column(name))
    {
        return Err(unavailable("chanbin"));
    }
    Ok(())
}

fn options(task: &TaskInvocation<'_>) -> Result<ClassicScalarStatWt, TaskFailure> {
    for (name, expected) in [("chanbin", "spw"), ("statalg", "classic"), ("fitspw", "")] {
        if task.text(name)?.as_deref() != Some(expected) {
            return Err(unavailable(name));
        }
    }
    if task.flag("excludechans")? {
        return Err(unavailable("excludechans"));
    }
    let combined = task
        .text("combine")?
        .unwrap_or_default()
        .to_ascii_lowercase();
    let mut combine = StatWtCombine::default();
    for item in combined.split(',').map(str::trim) {
        match item {
            "corr" => combine.correlations = true,
            "scan" => combine.scans = true,
            "state" => combine.states = true,
            "field" => combine.fields = true,
            _ => {}
        }
    }
    let minimum_samples = usize::try_from(task.integer("minsamp")?).unwrap_or(0);
    if minimum_samples < 2 {
        return Err(TaskFailure::execution("minsamp must be at least 2").at("minsamp"));
    }
    let bounds = task.numbers("wtrange")?;
    let weight_range = match bounds.as_slice() {
        [] => None,
        &[low, high] if low >= 0.0 && high >= low => Some([low as f32, high as f32]),
        _ => {
            return Err(
                TaskFailure::execution("wtrange requires two ordered, nonnegative bounds")
                    .at("wtrange"),
            );
        }
    };
    let data_column = parse_data_column(&task.text("datacolumn")?.unwrap_or_default())?;
    Ok(ClassicScalarStatWt {
        data_column,
        minimum_samples,
        combine,
        time_bin: time_bin(task)?,
        sliding: task.flag("slidetimebin")?,
        weight_range,
    })
}

fn time_bin(task: &TaskInvocation<'_>) -> Result<StatWtTimeBin, TaskFailure> {
    let bad_value = || {
        TaskFailure::execution("timebin requires a positive timestamp count or a duration")
            .at("timebin")
    };
    match task.value("timebin") {
        Some(ParameterValue::Integer(value)) if *value > 0 => usize::try_from(*value)
            .map(StatWtTimeBin::Timestamps)
            .map_err(|_| bad_value()),
        Some(ParameterValue::String(value)) => {
            let seconds = Unit::new("s").expect("seconds");
            let quantity: Quantity = value.parse().map_err(|_| bad_value())?;
            let width = quantity.get_value_in(&seconds).map_err(|_| bad_value())?;
            if width > 0.0 && width.is_finite() {
                Ok(StatWtTimeBin::Seconds(width))
            } else {
                Err(bad_value())
            }
        }
        _ => Err(bad_value()),
    }
}

fn parse_data_column(value: &str) -> Result<StatWtDataColumn, TaskFailure> {
    let value = value.to_ascii_lowercase();
    if !value.is_empty() {
        for (name, column) in [
            ("data", StatWtDataColumn::Data),
            ("corrected", StatWtDataColumn::Corrected),
            ("residual", StatWtDataColumn::ResidualCorrected),
            ("residual_data", StatWtDataColumn::ResidualData),
        ] {
            if name.starts_with(&value) {
                return Ok(column);
            }
        }
    }
    Err(TaskFailure::execution(format!("unknown statwt data column {value:?}")).at("datacolumn"))
}

fn runtime_error(error: impl std::fmt::Display) -> TaskFailure {
    TaskFailure::execution(error.to_string())
}

fn unavailable(parameter: &str) -> TaskFailure {
    TaskFailure::validation(format!("statwt {parameter} behavior is tracked in #1524"))
        .at(parameter)
}
