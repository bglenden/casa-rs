// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read the CASA statistical-weighting task surface.

use casa_ms::{MeasurementSet, MsSelection, save_next_flag_backup};
use casa_provider_contracts::{CasaValue, ParameterValue};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};
use casa_types::quanta::{Quantity, Unit};

use crate::{
    StatWtAlgorithm, StatWtChannelBin, StatWtCombine, StatWtDataColumn, StatWtOptions,
    StatWtTimeBin, estimate_statwt,
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
        let selection = crate::session::resolve_casa_calibration_selection(&ms, &selection)
            .map_err(runtime_error)?;
        estimate_statwt(&mut ms, &selection, &options, None).map_err(runtime_error)?
    } else {
        let mut opened = super::multi_ms::OpenedMs::open(&path)?;
        let selection =
            crate::session::resolve_casa_calibration_selection(&opened.measurement_set, &selection)
                .map_err(runtime_error)?;
        if task.flag("flagbackup")? {
            save_next_flag_backup(&opened.measurement_set, "statwt").map_err(runtime_error)?;
        }
        let result = estimate_statwt(
            &mut opened.measurement_set,
            &selection,
            &options,
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

fn options(task: &TaskInvocation<'_>) -> Result<StatWtOptions, TaskFailure> {
    let algorithm = StatWtAlgorithm::parse(
        &task.text("statalg")?.unwrap_or_default(),
        task.number("fence")?,
        &task.text("center")?.unwrap_or_default(),
        task.flag("lside")?,
        task.number("zscore")?,
        task.integer("maxiter")?,
    )
    .map_err(runtime_error)?;
    let combined = task.text("combine")?.unwrap_or_default();
    let folded = combined.to_ascii_lowercase();
    let combine = StatWtCombine {
        correlations: combined.contains("corr"),
        scans: folded.contains("scan"),
        states: folded.contains("state"),
        fields: folded.contains("field"),
    };
    let minimum_samples = usize::try_from(task.integer("minsamp")?).unwrap_or(0);
    if minimum_samples < 2 {
        return Err(TaskFailure::execution("minsamp must be at least 2").at("minsamp"));
    }
    let bounds = task.numbers("wtrange")?;
    let weight_range = match bounds.as_slice() {
        [] => None,
        &[left, right] if left >= 0.0 && right >= 0.0 && left != right => {
            Some([left.min(right), left.max(right)])
        }
        _ => {
            return Err(
                TaskFailure::execution("wtrange requires two distinct nonnegative bounds")
                    .at("wtrange"),
            );
        }
    };
    let data_column = parse_data_column(&task.text("datacolumn")?.unwrap_or_default())?;
    Ok(StatWtOptions {
        data_column,
        algorithm,
        minimum_samples,
        combine,
        time_bin: time_bin(task)?,
        sliding: task.flag("slidetimebin")?,
        channel_bin: channel_bin(task)?,
        fit_spw: task.text("fitspw")?.unwrap_or_default(),
        exclude_channels: task.flag("excludechans")?,
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

fn channel_bin(task: &TaskInvocation<'_>) -> Result<StatWtChannelBin, TaskFailure> {
    let bad_value = || {
        TaskFailure::execution("chanbin requires a positive channel count or a frequency width")
            .at("chanbin")
    };
    match task.value("chanbin") {
        Some(ParameterValue::Integer(value)) if *value > 0 => usize::try_from(*value)
            .map(StatWtChannelBin::Channels)
            .map_err(|_| bad_value()),
        Some(ParameterValue::String(value)) if value == "spw" => Ok(StatWtChannelBin::Window),
        Some(ParameterValue::String(value)) => {
            let hertz = Unit::new("Hz").expect("hertz");
            let quantity: Quantity = value.parse().map_err(|_| bad_value())?;
            let width = quantity.get_value_in(&hertz).map_err(|_| bad_value())?;
            if width > 0.0 && width.is_finite() {
                Ok(StatWtChannelBin::Hertz(width))
            } else {
                Err(bad_value())
            }
        }
        _ => Err(bad_value()),
    }
}

fn parse_data_column(value: &str) -> Result<StatWtDataColumn, TaskFailure> {
    let value = value.to_ascii_lowercase();
    if value.starts_with('d') {
        Ok(StatWtDataColumn::Data)
    } else if value.starts_with('c') {
        Ok(StatWtDataColumn::Corrected)
    } else if value.starts_with("residual_") {
        Ok(StatWtDataColumn::ResidualData)
    } else if value.starts_with("residual") {
        Ok(StatWtDataColumn::ResidualCorrected)
    } else {
        Err(
            TaskFailure::execution(format!("unknown statwt data column {value:?}"))
                .at("datacolumn"),
        )
    }
}

fn runtime_error(error: impl std::fmt::Display) -> TaskFailure {
    TaskFailure::execution(error.to_string())
}
