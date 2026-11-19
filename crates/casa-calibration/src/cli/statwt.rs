// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read the CASA statistical-weighting task surface.

use casa_ms::{MeasurementSet, MsSelection, save_next_flag_backup};
use casa_provider_contracts::{CasaValue, ParameterValue};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

use crate::{
    StatWtAlgorithm, StatWtChannelBin, StatWtCombine, StatWtDataColumn, StatWtOptions,
    StatWtTimeBin, estimate_statwt, resolve_statwt_selection, statwt_weight_range,
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
        let selection = resolve_statwt_selection(&ms, &selection).map_err(runtime_error)?;
        estimate_statwt(&mut ms, &selection, &options, None).map_err(runtime_error)?
    } else {
        let mut opened = super::multi_ms::OpenedMs::open(&path)?;
        let selection =
            resolve_statwt_selection(&opened.measurement_set, &selection).map_err(runtime_error)?;
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
    let combine = StatWtCombine::parse(&task.text("combine")?.unwrap_or_default());
    let minimum_samples = usize::try_from(task.integer("minsamp")?).unwrap_or(0);
    let weight_range = statwt_weight_range(&task.numbers("wtrange")?)
        .map_err(|error| runtime_error(error).at("wtrange"))?;
    let data_column = StatWtDataColumn::parse(&task.text("datacolumn")?.unwrap_or_default())
        .map_err(|error| runtime_error(error).at("datacolumn"))?;
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
            StatWtTimeBin::duration(value).map_err(|error| runtime_error(error).at("timebin"))
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
        Some(ParameterValue::String(value)) => {
            StatWtChannelBin::parse(value).map_err(|error| runtime_error(error).at("chanbin"))
        }
        _ => Err(bad_value()),
    }
}

fn runtime_error(error: impl std::fmt::Display) -> TaskFailure {
    TaskFailure::execution(error.to_string())
}
