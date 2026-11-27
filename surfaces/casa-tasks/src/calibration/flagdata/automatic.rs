// SPDX-License-Identifier: LGPL-3.0-or-later
//! Table-specific averaging and iterator controls for automatic flagging.

use crate::{TaskFailure, TaskInvocation};
use casa_contracts::{EmptyArrayType, NumpyScalarType, ParameterValue};
use casa_core::quanta::Quantity;
use casa_flagging::FlagDataMode;

use super::is_calibration_table;

pub(super) fn controls(
    task: &TaskInvocation<'_>,
    mode: FlagDataMode,
    calibration: bool,
) -> Result<(bool, Option<&'static str>), TaskFailure> {
    if !calibration || !matches!(mode, FlagDataMode::Tfcrop | FlagDataMode::Rflag) {
        return Ok((true, None));
    }
    let channel_average = average_switch(task, "channelavg")?;
    let time_average = average_switch(task, "timeavg")?;
    let vector = if channel_average.is_none() {
        Some("channelavg")
    } else {
        time_average.is_none().then_some("timeavg")
    };
    // The calibration iterator uses stored samples. The agent still
    // parses its time-bin string when time averaging was requested.
    if time_average == Some(true) {
        let timebin = task.required_text("timebin")?;
        let c_string = timebin
            .split_once('\0')
            .map_or(timebin.as_str(), |(head, _)| head);
        return Ok((Quantity::read(c_string).is_ok(), vector));
    }
    Ok((true, vector))
}

pub(super) fn average_switch(
    task: &TaskInvocation<'_>,
    name: &'static str,
) -> Result<Option<bool>, TaskFailure> {
    let boolean = |value: &ParameterValue| {
        matches!(
            value,
            ParameterValue::Bool(_)
                | ParameterValue::NumpyScalar {
                    dtype: NumpyScalarType::Bool,
                    ..
                }
        )
    };
    match task.value(name) {
        None => Ok(Some(false)),
        Some(ParameterValue::Bool(value)) => Ok(Some(*value)),
        Some(ParameterValue::NumpyScalar {
            dtype: NumpyScalarType::Bool,
            ..
        }) => Ok(Some(false)),
        Some(ParameterValue::Array(items)) if items.iter().all(boolean) => Ok(None),
        Some(ParameterValue::ScalarArray(value)) if boolean(value) => Ok(None),
        Some(ParameterValue::EmptyArray {
            dtype: EmptyArrayType::Bool,
            shape,
        }) if shape.len() == 1 => Ok(None),
        _ => Err(
            TaskFailure::validation(format!("{name} must be a Boolean or Boolean vector")).at(name),
        ),
    }
}

pub(super) fn table_kind(
    task: &TaskInvocation<'_>,
    mode: &str,
) -> Result<Option<bool>, TaskFailure> {
    if !matches!(mode, "tfcrop" | "rflag") {
        return Ok(None);
    }
    let calibration = is_calibration_table(&task.path("vis")?);
    if calibration {
        for name in ["channelavg", "timeavg"] {
            average_switch(task, name)?;
        }
    }
    Ok(Some(calibration))
}
