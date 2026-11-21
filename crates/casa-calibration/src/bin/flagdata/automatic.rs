// SPDX-License-Identifier: LGPL-3.0-or-later
//! Table-specific averaging and iterator controls for automatic flagging.

use casa_ms::FlagDataMode;
use casa_provider_contracts::{
    DefaultSpec, EmptyArrayType, NumpyScalarType, ParameterValue, python_repr,
};
use casa_task_runtime::{TaskFailure, TaskInvocation, semantic_eq};
use casa_types::quanta::Quantity;

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
    } else {
        validate_ms_automatic_controls(task, mode == "rflag")?;
    }
    Ok(Some(calibration))
}

fn validate_ms_automatic_controls(
    task: &TaskInvocation<'_>,
    rflag: bool,
) -> Result<(), TaskFailure> {
    if !rflag {
        return Ok(());
    }
    let bundle = task.session().bundle();
    for (name, issue) in [
        ("channelavg", 978),
        ("chanbin", 978),
        ("timeavg", 978),
        ("timebin", 978),
        ("ntime", 1616),
        ("combinescans", 1616),
    ] {
        let binding = bundle
            .surface
            .bindings()
            .iter()
            .find(|binding| binding.name == name)
            .expect("automatic controls are in the flagdata surface");
        let DefaultSpec::Literal { value: default } = &binding.default else {
            unreachable!("automatic controls have literal defaults");
        };
        let concept = bundle
            .catalog
            .concept(&binding.concept)
            .expect("validated contract");
        if let Some(value) = task.value(name)
            && !matches!(semantic_eq(value, default, concept), Ok(true))
        {
            return Err(TaskFailure::validation(format!(
                "parameter {name:?} is not implemented by casa-rs yet (#{issue}); only CASA's default {} is accepted",
                python_repr(default),
            )).at(name));
        }
    }
    Ok(())
}
