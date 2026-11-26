// SPDX-License-Identifier: LGPL-3.0-or-later

use std::num::IntErrorKind;

use crate::{TaskFailure, TaskInvocation};
use casa_contracts::ParameterValue;
use casa_ms::flagging::{TfcropFit, TfcropOptions, TfcropPasses, TfcropWindowStats};

pub(super) fn validate_types(task: &TaskInvocation<'_>) -> Result<(), TaskFailure> {
    integer_setting(task, "maxnpieces")?;
    integer_setting(task, "halfwin")?;
    Ok(())
}

pub(super) fn validate_agent_record(
    task: &TaskInvocation<'_>,
    mode: &str,
) -> Result<(), TaskFailure> {
    let names: &[&str] = if mode == "rflag" {
        &["winsize"]
    } else {
        &["maxnpieces", "halfwin"]
    };
    for &name in names {
        if let Some(text) = task.argument_text(name)
            && text.parse::<i64>().is_err_and(|error| {
                matches!(
                    error.kind(),
                    IntErrorKind::PosOverflow | IntErrorKind::NegOverflow
                )
            })
        {
            return Err(TaskFailure::execution(
                "<built-in function agentflagger_parseagentparameters> returned a result with an exception set",
            )
            .at(name));
        }
    }
    Ok(())
}

pub(super) fn parameters(task: &TaskInvocation<'_>) -> Result<Option<TfcropOptions>, TaskFailure> {
    let pieces = integer_setting(task, "maxnpieces")?;
    let half_window = integer_setting(task, "halfwin")?;
    let time_fit = task.required_text("timefit")?;
    let frequency_fit = task.required_text("freqfit")?;
    let passes = task.required_text("flagdimension")?;
    let statistics = task.required_text("usewindowstats")?;
    match agent_options(
        pieces,
        half_window,
        &time_fit,
        &frequency_fit,
        &passes,
        &statistics,
    ) {
        Ok(options) => Ok(Some(options)),
        Err(reason) => {
            tracing::warn!("Cannot create agent Tfcrop: {reason}");
            if !task.flag("extendflags")? {
                tracing::error!("There is no agent to run in list");
            }
            Ok(None)
        }
    }
}

pub(super) fn integer_setting(
    task: &TaskInvocation<'_>,
    name: &str,
) -> Result<Option<i64>, TaskFailure> {
    let value = match task.value(name) {
        Some(ParameterValue::NumpyScalar { value, .. }) => Some(value.as_ref()),
        other => other,
    };
    match value {
        // The CASA record narrows the Python long to a casacore Int.
        Some(ParameterValue::Integer(value)) => Ok(Some(i64::from(*value as i32))),
        Some(ParameterValue::Bool(_)) => Ok(None),
        _ => {
            Err(TaskFailure::validation(format!("parameter {name:?} must be an integer")).at(name))
        }
    }
}

fn agent_options(
    pieces: Option<i64>,
    half_window: Option<i64>,
    time_fit: &str,
    frequency_fit: &str,
    passes: &str,
    statistics: &str,
) -> Result<TfcropOptions, String> {
    let [time_fit, frequency_fit, passes, statistics] =
        [time_fit, frequency_fit, passes, statistics]
            .map(|text| text.split_once('\0').map_or(text, |(head, _)| head));
    let max_pieces = pieces.ok_or("Parameter 'maxnpieces' must be of type 'int'")?;
    if !(1..=9).contains(&max_pieces) {
        return Err(format!(
            "Unsupported maxnpieces : {max_pieces}. Supported values: 1-9"
        ));
    }
    let fit = |key, text| match text {
        "line" => Ok(TfcropFit::Line),
        "poly" => Ok(TfcropFit::Poly),
        _ => Err(format!(
            "Unsupported {key}: {text}. Supported modes: line,poly"
        )),
    };
    let time_fit = fit("timefit", time_fit)?;
    let frequency_fit = fit("freqfit", frequency_fit)?;
    let passes = match passes {
        "time" => TfcropPasses::Time,
        "freq" => TfcropPasses::Frequency,
        "timefreq" => TfcropPasses::TimeThenFrequency,
        "freqtime" => TfcropPasses::FrequencyThenTime,
        other => {
            return Err(format!(
                "Unsupported flagdimension: {other}. Supported modes: time,freq,timefreq,freqtime"
            ));
        }
    };
    let half_window = half_window.ok_or("Parameter 'halfwin' must be of type 'int'")?;
    if !(1..=3).contains(&half_window) {
        return Err(format!(
            "Unsupported halfwin: {half_window}. Supported values : 1,2,3"
        ));
    }
    let window_stats = match statistics {
        "none" => TfcropWindowStats::None,
        "sum" => TfcropWindowStats::Sum,
        "std" => TfcropWindowStats::Std,
        "both" => TfcropWindowStats::Both,
        other => {
            return Err(format!(
                "Unsupported usewindowstats: {other}. Supported modes : none,sum,std,both"
            ));
        }
    };
    Ok(TfcropOptions {
        time_fit,
        frequency_fit,
        passes,
        window_stats,
        max_pieces: usize::try_from(max_pieces).expect("validated piece count"),
        half_window: usize::try_from(half_window).expect("validated window width"),
    })
}
