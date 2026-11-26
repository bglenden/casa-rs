// SPDX-License-Identifier: LGPL-3.0-or-later
//! PlotMS pointing axis parameter parsing and validation for CLI tasks.

use crate::{TaskFailure, TaskInvocation};
use casa_contracts::ParameterValue;
use casa_plot_data::msexplore::MsAxis;
pub use casa_plot_data::msexplore::plotms_pointing::PointingAxes;
use casa_plot_data::msexplore::plotms_pointing::parse_axis as parse_pure_axis;

const CONTROLS: [&str; 4] = ["xframe", "xinterp", "yframe", "yinterp"];

pub(super) fn parse_pointing_axes(
    task: &TaskInvocation<'_>,
    [x, y]: [Option<MsAxis>; 2],
) -> Result<Option<PointingAxes>, TaskFailure> {
    let parse_axis = |axis: Option<MsAxis>, frame: &str, interp: &str| {
        parse_pure_axis(axis, frame, interp).map_err(TaskFailure::validation)
    };
    let y_is_pointing = y.is_some_and(MsAxis::is_pointing);
    if y_is_pointing {
        let frame = control_list(task, "yframe")?;
        let interp = control_list(task, "yinterp")?;
        let sides = control_list(task, "yaxislocation")?;
        if let Control::List(interpolations) = interp {
            let mut pairs = Vec::with_capacity(interpolations.len());
            let mut right_ordinates = Vec::with_capacity(interpolations.len());
            for (index, interp) in interpolations.iter().enumerate() {
                let frame = frame.parallel("yframe", index, interpolations.len())?;
                let side = sides.parallel("yaxislocation", index, interpolations.len())?;
                // The public task applies the ordinate's frame and interpolation to both axes.
                pairs.push([parse_axis(x, frame, interp)?, parse_axis(y, frame, interp)?]);
                right_ordinates.push(side == "right");
            }
            return Ok(Some(PointingAxes::new(pairs, right_ordinates)));
        }
        if matches!(frame, Control::List(_)) {
            return Err(TaskFailure::validation("not yet implemented: yframe=list").at("yframe"));
        }
    }
    let parse = |axis: Option<MsAxis>, frame, interp| {
        if !axis.is_some_and(MsAxis::is_pointing) {
            refuse_controls(task, [frame, interp])?;
            return Ok(None);
        }
        parse_axis(
            axis,
            scalar_control(task, frame)?,
            scalar_control(task, interp)?,
        )
    };
    let pair = [
        parse(x, CONTROLS[0], CONTROLS[1])?,
        parse(y, CONTROLS[2], CONTROLS[3])?,
    ];
    if pair.iter().all(Option::is_none) {
        return Ok(None);
    }
    let right = match control_list(task, "yaxislocation")? {
        Control::Scalar(side) => side == "right",
        Control::List(sides) => y_is_pointing && sides.first() == Some(&"right"),
    };
    Ok(Some(PointingAxes::new(vec![pair], vec![right])))
}

enum Control<'a> {
    Scalar(&'a str),
    List(Vec<&'a str>),
}

impl<'a> Control<'a> {
    fn parallel(&self, name: &str, index: usize, len: usize) -> Result<&'a str, TaskFailure> {
        match self {
            Self::Scalar(value) => Ok(value),
            Self::List(values) if values.len() == len => Ok(values[index]),
            Self::List(values) => Err(TaskFailure::validation(format!(
                "Length mismatch: {name} has {} entries and yinterp has {len}",
                values.len()
            ))
            .at(name)),
        }
    }
}

fn control_list<'a>(task: &'a TaskInvocation<'_>, name: &str) -> Result<Control<'a>, TaskFailure> {
    if let Some(ParameterValue::Array(values)) = task.value(name)
        && !(name != "yaxislocation" && values.is_empty())
    {
        let values =
            super::calibration::axis_names(task.value(name)).map_err(TaskFailure::validation)?;
        if name != "yaxislocation" && values.contains(&"") {
            return Err(TaskFailure::validation(format!("invalid {name} value ''")).at(name));
        }
        return Ok(Control::List(values));
    }
    scalar_control(task, name).map(Control::Scalar)
}

pub(super) fn refuse_nonpointing_controls(task: &TaskInvocation<'_>) -> Result<(), TaskFailure> {
    refuse_controls(task, CONTROLS)
}

fn refuse_controls<const N: usize>(
    task: &TaskInvocation<'_>,
    names: [&str; N],
) -> Result<(), TaskFailure> {
    for name in names {
        if !scalar_control(task, name)?.is_empty() {
            return Err(TaskFailure::validation(format!(
                "{name} requires a MeasurementSet Ant-Ra or Ant-Dec axis (#1032)"
            ))
            .at(name));
        }
    }
    Ok(())
}

fn scalar_control<'a>(task: &'a TaskInvocation<'_>, name: &str) -> Result<&'a str, TaskFailure> {
    match task.value(name) {
        None => Ok(""),
        Some(ParameterValue::String(text)) => Ok(text),
        Some(ParameterValue::Array(values)) if values.is_empty() => Ok(""),
        _ => Err(TaskFailure::validation(format!(
            "list-valued PlotMS pointing control {name} is pending #1032"
        ))
        .at(name)),
    }
}
