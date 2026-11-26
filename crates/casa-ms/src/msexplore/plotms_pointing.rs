// SPDX-License-Identifier: LGPL-3.0-or-later
//! Direction coordinates along the PlotMS visibility iterator.

use std::collections::BTreeMap;

use casa_contracts::ParameterValue;
use casa_task_runtime::{TaskFailure, TaskInvocation};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::epoch::{EpochRef, MEpoch};

use super::MsAxis;
use crate::MeasurementSet;
use crate::derived::engine::{MsCalEngine, resolve_direction_reference_selected};

const CONTROLS: [&str; 4] = ["xframe", "xinterp", "yframe", "yinterp"];

#[derive(Clone, Copy)]
struct DirectionAxis {
    frame: DirectionRef,
    spline: bool,
    longitude: bool,
}

pub(super) struct PointingAxes {
    pairs: Vec<[Option<DirectionAxis>; 2]>,
    right_ordinates: Vec<bool>,
}

impl PointingAxes {
    pub(super) fn from_task(
        task: &TaskInvocation<'_>,
        [x, y]: [Option<MsAxis>; 2],
    ) -> Result<Option<Self>, TaskFailure> {
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
                return Ok(Some(Self {
                    pairs,
                    right_ordinates,
                }));
            }
            if matches!(frame, Control::List(_)) {
                return Err(
                    TaskFailure::validation("not yet implemented: yframe=list").at("yframe")
                );
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
        Ok(Some(Self {
            pairs: vec![pair],
            right_ordinates: vec![right],
        }))
    }

    pub(super) fn len(&self) -> usize {
        self.pairs.len()
    }

    pub(super) fn right_ordinates(&self) -> &[bool] {
        &self.right_ordinates
    }

    pub(super) fn x_label(&self) -> Option<String> {
        let mut labels = Vec::new();
        for index in 0..self.len() {
            if let Some(label) = self.label(index, 0)
                && !labels.contains(&label)
            {
                labels.push(label);
            }
        }
        (!labels.is_empty()).then(|| labels.join(", "))
    }

    pub(super) fn label(&self, index: usize, component: usize) -> Option<String> {
        let axis = self.pairs[index][component]?;
        let component = match (axis.frame, axis.longitude) {
            (DirectionRef::GALACTIC, true) => "Longitude",
            (DirectionRef::GALACTIC, false) => "Latitude",
            (DirectionRef::AZELGEO, true) => "Azimuth",
            (DirectionRef::AZELGEO, false) => "Elevation",
            (_, true) => "Right Ascension",
            (_, false) => "Declination",
        };
        Some(format!("{} {component} (degrees)", axis.frame))
    }
}

fn parse_axis(
    axis: Option<MsAxis>,
    frame: &str,
    interp: &str,
) -> Result<Option<DirectionAxis>, TaskFailure> {
    if !axis.is_some_and(MsAxis::is_pointing) {
        return Ok(None);
    }
    let frame = match frame {
        "" | "icrs" => DirectionRef::ICRS,
        "j2000" => DirectionRef::J2000,
        "b1950" => DirectionRef::B1950,
        "galactic" => DirectionRef::GALACTIC,
        "azelgeo" => DirectionRef::AZELGEO,
        other => {
            return Err(TaskFailure::validation(format!(
                "invalid PlotMS pointing frame {other:?}"
            )));
        }
    };
    let spline = match interp {
        "" | "spline" | "cubic spline" => true,
        "nearest" => false,
        other => {
            return Err(TaskFailure::validation(format!(
                "invalid PlotMS pointing interpolation {other:?}"
            )));
        }
    };
    Ok(Some(DirectionAxis {
        frame,
        spline,
        longitude: axis == Some(MsAxis::AntRa),
    }))
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

pub(super) struct PointingCoordinates<'a> {
    axes: &'a PointingAxes,
    source: DirectionRef,
    tracks: BTreeMap<i32, PointingTrack>,
    geometry: Option<MsCalEngine>,
}

struct PointingTrack {
    times: Vec<f64>,
    angles: Vec<[f64; 2]>,
    second_derivatives: Vec<[f64; 2]>,
}

impl<'a> PointingCoordinates<'a> {
    pub(super) fn read(
        ms: &MeasurementSet,
        axes: &'a PointingAxes,
        antennas: &[i32],
    ) -> Result<Self, String> {
        let pointing = ms.pointing().map_err(|error| error.to_string())?;
        if pointing.row_count() == 0 {
            return Err("PlotMS antenna directions require a nonempty POINTING table".into());
        }
        let observation = ms.observation().map_err(|error| error.to_string())?;
        if observation.row_count() == 0 {
            return Err("PlotMS antenna directions require an observatory".into());
        }
        for row in 0..observation.row_count() {
            let name = observation
                .string(row, "TELESCOPE_NAME")
                .map_err(|error| error.to_string())?;
            if !matches!(name.as_str(), "ALMA" | "ASTE" | "NRO") {
                return Err(format!(
                    "PlotMS antenna directions are unsupported for {name}"
                ));
            }
        }
        let mut antennas = antennas.to_vec();
        antennas.sort_unstable();
        antennas.dedup();
        let samples = pointing
            .plotms_samples(&antennas)
            .map_err(|error| error.to_string())?;
        let frame =
            resolve_direction_reference_selected(pointing.table(), "POINTING", "DIRECTION", 0)
                .map_err(|error| error.to_string())?;
        if samples.iter().any(|sample| sample.2.refer() != frame) {
            return Err("PlotMS POINTING directions in multiple frames are pending #1032".into());
        }
        let mut tracks = BTreeMap::new();
        let direction_axes = || axes.pairs.iter().flatten().flatten();
        let spline = direction_axes().any(|axis| axis.spline);
        for samples in samples.chunk_by(|left, right| left.0 == right.0) {
            let times = samples.iter().map(|sample| sample.1).collect::<Vec<_>>();
            let angles = samples
                .iter()
                .map(|sample| {
                    let direction = &sample.2;
                    let [x, y, _] = direction.cosines();
                    [y.atan2(x), direction.latitude_rad()]
                })
                .collect::<Vec<_>>();
            let second_derivatives = if spline && times.len() >= 4 {
                spline_second_derivatives(&times, &angles)
            } else {
                Vec::new()
            };
            tracks.insert(
                samples[0].0,
                PointingTrack {
                    times,
                    angles,
                    second_derivatives,
                },
            );
        }
        let geometry = direction_axes()
            .any(|axis| axis.frame != frame)
            .then(|| MsCalEngine::for_calibration(ms))
            .transpose()
            .map_err(|error| error.to_string())?;
        Ok(Self {
            axes,
            source: frame,
            tracks,
            geometry,
        })
    }

    pub(super) fn values(
        &self,
        antenna: i32,
        time: f64,
        values: &mut Vec<[Option<f64>; 2]>,
    ) -> Result<(), String> {
        let sample = |axis: DirectionAxis| -> Result<f64, String> {
            let track = self
                .tracks
                .get(&antenna)
                .filter(|track| track.times.len() >= 4);
            if track.is_none()
                && !axis.spline
                && self.source != axis.frame
                && (self.source == DirectionRef::AZELGEO || axis.frame == DirectionRef::AZELGEO)
            {
                return Err("PlotMS nearest pointing frame conversion with fewer than four samples is pending #1032".into());
            }
            let (angles, epoch) = track.map_or(([0.0, 0.0], time), |track| {
                track.interpolate(time, axis.spline)
            });
            let direction = MDirection::from_angles(angles[0], angles[1], self.source);
            let direction = if self.source == axis.frame {
                direction
            } else {
                let frame = self
                    .geometry
                    .as_ref()
                    .expect("pointing conversion prepared geometry")
                    .antenna_frame(epoch, antenna as usize)
                    .map_err(|error| error.to_string())?
                    .with_epoch(MEpoch::from_mjd_seconds(epoch, EpochRef::UTC));
                direction
                    .convert_to(axis.frame, &frame)
                    .map_err(|error| error.to_string())?
            };
            Ok(if axis.longitude {
                direction.longitude_rad().to_degrees().rem_euclid(360.0)
            } else {
                direction.latitude_rad().to_degrees()
            })
        };
        values.clear();
        for [x, y] in &self.axes.pairs {
            values.push([x.map(sample).transpose()?, y.map(sample).transpose()?]);
        }
        Ok(())
    }
}

impl PointingTrack {
    fn interpolate(&self, time: f64, spline: bool) -> ([f64; 2], f64) {
        // CASA uses the segment start in nearest mode, including the final segment.
        let left = self
            .times
            .partition_point(|&epoch| epoch <= time)
            .saturating_sub(1)
            .min(self.times.len() - 2);
        if !spline {
            return (self.angles[left], self.times[left]);
        }
        let right = left + 1;
        let width = self.times[right] - self.times[left];
        let b = (time - self.times[left]) / width;
        let a = 1.0 - b;
        let angles = std::array::from_fn(|axis| {
            a * self.angles[left][axis]
                + b * self.angles[right][axis]
                + ((a.powi(3) - a) * self.second_derivatives[left][axis]
                    + (b.powi(3) - b) * self.second_derivatives[right][axis])
                    * width.powi(2)
                    / 6.0
        });
        (angles, time)
    }
}

fn spline_second_derivatives(times: &[f64], angles: &[[f64; 2]]) -> Vec<[f64; 2]> {
    let n = times.len();
    let mut upper = vec![0.0; n];
    let mut second = vec![[0.0; 2]; n];
    // Solve the tridiagonal system with zero second derivatives at the endpoints.
    for i in 1..n - 1 {
        let before = times[i] - times[i - 1];
        let after = times[i + 1] - times[i];
        let diagonal = 2.0 * (before + after) - before * upper[i - 1];
        upper[i] = after / diagonal;
        second[i] = std::array::from_fn(|axis| {
            let slope_before = (angles[i][axis] - angles[i - 1][axis]) / before;
            let slope_after = (angles[i + 1][axis] - angles[i][axis]) / after;
            (6.0 * (slope_after - slope_before) - before * second[i - 1][axis]) / diagonal
        });
    }
    for i in (1..n - 1).rev() {
        second[i] = std::array::from_fn(|axis| second[i][axis] - upper[i] * second[i + 1][axis]);
    }
    second
}
