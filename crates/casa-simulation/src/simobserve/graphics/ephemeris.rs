// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's 96-sample elevation curve, transit and observation time range.

use casa_ms::{
    MeasurementSet,
    plot_document::{PlotDocumentPanel, PlotLayerData, PlotLayerKind},
};
use casa_types::ArrayValue;
use casa_types::measures::{
    EpochRef, MEpoch, MeasFrame,
    direction::{DirectionRef, MDirection},
};
use casa_types::quanta::MvTime;

use super::document::{layer, panel};
use super::payload::Payload;
use super::{Array, Pointings, Run, SimobserveError, failure};
use crate::simutil::{direction, qa};

pub(super) fn prepare(
    run: &Run<'_>,
    array: &Array,
    pointings: &Pointings,
    payload: &mut Payload,
) -> Result<PlotDocumentPanel, SimobserveError> {
    let centre = direction::median_direction(&pointings.pointings)?.centre;
    let (refer, angles) = direction::direction_measure(&centre)?;
    let source = MDirection::from_angles(angles[0], angles[1], refer);
    let date = qa::value_in(&qa::read(&run.parameters.refdate)?, "d")?;
    let initial = MEpoch::from_mjd(date, EpochRef::TAI);
    let frame = MeasFrame::new()
        .with_epoch(initial)
        .with_position(array.config.observatory.clone())
        .with_measures(run.request.options.measures.clone());
    let ha = source
        .convert_to(DirectionRef::HADEC, &frame)
        .map_err(failure)?
        .longitude_rad();
    // MDirection exposes [0, 2π); CASA's hour angle is in (−π, π].
    let ha = direction::wrap_angle(ha, 0.0, std::f64::consts::TAU);
    let peak = date - ha / (2.0 * std::f64::consts::PI);
    let frame = frame.with_epoch(MEpoch::from_mjd(peak, EpochRef::TAI));
    let day = peak.floor();
    let hadec = source
        .convert_to(DirectionRef::HADEC, &frame)
        .map_err(failure)?;
    let ct = -hadec.latitude_rad().tan() * array.config.observatory.latitude_rad().tan();
    if ct >= 1.0 {
        return Err(failure("the source is never visible from this observatory"));
    }
    let mut offset = -0.5;
    if ct > -1.0 {
        let times = source.rise_set_times(&frame).map_err(failure)?;
        let tai = |utc| {
            MEpoch::from_mjd(utc, EpochRef::UTC)
                .convert_to(EpochRef::TAI, &frame)
                .map(|epoch| epoch.value().as_mjd())
                .map_err(failure)
        };
        if tai(times.set_mjd)? < peak {
            offset -= 0.5;
        }
        if tai(times.rise_mjd)? > peak {
            offset += 0.5;
        }
    }
    let mut times = Vec::with_capacity(96);
    let mut elevations = Vec::with_capacity(96);
    let mut time = peak + offset;
    for _ in 0..96 {
        times.push((time - day) * 24.0);
        let frame = frame
            .clone()
            .with_epoch(MEpoch::from_mjd(time, EpochRef::TAI));
        elevations.push(
            source
                .convert_to(DirectionRef::AZEL, &frame)
                .map_err(failure)?
                .latitude_rad()
                .to_degrees(),
        );
        time += 1.0 / 96.0;
    }
    let label = format!(
        "hours relative to {}/00:00:00",
        MvTime::from_mjd_days(day).format_dmy_date()
    );
    let mut output = panel(
        "ephemeris",
        "",
        [&label, "elevation"],
        [
            [(peak - day) * 24.0 - 12.0, (peak - day) * 24.0 + 12.0],
            [0.0, 90.0],
        ],
        false,
    );
    let x = payload.values(times)?;
    let y = payload.values(elevations)?;
    output.layers.push(layer(
        "elevation",
        PlotLayerKind::Line,
        PlotLayerData::Points { x, y },
        None,
        None,
    ));
    let x = payload.values([(peak - day) * 24.0; 2])?;
    let y = payload.values([0.0, 90.0])?;
    output.layers.push(layer(
        "transit",
        PlotLayerKind::Line,
        PlotLayerData::Points { x, y },
        None,
        None,
    ));
    let ms = MeasurementSet::open(run.measurement_set()).map_err(failure)?;
    let observation = ms.observation().map_err(failure)?;
    let mut lower = f64::INFINITY;
    let mut upper = f64::NEG_INFINITY;
    for row in 0..observation.row_count() {
        let values = observation.array(row, "TIME_RANGE").map_err(failure)?;
        if let ArrayValue::Float64(values) = values {
            for &value in values {
                lower = lower.min(value);
                upper = upper.max(value);
            }
        }
    }
    let x = payload.values([lower, upper].map(|seconds| (seconds / 86400.0 - day) * 24.0))?;
    let y = payload.values([89.0; 2])?;
    let mut observed = layer(
        "observed",
        PlotLayerKind::Line,
        PlotLayerData::Points { x, y },
        Some("r"),
        None,
    );
    observed.line_width = 3.0;
    output.layers.push(observed);
    Ok(output)
}
