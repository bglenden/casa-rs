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
    panel_for(
        &crate::simutil::graphics::EphemerisPlotRequest {
            source,
            date_mjd: date,
            position: array.config.observatory.clone(),
            measures: run.request.options.measures.clone(),
            use_hour_angle: true,
            measurement_set: Some(&run.measurement_set()),
            duration_hours: 0.0,
        },
        payload,
    )
}

pub(crate) fn panel_for(
    request: &crate::simutil::graphics::EphemerisPlotRequest<'_>,
    payload: &mut Payload,
) -> Result<PlotDocumentPanel, SimobserveError> {
    let date = request.date_mjd;
    let source = &request.source;
    let initial = MEpoch::from_mjd(date, EpochRef::TAI);
    let frame = MeasFrame::new()
        .with_epoch(initial)
        .with_position(request.position.clone())
        .with_measures(request.measures.clone());
    let ha = source
        .convert_to(DirectionRef::HADEC, &frame)
        .map_err(failure)?
        .longitude_rad();
    // MDirection exposes [0, 2π); CASA's hour angle is in (−π, π].
    let ha = direction::wrap_angle(ha, 0.0, std::f64::consts::TAU);
    let peak = date - ha / (2.0 * std::f64::consts::PI);
    let reference = if request.use_hour_angle { peak } else { date };
    let frame = frame.with_epoch(MEpoch::from_mjd(reference, EpochRef::TAI));
    let day = reference.floor();
    let hadec = source
        .convert_to(DirectionRef::HADEC, &frame)
        .map_err(failure)?;
    let ct = -hadec.latitude_rad().tan() * request.position.latitude_rad().tan();
    if ct >= 1.0 {
        return Err(failure("the source is never visible from this observatory"));
    }
    let mut offset = -0.5;
    if ct > -1.0 {
        // The native solver chooses a UTC calendar day. CASA's helper uses
        // the supplied TAI-labelled date's day; at TAI midnight those differ
        // by one day. Noon selects the intended day without changing the
        // solver's physical UTC rise/set times or the plotted frame.
        let rise_frame = frame
            .clone()
            .with_epoch(MEpoch::from_mjd(day + 0.5, EpochRef::TAI));
        let times = source.rise_set_times(&rise_frame).map_err(failure)?;
        let tai = |utc| {
            MEpoch::from_mjd(utc, EpochRef::UTC)
                .convert_to(EpochRef::TAI, &frame)
                .map(|epoch| epoch.value().as_mjd())
                .map_err(failure)
        };
        if tai(times.set_mjd)? < reference {
            offset -= 0.5;
        }
        if tai(times.rise_mjd)? > reference {
            offset += 0.5;
        }
    }
    let mut times = Vec::with_capacity(96);
    let mut elevations = Vec::with_capacity(96);
    let mut time = reference + offset;
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
            [
                (reference - day) * 24.0 - 12.0,
                (reference - day) * 24.0 + 12.0,
            ],
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
    let observed = if let Some(path) = request.measurement_set {
        let ms = MeasurementSet::open(path).map_err(failure)?;
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
        Some((
            [lower, upper].map(|seconds| (seconds / 86400.0 - day) * 24.0),
            89.0,
            3.0,
        ))
    } else if request.duration_hours > 0.0 {
        Some((
            [-0.5, 0.5].map(|sign| sign * request.duration_hours + (peak - day) * 24.0),
            80.0,
            1.5,
        ))
    } else {
        None
    };
    if let Some((times, elevation, width)) = observed {
        let x = payload.values(times)?;
        let y = payload.values([elevation; 2])?;
        let mut observed = layer(
            "observed",
            PlotLayerKind::Line,
            PlotLayerData::Points { x, y },
            Some("r"),
            None,
        );
        observed.line_width = width;
        output.layers.push(observed);
    }
    Ok(output)
}
