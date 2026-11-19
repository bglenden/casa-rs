// SPDX-License-Identifier: LGPL-3.0-or-later
//! Rise and set at a specified elevation, using the tool's explicit frame.

use std::f64::consts::{PI, TAU};

use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::{EpochRef, MEpoch, MeasFrame, MeasureError};
use casa_types::quanta::format::{TIME, YMD, format_angle, format_time};
use casa_types::quanta::{Dimension, UnitDim};
use pyo3::prelude::*;

use super::{PyMeasures, State, frame, no_measures, record::Measures};
use crate::convert::holder::{CasaMeasure, Measure};
use crate::convert::measures::direction_measure;
use crate::convert::value::CasaValue;
use crate::convert::{Variant, quantity};
use crate::tools::LogLine;

const FAILED: &str =
    "Failed to calculate rise/set;\n may be the frames have not been initialized properly";
const NO_COORDINATES: &str = "No rise/set coordinates specified";

enum HorizonCrossings {
    Always(&'static str),
    Angles { rise: f64, set: f64 },
}

fn elevation(value: &Variant) -> Result<f64, &'static str> {
    if matches!(value, Variant::Text(text) if text.is_empty()) {
        return Ok(0.0);
    }
    let quantity = quantity(value).map_err(|_| FAILED)?;
    let base = quantity.get_si_value();
    // Quantum::get("rad") uses the angular equivalent of a time.
    Ok(
        if quantity.unit().val().dim == UnitDim::basis(Dimension::Time) {
            base * (TAU / 86_400.0)
        } else {
            base
        },
    )
}

fn source(state: &mut State, coords: &Variant) -> PyResult<Result<MDirection, &'static str>> {
    let measure = match coords {
        Variant::Record(record) => {
            let Ok(values) = Measures::read_variant(record) else {
                return Ok(Err(FAILED));
            };
            let Ok(target) = values.values[0].refer.parse() else {
                return Ok(Err(FAILED));
            };
            return Ok(values.direction_without_frame(target).map_err(|_| FAILED));
        }
        Variant::Text(_) | Variant::Texts(_) => {
            let data = state.measures().map_err(no_measures)?;
            match direction_measure(coords, &data) {
                Ok(Some(measure)) => measure,
                Ok(None) => return Ok(Err(NO_COORDINATES)),
                Err(_) => return Ok(Err(FAILED)),
            }
        }
        _ => return Ok(Err(NO_COORDINATES)),
    };
    Ok(match measure.to_casa() {
        Ok(CasaMeasure::Direction(direction)) => Ok(direction),
        _ => Err(FAILED),
    })
}

fn crossings(
    state: &mut State,
    source: &MDirection,
    elevation: f64,
    frame: &MeasFrame,
) -> Result<HorizonCrossings, MeasureError> {
    let latitude = frame
        .position()
        .ok_or(MeasureError::MissingFrameData { what: "position" })?
        .geocentric_latitude_rad();
    let (source, distance_m) = frame::resolve_direction(state, source, frame)?;
    let apparent = if source.refer() == DirectionRef::TOPO {
        source.convert_with_parallax(DirectionRef::APP, frame, distance_m)?
    } else {
        source.to_apparent(frame)?
    };
    let hour_angle = match source.refer() {
        DirectionRef::HADEC => source,
        DirectionRef::TOPO => {
            source.convert_with_parallax(DirectionRef::HADEC, frame, distance_m)?
        }
        _ => apparent.convert_with_parallax(DirectionRef::HADEC, frame, distance_m)?,
    };
    let (sin_dec, cos_dec) = hour_angle.latitude_rad().sin_cos();
    let (sin_lat, cos_lat) = latitude.sin_cos();
    let cosine = (elevation.sin() - sin_dec * sin_lat) / (cos_dec * cos_lat);
    if cosine >= 1.0 {
        return Ok(HorizonCrossings::Always("below"));
    }
    if cosine <= -1.0 {
        return Ok(HorizonCrossings::Always("above"));
    }
    let angle = cosine.acos();
    let (ra, _) = crate::convert::holder::direction_angles(apparent.cosines());
    Ok(HorizonCrossings::Angles {
        rise: ra - angle,
        set: ra + angle,
    })
}

fn pair(rise: CasaValue, set: CasaValue) -> CasaValue {
    CasaValue::record([("rise", rise), ("set", set)])
}

fn utc_events(
    rise: f64,
    set: f64,
    frame: &MeasFrame,
    lines: &mut Vec<LogLine>,
) -> Result<CasaValue, MeasureError> {
    // CASA relabels the raw frame epoch as R_UTC, then converts to LAST
    // and razes the result to its whole sidereal day.
    let epoch = frame
        .epoch()
        .ok_or(MeasureError::MissingFrameData { what: "epoch" })?;
    let midnight = MEpoch::new(epoch.value(), EpochRef::UTC)
        .convert_to(EpochRef::LAST, frame)?
        .value()
        .day();
    let normalize = |angle: f64| (angle + PI).rem_euclid(TAU) - PI;
    let rise = normalize(rise);
    let set = normalize(set);
    let set = if set < rise { set + TAU } else { set };
    let to_event = |angle| {
        let last = MEpoch::from_mjd(midnight + angle / TAU, EpochRef::LAST);
        let utc = last.convert_to(EpochRef::UTC, frame)?;
        let utc_text = format_time(utc.value().as_mjd(), YMD, 0);
        let record = CasaValue::record([
            (
                "last",
                Measure::from_casa(&CasaMeasure::Epoch(last)).to_value(),
            ),
            (
                "utc",
                Measure::from_casa(&CasaMeasure::Epoch(utc)).to_value(),
            ),
        ]);
        Ok::<_, MeasureError>((record, utc_text))
    };
    let (rise_event, rise_utc) = to_event(rise)?;
    let (set_event, set_utc) = to_event(set)?;
    lines.push(LogLine::info(format!(
        "LAST of rise= {}",
        format_angle(rise, TIME, 8)
    )));
    lines.push(LogLine::info(format!(
        "LAST of  set= {}",
        format_angle(set, TIME, 8)
    )));
    lines.push(LogLine::info(format!(
        "UTC of rise= {rise_utc}\nUTC of  set= {set_utc}\n"
    )));
    Ok(pair(rise_event, set_event))
}

fn events(
    state: &mut State,
    coords: &Variant,
    height: &Variant,
    as_epochs: bool,
    lines: &mut Vec<LogLine>,
) -> PyResult<CasaValue> {
    let result: PyResult<Result<CasaValue, &'static str>> = (|| {
        let Ok(height) = elevation(height) else {
            return Ok(Err(FAILED));
        };
        let source = match source(state, coords)? {
            Ok(source) => source,
            Err(reason) => return Ok(Err(reason)),
        };
        let Ok(native) = frame::native_frame(state)? else {
            return Ok(Err(FAILED));
        };
        Ok(match crossings(state, &source, height, &native) {
            Ok(HorizonCrossings::Always(which)) => Ok(pair(
                CasaValue::Text(which.into()),
                CasaValue::Text(which.into()),
            )),
            Ok(HorizonCrossings::Angles { rise, set }) if as_epochs => {
                utc_events(rise, set, &native, lines).map_err(|_| FAILED)
            }
            Ok(HorizonCrossings::Angles { rise, set }) => Ok(pair(
                CasaValue::quantity(rise, "rad"),
                CasaValue::quantity(set, "rad"),
            )),
            Err(_) => Err(FAILED),
        })
    })();
    result.map(|result| {
        result.unwrap_or_else(|reason| {
            lines.push(LogLine::error(reason));
            CasaValue::empty_record()
        })
    })
}

#[pymethods]
impl PyMeasures {
    /// Local apparent sidereal angles at which a source crosses an elevation.
    #[pyo3(
        signature = (crd = Variant::Empty, ev = Variant::Empty),
        text_signature = "($self, crd=[], ev=[])"
    )]
    fn rise(&self, py: Python<'_>, crd: Variant, ev: Variant) -> PyResult<Py<PyAny>> {
        self.call(py, |state, lines| events(state, &crd, &ev, false, lines))
    }

    /// Local apparent sidereal and UTC epochs of the elevation crossings.
    #[pyo3(
        signature = (crd = Variant::Empty, ev = Variant::Empty),
        text_signature = "($self, crd=[], ev=[])"
    )]
    fn riseset(&self, py: Python<'_>, crd: Variant, ev: Variant) -> PyResult<Py<PyAny>> {
        self.call(py, |state, lines| events(state, &crd, &ev, true, lines))
    }
}
