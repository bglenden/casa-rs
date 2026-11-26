// SPDX-License-Identifier: LGPL-3.0-or-later
//! Direction offsets on the sphere, north through east.

use casa_types::quanta::{Quantity, UnitVal};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::{PyMeasures, arguments, record::Measures};
use crate::convert::holder::{MeasureValue, direction_angles, direction_cosines};
use crate::convert::value::CasaValue;
use crate::convert::{RecordArg, Variant, quantity, quantity_variant_record};
use crate::tools::LogLine;

fn angular_value(value: &Variant, name: &str) -> Result<f64, String> {
    let empty = || "Empty QuantumHolder argument for asQuantumDouble".to_owned();
    let quantity = match value {
        Variant::Text(text) => Quantity::read(text).map_err(|_| empty())?,
        Variant::Texts(_) => return Err(empty()),
        Variant::Record(record) => quantity_variant_record(record).map_err(|error| {
            let message = error.to_string();
            if message.starts_with("Illegal Quantum record") {
                empty()
            } else {
                message
            }
        })?,
        other => quantity(other).map_err(|error| error.to_string())?,
    };
    if !quantity.unit().val().conformant(&UnitVal::ANGLE) {
        return Err(format!("Exception: {name} must be an angular quantity."));
    }
    Ok(quantity.get_si_value())
}

fn shifted_record(
    v: &RecordArg,
    offset: &Variant,
    position_angle: &Variant,
    lines: &mut Vec<LogLine>,
) -> Result<CasaValue, String> {
    let offset = angular_value(offset, "offset")?;
    let pa = angular_value(position_angle, "pa")?;
    let mut direction = Measures::read(v)
        .map_err(|_| "Exception: v is not a valid measure.".to_string())?
        .into_reference_origin();
    if let Some(warning) = direction.warning.take() {
        lines.push(LogLine::warning(warning));
    }
    let value = &mut direction.values[0].value;
    let MeasureValue::Direction(original) = *value else {
        return Err("Exception: v is not a direction measure.".into());
    };
    let (longitude, latitude) = direction_angles(original);

    let (sin_lat, cos_lat) = latitude.sin_cos();
    let (sin_offset, cos_offset) = offset.sin_cos();
    let (sin_pa, cos_pa) = pa.sin_cos();
    let new_latitude = (cos_offset * sin_lat + sin_offset * cos_lat * cos_pa)
        .clamp(-1.0, 1.0)
        .asin();
    // The two components retain the longitude quadrant for large shifts
    // and when the great circle crosses a pole.
    let east = sin_offset * sin_pa;
    let meridian = cos_offset * cos_lat - sin_offset * sin_lat * cos_pa;
    let longitude_offset = if latitude.abs() == std::f64::consts::FRAC_PI_2 {
        // A direction starting exactly at a pole has no unique north axis.
        // Keep the casacore convention for that position angle.
        (east / new_latitude.cos()).clamp(-1.0, 1.0).asin()
    } else {
        east.atan2(meridian)
    };
    let new_longitude = longitude + longitude_offset;
    *value = MeasureValue::Direction(direction_cosines(new_longitude, new_latitude));
    Ok(direction.to_value())
}

#[pymethods]
impl PyMeasures {
    /// Shift a direction by an angular offset at a given position angle.
    #[pyo3(
        signature = (v = None, offset = Variant::Empty, pa = Variant::Empty),
        text_signature = "($self, v={}, offset=[], pa=[])"
    )]
    fn shift(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with=arguments::supplied)] v: Option<Bound<'_, PyAny>>,
        offset: Variant,
        pa: Variant,
    ) -> PyResult<Py<PyAny>> {
        let v = arguments::record(v.as_ref(), "v")?;
        self.call(py, |_, lines| {
            shifted_record(&v, &offset, &pa, lines).map_err(|error| {
                lines.push(LogLine::error(format!("Exception Reports: {error}")));
                PyRuntimeError::new_err(error)
            })
        })
    }
}
