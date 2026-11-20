// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::display::{
    format_latitude_dms_with_precision, format_longitude_hours_with_precision,
    format_stream_value_with_precision,
};
use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType, StokesType};
use casa_types::measures::{
    MeasFrame,
    direction::{DirectionRef, MDirection},
    doppler::{DopplerRef, MDoppler},
};
use casa_types::{RecordValue, Value};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::convert::value::CasaValue;

use super::inspect::{coordinate_type, numbered_coordinates};
use super::{CoordinateState, PyCoordsys};

mod spectral_text;

fn quantity(value: f64, unit: &str) -> CasaValue {
    CasaValue::record([
        ("unit", CasaValue::Text(unit.into())),
        ("value", CasaValue::Float(value)),
    ])
}

fn numbered_quantities<'a>(values: impl Iterator<Item = (f64, &'a str)>) -> CasaValue {
    CasaValue::record(
        values
            .enumerate()
            .map(|(index, (value, unit))| (format!("*{}", index + 1), quantity(value, unit))),
    )
}

fn stokes(value: f64) -> String {
    StokesType::from_code(value.round() as i32)
        .unwrap_or(StokesType::Undefined)
        .name()
        .into()
}

fn scalar_measure(kind: &str, frame: &str, value: f64, unit: &str) -> CasaValue {
    CasaValue::record([
        ("type", CasaValue::Text(kind.into())),
        ("refer", CasaValue::Text(frame.into())),
        ("m0", quantity(value, unit)),
    ])
}

fn scientific(value: f64) -> String {
    scientific_with_precision(value, 8)
}

fn scientific_with_precision(value: f64, precision: usize) -> String {
    let text = format!("{value:.precision$e}");
    let Some((mantissa, exponent)) = text.split_once('e') else {
        return format_stream_value_with_precision(value, precision);
    };
    let exponent: i32 = exponent.parse().expect("exponent is decimal");
    format!("{mantissa}e{exponent:+03}")
}

fn format_number(value: f64, increment: f64, scientific_style: bool) -> String {
    let mut precision = if scientific_style { 8 } else { 6 };
    if value.is_finite() && value.fract() != 0.0 && increment.is_finite() {
        let mut needed = 1;
        let mut scale = 1.0;
        while value.abs() > scale {
            needed += 1;
            scale *= 10.0;
        }
        let increment = increment.abs();
        if increment < 1.0 {
            let mut scale = 0.1;
            while increment < scale {
                needed += 1;
                scale /= 10.0;
            }
        } else {
            let mut adjustment = 1;
            let mut scale = 1.0;
            while increment > scale {
                adjustment += 1;
                scale *= 10.0;
            }
            if adjustment < needed {
                needed -= adjustment;
            }
        }
        if needed > 5 {
            precision = needed + 1;
        }
    }
    if scientific_style {
        scientific_with_precision(value, precision)
    } else {
        format_stream_value_with_precision(value, precision)
    }
}

fn signed_angle(radians: f64) -> f64 {
    let (sin, cos) = radians.sin_cos();
    sin.atan2(cos)
}

fn direction_text(
    frame: DirectionRef,
    axis: usize,
    radians: f64,
    absolute: bool,
    scientific_style: bool,
) -> String {
    let radians = if radians == 0.0 { 0.0 } else { radians };
    let name = frame.as_str();
    let angle_frame = matches!(
        name,
        "GALACTIC"
            | "SUPERGAL"
            | "ECLIPTIC"
            | "MECLIPTIC"
            | "TECLIPTIC"
            | "AZEL"
            | "AZELSW"
            | "AZELGEO"
            | "AZELSWGEO"
    );
    let hour_axis = axis == 0 && name == "HADEC";
    if absolute && !scientific_style && !angle_frame {
        if axis != 0 {
            return format_latitude_dms_with_precision(radians, 8);
        }
        if hour_axis {
            let signed = signed_angle(radians);
            let sign = if signed.is_sign_negative() { '-' } else { '+' };
            return format!(
                "{sign}{}",
                format_longitude_hours_with_precision(signed.abs(), 8)
            );
        }
        return format_longitude_hours_with_precision(radians, 8);
    }
    let (number, unit) = if hour_axis {
        let hour_angle = if absolute {
            signed_angle(radians)
        } else {
            radians
        };
        let hours = hour_angle * 12.0 / std::f64::consts::PI;
        (hours, "h")
    } else if !absolute {
        (radians.to_degrees() * 3600.0, "arcsec")
    } else {
        let degrees = radians.to_degrees();
        let degrees = if axis != 0 {
            degrees
        } else if matches!(name, "ECLIPTIC" | "MECLIPTIC" | "TECLIPTIC") {
            signed_angle(radians).to_degrees()
        } else {
            degrees.rem_euclid(360.0)
        };
        (degrees, "deg")
    };
    if scientific_style || !angle_frame {
        format!("{} {unit}", scientific(number))
    } else {
        format!("{number:.8} {unit}")
    }
}

struct Values<'a> {
    coordinate: &'a CoordinateModel,
    record: Option<&'a RecordValue>,
    native: Vec<f64>,
    declared: Vec<f64>,
    units: Vec<String>,
    converted: bool,
    visible_axes: Vec<usize>,
    full_measure: Option<(Vec<f64>, Vec<String>)>,
}

impl<'a> Values<'a> {
    fn new(
        coordinate: &'a CoordinateModel,
        record: Option<&'a RecordValue>,
        absolute: bool,
    ) -> Self {
        let native = if absolute {
            coordinate.reference_value()
        } else {
            coordinate.increment()
        };
        let (declared, units) = match coordinate {
            CoordinateModel::Direction(direction) => {
                let (reference, increment) = direction.reference_and_increment_in_world_units();
                (
                    if absolute { reference } else { increment },
                    direction.visible_world_units(),
                )
            }
            _ => (native.clone(), coordinate.axis_units()),
        };
        let visible_axes = (0..native.len()).collect();
        Self {
            coordinate,
            record,
            native,
            declared,
            units,
            converted: false,
            visible_axes,
            full_measure: None,
        }
    }

    fn at_world(
        coordinate: &'a CoordinateModel,
        record: Option<&'a RecordValue>,
        native: &[f64],
        visible_axes: Vec<usize>,
        full_coordinate: Option<CoordinateModel>,
    ) -> PyResult<Self> {
        let units = match coordinate {
            CoordinateModel::Direction(direction) => direction.visible_world_units(),
            _ => coordinate.axis_units(),
        };
        let declared = if matches!(coordinate, CoordinateModel::Direction(_)) {
            use casa_types::quanta::{Quantity, Unit};
            native
                .iter()
                .zip(&units)
                .map(|(&value, unit)| {
                    Unit::new(unit)
                        .and_then(|unit| Quantity::new(value, "rad")?.get_value_in(&unit))
                        .map_err(|error| PyRuntimeError::new_err(error.to_string()))
                })
                .collect::<PyResult<_>>()?
        } else {
            native.to_vec()
        };
        let full_measure = full_coordinate.map(|full| {
            let mut components = full.reference_value();
            for (&axis, &value) in visible_axes.iter().zip(native) {
                components[axis] = value;
            }
            (components, full.axis_units())
        });
        Ok(Self {
            coordinate,
            record,
            native: native.to_vec(),
            declared,
            units,
            converted: true,
            visible_axes,
            full_measure,
        })
    }

    fn quantities(&self) -> impl Iterator<Item = (f64, &str)> {
        self.declared
            .iter()
            .copied()
            .zip(self.units.iter().map(String::as_str))
    }

    fn text(&self, axis: usize, absolute: bool, scientific_style: bool) -> PyResult<String> {
        let value = self.declared[axis];
        let unit = &self.units[axis];
        Ok(match self.coordinate {
            CoordinateModel::Stokes(_) => stokes(value),
            CoordinateModel::Direction(direction) => direction_text(
                direction.direction_ref(),
                self.visible_axes[axis],
                self.native[axis],
                absolute,
                scientific_style,
            ),
            CoordinateModel::Spectral(spectral) => {
                return spectral_text::text(
                    spectral,
                    self.record,
                    value,
                    unit,
                    absolute,
                    scientific_style,
                );
            }
            _ => {
                let mut text =
                    format_number(value, self.coordinate.increment()[0], scientific_style);
                if !unit.trim().is_empty() {
                    text.push(' ');
                    text.push_str(unit);
                }
                text
            }
        })
    }

    fn measure(&self, absolute: bool, velocities: bool) -> PyResult<(&'static str, CasaValue)> {
        let invalid = |name| {
            PyRuntimeError::new_err(format!(
                "It is not possible to have a relative {name} measure"
            ))
        };
        let components = self
            .full_measure
            .as_ref()
            .map_or(self.native.as_slice(), |(value, _)| value.as_slice());
        match self.coordinate {
            CoordinateModel::Linear(_) => {
                let units = self
                    .full_measure
                    .as_ref()
                    .map_or(self.units.as_slice(), |(_, units)| units.as_slice());
                Ok((
                    "linear",
                    numbered_quantities(
                        components
                            .iter()
                            .copied()
                            .zip(units.iter().map(String::as_str)),
                    ),
                ))
            }
            CoordinateModel::Tabular(_) => Ok(("tabular", numbered_quantities(self.quantities()))),
            CoordinateModel::Stokes(_) if absolute => {
                Ok(("stokes", CasaValue::Text(stokes(self.native[0]))))
            }
            CoordinateModel::Direction(direction) if absolute && components.len() == 2 => {
                let frame = if self.converted {
                    direction.conversion_direction_ref()
                } else {
                    direction.direction_ref()
                };
                let angle = MDirection::from_angles(components[0], components[1], frame);
                let [x, y, _] = angle.cosines();
                Ok((
                    "direction",
                    CasaValue::record([
                        ("type", CasaValue::Text("direction".into())),
                        ("refer", CasaValue::Text(frame.to_string())),
                        ("m0", quantity(y.atan2(x), "rad")),
                        ("m1", quantity(angle.latitude_rad(), "rad")),
                    ]),
                ))
            }
            CoordinateModel::Spectral(spectral) if absolute => {
                let hz = spectral
                    .value_in_hz(self.declared[0])
                    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
                let frame = if spectral.has_undefined_frame() {
                    "Undefined".to_owned()
                } else if self.converted {
                    spectral.world_frequency_ref().to_string()
                } else {
                    spectral.frequency_ref().to_string()
                };
                let mut record = vec![("frequency", scalar_measure("frequency", &frame, hz, "Hz"))];
                if !velocities {
                    return Ok(("spectral", CasaValue::record(record)));
                }
                let rest = spectral
                    .rest_frequency_hz()
                    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
                if rest <= 0.0 {
                    return Err(PyRuntimeError::new_err("velocity is NaN"));
                }
                let ratio = MDoppler::new(hz.abs() / rest, DopplerRef::RATIO);
                for (name, frame, kind) in [
                    ("radiovelocity", "RADIO", DopplerRef::RADIO),
                    ("opticalvelocity", "OPTICAL", DopplerRef::Z),
                    ("betavelocity", "TRUE", DopplerRef::BETA),
                ] {
                    let velocity = ratio
                        .convert_to(kind, &MeasFrame::new())
                        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?
                        .value()
                        * spectral_text::SPEED_OF_LIGHT;
                    record.push((name, scalar_measure("doppler", frame, velocity, "m/s")));
                }
                Ok(("spectral", CasaValue::record(record)))
            }
            CoordinateModel::Direction(_) if absolute => Err(PyRuntimeError::new_err(
                "Direction measure requires its two axes",
            )),
            CoordinateModel::Direction(_) => Err(invalid("casacore::MDirection")),
            CoordinateModel::Spectral(_) => Err(invalid("MFrequency")),
            CoordinateModel::Stokes(_) => Err(invalid("Stokes")),
        }
    }
}

fn world_reference(
    state: &CoordinateState,
    format: &str,
    kind: &str,
    absolute: bool,
) -> PyResult<CasaValue> {
    let format = format.to_ascii_lowercase();
    let complete =
        || (0..state.coordinates.n_coordinates()).map(|index| state.coordinates.coordinate(index));
    let raw = |name: &str| match state.record.get(name) {
        Some(Value::Record(record)) => Some(record),
        _ => None,
    };
    let selected = if kind.is_empty() {
        let placements = if format.contains('s') {
            CoordinateSystem::record_coordinates(&state.record)
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?
        } else {
            Vec::new()
        };
        complete()
            .enumerate()
            .map(|(index, coordinate)| {
                (
                    coordinate,
                    placements.get(index).and_then(|entry| raw(&entry.field)),
                )
            })
            .collect::<Vec<_>>()
    } else {
        let kind = coordinate_type(kind);
        let Some((_, coordinate, entry)) = numbered_coordinates(state)?
            .into_iter()
            .find(|(_, coordinate, _)| coordinate.coordinate_type() == kind)
        else {
            if absolute {
                return Err(PyRuntimeError::new_err(format!(
                    "A coordinate of type {kind} does not exist"
                )));
            }
            return Ok(CasaValue::empty_record());
        };
        vec![(coordinate, raw(&entry.field))]
    };
    let values = selected
        .into_iter()
        .map(|(coordinate, record)| Values::new(coordinate, record, absolute))
        .collect::<Vec<_>>();
    let styles = complete().flat_map(|coordinate| {
        let scientific = scientific_coordinate(coordinate);
        std::iter::repeat_n(scientific, coordinate.n_world_axes())
    });
    format_world_record(&values, styles, &format, absolute, true, true)
}

fn scientific_coordinate(coordinate: &CoordinateModel) -> bool {
    !matches!(
        coordinate.coordinate_type(),
        CoordinateType::Direction | CoordinateType::Stokes
    )
}

pub(super) fn format_transformed_world(
    state: &CoordinateState,
    world: &[f64],
    format: &str,
    velocities: bool,
    coordinate_tags: bool,
) -> PyResult<CasaValue> {
    let format = format.to_ascii_lowercase();
    let placements = if format.contains('s') || format.contains('m') {
        CoordinateSystem::record_coordinates(&state.record)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?
    } else {
        Vec::new()
    };
    let mut offset = 0;
    let mut values = Vec::with_capacity(state.coordinates.n_coordinates());
    for index in 0..state.coordinates.n_coordinates() {
        let coordinate = state.coordinates.coordinate(index);
        let placement = placements.get(index);
        let record = placement.and_then(|entry| match state.record.get(&entry.field) {
            Some(Value::Record(record)) => Some(record),
            _ => None,
        });
        let end = offset + coordinate.n_world_axes();
        let visible_axes = placement.map_or_else(
            || (0..end - offset).collect(),
            |placement| {
                placement
                    .pixel_axes
                    .iter()
                    .enumerate()
                    .filter_map(|(local, axis)| axis.map(|_| local))
                    .collect()
            },
        );
        let full_coordinate = placement
            .filter(|placement| format.contains('m') && placement.pixel_axes.contains(&None))
            .map(|placement| placement.model(&state.record))
            .transpose()
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        values.push(Values::at_world(
            coordinate,
            record,
            &world[offset..end],
            visible_axes,
            full_coordinate,
        )?);
        offset = end;
    }
    let styles = values.iter().flat_map(|axis| {
        std::iter::repeat_n(scientific_coordinate(axis.coordinate), axis.native.len())
    });
    format_world_record(&values, styles, &format, true, velocities, coordinate_tags)
}

fn format_world_record(
    values: &[Values<'_>],
    mut styles: impl Iterator<Item = bool>,
    format: &str,
    absolute: bool,
    velocities: bool,
    coordinate_tags: bool,
) -> PyResult<CasaValue> {
    let mut record = Vec::new();
    if coordinate_tags {
        record.extend([
            ("ar_type", CasaValue::Text("absolute".into())),
            ("pw_type", CasaValue::Text("world".into())),
        ]);
    }
    if format.contains('n') {
        record.push((
            "numeric",
            CasaValue::floats(
                values
                    .iter()
                    .flat_map(|axes| axes.declared.iter().copied())
                    .collect(),
            ),
        ));
    }
    if format.contains('q') {
        record.push((
            "quantity",
            numbered_quantities(values.iter().flat_map(Values::quantities)),
        ));
    }
    if format.contains('s') {
        let mut strings = Vec::new();
        for value in values {
            for axis in 0..value.native.len() {
                strings.push(value.text(axis, absolute, styles.next().unwrap_or(true))?);
            }
        }
        record.push(("string", CasaValue::Texts(strings)));
    }
    if format.contains('m') {
        let mut measures = std::collections::BTreeMap::new();
        for value in values {
            match value.measure(absolute, velocities) {
                Ok((key, measure)) => {
                    measures.insert(key, measure);
                }
                Err(_) if !absolute => return Ok(CasaValue::empty_record()),
                Err(error) => return Err(error),
            }
        }
        record.push(("measure", CasaValue::record(measures)));
    }
    Ok(CasaValue::record(record))
}

#[pymethods]
impl PyCoordsys {
    #[pyo3(signature = (format = "n", r#type = ""), text_signature = "($self, format='n', type='')")]
    fn increment(&self, py: Python<'_>, format: &str, r#type: &str) -> PyResult<Py<PyAny>> {
        py.detach(|| world_reference(&self.state(), format, r#type, false))?
            .into_python(py)
    }

    #[pyo3(signature = (format = "n", r#type = ""), text_signature = "($self, format='n', type='')")]
    fn referencevalue(&self, py: Python<'_>, format: &str, r#type: &str) -> PyResult<Py<PyAny>> {
        py.detach(|| world_reference(&self.state(), format, r#type, true))?
            .into_python(py)
    }
}
