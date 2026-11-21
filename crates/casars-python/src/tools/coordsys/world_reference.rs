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

use super::inspect::coordinate_type;
use super::{CoordinateState, PyCoordsys};

pub(in crate::tools) mod spectral_text;

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

pub(in crate::tools) fn scientific_with_precision(value: f64, precision: usize) -> String {
    let text = format!("{value:.precision$e}");
    let Some((mantissa, exponent)) = text.split_once('e') else {
        return format_stream_value_with_precision(value, precision);
    };
    let exponent: i32 = exponent.parse().expect("exponent is decimal");
    format!("{mantissa}e{exponent:+03}")
}

pub(in crate::tools) fn format_number(
    value: f64,
    increment: f64,
    scientific_style: bool,
) -> String {
    let precision = if scientific_style { 8 } else { 6 };
    format_number_with_precision(value, increment, scientific_style, precision)
}

pub(in crate::tools) fn format_number_with_precision(
    value: f64,
    increment: f64,
    scientific_style: bool,
    mut precision: usize,
) -> String {
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

pub(in crate::tools) fn format_direction(
    frame: DirectionRef,
    axis: usize,
    radians: f64,
    absolute: bool,
    scientific_style: bool,
    precision: usize,
) -> (String, &'static str) {
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
            return (format_latitude_dms_with_precision(radians, precision), "");
        }
        if hour_axis {
            let signed = signed_angle(radians);
            let sign = if signed.is_sign_negative() { '-' } else { '+' };
            return (
                format!(
                    "{sign}{}",
                    format_longitude_hours_with_precision(signed.abs(), precision)
                ),
                "",
            );
        }
        return (
            format_longitude_hours_with_precision(radians, precision),
            "",
        );
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
    let text = if scientific_style || !angle_frame {
        scientific_with_precision(number, precision)
    } else {
        format!("{number:.precision$}")
    };
    (text, unit)
}

fn direction_text(
    frame: DirectionRef,
    axis: usize,
    radians: f64,
    absolute: bool,
    scientific_style: bool,
) -> String {
    let (text, unit) = format_direction(frame, axis, radians, absolute, scientific_style, 8);
    if unit.is_empty() {
        text
    } else {
        format!("{text} {unit}")
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
    fn at_world(
        coordinate: &'a CoordinateModel,
        record: Option<&'a RecordValue>,
        native: &[f64],
        visible_axes: Vec<usize>,
        full_reference: Option<Vec<f64>>,
        converted: bool,
    ) -> PyResult<Self> {
        let full_units = super::inspect::units(coordinate);
        let units: Vec<_> = visible_axes
            .iter()
            .map(|&axis| full_units[axis].clone())
            .collect();
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
        let full_measure = full_reference.map(|mut components| {
            for (&axis, &value) in visible_axes.iter().zip(native) {
                components[axis] = value;
            }
            (components, coordinate.axis_units())
        });
        Ok(Self {
            coordinate,
            record,
            native: native.to_vec(),
            declared,
            units,
            converted,
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
    let coords = &state.coordinates;
    let world = if absolute {
        coords.reference_value()
    } else {
        coords.increment()
    };
    let selected = if kind.is_empty() {
        None
    } else {
        let kind = coordinate_type(kind);
        match coords.find_coordinate(kind) {
            Some(index) => Some(index),
            None if !absolute => return Ok(CasaValue::empty_record()),
            None => {
                return Err(PyRuntimeError::new_err(format!(
                    "A coordinate of type {kind} does not exist"
                )));
            }
        }
    };
    if let Some(index) = selected {
        let entry = CoordinateSystem::record_coordinates(&state.record)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let value = axis_values(state, &world, index, entry.get(index), absolute, false)?;
        let axes: Vec<_> = (0..value.native.len()).map(|axis| (0, axis)).collect();
        // CASA uses the full world's style order when a type is selected.
        let styles = world_order(coords)
            .map(|(coordinate, _)| scientific_coordinate(coords.coordinate(coordinate)));
        return format_world_record(
            &[value],
            &axes,
            styles,
            &format.to_ascii_lowercase(),
            absolute,
            true,
            true,
        );
    }
    format_world_at(state, &world, format, absolute, true, true, false)
}

fn world_order(coords: &CoordinateSystem) -> impl Iterator<Item = (usize, usize)> {
    (0..coords.n_world_axes()).map(|axis| coords.world_axis(axis).expect("dense world axes"))
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
    format_world_at(
        state,
        world,
        format,
        true,
        velocities,
        coordinate_tags,
        true,
    )
}

pub(super) fn format_world_offsets(
    state: &CoordinateState,
    world: &[f64],
    format: &str,
    absolute: bool,
) -> PyResult<CasaValue> {
    if !absolute && format.contains('m') {
        for index in 0..state.coordinates.n_coordinates() {
            let name = match state.coordinates.coordinate(index).coordinate_type() {
                CoordinateType::Direction => "casacore::MDirection",
                CoordinateType::Spectral => "MFrequency",
                CoordinateType::Stokes => "Stokes",
                _ => continue,
            };
            return Err(PyRuntimeError::new_err(format!(
                "It is not possible to have a relative {name} measure"
            )));
        }
    }
    let mut result = format_world_at(state, world, format, absolute, true, true, false)?;
    if !absolute
        && let CasaValue::Record(fields) = &mut result
        && let Some((_, tag)) = fields.iter_mut().find(|(name, _)| name == "ar_type")
    {
        *tag = CasaValue::Text("relative".into());
    }
    Ok(result)
}

fn format_world_at(
    state: &CoordinateState,
    world: &[f64],
    format: &str,
    absolute: bool,
    velocities: bool,
    coordinate_tags: bool,
    converted: bool,
) -> PyResult<CasaValue> {
    let format = format.to_ascii_lowercase();
    let placements = if format.contains('s') || format.contains('m') {
        CoordinateSystem::record_coordinates(&state.record)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?
    } else {
        Vec::new()
    };
    let coords = &state.coordinates;
    let values = (0..coords.n_coordinates())
        .map(|index| {
            axis_values(
                state,
                world,
                index,
                placements.get(index),
                absolute,
                converted,
            )
        })
        .collect::<PyResult<Vec<_>>>()?;
    let axes: Vec<_> = world_order(coords)
        .map(|(index, local)| {
            let axis = values[index]
                .visible_axes
                .iter()
                .position(|&axis| axis == local)
                .expect("visible world axis");
            (index, axis)
        })
        .collect();
    let styles = axes
        .iter()
        .map(|&(index, _)| scientific_coordinate(values[index].coordinate));
    format_world_record(
        &values,
        &axes,
        styles,
        &format,
        absolute,
        velocities,
        coordinate_tags,
    )
}

fn axis_values<'a>(
    state: &'a CoordinateState,
    world: &[f64],
    index: usize,
    placement: Option<&casa_coordinates::RecordCoordinate>,
    absolute: bool,
    converted: bool,
) -> PyResult<Values<'a>> {
    let coords = &state.coordinates;
    let coordinate = coords.coordinate(index);
    let record = placement.and_then(|entry| match state.record.get(&entry.field) {
        Some(Value::Record(record)) => Some(record),
        _ => None,
    });
    let map = coords.world_axes(index);
    let visible: Vec<_> = map
        .iter()
        .enumerate()
        .filter_map(|(local, axis)| axis.map(|axis| (local, world[axis])))
        .collect();
    let (visible_axes, native): (Vec<_>, Vec<_>) = visible.into_iter().unzip();
    let full = (absolute && map.contains(&None)).then(|| coordinate.reference_value());
    Values::at_world(coordinate, record, &native, visible_axes, full, converted)
}

fn format_world_record(
    values: &[Values<'_>],
    axes: &[(usize, usize)],
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
                axes.iter()
                    .map(|&(index, axis)| values[index].declared[axis])
                    .collect(),
            ),
        ));
    }
    if format.contains('q') {
        record.push((
            "quantity",
            numbered_quantities(axes.iter().map(|&(index, axis)| {
                (
                    values[index].declared[axis],
                    values[index].units[axis].as_str(),
                )
            })),
        ));
    }
    if format.contains('s') {
        let mut strings = Vec::new();
        for &(index, axis) in axes {
            strings.push(values[index].text(axis, absolute, styles.next().unwrap_or(true))?);
        }
        record.push(("string", CasaValue::Texts(strings)));
    }
    if format.contains('m') {
        let mut measures = std::collections::BTreeMap::new();
        for value in values.iter().filter(|value| !value.native.is_empty()) {
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
