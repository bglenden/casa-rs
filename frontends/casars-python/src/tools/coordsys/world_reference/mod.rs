// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{CoordinateModel, CoordinateType, QualityType, StokesType};
use casa_core::measures::{
    MeasFrame,
    direction::{DirectionRef, MDirection},
    doppler::{DopplerRef, MDoppler},
};
use casa_core::{RecordValue, Value};
pub(in crate::tools) use casa_imageanalysis::tool::coords::{
    format_direction, format_number, scientific_with_precision,
};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::convert::value::CasaValue;

use super::inspect::{axis_values, coordinate_type, numbered_coordinates};
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

pub(in crate::tools) fn nested_axis_display(
    model: &CoordinateModel,
    axis: usize,
    value: f64,
    increment: f64,
) -> PyResult<Option<(String, String, String)>> {
    casa_imageanalysis::tool::coords::nested_axis_display(model, axis, value, increment)
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))
}

struct Values<'a> {
    coordinate: &'a CoordinateModel,
    record: Option<&'a RecordValue>,
    native: Vec<f64>,
    declared: Vec<f64>,
    units: Vec<String>,
    converted: bool,
    visible_axes: Vec<usize>,
    world_order: Vec<usize>,
    full_measure: Option<(Vec<f64>, Vec<String>)>,
}

impl<'a> Values<'a> {
    fn new(
        coordinate: &'a CoordinateModel,
        record: Option<&'a RecordValue>,
        absolute: bool,
        axis_map: &[Option<usize>],
        full: Option<CoordinateModel>,
    ) -> Self {
        let full_measure = full.map(|model| (model.reference_value(), model.axis_units()));
        let native = if absolute {
            coordinate.reference_value()
        } else {
            coordinate.increment()
        };
        let (reference, increment) = coordinate.reference_and_increment_in_declared_units();
        let declared = if absolute { reference } else { increment };
        let units = coordinate.declared_axis_units();
        let visible_axes: Vec<_> = axis_map
            .iter()
            .enumerate()
            .filter_map(|(local, axis)| axis.map(|_| local))
            .collect();
        let native = visible_axes.iter().map(|&axis| native[axis]).collect();
        let declared = visible_axes.iter().map(|&axis| declared[axis]).collect();
        let units = visible_axes
            .iter()
            .map(|&axis| units[axis].clone())
            .collect();
        Self {
            coordinate,
            record,
            native,
            declared,
            units,
            converted: false,
            visible_axes,
            world_order: axis_map.iter().copied().flatten().collect(),
            full_measure,
        }
    }

    fn at_world(
        coordinate: &'a CoordinateModel,
        record: Option<&'a RecordValue>,
        native: &[f64],
        visible_axes: Vec<usize>,
        world_order: Vec<usize>,
        full_coordinate: Option<CoordinateModel>,
        converted: bool,
    ) -> PyResult<Self> {
        use casa_core::quanta::{Quantity, Unit};
        let visible = |units: Vec<String>| {
            if units.len() == visible_axes.len() {
                units
            } else {
                visible_axes
                    .iter()
                    .map(|&axis| units[axis].clone())
                    .collect()
            }
        };
        let units = visible(coordinate.declared_axis_units());
        let native_units = visible(coordinate.axis_units());
        let declared = native
            .iter()
            .zip(native_units)
            .zip(&units)
            .map(|((&value, native_unit), unit)| {
                if native_unit == *unit {
                    return Ok(value);
                }
                Unit::new(unit)
                    .and_then(|unit| Quantity::new(value, &native_unit)?.get_value_in(&unit))
                    .map_err(|error| PyRuntimeError::new_err(error.to_string()))
            })
            .collect::<PyResult<_>>()?;
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
            converted,
            visible_axes,
            world_order,
            full_measure,
        })
    }

    fn text(&self, axis: usize, absolute: bool, scientific_style: bool) -> PyResult<String> {
        let value = self.declared[axis];
        let unit = &self.units[axis];
        if matches!(self.coordinate, CoordinateModel::System(_))
            && let Some((leaf, local)) = self.coordinate.world_axis_leaf(self.visible_axes[axis])
        {
            match leaf {
                CoordinateModel::Stokes(_) => return Ok(stokes(value)),
                CoordinateModel::Quality(_) => {
                    return Ok(QualityType::from_world(value).name().into());
                }
                CoordinateModel::Direction(direction) if !absolute => {
                    return Ok(direction_text(
                        direction.direction_ref(),
                        if direction.longitude_axis().is_none() {
                            1
                        } else {
                            local
                        },
                        self.native[axis],
                        false,
                        scientific_style,
                    ));
                }
                _ => (),
            }
        }
        Ok(match self.coordinate {
            CoordinateModel::Stokes(_) => stokes(value),
            CoordinateModel::Quality(_) => QualityType::from_world(value).name().into(),
            CoordinateModel::Direction(direction) => direction_text(
                direction.direction_ref(),
                if direction.longitude_axis().is_none() {
                    1
                } else {
                    self.visible_axes[axis]
                },
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
            CoordinateModel::Tabular(_) => Ok((
                "tabular",
                numbered_quantities(
                    self.declared
                        .iter()
                        .copied()
                        .zip(self.units.iter().map(String::as_str)),
                ),
            )),
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
            CoordinateModel::Quality(_) => Err(PyRuntimeError::new_err(
                "Cannot handle Coordinates of type Quality",
            )),
            CoordinateModel::System(_) => Err(PyRuntimeError::new_err(
                "Cannot handle Coordinates of type System",
            )),
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
        let placements = if format.contains('s') || format.contains('m') {
            state
                .coordinates
                .coordinates_in_record(&state.record)
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?
        } else {
            Vec::new()
        };
        complete()
            .enumerate()
            .map(|(index, coordinate)| {
                let placement = placements.get(index);
                (
                    coordinate,
                    placement.and_then(|entry| raw(&entry.field)),
                    state
                        .coordinates
                        .coordinate_world_axes(index)
                        .expect("existing coordinate"),
                    placement,
                )
            })
            .map(|(coordinate, record, axes, placement)| {
                let full = placement
                    .filter(|entry| format.contains('m') && entry.world_axes.contains(&None))
                    .map(|entry| entry.model(&state.record))
                    .transpose()
                    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
                Ok(Values::new(coordinate, record, absolute, &axes, full))
            })
            .collect::<PyResult<Vec<_>>>()?
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
        let axes: Vec<_> = (0..coordinate.n_world_axes()).map(Some).collect();
        let full = (format.contains('m') && entry.world_axes.contains(&None))
            .then(|| entry.model(&state.record))
            .transpose()
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        vec![Values::new(
            coordinate,
            raw(&entry.field),
            absolute,
            &axes,
            full,
        )]
    };
    let styles = axis_values(&state.coordinates, true, |coordinate| {
        vec![scientific_coordinate(coordinate); coordinate.n_world_axes()]
    });
    format_world_record(&selected, styles.into_iter(), &format, absolute, true, true)
}

fn scientific_coordinate(coordinate: &CoordinateModel) -> bool {
    !matches!(
        coordinate.coordinate_type(),
        CoordinateType::Direction | CoordinateType::Stokes | CoordinateType::Quality
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
        state
            .coordinates
            .coordinates_in_record(&state.record)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?
    } else {
        Vec::new()
    };
    let mut values = Vec::with_capacity(state.coordinates.n_coordinates());
    for index in 0..state.coordinates.n_coordinates() {
        let coordinate = state.coordinates.coordinate(index);
        let placement = placements.get(index);
        let record = placement.and_then(|entry| match state.record.get(&entry.field) {
            Some(Value::Record(record)) => Some(record),
            _ => None,
        });
        let axis_map = state
            .coordinates
            .coordinate_world_axes(index)
            .expect("existing coordinate");
        let world_order: Vec<_> = axis_map.iter().copied().flatten().collect();
        let visible_axes = placement.map_or_else(
            || {
                axis_map
                    .iter()
                    .enumerate()
                    .filter_map(|(local, axis)| axis.map(|_| local))
                    .collect()
            },
            |placement| {
                placement
                    .world_axes
                    .iter()
                    .enumerate()
                    .filter_map(|(local, axis)| axis.map(|_| local))
                    .collect()
            },
        );
        let full_coordinate = placement
            .filter(|placement| format.contains('m') && placement.world_axes.contains(&None))
            .map(|placement| placement.model(&state.record))
            .transpose()
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let native: Vec<_> = world_order.iter().map(|&axis| world[axis]).collect();
        values.push(Values::at_world(
            coordinate,
            record,
            &native,
            visible_axes,
            world_order,
            full_coordinate,
            converted,
        )?);
    }
    let styles = axis_values(&state.coordinates, true, |coordinate| {
        vec![scientific_coordinate(coordinate); coordinate.n_world_axes()]
    });
    format_world_record(
        &values,
        styles.into_iter(),
        &format,
        absolute,
        velocities,
        coordinate_tags,
    )
}

fn format_world_record(
    values: &[Values<'_>],
    mut styles: impl Iterator<Item = bool>,
    format: &str,
    absolute: bool,
    velocities: bool,
    coordinate_tags: bool,
) -> PyResult<CasaValue> {
    let mut ordered: Vec<_> = values
        .iter()
        .flat_map(|value| (0..value.native.len()).map(move |local| (value, local)))
        .collect();
    ordered.sort_unstable_by_key(|(value, local)| value.world_order[*local]);
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
                ordered
                    .iter()
                    .map(|(axes, local)| axes.declared[*local])
                    .collect(),
            ),
        ));
    }
    if format.contains('q') {
        record.push((
            "quantity",
            numbered_quantities(
                ordered
                    .iter()
                    .map(|(value, local)| (value.declared[*local], value.units[*local].as_str())),
            ),
        ));
    }
    if format.contains('s') {
        let mut strings = Vec::new();
        for &(value, local) in &ordered {
            strings.push(value.text(local, absolute, styles.next().unwrap_or(true))?);
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
