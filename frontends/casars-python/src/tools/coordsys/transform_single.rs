// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType, StokesType};
use casa_types::measures::{
    MeasFrame,
    direction::{DirectionRef, MDirection, casa_direction_frame_name},
    direction_from_record,
    frequency::CasaFrequencyFrame,
    frequency_from_record,
};
use casa_types::quanta::casacore::{get_value, mvangle_radians};
use casa_types::quanta::{Quantity, Unit};
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};
use numpy::{PyArrayDescrMethods, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyInt, PyList, PyTuple};

use crate::convert::{Variant, value::CasaValue};
use crate::tools::table::casa_record;

use super::inspect::axis_values;
use super::world_reference::format_transformed_world;
use super::{CoordinateState, PyCoordsys};

fn raised(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

pub(super) fn numeric_record_value(value: &Value) -> PyResult<Vec<f64>> {
    match value {
        Value::Scalar(ScalarValue::Float64(value)) => Ok(vec![*value]),
        Value::Scalar(ScalarValue::Float32(value)) => Ok(vec![f64::from(*value)]),
        Value::Array(ArrayValue::Float64(values))
            if values.shape().iter().filter(|&&size| size != 1).count() <= 1 =>
        {
            Ok(values.iter().copied().collect())
        }
        Value::Array(ArrayValue::Float32(values))
            if values.shape().iter().filter(|&&size| size != 1).count() <= 1 =>
        {
            Ok(values.iter().copied().map(f64::from).collect())
        }
        Value::Array(ArrayValue::Float32(_) | ArrayValue::Float64(_)) => {
            Err(raised("Coordinate numeric record must be one-dimensional"))
        }
        _ => Err(raised(
            "RecordRep::get_pointer - incorrect data type for field numeric",
        )),
    }
}

pub(super) fn bool_vector(value: &Bound<'_, PyAny>) -> PyResult<Option<bool>> {
    if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() {
        let mut empty = true;
        for item in value.try_iter()? {
            if !item?.is_instance_of::<PyBool>() {
                return Ok(None);
            }
            empty = false;
        }
        return Ok(Some(empty));
    }
    if let Ok(array) = value.cast::<PyUntypedArray>()
        && !matches!(
            array.dtype().char(),
            b'b' | b'B'
                | b'h'
                | b'H'
                | b'i'
                | b'I'
                | b'l'
                | b'L'
                | b'q'
                | b'Q'
                | b'f'
                | b'd'
                | b'g'
                | b'F'
                | b'D'
                | b'G'
                | b'S'
                | b'U'
        )
    {
        // Unsupported NumPy dtypes also reach CASA as the unset bool variant.
        return Ok(Some(array.dtype().char() != b'?' || array.is_empty()));
    }
    Ok(None)
}

pub(in crate::tools) fn image_pixel_argument(
    py: Python<'_>,
    value: Option<&Bound<'_, PyAny>>,
) -> PyResult<Vec<f64>> {
    pixel_argument(
        py,
        value,
        "Unsupported record type for value",
        "Unsupported data type for value",
    )
}

fn pixel_argument(
    py: Python<'_>,
    value: Option<&Bound<'_, PyAny>>,
    record_error: &str,
    type_error: &str,
) -> PyResult<Vec<f64>> {
    let Some(value) = value.filter(|value| !value.is_none()) else {
        return Ok(Vec::new());
    };
    if let Ok(dict) = value.cast::<PyDict>() {
        let record = casa_record(py, Some(dict))?;
        return numeric_record_value(record.get("numeric").ok_or_else(|| raised(record_error))?);
    }
    if let Some(empty) = bool_vector(value)? {
        return if empty {
            Ok(Vec::new())
        } else {
            Err(raised(type_error))
        };
    }
    if let Ok(array) = value.cast::<PyUntypedArray>()
        && array.ndim() == 0
        && matches!(array.dtype().kind(), b'i' | b'u' | b'f')
    {
        return Ok(vec![value.call_method0("item")?.extract()?]);
    }
    match value.extract::<Variant>()? {
        Variant::Numbers(values) | Variant::Array { values, .. } => Ok(values),
        Variant::Empty => Ok(Vec::new()),
        _ => Err(raised(type_error)),
    }
}

pub(in crate::tools) enum WorldArgument {
    Unset,
    Numeric(Vec<f64>),
    Text(String),
    Texts(Vec<String>),
    Record(RecordValue),
}

impl WorldArgument {
    pub(in crate::tools) fn capture(
        py: Python<'_>,
        value: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let Some(value) = value.filter(|value| !value.is_none()) else {
            return Ok(Self::Unset);
        };
        if let Ok(dict) = value.cast::<PyDict>() {
            return Ok(Self::Record(casa_record(py, Some(dict))?));
        }
        if bool_vector(value)?.is_some() {
            return Ok(Self::Unset);
        }
        if let Ok(array) = value.cast::<PyUntypedArray>()
            && matches!(array.dtype().char(), b'S' | b'U')
        {
            let flattened = value.call_method1("ravel", ("F",))?;
            let texts = flattened
                .try_iter()?
                .map(|item| match item?.extract::<Variant>()? {
                    Variant::Text(text) => Ok(text),
                    _ => Err(raised("Unrecognized format for world coordinate")),
                })
                .collect::<PyResult<_>>()?;
            return Ok(Self::Texts(texts));
        }
        match value.extract::<Variant>()? {
            Variant::Number(number) if !value.is_instance_of::<PyInt>() => {
                Ok(Self::Numeric(vec![number]))
            }
            Variant::Numbers(values) | Variant::Array { values, .. } => Ok(Self::Numeric(values)),
            Variant::Empty => Ok(Self::Numeric(Vec::new())),
            Variant::Texts(text) => Ok(Self::Texts(text)),
            Variant::Text(text) => Ok(Self::Text(text)),
            _ => Err(raised("Unrecognized format for world coordinate")),
        }
    }

    pub(super) fn values(self, state: &CoordinateState) -> PyResult<Vec<f64>> {
        self.values_with_defaults(state, None, world_reference(&state.coordinates))
    }

    pub(super) fn values_with_defaults(
        self,
        state: &CoordinateState,
        selected: Option<(usize, &CoordinateState)>,
        mut world: Vec<f64>,
    ) -> PyResult<Vec<f64>> {
        let coordinates = selected.map_or(&state.coordinates, |(_, part)| &part.coordinates);
        let input = WorldInput {
            state,
            coordinates,
            selected: selected.map(|(index, _)| index),
            units: native_world_units(coordinates),
        };
        let decoded = match self {
            Self::Unset => input.reference_prefix()?,
            Self::Numeric(values) => input.numeric(values)?,
            Self::Text(text) => {
                let parts: Vec<_> = text.split_whitespace().take(50).collect();
                if parts.len() == 1 {
                    vec![read_quantity(
                        parts[0],
                        input.units.first().map_or("", String::as_str),
                    )?]
                } else {
                    input.texts(parts.into_iter())?
                }
            }
            Self::Texts(texts) => input.texts(texts.iter().map(String::as_str))?,
            Self::Record(record) => world_record_values(&record, &input)?,
        };
        overwrite_prefix(&mut world, decoded);
        Ok(world)
    }
}

struct WorldInput<'a> {
    state: &'a CoordinateState,
    coordinates: &'a CoordinateSystem,
    selected: Option<usize>,
    units: Vec<String>,
}

impl WorldInput<'_> {
    fn reference_prefix(&self) -> PyResult<Vec<f64>> {
        if self.selected.is_none() {
            return Ok(world_reference(&self.state.coordinates));
        }
        // CASA uses the full declared reference vector as a prefix for a
        // selected coordinate's string vector or unset argument.
        let declared = axis_values(
            &self.state.coordinates,
            true,
            |coordinate| match coordinate {
                CoordinateModel::Direction(direction) => {
                    direction.reference_and_increment_in_world_units().0
                }
                _ => coordinate.reference_value(),
            },
        );
        declared_to_native(declared, self.coordinates)
    }

    fn texts<'a>(&self, texts: impl Iterator<Item = &'a str>) -> PyResult<Vec<f64>> {
        let mut values = self.reference_prefix()?;
        let supplied = text_values(&self.state.coordinates, &self.units, texts)?;
        overwrite_prefix(&mut values, supplied);
        Ok(values)
    }

    fn numeric(&self, mut values: Vec<f64>) -> PyResult<Vec<f64>> {
        if let Some(index) = self.selected {
            let axes = self
                .state
                .coordinates
                .coordinate_world_axes(index)
                .expect("selected coordinate");
            let visible = axes.iter().flatten().count();
            // A typed edit can supply a full local coordinate or just its
            // visible slots. Hidden components are never written.
            if visible < axes.len() && values.len() == visible {
                let mut compact = values.into_iter();
                values = axes
                    .into_iter()
                    .map(|axis| {
                        axis.map_or(0.0, |_| compact.next().expect("validated visible count"))
                    })
                    .collect();
            }
        }
        declared_to_native(values, self.coordinates)
    }

    fn measures(&self, record: &RecordValue) -> PyResult<Vec<f64>> {
        let system = &self.state.coordinates;
        let mut values = world_reference(system);
        update_measures(record, self.state, &mut values)?;
        if let Some(index) = self.selected {
            let model = system.coordinate(index);
            let reference = model.reference_value();
            return Ok(system
                .coordinate_world_axes(index)
                .expect("selected coordinate")
                .into_iter()
                .zip(reference)
                .map(|(axis, reference)| axis.map_or(reference, |axis| values[axis]))
                .collect());
        }
        Ok(values)
    }
}

fn world_reference(coordinates: &CoordinateSystem) -> Vec<f64> {
    axis_values(coordinates, true, CoordinateModel::reference_value)
}

fn models(coordinates: &CoordinateSystem) -> impl Iterator<Item = &CoordinateModel> {
    (0..coordinates.n_coordinates()).map(|index| coordinates.coordinate(index))
}

// Angles reach casa-coordinates in radians. Other coordinates already use their declared unit.
fn native_world_units(coordinates: &CoordinateSystem) -> Vec<String> {
    coordinates.world_axis_units()
}

fn declared_to_native(values: Vec<f64>, coordinates: &CoordinateSystem) -> PyResult<Vec<f64>> {
    let conversions = axis_values(coordinates, true, CoordinateModel::declared_axis_units)
        .into_iter()
        .zip(coordinates.world_axis_units())
        .map(|(declared, internal)| (declared != internal).then_some(declared));
    values
        .into_iter()
        .zip(conversions)
        .map(|(value, unit)| match unit {
            Some(unit) => Quantity::new(value, &unit)
                .map(|quantity| quantity.get_si_value())
                .map_err(raised),
            None => Ok(value),
        })
        .collect()
}

fn read_quantity(text: &str, unit: &str) -> PyResult<f64> {
    let quantity = Quantity::read(text).map_err(raised)?;
    Ok(get_value(&quantity, &Unit::new(unit).map_err(raised)?))
}

fn text_values<'a>(
    coordinates: &CoordinateSystem,
    units: &[String],
    values: impl Iterator<Item = &'a str>,
) -> PyResult<Vec<f64>> {
    let mut types = axis_values(coordinates, true, |coordinate| {
        vec![coordinate.coordinate_type(); coordinate.n_world_axes()]
    })
    .into_iter()
    .zip(units);
    values
        .map(|text| {
            let (kind, unit) = types.next().ok_or_else(|| {
                raised("Failed AlwaysAssert axisInCoordinateSystem < nWorldAxes()")
            })?;
            if kind == CoordinateType::Stokes {
                return Ok(StokesType::from_name(text)
                    .unwrap_or(StokesType::Undefined)
                    .code() as f64);
            }
            if kind == CoordinateType::Quality {
                return Quantity::read(text)
                    .map(|value| value.value())
                    .map_err(|_| {
                        raised(format!(
                            "Failed to convert string formatted world [{text}] to double"
                        ))
                    });
            }
            if kind == CoordinateType::Direction {
                let invalid = || {
                    raised(format!(
                        "Failed to convert string formatted world direction {text} to double"
                    ))
                };
                let value = Quantity::read(text).map_err(|_| invalid())?;
                if value.unit().name().is_empty() {
                    return Ok(value.value());
                }
                let radians = mvangle_radians(&value).map_err(|_| invalid())?;
                let angle = Quantity::new(radians, "rad").map_err(raised)?;
                return Ok(get_value(&angle, &Unit::new(unit).map_err(raised)?));
            }
            read_quantity(text, unit)
        })
        .collect()
}

fn subrecord<'a>(value: &'a Value, field: &str) -> PyResult<&'a RecordValue> {
    match value {
        Value::Record(record) => Ok(record),
        _ => Err(raised(format!("{field} must be a record"))),
    }
}

fn scalar_text<'a>(value: &'a Value, field: &str) -> PyResult<&'a str> {
    match value {
        Value::Scalar(ScalarValue::String(text)) => Ok(text),
        _ => Err(raised(format!("{field} must be a string"))),
    }
}

fn quantity_record(record: &RecordValue, target_unit: &str) -> PyResult<f64> {
    let malformed = || raised("Illegal Quantum record in QuantumHolder::fromRecord");
    let Some(Value::Scalar(ScalarValue::String(unit))) = record.get("unit") else {
        return Err(malformed());
    };
    let value = match record.get("value") {
        Some(Value::Scalar(ScalarValue::Float64(value))) => *value,
        Some(Value::Scalar(ScalarValue::Float32(value))) => f64::from(*value),
        Some(Value::Scalar(ScalarValue::Int32(value))) => f64::from(*value),
        Some(Value::Scalar(ScalarValue::Int64(value))) => *value as f64,
        Some(value @ Value::Array(array))
            if !array.shape().is_empty() && array.shape().iter().all(|&length| length == 1) =>
        {
            if let CasaValue::Floats { values, .. } = CasaValue::from_value(value) {
                values[0]
            } else {
                return Err(raised("Wrong QuantumHolder to convert asQuantumDouble"));
            }
        }
        Some(Value::Array(_)) => {
            return Err(raised("Wrong QuantumHolder to convert asQuantumDouble"));
        }
        _ => return Err(malformed()),
    };
    let quantity = Quantity::new(value, unit).map_err(raised)?;
    Ok(get_value(
        &quantity,
        &Unit::new(target_unit).map_err(raised)?,
    ))
}

fn quantity_vector(record: &RecordValue, units: &[String]) -> PyResult<Vec<f64>> {
    if record.fields().len() != units.len() {
        return Err(raised(
            "Number of axes must equal number of fields in record!",
        ));
    }
    let mut fields: Vec<_> = record.fields().iter().collect();
    fields.sort_by(|left, right| left.name.cmp(&right.name));
    fields
        .into_iter()
        .zip(units)
        .map(|(field, unit)| quantity_record(subrecord(&field.value, "quantity")?, unit))
        .collect()
}

fn world_record_values(record: &RecordValue, input: &WorldInput<'_>) -> PyResult<Vec<f64>> {
    if let Some(kind) = record.get("pw_type") {
        scalar_text(kind, "pw_type")?;
    }
    if matches!(
        record.get("ar_type"),
        Some(Value::Scalar(ScalarValue::String(value))) if value == "relative"
    ) {
        return Err(raised(
            "The value is relative, not absolute', origin='coordsys.checkAbsRel",
        ));
    }
    if let Some(value) = record.get("numeric") {
        return input.numeric(numeric_record_value(value)?);
    }
    if let Some(value) = record.get("quantity") {
        return quantity_vector(subrecord(value, "quantity")?, &input.units);
    }
    if let Some(value) = record.get("measure") {
        return input.measures(subrecord(value, "measure")?);
    }
    if let Some(value) = record.get("string") {
        return match value {
            Value::Array(ArrayValue::String(values)) if values.ndim() == 1 => {
                input.texts(values.iter().map(String::as_str))
            }
            Value::Scalar(ScalarValue::String(value)) => {
                input.texts(std::iter::once(value.as_str()))
            }
            _ => Err(raised("Coordinate string record must be one-dimensional")),
        };
    }
    input.measures(record)
}

fn update_measures(
    record: &RecordValue,
    state: &CoordinateState,
    world: &mut [f64],
) -> PyResult<()> {
    let placements = state
        .coordinates
        .coordinates_in_record(&state.record)
        .map_err(raised)?;
    let mut seen = Vec::new();
    for (coordinate, placement) in models(&state.coordinates).zip(placements) {
        let kind = coordinate.coordinate_type();
        if seen.contains(&kind) {
            continue;
        }
        seen.push(kind);
        let key = kind.to_string().to_ascii_lowercase();
        let Some(value) = record.get(&key) else {
            continue;
        };
        let values = match coordinate {
            CoordinateModel::Direction(_) => {
                let angle =
                    direction_from_record(subrecord(value, "direction")?).map_err(raised)?;
                vec![angle.longitude_rad(), angle.latitude_rad()]
            }
            CoordinateModel::Spectral(spectral) => {
                let measure = subrecord(value, "spectral")?
                    .get("frequency")
                    .ok_or_else(|| {
                        raised("This spectral record does not contain a frequency field")
                    })?;
                let frequency =
                    frequency_from_record(subrecord(measure, "frequency")?).map_err(raised)?;
                vec![spectral.value_from_hz(frequency.hz()).map_err(raised)?]
            }
            CoordinateModel::Stokes(stokes) => {
                let name = scalar_text(value, "stokes")?;
                let polarization = StokesType::from_name(name).unwrap_or(StokesType::Undefined);
                if !stokes.stokes().contains(&polarization) {
                    return Err(raised(format!("Illegal Stokes coordinate {name}")));
                }
                vec![f64::from(polarization.code())]
            }
            CoordinateModel::Linear(_) => {
                let units = if placement.world_axes.len() == coordinate.n_world_axes() {
                    coordinate.axis_units()
                } else {
                    placement.model(&state.record).map_err(raised)?.axis_units()
                };
                quantity_vector(subrecord(value, "linear")?, &units)?
            }
            CoordinateModel::Quality(_) | CoordinateModel::System(_) => continue,
            CoordinateModel::Tabular(_) => {
                vec![quantity_record(
                    subrecord(value, "tabular")?,
                    &coordinate.axis_units()[0],
                )?]
            }
        };
        for (axis, value) in placement.world_axes.into_iter().zip(values) {
            if let Some(axis) = axis {
                world[axis] = value;
            }
        }
    }
    Ok(())
}

fn overwrite_prefix<T>(target: &mut [T], values: impl IntoIterator<Item = T>) {
    for (slot, value) in target.iter_mut().zip(values) {
        *slot = value;
    }
}

impl PyCoordsys {
    pub(in crate::tools) fn image_world(
        &self,
        pixel: Vec<f64>,
        format: &str,
        velocities: bool,
    ) -> PyResult<CasaValue> {
        image_world_record(&self.state(), pixel, format, velocities, false)
    }

    pub(in crate::tools) fn image_measures(
        &self,
        pixel: Vec<f64>,
        direction_frame: &str,
        spectral_frame: &str,
    ) -> PyResult<CasaValue> {
        let state = self.state();
        if direction_frame.eq_ignore_ascii_case("cl") && spectral_frame.eq_ignore_ascii_case("cl") {
            return image_world_record(&state, pixel, "m", true, false);
        }
        let mut state = state.clone();
        select_measure_frames(&mut state.coordinates, direction_frame, spectral_frame)?;
        image_world_record(&state, pixel, "m", true, false)
    }

    pub(in crate::tools) fn image_pixel(&self, value: WorldArgument) -> PyResult<CasaValue> {
        let state = self.state();
        let world = value.values(&state)?;
        let pixel = state.coordinates.to_pixel_or_nan(&world).map_err(raised)?;
        Ok(CasaValue::record([
            ("numeric", CasaValue::floats(pixel)),
            ("pw_type", CasaValue::Text("pixel".into())),
            ("ar_type", CasaValue::Text("absolute".into())),
        ]))
    }
}

#[pymethods]
impl PyCoordsys {
    #[pyo3(signature = (value=None), text_signature = "($self, value=[])")]
    fn topixel(&self, py: Python<'_>, value: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        let world = WorldArgument::capture(py, value)?;
        py.detach(|| self.image_pixel(world))?.into_python(py)
    }

    #[pyo3(signature = (value=None, format="n"), text_signature = "($self, value=[], format='n')")]
    fn toworld(
        &self,
        py: Python<'_>,
        value: Option<&Bound<'_, PyAny>>,
        format: &str,
    ) -> PyResult<Py<PyAny>> {
        let pixel = pixel_argument(
            py,
            value,
            "unsupported record type for pixel",
            "unsupported data type for pixel",
        )?;
        py.detach(|| image_world_record(&self.state(), pixel, format, true, true))?
            .into_python(py)
    }
}

fn image_world_record(
    state: &CoordinateState,
    pixel: Vec<f64>,
    format: &str,
    velocities: bool,
    coordinate_tags: bool,
) -> PyResult<CasaValue> {
    let mut complete = axis_values(&state.coordinates, false, CoordinateModel::reference_pixel);
    overwrite_prefix(&mut complete, pixel);
    let world = state.coordinates.to_world(&complete).map_err(|error| {
        raised(format!(
            "Exception: Error converting to world coordinates: {error}"
        ))
    })?;
    format_transformed_world(state, &world, format, velocities, coordinate_tags)
}

fn select_measure_frames(
    coordinates: &mut CoordinateSystem,
    direction_frame: &str,
    spectral_frame: &str,
) -> PyResult<()> {
    if !direction_frame.eq_ignore_ascii_case("cl")
        && let Some(index) = coordinates.find_coordinate(CoordinateType::Direction)
        && let CoordinateModel::Direction(direction) = coordinates.coordinate(index)
    {
        let frame = if direction_frame.eq_ignore_ascii_case("native") {
            direction.direction_ref()
        } else {
            casa_direction_frame_name(direction_frame)
                .and_then(|name| name.parse::<DirectionRef>().ok())
                .ok_or_else(|| {
                    raised(format!(
                        "Exception: Unknown direction reference frame {direction_frame}."
                    ))
                })?
        };
        coordinates.replace_coordinate(
            index,
            direction.clone().with_conversion_direction_ref(frame),
        );
    }
    if !spectral_frame.eq_ignore_ascii_case("cl")
        && let Some(index) = coordinates.find_coordinate(CoordinateType::Spectral)
        && let CoordinateModel::Spectral(spectral) = coordinates.coordinate(index)
    {
        let replacement = if spectral_frame.eq_ignore_ascii_case("native") {
            spectral.clone().without_conversion()
        } else {
            let reference = spectral_frame.parse::<CasaFrequencyFrame>().map_err(|_| {
                raised(format!(
                    "Exception: Unknown frequency reference frame {spectral_frame}."
                ))
            })?;
            let frame = spectral.conversion_frame().cloned().unwrap_or_else(|| {
                let mut frame = MeasFrame::new().with_direction(MDirection::from_angles(
                    0.,
                    std::f64::consts::FRAC_PI_2,
                    DirectionRef::J2000,
                ));
                if let Some(epoch) = &coordinates.obs_info().date {
                    frame = frame.with_epoch(epoch.clone());
                }
                if let Some(position) = &coordinates.obs_info().telescope_position {
                    frame = frame.with_position(position.clone());
                }
                frame
            });
            let CasaFrequencyFrame::Defined(reference) = reference else {
                return Err(raised(
                    "Transformations to/from frame \"Undefined\" are not possible.",
                ));
            };
            spectral.clone().with_conversion(reference, frame)
        };
        coordinates.replace_coordinate(index, replacement);
    }
    Ok(())
}
