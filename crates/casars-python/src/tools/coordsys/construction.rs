// SPDX-License-Identifier: LGPL-3.0-or-later

use std::sync::Mutex;

use casa_coordinates::{
    CoordinateModel, CoordinateSystem, CoordinateType, ObsInfo, StokesCoordinate, StokesType,
    TabularCoordinate, defaults,
};
use casa_types::measures::{EpochRef, MEpoch, MPosition, epoch_to_record};
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};

use crate::convert::{StringList, value::CasaValue};
use crate::tools::table::casa_record;

use super::linear_transform::validate_restored_linear_coordinates;
use super::{CoordinateState, PyCoordsys, current_epoch, default_spectral_conversion};

fn raised(message: impl Into<String>) -> PyErr {
    PyRuntimeError::new_err(message.into())
}

pub(super) fn string_arguments(
    value: Option<&Bound<'_, PyAny>>,
    argument: &str,
) -> PyResult<Vec<String>> {
    let Some(value) = value
        .filter(|value| value.is_instance_of::<PyList>() || value.is_instance_of::<PyString>())
    else {
        return Ok(Vec::new());
    };
    value
        .extract::<StringList>()
        .map(|names| names.0)
        .map_err(|_| PyTypeError::new_err(format!("list _{argument} must contain strings")))
}

fn stokes_coordinate(names: Vec<String>) -> PyResult<Option<StokesCoordinate>> {
    let names = match names.as_slice() {
        [name] if name.is_empty() || name == " " => return Ok(None),
        [name] => name
            .split(if name.contains(',') { ',' } else { ' ' })
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .collect::<Vec<_>>(),
        _ => names,
    };
    let values = stokes_values(names.iter().map(String::as_str))?;
    Ok((!values.is_empty()).then(|| StokesCoordinate::new(values)))
}

pub(super) fn stokes_values<'a>(
    names: impl IntoIterator<Item = &'a str>,
) -> PyResult<Vec<StokesType>> {
    let values = names
        .into_iter()
        .map(|name| StokesType::from_name(name).unwrap_or(StokesType::Undefined))
        .collect::<Vec<_>>();
    if values
        .iter()
        .enumerate()
        .any(|(index, value)| values[..index].contains(value))
    {
        return Err(raised("You have specified the same Stokes more than once"));
    }
    Ok(values)
}

fn native_coordinate_field(name: &str) -> Option<(&str, usize)> {
    let offset = name.find(|character: char| character.is_ascii_digit())?;
    let (kind, index) = name.split_at(offset);
    let index = index.parse().ok()?;
    match kind {
        "direction" | "spectral" | "stokes" | "linear" | "tabular" | "quality" | "coordsys"
        | "coordinate" => Some((kind, index)),
        _ => None,
    }
}

pub(in crate::tools) fn validate_coordinate_indices(record: &RecordValue) -> PyResult<usize> {
    let mut indices = Vec::new();
    for field in record.fields() {
        let Some((_, index)) = native_coordinate_field(&field.name) else {
            continue;
        };
        indices.push(index);
    }
    indices.sort_unstable();
    if !indices.iter().copied().eq(0..indices.len()) {
        return Err(raised(
            "Coordinate record numbering must be contiguous and unique",
        ));
    }
    Ok(indices.len())
}

/// Append a coordinate and its axis-mapping fields, keeping the existing record intact.
pub(in crate::tools) fn append_coordinate(
    coordinates: &mut CoordinateSystem,
    record: &mut RecordValue,
    coordinate: impl Into<CoordinateModel>,
) -> PyResult<()> {
    let coordinate = coordinate.into();
    let index = coordinates.n_coordinates();
    let first_pixel = coordinates.n_pixel_axes() as i32;
    let first_world = coordinates.n_world_axes() as i32;
    let n_pixel = coordinate.n_pixel_axes();
    let n_world = coordinate.n_world_axes();
    let replacements = coordinate
        .to_world(&vec![0.0; n_pixel])
        .map_err(|error| raised(error.to_string()))?;
    let kind = coordinate.coordinate_type();
    let name = kind.record_prefix();
    let mut axis_record = coordinate.to_record();
    match kind {
        CoordinateType::Spectral => {
            axis_record.upsert("conversion", default_spectral_conversion().into());
        }
        CoordinateType::Linear => {
            axis_record.remove("coordinate_type");
            axis_record.remove("naxes");
        }
        _ => {}
    }
    record.upsert(format!("{name}{index}"), axis_record.into());
    record.upsert(
        format!("pixelmap{index}"),
        ArrayValue::from_i32_vec((first_pixel..first_pixel + n_pixel as i32).collect()).into(),
    );
    record.upsert(
        format!("worldmap{index}"),
        ArrayValue::from_i32_vec((first_world..first_world + n_world as i32).collect()).into(),
    );
    record.upsert(
        format!("pixelreplace{index}"),
        ArrayValue::from_f64_vec(vec![0.0; n_pixel]).into(),
    );
    record.upsert(
        format!("worldreplace{index}"),
        ArrayValue::from_f64_vec(replacements).into(),
    );
    coordinates.add_coordinate(coordinate);
    Ok(())
}

impl CoordinateState {
    fn append(&mut self, coordinate: impl Into<CoordinateModel>) -> PyResult<()> {
        append_coordinate(&mut self.coordinates, &mut self.record, coordinate)
    }

    fn add_defaults(
        &mut self,
        direction: bool,
        spectral: bool,
        stokes: Vec<String>,
        linear: i32,
        tabular: bool,
    ) -> PyResult<()> {
        validate_coordinate_indices(&self.record)?;
        if direction {
            self.append(defaults::direction_coordinate())?;
        }
        if let Some(stokes) = stokes_coordinate(stokes)? {
            self.append(stokes)?;
        }
        if spectral {
            self.append(defaults::spectral_coordinate())?;
        }
        if linear > 0 {
            let names = (0..linear)
                .map(|axis| format!("LinAxis{axis}"))
                .collect::<Vec<_>>();
            self.append(defaults::linear_coordinate(&names, &[]))?;
        }
        if tabular {
            self.append(TabularCoordinate::linear(0.0, 1.0, 0.0, "TabAxis1", "km"))?;
        }
        Ok(())
    }
}

fn evla_observation() -> ObsInfo {
    let (sin_lon, cos_lon) = (-1.878_288_434_411_257_6_f64).sin_cos();
    let (sin_lat, cos_lat) = (0.591_675_343_072_337_6_f64).sin_cos();
    let radius = 6_373_580.000_000_001_f64;
    current_epoch(
        ObsInfo::new("EVLA")
            .with_observer("Karl Jansky")
            .with_telescope_position(MPosition::new_itrf(
                cos_lon * cos_lat * radius,
                sin_lon * cos_lat * radius,
                sin_lat * radius,
            )),
    )
}

fn normalize_stokes_records(record: &mut RecordValue) -> PyResult<()> {
    for field in record.fields_mut() {
        let Some((kind, _)) = native_coordinate_field(&field.name) else {
            continue;
        };
        let Value::Record(subrecord) = &mut field.value else {
            continue;
        };
        let declared_system = matches!(subrecord.get("coordinate_type"),
            Some(Value::Scalar(ScalarValue::String(kind))) if kind == "System");
        if kind == "coordsys" || declared_system {
            *subrecord = restore(std::mem::take(subrecord), "unknown".into())?.record;
            if declared_system {
                subrecord.upsert(
                    "coordinate_type",
                    Value::Scalar(ScalarValue::String("System".into())),
                );
            }
            continue;
        }
        if kind != "stokes"
            && !matches!(subrecord.get("coordinate_type"), Some(Value::Scalar(ScalarValue::String(kind))) if kind == "Stokes")
        {
            continue;
        }
        let Some(Value::Array(ArrayValue::String(values))) = subrecord.get("stokes") else {
            return Err(raised("Incorrect data type for field stokes"));
        };
        if values.ndim() != 1 || values.is_empty() {
            return Err(raised("The stokes field must be a nonempty vector"));
        }
        let values = stokes_values(values.iter().map(String::as_str))?;
        subrecord.upsert(
            "stokes",
            ArrayValue::from_string_vec(
                values
                    .into_iter()
                    .map(|value| value.name().into())
                    .collect(),
            )
            .into(),
        );
    }
    Ok(())
}

fn replacement(value: &Value, key: &str) -> PyResult<Value> {
    let values = match value {
        Value::Array(ArrayValue::Float64(values)) if values.ndim() == 1 => {
            return Ok(value.clone());
        }
        Value::Array(ArrayValue::Float32(values)) if values.ndim() == 1 => {
            values.iter().copied().map(f64::from).collect()
        }
        Value::Array(ArrayValue::Int32(values)) if values.ndim() == 1 => {
            values.iter().copied().map(f64::from).collect()
        }
        Value::Array(ArrayValue::Int64(values)) if values.ndim() == 1 => {
            values.iter().map(|value| *value as f64).collect()
        }
        _ => {
            return Err(raised(format!(
                "Incorrect data type for field {key}, expected a numeric vector"
            )));
        }
    };
    Ok(ArrayValue::from_f64_vec(values).into())
}

pub(super) fn restore(mut record: RecordValue, parent_name: String) -> PyResult<CoordinateState> {
    validate_coordinate_indices(&record)?;
    normalize_stokes_records(&mut record)?;

    let mut coordinates =
        CoordinateSystem::from_record(&record).map_err(|error| raised(error.to_string()))?;
    validate_restored_linear_coordinates(&coordinates, &record)?;
    let info = coordinates.obs_info_mut();
    if record.get("observer").is_none() && info.observer.is_empty() {
        info.observer = "UNKNOWN".into();
    }
    if record.get("telescope").is_none() && info.telescope.is_empty() {
        info.telescope = "UNKNOWN".into();
    }
    if info.date.is_none() {
        info.date = Some(MEpoch::from_mjd(0.0, EpochRef::LAST));
    }

    let placements = coordinates
        .coordinates_in_record(&record)
        .map_err(|error| raised(error.to_string()))?;
    let mut restored = coordinates.obs_info().to_record();
    for (model_index, placement) in placements.into_iter().enumerate() {
        let field = placement.field;
        let index = field.trim_start_matches(|character: char| !character.is_ascii_digit());
        for prefix in ["pixelmap", "worldmap", "pixelreplace", "worldreplace"] {
            let key = format!("{prefix}{index}");
            let value = record
                .get(&key)
                .ok_or_else(|| raised(format!("RecordInterface: unknown field {key}")))?;
            let value = if prefix.ends_with("replace") {
                replacement(value, &key)?
            } else {
                value.clone()
            };
            restored.push(RecordField::new(key, value));
        }
        let model = coordinates.coordinate(model_index);
        let value = if matches!(
            model,
            CoordinateModel::Stokes(_) | CoordinateModel::Quality(_)
        ) {
            let mut canonical = model.to_record();
            if field.starts_with("coordinate") {
                canonical.upsert(
                    "coordinate_type",
                    Value::Scalar(ScalarValue::String(model.coordinate_type().to_string())),
                );
            }
            canonical.into()
        } else if let Some(Value::Record(original)) = record.get(&field)
            && let Some(normalized) = super::canonical_restored_coordinate(original, model)?
        {
            Value::Record(normalized)
        } else {
            record.get(&field).expect("coordinate field exists").clone()
        };
        restored.push(RecordField::new(field, value));
    }
    Ok(CoordinateState {
        coordinates,
        record: restored,
        parent_name,
    })
}

#[pymethods]
#[allow(clippy::too_many_arguments)]
impl PyCoordsys {
    #[pyo3(signature = (direction=false, spectral=false, stokes=None, linear=0, tabular=false), text_signature = "($self, direction=False, spectral=False, stokes=[], linear=0, tabular=False)")]
    fn newcoordsys(
        &self,
        py: Python<'_>,
        direction: bool,
        spectral: bool,
        stokes: Option<&Bound<'_, PyAny>>,
        linear: i32,
        tabular: bool,
    ) -> PyResult<Py<Self>> {
        let stokes = string_arguments(stokes, "stokes")?;
        let child = py.detach(|| {
            let mut state = self.state();
            let parent_name = std::mem::take(&mut state.parent_name);
            let unset = ObsInfo::new("UNKNOWN")
                .with_observer("UNKNOWN")
                .with_date(MEpoch::from_mjd(0.0, EpochRef::LAST));
            *state = CoordinateState::new(CoordinateSystem::new().with_obs_info(unset));
            state.parent_name = parent_name;
            state
                .add_defaults(direction, spectral, stokes, linear, tabular)
                .map_err(|_| raised("Unable to create new coordsys"))?;
            *state.coordinates.obs_info_mut() = evla_observation();
            for field in state.coordinates.obs_info().to_record().into_fields() {
                state.record.upsert(field.name, field.value);
            }
            let mut child = state.clone();
            child.parent_name = "unknown".into();
            Ok::<_, PyErr>(child)
        })?;
        Py::new(
            py,
            Self {
                state: Mutex::new(child),
            },
        )
    }

    #[pyo3(signature = (direction=false, spectral=false, stokes=None, linear=0, tabular=false), text_signature = "($self, direction=False, spectral=False, stokes=[], linear=0, tabular=False)")]
    fn addcoordinate(
        &self,
        py: Python<'_>,
        direction: bool,
        spectral: bool,
        stokes: Option<&Bound<'_, PyAny>>,
        linear: i32,
        tabular: bool,
    ) -> PyResult<bool> {
        let stokes = string_arguments(stokes, "stokes")?;
        py.detach(|| {
            self.state()
                .add_defaults(direction, spectral, stokes, linear, tabular)?;
            Ok(true)
        })
    }

    fn fromrecord(&self, py: Python<'_>, record: &Bound<'_, PyDict>) -> PyResult<bool> {
        let mut record = casa_record(py, Some(record))?;
        py.detach(|| {
            let mut state = self.state();
            let parent_name = match record.remove("parentName") {
                Some(Value::Scalar(ScalarValue::String(name))) => name,
                None => state.parent_name.clone(),
                _ => return Err(raised("Incorrect data type String for field parentName")),
            };
            *state = restore(record, parent_name)?;
            Ok(true)
        })
    }

    fn epoch(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let record = py.detach(|| {
            let state = self.state();
            let missing = MEpoch::from_mjd(0.0, EpochRef::LAST);
            epoch_to_record(
                state
                    .coordinates
                    .obs_info()
                    .date
                    .as_ref()
                    .unwrap_or(&missing),
            )
        });
        CasaValue::from_record(&record).into_python(py)
    }
}
