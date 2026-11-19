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

use super::{CoordinateState, PyCoordsys, current_epoch, default_spectral_conversion};

fn raised(message: impl Into<String>) -> PyErr {
    PyRuntimeError::new_err(message.into())
}

fn stokes_argument(value: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<String>> {
    let Some(value) = value
        .filter(|value| value.is_instance_of::<PyList>() || value.is_instance_of::<PyString>())
    else {
        return Ok(Vec::new());
    };
    value
        .extract::<StringList>()
        .map(|names| names.0)
        .map_err(|_| PyTypeError::new_err("list _stokes must contain strings"))
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
    let values = names
        .iter()
        .map(|name| StokesType::from_name(name).unwrap_or(StokesType::Undefined))
        .collect::<Vec<_>>();
    if values
        .iter()
        .enumerate()
        .any(|(index, value)| values[..index].contains(value))
    {
        return Err(raised("You have specified the same Stokes more than once"));
    }
    Ok((!values.is_empty()).then(|| StokesCoordinate::new(values)))
}

impl CoordinateState {
    fn append(&mut self, coordinate: impl Into<CoordinateModel>) -> PyResult<()> {
        let coordinate = coordinate.into();
        let index = self.coordinates.n_coordinates();
        let first_pixel = self.coordinates.n_pixel_axes() as i32;
        let first_world = self.coordinates.n_world_axes() as i32;
        let n_pixel = coordinate.n_pixel_axes();
        let n_world = coordinate.n_world_axes();
        let replacements = coordinate
            .to_world(&vec![0.0; n_pixel])
            .map_err(|error| raised(error.to_string()))?;
        let kind = coordinate.coordinate_type();
        let name = kind.to_string().to_ascii_lowercase();
        let mut record = coordinate.to_record();
        match kind {
            CoordinateType::Spectral => {
                record.upsert("conversion", default_spectral_conversion().into());
            }
            CoordinateType::Linear => {
                record.remove("coordinate_type");
                record.remove("naxes");
            }
            _ => {}
        }
        self.record.upsert(format!("{name}{index}"), record.into());
        self.record.upsert(
            format!("pixelmap{index}"),
            ArrayValue::from_i32_vec((first_pixel..first_pixel + n_pixel as i32).collect()).into(),
        );
        self.record.upsert(
            format!("worldmap{index}"),
            ArrayValue::from_i32_vec((first_world..first_world + n_world as i32).collect()).into(),
        );
        self.record.upsert(
            format!("pixelreplace{index}"),
            ArrayValue::from_f64_vec(vec![0.0; n_pixel]).into(),
        );
        self.record.upsert(
            format!("worldreplace{index}"),
            ArrayValue::from_f64_vec(replacements).into(),
        );
        self.coordinates.add_coordinate(coordinate);
        Ok(())
    }

    fn add_defaults(
        &mut self,
        direction: bool,
        spectral: bool,
        stokes: Vec<String>,
        linear: i32,
        tabular: bool,
    ) -> PyResult<()> {
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

fn restore(record: RecordValue, parent_name: String) -> PyResult<CoordinateState> {
    let mut coordinates =
        CoordinateSystem::from_record(&record).map_err(|error| raised(error.to_string()))?;
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

    let placements =
        CoordinateSystem::record_coordinates(&record).map_err(|error| raised(error.to_string()))?;
    let mut restored = coordinates.obs_info().to_record();
    for placement in placements {
        let field = placement.field;
        let index = field.trim_start_matches(|character: char| !character.is_ascii_digit());
        for key in [
            format!("pixelmap{index}"),
            format!("worldmap{index}"),
            format!("pixelreplace{index}"),
            format!("worldreplace{index}"),
            field,
        ] {
            let value = record
                .get(&key)
                .ok_or_else(|| raised(format!("RecordInterface: unknown field {key}")))?;
            restored.push(RecordField::new(key, value.clone()));
        }
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
        let stokes = stokes_argument(stokes)?;
        let child = py.detach(|| {
            let mut state = self.state();
            let parent_name = std::mem::take(&mut state.parent_name);
            *state = CoordinateState::empty();
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
        let stokes = stokes_argument(stokes)?;
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
