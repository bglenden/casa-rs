// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType, RecordCoordinate};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::{CoordinateState, PyCoordsys};
use crate::convert::value::CasaValue;

fn raised(message: impl Into<String>) -> PyErr {
    PyRuntimeError::new_err(message.into())
}

pub(super) fn coordinate_type(name: &str) -> CoordinateType {
    match name.to_ascii_uppercase().as_bytes() {
        [b'D', ..] => CoordinateType::Direction,
        [b'S', b'T', ..] => CoordinateType::Stokes,
        [b'S', b'P', ..] => CoordinateType::Spectral,
        [b'T', ..] => CoordinateType::Tabular,
        _ => CoordinateType::Linear,
    }
}

fn coordinate_index(placement: &RecordCoordinate) -> usize {
    placement
        .field
        .trim_start_matches(|character: char| !character.is_ascii_digit())
        .parse()
        .expect("record coordinate fields end in their numeric index")
}

pub(super) fn numbered_coordinates(
    state: &CoordinateState,
) -> PyResult<Vec<(usize, &CoordinateModel, RecordCoordinate)>> {
    let placements = CoordinateSystem::record_coordinates(&state.record)
        .map_err(|error| raised(error.to_string()))?;
    let mut numbered = placements
        .into_iter()
        .enumerate()
        .map(|(index, placement)| {
            (
                coordinate_index(&placement),
                state.coordinates.coordinate(index),
                placement,
            )
        })
        .collect::<Vec<_>>();
    numbered.sort_by_key(|(index, _, _)| *index);
    Ok(numbered)
}

fn axis_names(coordinates: &CoordinateSystem) -> Vec<String> {
    (0..coordinates.n_coordinates())
        .flat_map(|index| coordinates.coordinate(index).axis_names())
        .collect()
}

pub(super) fn units(coordinate: &CoordinateModel) -> Vec<String> {
    match coordinate {
        CoordinateModel::Direction(direction) => direction.visible_world_units(),
        _ => coordinate.axis_units(),
    }
}

fn ints(values: Vec<i64>) -> CasaValue {
    CasaValue::Ints {
        shape: vec![values.len()],
        values,
    }
}

fn friendly_name(name: &str) -> &str {
    match name {
        "right ascension" => "ra",
        "frequency" | "velocity" => "spectral",
        _ => name,
    }
}

#[pymethods]
impl PyCoordsys {
    #[pyo3(signature = (toworld=true), text_signature = "($self, toworld=True)")]
    fn axesmap(&self, py: Python<'_>, toworld: bool) -> PyResult<Py<PyAny>> {
        let value = py.detach(|| {
            let state = self.state();
            let count = if toworld {
                state.coordinates.n_pixel_axes()
            } else {
                state.coordinates.n_world_axes()
            };
            // The native reader currently accepts matching visible maps.
            ints((0..count as i64).collect())
        });
        value.into_python(py)
    }

    #[pyo3(signature = (world=true), text_signature = "($self, world=True)")]
    fn axiscoordinatetypes(&self, py: Python<'_>, world: bool) -> Vec<String> {
        py.detach(|| {
            let state = self.state();
            (0..state.coordinates.n_coordinates())
                .flat_map(|index| {
                    let coordinate = state.coordinates.coordinate(index);
                    let count = if world {
                        coordinate.n_world_axes()
                    } else {
                        coordinate.n_pixel_axes()
                    };
                    std::iter::repeat_n(coordinate.coordinate_type().to_string(), count)
                })
                .collect()
        })
    }

    #[pyo3(signature = (which=-1), text_signature = "($self, which=-1)")]
    fn coordinatetype(&self, py: Python<'_>, which: i64) -> PyResult<Vec<String>> {
        py.detach(|| {
            let state = self.state();
            let numbered = numbered_coordinates(&state)?;
            if numbered.is_empty() {
                return Err(raised("This CoordinateSystem is empty"));
            }
            if which < 0 {
                return Ok(numbered
                    .iter()
                    .map(|(_, coordinate, _)| coordinate.coordinate_type().to_string())
                    .collect());
            }
            let (_, coordinate, _) = numbered.get(which as usize).ok_or_else(|| {
                raised(format!(
                    "There are only {} coordinates available",
                    numbered.len()
                ))
            })?;
            Ok(vec![coordinate.coordinate_type().to_string()])
        })
    }

    fn copy(&self, py: Python<'_>) -> PyResult<Py<Self>> {
        let state = py.detach(|| {
            let mut copy = self.state().clone();
            copy.parent_name = "unknown".into();
            copy
        });
        Py::new(
            py,
            Self {
                state: std::sync::Mutex::new(state),
            },
        )
    }

    #[pyo3(signature = (world=true, axis=0), text_signature = "($self, world=True, axis=0)")]
    fn findaxis(&self, py: Python<'_>, world: bool, axis: i64) -> PyResult<Py<PyAny>> {
        let located = py.detach(|| {
            if axis < 0 {
                return Err(raised("Value of axis cannot be negative"));
            }
            let state = self.state();
            numbered_coordinates(&state)?
                .into_iter()
                .find_map(|(index, _, placement)| {
                    placement
                        .pixel_axes
                        .iter()
                        .position(|entry| *entry == Some(axis as usize))
                        .map(|local| (index, local))
                })
                .ok_or_else(|| {
                    let name = if world { "World" } else { "Pixel" };
                    raised(format!(
                        "Failed AlwaysAssert axisInCoordinateSystem < n{name}Axes()"
                    ))
                })
        })?;
        CasaValue::record([
            ("axisincoordinate", CasaValue::Int(located.1 as i64)),
            ("coordinate", CasaValue::Int(located.0 as i64)),
        ])
        .into_python(py)
    }

    #[pyo3(signature = (axisname="", allowfriendlyname=true), text_signature = "($self, axisname='', allowfriendlyname=True)")]
    fn findaxisbyname(
        &self,
        py: Python<'_>,
        axisname: &str,
        allowfriendlyname: bool,
    ) -> PyResult<usize> {
        py.detach(|| {
            let requested = axisname.to_ascii_lowercase();
            let names = axis_names(&self.state().coordinates)
                .into_iter()
                .map(|name| name.to_ascii_lowercase())
                .collect::<Vec<_>>();
            let matches = names
                .iter()
                .enumerate()
                .filter(|(_, name)| {
                    name.starts_with(&requested)
                        || (allowfriendlyname && friendly_name(name).starts_with(&requested))
                })
                .collect::<Vec<_>>();
            match matches.as_slice() {
                [(axis, _)] => Ok(*axis),
                [] => Err(raised(format!(
                    "No axis matches requested axis {requested}. Image axis names are [{}]",
                    names.join(", ")
                ))),
                _ => Err(raised(format!(
                    "Multiple axes [{}] match requested axis {requested}",
                    matches
                        .iter()
                        .map(|(_, name)| name.as_str())
                        .collect::<Vec<_>>()
                        .join(",")
                ))),
            }
        })
    }

    #[pyo3(signature = (r#type="direction", which=0), text_signature = "($self, type='direction', which=0)")]
    fn findcoordinate(&self, py: Python<'_>, r#type: &str, which: i64) -> PyResult<Py<PyAny>> {
        let value = py.detach(|| {
            let state = self.state();
            let kind = coordinate_type(r#type);
            let placement = numbered_coordinates(&state)?
                .into_iter()
                .filter(|(_, coordinate, _)| coordinate.coordinate_type() == kind)
                .nth(which.max(0) as usize)
                .map(|(_, _, placement)| placement);
            let found = placement.is_some();
            let axes = placement.map_or_else(Vec::new, |placement| {
                placement
                    .pixel_axes
                    .into_iter()
                    .map(|axis| axis.map_or(-1, |axis| axis as i64))
                    .collect()
            });
            Ok::<_, PyErr>(CasaValue::record([
                ("pixel", ints(axes.clone())),
                ("return", CasaValue::Bool(found)),
                ("world", ints(axes)),
            ]))
        })?;
        value.into_python(py)
    }

    #[pyo3(signature = (r#type=""), text_signature = "($self, type='')")]
    fn names(&self, py: Python<'_>, r#type: &str) -> PyResult<Vec<String>> {
        py.detach(|| {
            let state = self.state();
            if r#type.is_empty() {
                return Ok(axis_names(&state.coordinates));
            }
            let kind = coordinate_type(r#type);
            Ok(numbered_coordinates(&state)?
                .into_iter()
                .find(|(_, coordinate, _)| coordinate.coordinate_type() == kind)
                .and_then(|(_, coordinate, _)| coordinate.axis_names().into_iter().next())
                .into_iter()
                .collect())
        })
    }

    #[pyo3(signature = (world=true), text_signature = "($self, world=True)")]
    fn naxes(&self, py: Python<'_>, world: bool) -> usize {
        py.detach(|| {
            let state = self.state();
            if world {
                state.coordinates.n_world_axes()
            } else {
                state.coordinates.n_pixel_axes()
            }
        })
    }

    fn ncoordinates(&self, py: Python<'_>) -> usize {
        py.detach(|| self.state().coordinates.n_coordinates())
    }

    fn observer(&self, py: Python<'_>) -> String {
        py.detach(|| self.state().coordinates.obs_info().observer.clone())
    }

    fn parentname(&self, py: Python<'_>) -> String {
        py.detach(|| self.lock_state().parent_name.clone())
    }

    fn setparentname(&self, py: Python<'_>, imagename: String) -> bool {
        py.detach(|| self.lock_state().parent_name = imagename);
        true
    }

    fn stokes(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        py.detach(|| {
            let state = self.state();
            numbered_coordinates(&state)?
                .into_iter()
                .find_map(|(_, coordinate, _)| match coordinate {
                    CoordinateModel::Stokes(stokes) => Some(
                        stokes
                            .stokes()
                            .iter()
                            .map(|value| value.name().into())
                            .collect(),
                    ),
                    _ => None,
                })
                .ok_or_else(|| raised("No coordinate of type Stokes in this CoordinateSystem"))
        })
    }

    fn telescope(&self, py: Python<'_>) -> String {
        py.detach(|| self.state().coordinates.obs_info().telescope.clone())
    }

    #[pyo3(signature = (r#type=""), text_signature = "($self, type='')")]
    fn units(&self, py: Python<'_>, r#type: &str) -> PyResult<Vec<String>> {
        py.detach(|| {
            let state = self.state();
            if r#type.is_empty() {
                return Ok((0..state.coordinates.n_coordinates())
                    .flat_map(|index| units(state.coordinates.coordinate(index)))
                    .collect());
            }
            let kind = coordinate_type(r#type);
            Ok(numbered_coordinates(&state)?
                .into_iter()
                .find(|(_, coordinate, _)| coordinate.coordinate_type() == kind)
                .map_or_else(Vec::new, |(_, coordinate, _)| units(coordinate)))
        })
    }
}
