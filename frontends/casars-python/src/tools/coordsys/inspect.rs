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

pub(super) fn coordinate_index(placement: &RecordCoordinate) -> usize {
    placement
        .field
        .trim_start_matches(|character: char| !character.is_ascii_digit())
        .parse()
        .expect("record coordinate fields end in their numeric index")
}

pub(super) fn numbered_coordinates(
    state: &CoordinateState,
) -> PyResult<Vec<(usize, &CoordinateModel, RecordCoordinate)>> {
    let placements = state
        .coordinates
        .coordinates_in_record(&state.record)
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
    axis_values(coordinates, true, CoordinateModel::axis_names)
}

pub(super) fn axis_values<T>(
    system: &CoordinateSystem,
    world: bool,
    local_values: impl Fn(&CoordinateModel) -> Vec<T>,
) -> Vec<T> {
    let rank = if world {
        system.n_world_axes()
    } else {
        system.n_pixel_axes()
    };
    let mut values: Vec<Option<T>> = (0..rank).map(|_| None).collect();
    for index in 0..system.n_coordinates() {
        let axes = if world {
            system.coordinate_world_axes(index)
        } else {
            system.coordinate_pixel_axes(index)
        };
        let axes = axes.expect("existing coordinate");
        for (axis, value) in axes.into_iter().zip(local_values(system.coordinate(index))) {
            if let Some(axis) = axis {
                values[axis] = Some(value);
            }
        }
    }
    values
        .into_iter()
        .map(|value| value.expect("values cover public coordinate axes"))
        .collect()
}

pub(super) fn units(coordinate: &CoordinateModel) -> Vec<String> {
    coordinate.declared_axis_units()
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
            let mut axes = vec![-1; count];
            for (_, _, placement) in numbered_coordinates(&state)? {
                let (source, target) = if toworld {
                    (placement.pixel_axes, placement.world_axes)
                } else {
                    (placement.world_axes, placement.pixel_axes)
                };
                for (source, target) in source.into_iter().zip(target) {
                    if let (Some(source), Some(target)) = (source, target) {
                        axes[source] = target as i64;
                    }
                }
            }
            Ok::<_, PyErr>(ints(axes))
        })?;
        value.into_python(py)
    }

    #[pyo3(signature = (world=true), text_signature = "($self, world=True)")]
    fn axiscoordinatetypes(&self, py: Python<'_>, world: bool) -> Vec<String> {
        py.detach(|| {
            let state = self.state();
            axis_values(&state.coordinates, world, |coordinate| {
                let count = if world {
                    coordinate.n_world_axes()
                } else {
                    coordinate.n_pixel_axes()
                };
                vec![coordinate.coordinate_type().to_string(); count]
            })
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
            copy.coordinates.reset_direction_mix_ranges();
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
                    let axes = if world {
                        placement.world_axes
                    } else {
                        placement.pixel_axes
                    };
                    axes.iter()
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
            let convert = |axes: Vec<Option<usize>>| {
                ints(
                    axes.into_iter()
                        .map(|axis| axis.map_or(-1, |axis| axis as i64))
                        .collect(),
                )
            };
            let (pixel, world) = placement.map_or_else(
                || (ints(Vec::new()), ints(Vec::new())),
                |placement| (convert(placement.pixel_axes), convert(placement.world_axes)),
            );
            Ok::<_, PyErr>(CasaValue::record([
                ("pixel", pixel),
                ("return", CasaValue::Bool(found)),
                ("world", world),
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
                .and_then(|(_, coordinate, placement)| {
                    let names = coordinate.axis_names();
                    let first = if names.len() == placement.world_axes.len() {
                        placement.world_axes.iter().position(Option::is_some)?
                    } else {
                        0
                    };
                    names.into_iter().nth(first)
                })
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
                return Ok(axis_values(&state.coordinates, true, units));
            }
            let kind = coordinate_type(r#type);
            Ok(numbered_coordinates(&state)?
                .into_iter()
                .find(|(_, coordinate, _)| coordinate.coordinate_type() == kind)
                .map_or_else(Vec::new, |(_, coordinate, placement)| {
                    let values = units(coordinate);
                    if values.len() != placement.world_axes.len() {
                        return values;
                    }
                    values
                        .into_iter()
                        .zip(placement.world_axes)
                        .filter_map(|(value, axis)| axis.map(|_| value))
                        .collect()
                }))
        })
    }
}
