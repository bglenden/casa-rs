// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{BTreeMap, BTreeSet};

use casa_ms::subtables::antenna_observatory_offsets;
use casa_ms::{MeasurementSet, MsAntenna, MsError, MsResult, SubTable};
use casa_tables::table_measures::{MeasRefDesc, TableMeasDesc};
use casa_types::measures::PositionRef;
use casa_types::quanta::{Dimension, UnitDim};
use numpy::{PyArrayDescrMethods, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

use super::PyMsMetadata;
use super::column_units::column_unit_factor;
use super::intents::wildcard_pattern;
use super::names::{Indices, checked_row, selector_text};
use super::observation_metadata::observatory_position;
use super::scan_index::ScanIndex;
use super::scan_queries::{FieldSelector, int_array};
use crate::convert::holder::{Measure, MeasureValue};
use crate::convert::value::CasaValue;
use crate::convert::{Variant, nested_sequence_items, quantity};

enum AntennaSelection {
    Indices(Indices),
    Name(String),
    Names(Vec<String>),
}

impl AntennaSelection {
    fn capture(value: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let Some(value) = value else {
            return Ok(Self::Indices(Indices::Empty));
        };
        if let Some(name) = selector_text(value)? {
            return Ok(Self::Name(name));
        }
        let names = if let Ok(array) = value.cast::<PyUntypedArray>() {
            let kind = array.dtype().kind();
            if matches!(kind, b'S' | b'U') {
                if array.len() == 0 {
                    return Ok(Self::Names(Vec::new()));
                }
                Some(
                    array
                        .call_method1("ravel", ("F",))?
                        .call_method0("tolist")?
                        .try_iter()?
                        .collect::<PyResult<Vec<_>>>()?,
                )
            } else {
                None
            }
        } else if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() {
            if let Some((_, items)) = nested_sequence_items(value)? {
                Some(items)
            } else {
                Some(value.try_iter()?.collect::<PyResult<Vec<_>>>()?)
            }
        } else {
            None
        };
        if let Some(items) = names.filter(|items| !items.is_empty()) {
            let names = items
                .iter()
                .map(selector_text)
                .collect::<PyResult<Option<Vec<_>>>>()?;
            if let Some(names) = names {
                return Ok(Self::Names(names));
            }
        }
        Ok(Self::Indices(antenna_indices(Some(value))?))
    }
}

fn antenna_indices(value: Option<&Bound<'_, PyAny>>) -> PyResult<Indices> {
    if let Some(value) = value
        && value.is_instance(&value.py().import("numpy")?.getattr("bool_")?)?
    {
        return Ok(Indices::Unsupported);
    }
    Indices::capture(value)
}

fn diameter_limit(value: Option<&Bound<'_, PyAny>>, default: &str) -> PyResult<f64> {
    let input = match Indices::capture(value)? {
        Indices::Empty | Indices::BooleanVector => Variant::Text(default.to_owned()),
        _ => match value {
            Some(value) => match selector_text(value)? {
                Some(text) => Variant::Text(text),
                None => value.extract::<Variant>()?,
            },
            None => Variant::Text(default.to_owned()),
        },
    };
    let value = quantity(&input)?;
    let unit = value.unit().val();
    if unit.dim != UnitDim::basis(Dimension::Length) {
        return Err(PyRuntimeError::new_err(
            "Antenna diameter limits must have units of length",
        ));
    }
    Ok(unit.factor * value.value())
}

struct AntennaNames {
    ordered: Vec<String>,
    ids: BTreeMap<String, Vec<usize>>,
}

impl AntennaNames {
    fn read(antenna: &MsAntenna<'_>) -> MsResult<Self> {
        let ordered = (0..antenna.row_count())
            .map(|row| antenna.name(row))
            .collect::<MsResult<Vec<_>>>()?;
        let mut ids = BTreeMap::<String, Vec<usize>>::new();
        for (row, name) in ordered.iter().enumerate() {
            ids.entry(name.clone()).or_default().push(row);
        }
        Ok(Self { ordered, ids })
    }

    fn exact(&self, name: &str) -> MsResult<&[usize]> {
        self.ids
            .get(name)
            .map(Vec::as_slice)
            .ok_or_else(|| MsError::InvalidInput(format!("Unknown antenna {name}")))
    }

    fn select(
        &self,
        selectors: Vec<String>,
        observed: Option<&BTreeSet<usize>>,
    ) -> MsResult<Vec<usize>> {
        let mut visited = BTreeSet::new();
        let mut found = Vec::new();
        for selector in &selectors {
            if let Some(pattern) = wildcard_pattern(selector)? {
                for name in self.ordered.iter().filter(|name| pattern.is_match(name)) {
                    self.append(name, &mut visited, &mut found, observed)?;
                }
            } else {
                self.append(selector, &mut visited, &mut found, observed)?;
            }
        }
        Ok(found)
    }

    fn append<'a>(
        &'a self,
        name: &'a str,
        visited: &mut BTreeSet<&'a str>,
        found: &mut Vec<usize>,
        observed: Option<&BTreeSet<usize>>,
    ) -> MsResult<()> {
        if visited.insert(name) {
            let rows = self.exact(name)?;
            if let Some(observed) = observed {
                found.extend(rows.iter().filter(|row| observed.contains(row)));
            } else if let Some(&row) = rows.last() {
                found.push(row);
            }
        }
        Ok(())
    }
}

fn scalar_row(antenna: &MsAntenna<'_>, selector: FieldSelector, last: bool) -> MsResult<usize> {
    match selector {
        FieldSelector::Id(id) => checked_row(id, antenna.row_count(), "Antenna ID"),
        FieldSelector::Name(name) => {
            let names = AntennaNames::read(antenna)?;
            let ids = names.exact(&name)?;
            Ok(if last { ids[ids.len() - 1] } else { ids[0] })
        }
        FieldSelector::Unsupported => Err(MsError::InvalidInput(
            "Antenna must be an integer or name".into(),
        )),
    }
}

fn observed_antennas(
    ms: &MeasurementSet,
    index: &ScanIndex,
    obsid: i64,
) -> MsResult<BTreeSet<usize>> {
    index.check_scope(ms, obsid, -1, false)?;
    Ok(index
        .matching(obsid, -1)
        .flat_map(|(_, scan)| scan.antennas.iter())
        .map(|&id| id as usize)
        .collect())
}

fn position_measure(antenna: &MsAntenna<'_>, row: usize) -> MsResult<Measure> {
    let description = TableMeasDesc::reconstruct(antenna.table(), "POSITION").ok_or_else(|| {
        MsError::InvalidInput("ANTENNA.POSITION has no measure descriptor".into())
    })?;
    let MeasRefDesc::Fixed { refer } = description.ref_desc() else {
        return Err(MsError::InvalidInput(
            "ANTENNA.POSITION requires a fixed position reference".into(),
        ));
    };
    let refer = match refer.parse::<PositionRef>()? {
        PositionRef::ITRF => "ITRF",
        PositionRef::WGS84 => "WGS84",
    };
    Ok(Measure {
        refer,
        value: MeasureValue::Position(position_metres(antenna, row)?),
    })
}

fn position_metres(antenna: &MsAntenna<'_>, row: usize) -> MsResult<[f64; 3]> {
    let mut xyz = antenna.position(row)?;
    for (component, value) in xyz.iter_mut().enumerate() {
        *value *= column_unit_factor(antenna.table(), "POSITION", component, "m")?;
    }
    Ok(xyz)
}

fn selected_antenna_ids(
    antenna: &MsAntenna<'_>,
    selection: AntennaSelection,
    observed: Option<&BTreeSet<usize>>,
) -> MsResult<Vec<usize>> {
    let names = match selection {
        AntennaSelection::Name(name) if name.is_empty() => None,
        AntennaSelection::Name(name) => Some(vec![name]),
        AntennaSelection::Names(names) => Some(names),
        AntennaSelection::Indices(Indices::Empty | Indices::BooleanVector) => None,
        AntennaSelection::Indices(_) => {
            return Err(MsError::InvalidInput(
                "Antenna names must be a string or string array".into(),
            ));
        }
    };
    match names {
        Some(names) => AntennaNames::read(antenna)?.select(names, observed),
        None => Ok(observed.map_or_else(
            || (0..antenna.row_count()).collect(),
            |rows| rows.iter().copied().collect(),
        )),
    }
}

fn station_rows(
    antenna: &MsAntenna<'_>,
    selection: AntennaSelection,
    observed: Option<&BTreeSet<usize>>,
) -> MsResult<Vec<usize>> {
    match selection {
        AntennaSelection::Indices(indices) => {
            let indices = match indices {
                Indices::BooleanVector => Indices::Empty,
                Indices::Scalar(id) if id < 0 => Indices::Empty,
                Indices::Vector(ref ids) if ids.len() == 1 && ids[0] < 0 => Indices::Empty,
                indices => indices,
            };
            let rows = if let Some(observed) = observed {
                let input = match indices {
                    Indices::Empty => return Ok(observed.iter().copied().collect()),
                    Indices::Vector(ids) if ids.is_empty() => {
                        return Ok(observed.iter().copied().collect());
                    }
                    Indices::Scalar(id) => vec![id],
                    Indices::Vector(ids) => ids,
                    _ => {
                        return Err(MsError::InvalidInput(
                            "Unsupported antenna selection".into(),
                        ));
                    }
                };
                if input.iter().any(|&id| id < 0) {
                    return Err(MsError::InvalidInput(
                        "Antenna IDs must be nonnegative".into(),
                    ));
                }
                input
                    .into_iter()
                    .map(|id| id as usize)
                    .filter(|id| observed.contains(id))
                    .collect()
            } else {
                indices.rows(antenna.row_count(), "Antenna ID")?
            };
            Ok(rows)
        }
        AntennaSelection::Name(name) => {
            let names = AntennaNames::read(antenna)?;
            let rows = names.exact(&name)?;
            Ok(match observed {
                Some(observed) => rows
                    .iter()
                    .copied()
                    .filter(|id| observed.contains(id))
                    .collect(),
                None => rows.to_vec(),
            })
        }
        AntennaSelection::Names(selectors) => {
            let names = AntennaNames::read(antenna)?;
            if let Some(observed) = observed {
                names.select(selectors, Some(observed))
            } else {
                selectors
                    .iter()
                    .map(|name| names.exact(name).map(|rows| rows[rows.len() - 1]))
                    .collect()
            }
        }
    }
}

#[pymethods]
impl PyMsMetadata {
    #[pyo3(signature = (antenna = FieldSelector::Id(-1)), text_signature = "($self, antenna=-1)")]
    fn antennadiameter(&self, py: Python<'_>, antenna: FieldSelector) -> PyResult<Py<PyAny>> {
        self.with_ms(py, |ms| {
            let table = ms.antenna()?;
            let diameter = |row| {
                table
                    .dish_diameter(row)
                    .map(|value| CasaValue::quantity(value, "m"))
            };
            if matches!(antenna, FieldSelector::Id(id) if id < 0) {
                let values = (0..table.row_count())
                    .map(|row| diameter(row).map(|value| (row.to_string(), value)))
                    .collect::<MsResult<Vec<_>>>()?;
                Ok(CasaValue::record(values))
            } else {
                diameter(scalar_row(&table, antenna, true)?)
            }
        })?
        .into_python(py)
    }

    #[pyo3(signature = (name = None, mindiameter = None, maxdiameter = None, obsid = -1), text_signature = "($self, name='', mindiameter='0m', maxdiameter='1pc', obsid=-1)")]
    fn antennaids(
        &self,
        py: Python<'_>,
        name: Option<&Bound<'_, PyAny>>,
        mindiameter: Option<&Bound<'_, PyAny>>,
        maxdiameter: Option<&Bound<'_, PyAny>>,
        obsid: i64,
    ) -> PyResult<Py<PyAny>> {
        let selection = AntennaSelection::capture(name)?;
        let min = diameter_limit(mindiameter, "0m")?;
        let max = diameter_limit(maxdiameter, "1pc")?;
        let select = |ms: &MeasurementSet, observed: Option<&BTreeSet<usize>>| {
            let antenna = ms.antenna()?;
            let rows = selected_antenna_ids(&antenna, selection, observed)?;
            let mut ids = Vec::new();
            for row in rows {
                let diameter = antenna.dish_diameter(row)?;
                if diameter >= min && diameter <= max {
                    ids.push(row as i32);
                }
            }
            Ok(int_array(ids))
        };
        let ids = if obsid < 0 {
            self.with_ms(py, |ms| select(ms, None))
        } else {
            self.with_scan_index(py, |ms, index| {
                select(ms, Some(&observed_antennas(ms, index, obsid)?))
            })
        }?;
        ids.into_python(py)
    }

    #[pyo3(signature = (which = FieldSelector::Id(0)), text_signature = "($self, which=0)")]
    fn antennaoffset(&self, py: Python<'_>, which: FieldSelector) -> PyResult<Py<PyAny>> {
        self.with_ms(py, |ms| {
            let antenna = ms.antenna()?;
            let row = scalar_row(&antenna, which, false)?;
            let [longitude, latitude, elevation] = antenna_observatory_offsets(
                position_metres(&antenna, row)?,
                observatory_position(ms, 0)?.as_itrf(),
            );
            Ok(CasaValue::record([
                ("longitude offset", CasaValue::quantity(longitude, "m")),
                ("latitude offset", CasaValue::quantity(latitude, "m")),
                ("elevation offset", CasaValue::quantity(elevation, "m")),
            ]))
        })?
        .into_python(py)
    }

    #[pyo3(signature = (which = None), text_signature = "($self, which=0)")]
    fn antennaposition(
        &self,
        py: Python<'_>,
        which: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let which = match antenna_indices(which)? {
            Indices::Empty | Indices::BooleanVector => FieldSelector::Id(0),
            Indices::Scalar(id) => FieldSelector::Id(id),
            _ => match which {
                Some(value) => value.extract::<FieldSelector>()?,
                None => FieldSelector::Id(0),
            },
        };
        self.with_ms(py, |ms| {
            let antenna = ms.antenna()?;
            let row = scalar_row(&antenna, which, true)?;
            Ok(position_measure(&antenna, row)?.to_value())
        })?
        .into_python(py)
    }

    #[pyo3(signature = (which = None, obsid = -1), text_signature = "($self, which=-1, obsid=-1)")]
    fn antennastations(
        &self,
        py: Python<'_>,
        which: Option<&Bound<'_, PyAny>>,
        obsid: i64,
    ) -> PyResult<Vec<String>> {
        let selection = AntennaSelection::capture(which)?;
        let select = |ms: &MeasurementSet, observed: Option<&BTreeSet<usize>>| {
            let antenna = ms.antenna()?;
            station_rows(&antenna, selection, observed)?
                .into_iter()
                .map(|row| antenna.station(row))
                .collect()
        };
        if obsid < 0 {
            self.with_ms(py, |ms| select(ms, None))
        } else {
            self.with_scan_index(py, |ms, index| {
                let observed = observed_antennas(ms, index, obsid)?;
                if observed.is_empty() {
                    Ok(Vec::new())
                } else {
                    select(ms, Some(&observed))
                }
            })
        }
    }
}
