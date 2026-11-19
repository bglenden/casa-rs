// SPDX-License-Identifier: LGPL-3.0-or-later
//! The active MeasComet table and the queries CASA exposes on it.

use std::borrow::Cow;
use std::path::{Path, PathBuf};

use casa_aipsio::{AipsIo, AipsOpenOption};
use casa_measures_data::{CometTable, MeasuresDataError};
use pyo3::prelude::*;

use super::{PyMeasures, State, arguments};
use crate::convert::holder::MeasureValue;
use crate::convert::value::CasaValue;
use crate::tools::LogLine;

fn load(state: &mut State, name: &str, lines: &mut Vec<LogLine>) -> CasaValue {
    state.comet = None;
    if name.is_empty() {
        return CasaValue::Bool(false);
    }
    let loaded = expand_path(name)
        .and_then(|name| resolve(Path::new(name.as_ref())))
        .ok_or_else(|| MeasuresDataError::MissingData(format!("comet table {name}")))
        .and_then(|table| CometTable::open(&table));
    match loaded {
        Ok(comet) => {
            state.comet = Some(comet);
            CasaValue::Bool(true)
        }
        Err(error) => {
            lines.push(LogLine::error(format!(
                "Exception Reports from framecomet: {error}"
            )));
            CasaValue::Bool(false)
        }
    }
}

fn resolve(path: &Path) -> Option<PathBuf> {
    if is_readable_table(path) {
        return Some(path.to_owned());
    }
    if path.is_absolute() {
        return None;
    }
    let root = match std::env::var_os("CASA_RS_MEASURESPATH") {
        Some(root) => PathBuf::from(root),
        None => PathBuf::from(std::env::var_os("HOME")?).join(".casa/data"),
    };
    // MeasIERS::findTab checks the named table in these two directories.
    ["ephemerides", "geodetic"]
        .into_iter()
        .map(|directory| root.join(directory).join(path))
        .find(|table| is_readable_table(table))
}

fn is_readable_table(path: &Path) -> bool {
    AipsIo::open(path.join("table.dat"), AipsOpenOption::Old)
        .is_ok_and(|mut io| io.get_next_type().is_ok_and(|kind| kind == "Table"))
}

fn expand_path(name: &str) -> Option<Cow<'_, str>> {
    if !name.starts_with('~') && !name.contains('$') {
        return Some(Cow::Borrowed(name));
    }
    let mut path = Cow::Borrowed(name);
    // casacore Path::expandName follows environment references recursively
    // and stops expanding if the substitutions form a cycle.
    for _ in 0..25 {
        if let Some(suffix) = path
            .strip_prefix('~')
            .filter(|s| s.is_empty() || s.starts_with('/'))
            && let Ok(home) = std::env::var("HOME")
            && !home.is_empty()
        {
            path = Cow::Owned(format!("{home}{suffix}"));
        }
        let mut changed = false;
        let next = path
            .split('/')
            .map(|part| match expand_variable(part) {
                Some(expanded) => {
                    changed = true;
                    Cow::Owned(expanded)
                }
                None => Cow::Borrowed(part),
            })
            .collect::<Vec<_>>()
            .join("/");
        if !changed {
            return Some(path);
        }
        path = Cow::Owned(next);
    }
    None
}

fn expand_variable(part: &str) -> Option<String> {
    let (prefix, expression) = part.split_once('$')?;
    let (variable, suffix) = match expression.strip_prefix('{') {
        Some(braced) => braced.split_once('}')?,
        None => (expression, ""),
    };
    let value = std::env::var(variable)
        .ok()
        .filter(|value| !value.is_empty())?;
    Some(format!("{prefix}{value}{suffix}"))
}

fn read_distance_or_diameter(
    state: &mut State,
    angular_diameter: bool,
    lines: &mut Vec<LogLine>,
) -> CasaValue {
    let unit = if angular_diameter { "rad" } else { "AU" };
    let Some(comet) = state.comet.as_mut() else {
        return CasaValue::quantity(-1.0, unit);
    };
    let mjd = match state.frame.epoch.map(|epoch| epoch.value) {
        Some(MeasureValue::Epoch(epoch)) => epoch.as_mjd(),
        _ => {
            lines.push(LogLine::error(
                "Cannot convert due to missing frame information",
            ));
            return CasaValue::quantity(-1.0, unit);
        }
    };
    let distance = match comet.distance_au(mjd) {
        Ok(Some(distance)) => distance,
        Ok(None) => {
            lines.push(LogLine::error(
                "cometdist:  No comet table with a distance for the right time present.",
            ));
            return CasaValue::quantity(-1.0, unit);
        }
        Err(error) => {
            lines.push(LogLine::error(format!("Exception reported: {error}")));
            return CasaValue::quantity(-1.0, unit);
        }
    };
    if !angular_diameter {
        return CasaValue::quantity(distance, unit);
    }
    let radius = comet.mean_radius_au().unwrap_or(-1.0);
    if radius < 0.0 {
        lines.push(LogLine::error(
            "The table is missing the meanrad keyword, needed to calculate the apparent diameter.",
        ));
    }
    CasaValue::quantity(2.0 * radius / distance, unit)
}

#[pymethods]
impl PyMeasures {
    /// Set the frame's comet ephemeris from a CASA table.
    #[pyo3(signature = (v = None), text_signature = "($self, v='')")]
    fn framecomet(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with=arguments::supplied)] v: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let name = arguments::text(v.as_ref(), "v", "")?;
        self.call(py, |state, lines| Ok(load(state, &name, lines)))
    }

    /// The current object's distance in AU.
    fn cometdist(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.call(py, |state, lines| {
            Ok(read_distance_or_diameter(state, false, lines))
        })
    }

    /// The current object's apparent diameter in radians.
    fn cometangdiam(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.call(py, |state, lines| {
            Ok(read_distance_or_diameter(state, true, lines))
        })
    }

    /// Return the current comet table's object name.
    fn cometname(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.call(py, |state, lines| {
            let name = if let Some(comet) = &state.comet {
                comet.name()
            } else {
                lines.push(LogLine::warning(
                    "Method cometname fails! No Comet table present",
                ));
                ""
            };
            Ok(CasaValue::Text(name.to_owned()))
        })
    }

    /// Return TOPO for a topocentric table, APP for another loaded table,
    /// or none when no table is loaded.
    fn comettype(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.call(py, |state, _| {
            let refer = match &state.comet {
                Some(comet) if comet.topocentre_m().is_some() => "TOPO",
                Some(_) => "APP",
                None => "none",
            };
            Ok(CasaValue::Text(refer.into()))
        })
    }

    /// Return the current table's Cartesian observing location in metres.
    fn comettopo(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.call(py, |state, lines| {
            let (xyz, unit) =
                if let Some(topo) = state.comet.as_ref().and_then(CometTable::topocentre_m) {
                    (topo.to_vec(), "m")
                } else {
                    lines.push(LogLine::warning(
                        "Method comettopo fails!  No Topocentric Comet table present",
                    ));
                    (vec![0.0], "")
                };
            Ok(CasaValue::record([
                ("unit", CasaValue::Text(unit.into())),
                ("value", CasaValue::floats(xyz)),
            ]))
        })
    }
}
