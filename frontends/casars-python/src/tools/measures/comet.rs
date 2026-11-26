// SPDX-License-Identifier: LGPL-3.0-or-later
//! The active MeasComet table and the queries CASA exposes on it.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_measures_data::{CometTable, MeasuresDataError};
use casa_tables::table_type_tag;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::{EpochRef, MeasFrame, MeasureError};
use pyo3::prelude::*;

use super::{PyMeasures, State, arguments};
use crate::convert::holder::MeasureValue;
use crate::convert::value::CasaValue;
use crate::tools::LogLine;
use crate::tools::casacore_path::expand_path;

pub(super) const COMET_UNAVAILABLE: &str = "No or outside range comet table specified";

/// Casacore looks up the attached frame table at TDB. The stored direction
/// attached at the call site can carry any angles because R_COMET0 replaces
/// those angles with the interpolated position.
pub(super) fn resolve_direction(
    state: &State,
    frame: &MeasFrame,
) -> Result<(MDirection, f64), MeasureError> {
    let unavailable = MeasureError::MissingFrameData {
        what: COMET_UNAVAILABLE,
    };
    let table = state
        .frame
        .comet
        .as_ref()
        .ok_or_else(|| unavailable.clone())?;
    let epoch = frame.epoch().ok_or_else(|| unavailable.clone())?;
    let tdb = epoch.convert_to(EpochRef::TDB, frame)?.value().as_mjd();
    let sample = table
        .direction_at(tdb)
        .map_err(|error| MeasureError::ModelError {
            model: "comet ephemeris",
            reason: error.to_string(),
        })?
        .ok_or(unavailable)?;
    let resolved = if sample.remove_parallax {
        // R_COMET de-applies the observer displacement in the table's axes
        // before starting the normal route at its declared reference.
        let topocentric = MDirection::from_cosines(sample.direction.cosines(), DirectionRef::TOPO);
        let corrected =
            topocentric.convert_with_parallax(DirectionRef::APP, frame, sample.distance_m)?;
        MDirection::from_cosines(corrected.cosines(), sample.direction.refer())
    } else {
        sample.direction
    };
    Ok((resolved, sample.distance_m))
}

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
            let comet = Arc::new(comet);
            state.frame.comet = Some(Arc::clone(&comet));
            state.frame.rendered = None;
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
    table_type_tag(path).is_ok_and(|kind| kind == "Table")
}

fn read_distance_or_diameter(
    state: &mut State,
    angular_diameter: bool,
    lines: &mut Vec<LogLine>,
) -> CasaValue {
    let unit = if angular_diameter { "rad" } else { "AU" };
    let Some(comet) = state.comet.as_ref() else {
        return CasaValue::quantity(-1.0, unit);
    };
    let mjd = match state.frame.epoch.as_ref().map(|epoch| epoch.measure.value) {
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
                if let Some(topo) = state.comet.as_ref().and_then(|comet| comet.topocentre_m()) {
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
