// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's `coordsys` tool (cs): every method of CASA 6.7.6.14's binding,
//! with its signature (ADR-0017, #1025). A method casa-rs does not implement
//! yet raises `NotImplementedError` naming #1027 (`stubs.rs`).

mod reference;
mod stubs;
pub(crate) mod surface;

use std::sync::{Mutex, MutexGuard, PoisonError};

use casa_coordinates::{CoordinateSystem, ObsInfo, defaults};
use casa_types::RecordValue;
use casa_types::measures::{EpochRef, MEpoch, MPosition};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::tools::not_yet_implemented_in;

/// The issue that records the methods casa-rs refuses.
const ISSUE: u32 = 1027;

/// A method casa-rs does not implement yet.
fn stub(method: &str) -> PyResult<Py<PyAny>> {
    not_yet_implemented_in("coordsys", method, ISSUE)
}

/// CASA's `coordsys` tool.
#[pyclass(name = "coordsys", module = "casars.tools", frozen)]
pub(crate) struct PyCoordsys {
    state: Mutex<CoordinateState>,
}

struct CoordinateState {
    coordinates: CoordinateSystem,
    record: RecordValue,
}

pub(crate) fn default_image_obs_info() -> ObsInfo {
    // ITRF position of ALMA from the casacore Observatories table.
    let (sin_lon, cos_lon) = (-1.182_546_595_504_989_2_f64).sin_cos();
    let (sin_lat, cos_lat) = (-0.399_414_986_926_273_8_f64).sin_cos();
    let radius = 6_379_946.013_264_43_f64;
    defaults::obs_info().with_telescope_position(MPosition::new_itrf(
        cos_lon * cos_lat * radius,
        sin_lon * cos_lat * radius,
        sin_lat * radius,
    ))
}

impl CoordinateState {
    fn from_record(record: RecordValue) -> PyResult<Self> {
        let coordinates = CoordinateSystem::from_record(&record)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        Ok(Self {
            coordinates,
            record,
        })
    }

    fn empty() -> Self {
        let mut info = default_image_obs_info();
        if let Ok(now) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            info.date = Some(MEpoch::from_mjd(
                40_587.0 + now.as_secs_f64() / 86_400.0,
                EpochRef::UTC,
            ));
        }
        let coordinates = CoordinateSystem::new().with_obs_info(info);
        let record = coordinates.to_record();
        Self {
            coordinates,
            record,
        }
    }
}

impl PyCoordsys {
    pub(crate) fn empty() -> Self {
        Self {
            state: Mutex::new(CoordinateState::empty()),
        }
    }

    pub(crate) fn from_record(record: RecordValue) -> PyResult<Self> {
        Ok(Self {
            state: Mutex::new(CoordinateState::from_record(record)?),
        })
    }

    fn state(&self) -> MutexGuard<'_, CoordinateState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[pymethods]
impl PyCoordsys {
    #[new]
    fn new() -> Self {
        Self::empty()
    }
}
