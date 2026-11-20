// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's `coordsys` tool (cs): every method of CASA 6.7.6.14's binding,
//! with its signature (ADR-0017, #1025). A method casa-rs does not implement
//! yet raises `NotImplementedError` naming #1027 (`stubs.rs`).

mod axis_labels;
mod construction;
pub(super) use construction::{append_coordinate, validate_coordinate_indices};
mod frame_metadata;
mod inspect;
mod observation;
mod reference;
mod stubs;
pub(crate) mod surface;
mod transform_many;
pub(super) mod world_reference;

mod transform_single;
mod world_edits;
pub(super) use transform_single::{WorldArgument, image_pixel_argument};

use std::sync::{Mutex, MutexGuard, PoisonError};

use casa_coordinates::{CoordinateSystem, ObsInfo, defaults};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::{EpochRef, MEpoch, MPosition};
use casa_types::measures::{direction_to_record, epoch_to_record, position_to_record};
use casa_types::{RecordField, RecordValue, ScalarValue, Value};
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

#[derive(Clone)]
struct CoordinateState {
    coordinates: CoordinateSystem,
    record: RecordValue,
    parent_name: String,
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

pub(crate) fn default_spectral_conversion() -> RecordValue {
    // LAST is casacore's placeholder for an unset conversion frame.
    let epoch = epoch_to_record(&MEpoch::from_mjd(0.0, EpochRef::LAST));
    RecordValue::new(vec![
        RecordField::new("epoch", Value::Record(epoch)),
        RecordField::new(
            "direction",
            Value::Record(direction_to_record(&MDirection::from_angles(
                0.0,
                std::f64::consts::FRAC_PI_2,
                DirectionRef::J2000,
            ))),
        ),
        RecordField::new(
            "position",
            Value::Record(position_to_record(&MPosition::new_itrf(0.0, 0.0, 0.0))),
        ),
        RecordField::new("system", Value::Scalar(ScalarValue::String("LSRK".into()))),
    ])
}

fn current_epoch(mut info: ObsInfo) -> ObsInfo {
    if let Ok(now) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        info.date = Some(MEpoch::from_mjd(
            40_587.0 + now.as_secs_f64() / 86_400.0,
            EpochRef::UTC,
        ));
    }
    info
}

impl CoordinateState {
    fn from_record(record: RecordValue) -> PyResult<Self> {
        let coordinates = CoordinateSystem::from_record(&record)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        Ok(Self {
            coordinates,
            record,
            parent_name: "unknown".into(),
        })
    }

    fn empty() -> Self {
        Self::new(CoordinateSystem::new().with_obs_info(current_epoch(default_image_obs_info())))
    }

    fn new(coordinates: CoordinateSystem) -> Self {
        let record = coordinates.to_record();
        Self {
            coordinates,
            record,
            parent_name: "unknown".into(),
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
        let mut state = self.lock_state();
        if state.parent_name.is_empty() {
            state.parent_name = "unknown".into();
        }
        state
    }

    fn lock_state(&self) -> MutexGuard<'_, CoordinateState> {
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
