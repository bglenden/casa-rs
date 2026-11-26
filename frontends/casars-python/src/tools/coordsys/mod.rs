// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's `coordsys` tool (cs) and the signature inventory for the
//! CASA 6.7.6.14 binding (ADR-0017, #1025).

mod axis_labels;
mod construction;
pub(super) use construction::{append_coordinate, validate_coordinate_indices};
mod conversion_frames;
pub(super) use conversion_frames::profile_frequency_frame;
mod convert_direction;
mod direction_edit;
mod doppler;
pub(super) use doppler::stored_velocity_settings;
mod frame_metadata;
mod inspect;
mod linear_transform;
pub(super) use linear_transform::validate_restored_linear_coordinates;
mod mixed_transform;
mod observation;
mod ordering;
mod projection;
mod reference;
mod reference_code;
mod replace;
mod rest_frequency;
mod spectral_samples;
mod subimage;
mod summary;
pub(crate) mod surface;
mod tabular;
mod transform_many;
mod transform_relative;
pub(super) mod world_reference;

mod transform_single;
mod world_edits;
pub(super) use transform_single::{WorldArgument, image_pixel_argument};

use std::sync::{Mutex, MutexGuard, PoisonError};

use casa_coordinates::{CoordinateModel, CoordinateSystem, ObsInfo, defaults};
use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::{EpochRef, MEpoch, MPosition};
use casa_core::measures::{direction_to_record, epoch_to_record, position_to_record};
use casa_core::{RecordField, RecordValue, ScalarValue, Value};
use pyo3::exceptions::{PyOverflowError, PyRuntimeError, PyTypeError};
use pyo3::prelude::*;

fn index_argument(value: &Bound<'_, PyAny>, name: &str) -> PyResult<i32> {
    if value.is_none() {
        return Ok(-1);
    }
    match value.extract::<i64>() {
        Ok(value) => Ok(value as i32),
        Err(error) if error.is_instance_of::<PyOverflowError>(value.py()) => Ok(-1),
        Err(_) => Err(PyTypeError::new_err(format!(
            "argument _{name} must be an integer"
        ))),
    }
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

fn canonical_restored_coordinate(
    original: &RecordValue,
    model: &CoordinateModel,
) -> PyResult<Option<RecordValue>> {
    let CoordinateModel::Spectral(spectral) = model else {
        return linear_transform::normalize_restored_matrix(original, model);
    };
    if !matches!(original.get("system"),
        Some(Value::Scalar(ScalarValue::String(system))) if system == "LSR")
    {
        return linear_transform::normalize_restored_matrix(original, model);
    }
    // CASA reserializes the restored legacy axis as a current spectral record.
    let mut normalized = model.to_record();
    for field in ["coordinate_type", "formatUnit"] {
        if let Some(value) = original.get(field) {
            normalized.upsert(field, value.clone());
        }
    }
    if let Some(unit) = original
        .get("velUnit")
        .or_else(|| original.get("prefVelUnit"))
        .filter(|unit| matches!(unit, Value::Scalar(ScalarValue::String(name)) if !name.is_empty()))
    {
        normalized.upsert("velUnit", unit.clone());
    }
    if normalized.get("conversion").is_none()
        || spectral.conversion_frequency_ref() == Some(spectral.frequency_ref())
    {
        let mut conversion = default_spectral_conversion();
        if let Some(system) = normalized.get("system") {
            conversion.upsert("system", system.clone());
        }
        normalized.upsert("conversion", Value::Record(conversion));
    }
    Ok(Some(normalized))
}

impl CoordinateState {
    fn from_coordinates(coordinates: CoordinateSystem, mut record: RecordValue) -> PyResult<Self> {
        coordinates
            .normalize_quality_records(&mut record)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        for (index, placement) in coordinates
            .coordinates_in_record(&record)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?
            .into_iter()
            .enumerate()
        {
            if let Some(Value::Record(original)) = record.get(&placement.field)
                && let Some(normalized) =
                    canonical_restored_coordinate(original, coordinates.coordinate(index))?
            {
                record.upsert(placement.field, Value::Record(normalized));
            }
        }
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

    pub(crate) fn from_image_coordinates(
        coordinates: &CoordinateSystem,
        record: RecordValue,
    ) -> PyResult<Self> {
        Ok(Self {
            state: Mutex::new(CoordinateState::from_coordinates(
                coordinates.clone(),
                record,
            )?),
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
