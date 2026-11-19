// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{CoordinateSystem, defaults};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::record::{direction_to_record, epoch_to_record, position_to_record};
use casa_types::measures::{EpochRef, MEpoch, MPosition};
use casa_types::{RecordField, RecordValue, ScalarValue, Value};
use pyo3::prelude::*;

use super::handle::raised;

/// ImageFactory makes the CoordinateUtil record before centering its axes.
pub(super) fn for_shape(shape: &[usize], linear: bool) -> PyResult<RecordValue> {
    let coordinates = defaults::coordinate_system_for_shape(shape, linear)
        .with_obs_info(crate::tools::coordsys::default_image_obs_info());
    let mut record = coordinates.to_record();
    if let Some(index) = coordinates.find_coordinate(casa_coordinates::CoordinateType::Spectral) {
        let field = format!("spectral{index}");
        if let Some(Value::Record(spectral)) = record.get_mut(&field) {
            spectral.upsert("conversion", Value::Record(spectral_conversion()));
        }
    }
    for index in 0..coordinates.n_coordinates() {
        if let Some(Value::Record(linear)) = record.get_mut(&format!("linear{index}")) {
            linear.remove("coordinate_type");
            linear.remove("naxes");
        }
    }
    for (axis, &length) in shape.iter().enumerate() {
        CoordinateSystem::set_reference_pixel_in_record(&mut record, axis, (length / 2) as f64)
            .map_err(raised)?;
    }
    Ok(record)
}

fn spectral_conversion() -> RecordValue {
    let mut epoch = epoch_to_record(&MEpoch::from_mjd(0.0, EpochRef::UTC));
    // LAST is casacore's placeholder for an unset conversion frame.
    epoch.upsert("refer", Value::Scalar(ScalarValue::String("LAST".into())));
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
