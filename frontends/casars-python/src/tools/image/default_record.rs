// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{CoordinateSystem, defaults};
use casa_types::{RecordValue, Value};
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
            spectral.upsert(
                "conversion",
                Value::Record(crate::tools::coordsys::default_spectral_conversion()),
            );
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
