// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType, SpectralCoordinate};
use casa_types::measures::direction::{format_declination, format_right_ascension};
use casa_types::measures::doppler::{DopplerRef, MDoppler};
use casa_types::measures::frame::MeasFrame;
use casa_types::quanta::Unit;
use casa_types::{ScalarValue, Value};

use crate::ImageError;

use super::selection::ProfileSelection;

pub(super) struct ProfileAxis<'a> {
    coordinates: &'a CoordinateSystem,
    pixel_axis: usize,
    world_axis: usize,
    spectral: Option<&'a SpectralCoordinate>,
    rest_hz: f64,
    speed_scale: f64,
    pub(super) unit: String,
}

impl<'a> ProfileAxis<'a> {
    pub(super) fn new(
        selection: &'a ProfileSelection,
        power_law: bool,
    ) -> Result<Self, ImageError> {
        let coordinates = &selection.plan.coordinates;
        let pixel_axis = selection.axis;
        let Some(world_axis) = coordinates.pixel_axis_to_world_axis(pixel_axis) else {
            return Err(invalid("The fit axis has no corresponding world axis"));
        };
        let owner = coordinates.world_axis_owner(world_axis);
        let spectral_owner = coordinates
            .find_coordinate(CoordinateType::Spectral)
            .ok_or_else(|| invalid("Image has no spectral coordinate"))?;
        let spectral_unit = coordinates.coordinate(spectral_owner).world_axis_units()[0].clone();
        let spectral = owner.and_then(|(index, _)| match coordinates.coordinate(index) {
            CoordinateModel::Spectral(coordinate) => Some(coordinate.as_ref()),
            _ => None,
        });
        let rest_hz = if power_law {
            0.
        } else {
            spectral
                .map(SpectralCoordinate::rest_frequency_hz)
                .transpose()?
                .unwrap_or(0.)
        };
        let (speed_scale, unit) = if rest_hz > 0. {
            let unit = velocity_unit(selection).unwrap_or_else(|| "km/s".to_owned());
            let scale = Unit::new(&unit)
                .map_err(|error| invalid(error.to_string()))?
                .val()
                .factor;
            (299_792_458. / scale, unit)
        } else {
            (0., spectral_unit)
        };
        Ok(Self {
            coordinates,
            pixel_axis,
            world_axis,
            spectral,
            rest_hz,
            speed_scale,
            unit,
        })
    }

    pub(super) fn native(&self, position: &[f64], center: f64) -> Result<f64, ImageError> {
        Ok(self.coordinates.to_world(&self.pixels(position, center))?[self.world_axis])
    }

    pub(super) fn world(&self, position: &[f64], center: f64) -> Result<f64, ImageError> {
        if self.rest_hz > 0. {
            let spectral = self
                .spectral
                .expect("velocity fit uses a spectral coordinate");
            let frequency = spectral.world_frequency_hz(center)?;
            let velocity = MDoppler::new(frequency / self.rest_hz, DopplerRef::RATIO)
                .convert_to(spectral.velocity_convention(), &MeasFrame::default())
                .map_err(|error| invalid(error.to_string()))?;
            return Ok(self.speed_scale * velocity.value());
        }
        self.native(position, center)
    }

    pub(super) fn increment(&self, position: &[f64]) -> Result<f64, ImageError> {
        if self.rest_hz > 0. {
            Ok((self.world(position, 1.)? - self.world(position, 0.)?).abs())
        } else {
            let (owner, index) = self
                .coordinates
                .world_axis_owner(self.world_axis)
                .ok_or_else(|| invalid("The fit axis has no corresponding coordinate"))?;
            Ok(self.coordinates.coordinate(owner).increment()[index].abs())
        }
    }

    pub(super) fn direction(&self, position: &[f64]) -> String {
        let Some(owner) = self.coordinates.find_coordinate(CoordinateType::Direction) else {
            return String::new();
        };
        let CoordinateModel::Direction(direction) = self.coordinates.coordinate(owner) else {
            unreachable!("direction coordinate selected by type");
        };
        let Some(axes) = self.coordinates.coordinate_world_axes(owner) else {
            return String::new();
        };
        let [Some(lon), Some(lat)] = axes.as_slice() else {
            return String::new();
        };
        let Ok(world) = self.coordinates.to_world(position) else {
            return String::new();
        };
        format!(
            "{} {} {}",
            direction.conversion_direction_ref().as_str(),
            format_right_ascension(world[*lon], 3),
            format_declination(world[*lat], 3)
        )
    }

    fn pixels(&self, position: &[f64], center: f64) -> Vec<f64> {
        let mut pixel = position.to_vec();
        pixel[self.pixel_axis] = center;
        pixel
    }
}

fn velocity_unit(selection: &ProfileSelection) -> Option<String> {
    let record = selection.plan.coordinate_record.as_ref()?;
    let coordinates = &selection.plan.coordinates;
    let fields = coordinates.coordinates_in_record(record).ok()?;
    for field in fields {
        if field.pixel_axes.contains(&Some(selection.axis))
            && let Some(Value::Record(spectral)) = record.get(&field.field)
            && let Some(Value::Scalar(ScalarValue::String(unit))) = spectral.get("velUnit")
        {
            return Some(unit.clone());
        }
    }
    None
}

pub(super) fn automatic_divisor(x: &[f64]) -> f64 {
    let minimum = x
        .iter()
        .map(|value| value.abs())
        .fold(f64::INFINITY, f64::min);
    let maximum = x.iter().map(|value| value.abs()).fold(0., f64::max);
    if minimum > 0. && maximum.is_finite() {
        let magnitude = 0.5 * (minimum.log10() + maximum.log10());
        10_f64.powf(magnitude.trunc())
    } else {
        1.
    }
}

fn invalid(message: impl Into<String>) -> ImageError {
    ImageError::CasaSelection(message.into())
}
