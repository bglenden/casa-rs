// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType, SpectralCoordinate};
use casa_types::measures::direction::{DirectionRef, format_declination, format_right_ascension};
use casa_types::measures::doppler::{DopplerRef, MDoppler};
use casa_types::measures::frame::MeasFrame;
use casa_types::quanta::Unit;
use casa_types::quanta::format::{DIG2, TIME, format_angle};
use casa_types::{ScalarValue, Value};

use crate::ImageError;

use super::selection::ProfileSelection;

pub(super) struct ProfileAxis<'a> {
    coordinates: &'a CoordinateSystem,
    pixel_axis: usize,
    world_axis: usize,
    spectral: Option<&'a SpectralCoordinate>,
    source_spectral: Option<&'a SpectralCoordinate>,
    rest_hz: f64,
    speed_scale: f64,
    source_start: f64,
    source_stride: f64,
    pub(super) unit: String,
}

impl<'a> ProfileAxis<'a> {
    pub(super) fn new(
        selection: &'a ProfileSelection,
        source: &'a CoordinateSystem,
        power_law: bool,
    ) -> Result<Self, ImageError> {
        let coordinates = &selection.plan.coordinates;
        let pixel_axis = selection.axis;
        let Some(world_axis) = coordinates.pixel_axis_to_world_axis(pixel_axis) else {
            return Err(invalid("The fit axis has no corresponding world axis"));
        };
        let owner = coordinates.world_axis_owner(world_axis);
        if coordinates
            .find_coordinate(CoordinateType::Spectral)
            .is_none()
        {
            return Err(invalid("Image has no spectral coordinate"));
        }
        let spectral = owner.and_then(|(index, _)| match coordinates.coordinate(index) {
            CoordinateModel::Spectral(coordinate) => Some(coordinate.as_ref()),
            _ => None,
        });
        let source_spectral =
            source.pixel_axis_owner(pixel_axis).and_then(|(index, _)| {
                match source.coordinate(index) {
                    CoordinateModel::Spectral(coordinate) => Some(coordinate.as_ref()),
                    _ => None,
                }
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
            (0., coordinates.world_axis_units()[world_axis].clone())
        };
        Ok(Self {
            coordinates,
            pixel_axis,
            world_axis,
            spectral,
            source_spectral,
            rest_hz,
            speed_scale,
            source_start: selection.plan.selection.start[pixel_axis] as f64,
            source_stride: selection.stride[pixel_axis] as f64,
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
            return self.velocity(spectral, center);
        }
        self.native(position, center)
    }

    fn velocity(&self, spectral: &SpectralCoordinate, pixel: f64) -> Result<f64, ImageError> {
        let frequency = spectral.world_frequency_hz(pixel)?;
        let velocity = MDoppler::new(frequency / self.rest_hz, DopplerRef::RATIO)
            .convert_to(spectral.velocity_convention(), &MeasFrame::default())
            .map_err(|error| invalid(error.to_string()))?;
        Ok(self.speed_scale * velocity.value())
    }

    pub(super) fn report_pixels(
        &self,
        center: f64,
        center_world: [f64; 2],
        width_world: [f64; 2],
        selected_increment: f64,
    ) -> Option<([f64; 2], [f64; 2])> {
        let source_center = self.source_start + center * self.source_stride;
        let increment = if self.rest_hz > 0. {
            let spectral = self.source_spectral?;
            (self.velocity(spectral, source_center + 1.).ok()? - center_world[0]).abs()
        } else {
            selected_increment / self.source_stride
        };
        if !increment.is_finite() || increment <= 0. {
            return None;
        }
        Some((
            [source_center, center_world[1] / increment],
            width_world.map(|value| value / increment),
        ))
    }

    pub(super) fn signed_increment(&self, position: &[f64]) -> Result<f64, ImageError> {
        if self.rest_hz > 0. {
            Ok(self.world(position, 1.)? - self.world(position, 0.)?)
        } else {
            let (owner, index) = self
                .coordinates
                .world_axis_owner(self.world_axis)
                .ok_or_else(|| invalid("The fit axis has no corresponding coordinate"))?;
            Ok(self.coordinates.coordinate(owner).increment()[index])
        }
    }

    pub(super) fn has_direction(&self) -> bool {
        self.coordinates
            .find_coordinate(CoordinateType::Direction)
            .is_some_and(|owner| {
                self.coordinates
                    .coordinate_pixel_axes(owner)
                    .is_some_and(|axes| {
                        axes.iter()
                            .all(|axis| axis.is_some_and(|axis| axis != self.pixel_axis))
                    })
            })
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
        let frame = direction.conversion_direction_ref();
        let longitude = world[*lon];
        let latitude = world[*lat];
        let angles = match frame {
            DirectionRef::GALACTIC
            | DirectionRef::SUPERGAL
            | DirectionRef::ITRF
            | DirectionRef::AZEL
            | DirectionRef::AZELSW
            | DirectionRef::AZELGEO
            | DirectionRef::AZELSWGEO => {
                format!(
                    "{:.6} {:.6}",
                    longitude.to_degrees().rem_euclid(360.),
                    latitude.to_degrees()
                )
            }
            DirectionRef::ECLIPTIC | DirectionRef::MECLIPTIC | DirectionRef::TECLIPTIC => {
                let longitude = (longitude.to_degrees() + 180.).rem_euclid(360.) - 180.;
                format!("{longitude:.6} {:.6}", latitude.to_degrees())
            }
            DirectionRef::HADEC => {
                format!(
                    "{} {}",
                    format_angle(longitude, TIME | DIG2, 9),
                    format_declination(latitude, 3)
                )
            }
            _ => {
                format!(
                    "{} {}",
                    format_right_ascension(longitude, 3),
                    format_declination(latitude, 3)
                )
            }
        };
        format!("{} {angles}", frame.as_str())
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
