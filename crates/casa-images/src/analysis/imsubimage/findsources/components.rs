// SPDX-License-Identifier: LGPL-3.0-or-later

use std::f64::consts::{FRAC_PI_2, LN_2, PI};

use casa_components::{
    ComponentShape, Ellipse, Flux, Polarisation, ShapeKind, SkyComponent, SpectralModel,
};
use casa_coordinates::{Coordinate, DirectionCoordinate};
use casa_types::measures::direction::MDirection;
use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use casa_types::quanta::{Unit, UnitVal};
use casa_types::{RecordValue, ScalarValue, Value};
use num_complex::Complex64;

use crate::{GaussianBeam, ImageError};

use super::invalid;
use super::peaks::Peak;

pub(super) struct PixelEllipse {
    pub(super) major: f64,
    pub(super) minor: f64,
    pub(super) x_to_y: f64,
}

pub(super) struct SkyPlane<'a> {
    pub(super) direction: &'a DirectionCoordinate,
    pub(super) sky_order: [usize; 2],
    pub(super) brightness: &'a str,
    pub(super) beam: Option<GaussianBeam>,
}

impl SkyPlane<'_> {
    fn world(&self, pixel: [f64; 2]) -> Result<[f64; 2], ImageError> {
        Ok(self
            .direction
            .pixel_to_world(self.sky_order.map(|axis| pixel[axis]))?)
    }

    pub(super) fn component(
        &self,
        peak: Peak,
        ellipse: Option<PixelEllipse>,
    ) -> Result<SkyComponent, ImageError> {
        let center = self.world(peak.position)?;
        let direction =
            MDirection::from_angles(center[0], center[1], self.direction.direction_ref());
        let (shape, value, unit) = if let Some(ellipse) = ellipse {
            let axes = self.world_ellipse(peak.position, center, ellipse)?;
            let area = gaussian_area(axes.major_axis(), axes.minor_axis());
            let (value, unit) = self.integrated_flux(peak.height, area);
            let shape = ComponentShape::new(
                direction,
                ShapeKind::Gaussian {
                    axes,
                    errors: Default::default(),
                },
            )
            .map_err(|err| invalid(err.to_string()))?;
            (shape, value, unit)
        } else {
            (
                ComponentShape::point(direction),
                peak.height * point_gain(self.brightness),
                "Jy",
            )
        };
        let flux = Flux::new(
            [
                Complex64::new(value, 0.),
                Complex64::ZERO,
                Complex64::ZERO,
                Complex64::ZERO,
            ],
            unit,
            Polarisation::Stokes,
        )
        .map_err(|err| invalid(err.to_string()))?;
        let mut spectrum = SpectralModel::constant(MFrequency::new(1e9, FrequencyRef::LSRK));
        spectrum
            .set_frequency_unit(unit_named("GHz"))
            .map_err(|err| invalid(err.to_string()))?;
        Ok(SkyComponent::new(flux, shape, spectrum))
    }

    fn world_ellipse(
        &self,
        center_pixel: [f64; 2],
        center_world: [f64; 2],
        ellipse: PixelEllipse,
    ) -> Result<Ellipse, ImageError> {
        let endpoints = [
            (ellipse.major, ellipse.x_to_y),
            (ellipse.minor, ellipse.x_to_y + FRAC_PI_2),
        ];
        let axis = |(width, angle): (f64, f64)| {
            let (sin, cos) = angle.sin_cos();
            let tip = self.world([
                center_pixel[0] + 0.5 * width * cos,
                center_pixel[1] + 0.5 * width * sin,
            ])?;
            Ok::<_, ImageError>(sky_axis(center_world, tip))
        };
        let first = axis(endpoints[0])?;
        let second = axis(endpoints[1])?;
        let (major, minor) = if first.0 >= second.0 {
            (first, second)
        } else {
            (second, first)
        };
        Ellipse::new(major.0, minor.0, major.1.rem_euclid(PI))
            .map_err(|err| invalid(err.to_string()))
    }

    fn integrated_flux(&self, peak: f64, area: f64) -> (f64, &'static str) {
        let increments = self.direction.increment();
        let pixel_area = (increments[0] * increments[1]).abs();
        let beam_area = self.beam.map_or_else(
            || gaussian_area(increments[0].abs(), increments[1].abs()),
            |beam| beam.area(),
        );
        let rad2 = unit_named("rad2");
        let local = [
            (
                "pixel",
                UnitVal {
                    factor: pixel_area,
                    dim: rad2.val().dim,
                },
            ),
            (
                "beam",
                UnitVal {
                    factor: beam_area,
                    dim: rad2.val().dim,
                },
            ),
        ];
        if let Ok(unit) = Unit::with_local_definitions(self.brightness, &local) {
            let integrated = *unit.val() * *rad2.val();
            for name in ["Jy", "Jy.km/s", "K.rad.rad"] {
                let output = unit_named(name);
                if integrated.conformant(output.val()) {
                    return (peak * area * integrated.factor / output.val().factor, name);
                }
            }
        }
        (peak * area / pixel_area, "Jy")
    }
}

fn gaussian_area(major: f64, minor: f64) -> f64 {
    PI / (4. * LN_2) * major * minor
}

fn unit_named(name: &str) -> Unit {
    Unit::new(name).expect("standard CASA unit")
}

fn point_gain(brightness: &str) -> f64 {
    let dimensionless = [("pixel", UnitVal::NODIM), ("beam", UnitVal::NODIM)];
    let Ok(unit) = Unit::with_local_definitions(brightness, &dimensionless) else {
        return 1.;
    };
    ["Jy", "Jy.km/s", "K"]
        .into_iter()
        .map(unit_named)
        .find(|target| target.conformant(&unit))
        .map_or(1., |target| unit.val().factor / target.val().factor)
}

fn sky_axis(
    [longitude, latitude]: [f64; 2],
    [tip_longitude, tip_latitude]: [f64; 2],
) -> (f64, f64) {
    let delta_lon = tip_longitude - longitude;
    let (sin_lat, cos_lat) = latitude.sin_cos();
    let (sin_tip, cos_tip) = tip_latitude.sin_cos();
    let east = cos_tip * delta_lon.sin();
    let north = cos_lat * sin_tip - sin_lat * cos_tip * delta_lon.cos();
    let cosine = sin_lat * sin_tip + cos_lat * cos_tip * delta_lon.cos();
    (2. * east.hypot(north).atan2(cosine), east.atan2(north))
}

pub(super) fn format_shape_record(component: &mut RecordValue) {
    let Some(Value::Record(shape)) = component.get_mut("shape") else {
        return;
    };
    // The list's normal serializer uses the units of a table-read shape.
    // Findsources instead exposes the units assigned by pixel-to-sky fitting.
    for (name, unit, multiplier) in [
        ("majoraxis", "arcsec", 60.),
        ("minoraxis", "arcsec", 60.),
        ("majoraxiserror", "arcmin", 1.),
        ("minoraxiserror", "arcmin", 1.),
        ("positionangleerror", "deg", 1.),
    ] {
        if let Some(Value::Record(quantity)) = shape.get_mut(name) {
            if let Some(Value::Scalar(ScalarValue::Float64(value))) = quantity.get_mut("value") {
                *value *= multiplier;
            }
            quantity.upsert("unit", Value::Scalar(ScalarValue::String(unit.into())));
        }
    }
}
