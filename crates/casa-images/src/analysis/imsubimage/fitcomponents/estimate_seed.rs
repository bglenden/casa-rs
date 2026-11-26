// SPDX-License-Identifier: LGPL-3.0-or-later

use std::f64::consts::FRAC_PI_2;

use casa_coordinates::DirectionCoordinate;
use casa_core::quanta::Unit;

use crate::ImageError;

use super::estimates::Estimate;

impl Estimate {
    pub(super) fn parameters(
        &self,
        direction: &DirectionCoordinate,
        lower: [f64; 2],
        stride: [f64; 2],
    ) -> Result<[f64; 9], ImageError> {
        let radians = Unit::new("rad").expect("standard angular unit");
        let [major, minor, angle] = [&self.major, &self.minor, &self.angle].map(|value| {
            value.get_value_in(&radians).map_err(|error| {
                ImageError::CasaSelection(format!("Invalid estimate angle: {error}"))
            })
        });
        let (major, minor, angle) = (major?, minor?, angle?);
        if !major.is_finite()
            || !minor.is_finite()
            || !angle.is_finite()
            || major <= 0.
            || minor <= 0.
            || major < minor
        {
            return Err(ImageError::CasaSelection(
                "Estimated axes must be finite, positive and ordered from major to minor".into(),
            ));
        }
        let to_selected = |pixel: [f64; 2]| -> [f64; 2] {
            std::array::from_fn(|i| (pixel[i] - lower[i]) / stride[i])
        };
        let center = to_selected(self.center);
        let center_world = direction.pixel_to_world(self.center)?;
        let major_tip_world = offset_direction(center_world, 0.5 * major, angle);
        let major_tip = to_selected(direction.world_to_pixel(major_tip_world)?);
        let [dx, dy] = std::array::from_fn(|i| major_tip[i] - center[i]);
        let major_pixels = 2. * dx.hypot(dy);
        let pixel_pa = dy.atan2(dx);
        // CASA estimates the minor-axis length by interpolating angular
        // separations along the chosen pixel direction.
        let distance = |step: f64| {
            let pixel = [
                self.center[0] + stride[0] * step * pixel_pa.sin(),
                self.center[1] + stride[1] * step * pixel_pa.cos(),
            ];
            direction
                .pixel_to_world(pixel)
                .map(|tip| separation(center_world, tip))
        };
        let minor_pixels = 2. * angular_step(0.5 * minor, distance)?;
        let larger = major_pixels.max(minor_pixels);
        let smaller = major_pixels.min(minor_pixels);
        if smaller <= 0. || !larger.is_finite() {
            return Err(ImageError::CasaSelection(
                "Estimated Gaussian could not be placed in the pixel grid".into(),
            ));
        }
        Ok([
            self.peak,
            center[0],
            center[1],
            larger,
            smaller / larger,
            pixel_pa - FRAC_PI_2,
            0.,
            0.,
            0.,
        ])
    }
}

fn angular_step(
    target: f64,
    at: impl Fn(f64) -> Result<f64, casa_coordinates::CoordinateError>,
) -> Result<f64, ImageError> {
    let mut low = (0_u32, 0.);
    let mut high = (1_u32, at(1.)?);
    while high.1 <= target {
        low = high;
        let Some(next) = high.0.checked_mul(2) else {
            return Err(ImageError::CasaSelection(
                "Estimated axis extends beyond the WCS projection".into(),
            ));
        };
        high = (next, at(f64::from(next))?);
    }
    while high.0 - low.0 > 1 {
        let middle = low.0.midpoint(high.0);
        let distance = at(f64::from(middle))?;
        if distance <= target {
            low = (middle, distance);
        } else {
            high = (middle, distance);
        }
    }
    Ok(f64::from(low.0) + (target - low.1) / (high.1 - low.1))
}

fn offset_direction([lon, lat]: [f64; 2], distance: f64, pa: f64) -> [f64; 2] {
    let (sin_lat, cos_lat) = lat.sin_cos();
    let (sin_d, cos_d) = distance.sin_cos();
    let (sin_pa, cos_pa) = pa.sin_cos();
    let sin_tip = (sin_lat * cos_d + cos_lat * sin_d * cos_pa).clamp(-1., 1.);
    let delta_lon = (sin_pa * sin_d * cos_lat).atan2(cos_d - sin_lat * sin_tip);
    [lon + delta_lon, sin_tip.asin()]
}

fn separation([lon, lat]: [f64; 2], [tip_lon, tip_lat]: [f64; 2]) -> f64 {
    let hav = |angle: f64| (0.5 * angle).sin().powi(2);
    let distance = hav(tip_lat - lat) + lat.cos() * tip_lat.cos() * hav(tip_lon - lon);
    2. * distance.clamp(0., 1.).sqrt().asin()
}
