// SPDX-License-Identifier: LGPL-3.0-or-later
//! Slant zenithal perspective with a displaced projection point (FITS SZP).

use std::f64::consts::FRAC_PI_2;

use super::{deproject_tan, planeward_latitude, project_tan};
use crate::error::CoordinateError;

pub(super) struct SlantZenithalPerspective {
    distance: f64,
    center_x: f64,
    center_y: f64,
    depth: f64,
}

impl SlantZenithalPerspective {
    pub(super) fn new(parameters: &[f64]) -> Result<Self, CoordinateError> {
        let distance = parameters.first().copied().unwrap_or(0.0);
        let longitude = parameters.get(1).copied().unwrap_or(0.0);
        let latitude = parameters.get(2).copied().unwrap_or(90.0);
        let (sin_latitude, mut cos_latitude) = (latitude % 360.0).to_radians().sin_cos();
        if latitude.abs() % 180.0 == 90.0 {
            cos_latitude = 0.0;
        }
        let depth = 1.0 + distance * sin_latitude;
        if !distance.is_finite() || !longitude.is_finite() || !depth.is_finite() || depth == 0.0 {
            return Err(CoordinateError::ConversionFailed(
                "SZP requires a finite projection point off the projection plane".into(),
            ));
        }
        let (sin_longitude, cos_longitude) = (longitude % 360.0).to_radians().sin_cos();
        let transverse = distance * cos_latitude;
        Ok(Self {
            distance,
            center_x: -transverse * sin_longitude,
            center_y: transverse * cos_longitude,
            depth,
        })
    }

    /// Calabretta & Greisen (2002), section 5.1.2. WCSLIB limits this
    /// projection to its visible region before the map diverges or overlaps.
    pub(super) fn project(&self, phi: f64, theta: f64) -> Result<(f64, f64), CoordinateError> {
        if self.distance == 0.0 {
            return project_tan(phi, theta);
        }
        let (sin_phi, cos_phi) = phi.sin_cos();
        let (sin_theta, cos_theta) = theta.sin_cos();
        let z = 1.0 - sin_theta;
        let divisor = self.depth - z;
        let axial_distance = self.depth - 1.0;
        let diverges = axial_distance.abs() < 1.0 && theta < (-axial_distance).asin();
        let transverse = self.center_x * sin_phi - self.center_y * cos_phi;
        let tangent = (axial_distance * self.depth - 1.0 + transverse * transverse)
            .sqrt()
            .recip();
        let overlaps = self.distance.abs() > 1.0
            && tangent <= 1.0
            && theta < planeward_latitude(transverse.atan2(axial_distance), tangent.asin());
        if !divisor.is_finite() || divisor == 0.0 || diverges || overlaps {
            return Err(CoordinateError::ConversionFailed(
                "SZP project: direction outside the visible region".into(),
            ));
        }
        let radius = self.depth * cos_theta / divisor;
        let shift = z / divisor;
        Ok((
            radius * sin_phi - self.center_x * shift,
            -radius * cos_phi - self.center_y * shift,
        ))
    }

    pub(super) fn deproject(&self, x: f64, y: f64) -> Result<(f64, f64), CoordinateError> {
        if self.distance == 0.0 {
            return deproject_tan(x, y);
        }
        let radius_squared = x * x + y * y;
        if radius_squared == 0.0 {
            return Ok((0.0, FRAC_PI_2));
        }
        let ray_x = (x - self.center_x) / self.depth;
        let ray_y = (y - self.center_y) / self.depth;
        let dot = x * ray_x + y * ray_y;
        let (theta, z) = if radius_squared < 1e-10 {
            // WCSLIB uses the small angle form at the native pole.
            (
                FRAC_PI_2 - (radius_squared / (1.0 + dot)).sqrt(),
                radius_squared / 2.0,
            )
        } else {
            let ray_squared = ray_x * ray_x + ray_y * ray_y;
            let a = ray_squared + 1.0;
            let b = dot - ray_squared;
            let c = radius_squared - 2.0 * dot + ray_squared - 1.0;
            let root = (b * b - a * c).sqrt();
            let mut sine = (-b + root) / a;
            // Use the valid intersection closest to the native pole.
            const TOLERANCE: f64 = 1e-13;
            if sine > 1.0 + TOLERANCE {
                sine = (-b - root) / a;
            }
            if !(-1.0 - TOLERANCE..=1.0 + TOLERANCE).contains(&sine) {
                return Err(CoordinateError::ConversionFailed(
                    "SZP deproject: pixel outside the projected sphere".into(),
                ));
            }
            let sine = sine.clamp(-1.0, 1.0);
            (sine.asin(), 1.0 - sine)
        };
        if !theta.is_finite() {
            return Err(CoordinateError::ConversionFailed(
                "SZP deproject: pixel outside the projected sphere".into(),
            ));
        }
        Ok(((x - ray_x * z).atan2(-(y - ray_y * z)), theta))
    }
}
