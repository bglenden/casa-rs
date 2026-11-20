// SPDX-License-Identifier: LGPL-3.0-or-later
//! Zenithal perspective with a tilted projection plane (FITS AZP).

use std::f64::consts::FRAC_PI_2;

use super::{deproject_tan, planeward_latitude, project_tan};
use crate::error::CoordinateError;

pub(super) struct ZenithalPerspective {
    distance: f64,
    sin_tilt: f64,
    cos_tilt: f64,
}

impl ZenithalPerspective {
    pub(super) fn new(parameters: &[f64]) -> Result<Self, CoordinateError> {
        let distance = parameters.first().copied().unwrap_or(0.0);
        let tilt = parameters.get(1).copied().unwrap_or(0.0) % 360.0;
        if !distance.is_finite()
            || distance == -1.0
            || !tilt.is_finite()
            || tilt.abs() % 180.0 == 90.0
        {
            return Err(CoordinateError::ConversionFailed(
                "AZP requires a finite distance other than -1 and a finite tilt with nonzero cosine"
                    .into(),
            ));
        }
        let (sin_tilt, cos_tilt) = match tilt {
            0.0 => (0.0, 1.0),
            -180.0 | 180.0 => (0.0, -1.0),
            _ => tilt.to_radians().sin_cos(),
        };
        Ok(Self {
            distance,
            sin_tilt,
            cos_tilt,
        })
    }

    /// Calabretta & Greisen (2002), section 5.1.1. WCSLIB uses the
    /// plane-facing branch and excludes points behind either visibility limit.
    pub(super) fn project(&self, phi: f64, theta: f64) -> Result<(f64, f64), CoordinateError> {
        if self.distance == 0.0 && self.sin_tilt == 0.0 && self.cos_tilt == 1.0 {
            return project_tan(phi, theta);
        }
        let (sin_phi, cos_phi) = phi.sin_cos();
        let (sin_theta, cos_theta) = theta.sin_cos();
        let plane_slope = self.sin_tilt / self.cos_tilt * cos_phi;
        let denominator = self.distance + sin_theta + cos_theta * plane_slope;
        let overlap = self.distance.abs() > 1.0 && theta < (-1.0 / self.distance).asin();
        let asymptote = self.distance / plane_slope.hypot(1.0);
        let beyond_asymptote = (self.distance * self.cos_tilt).abs() < 1.0
            && asymptote.abs() <= 1.0
            && theta < planeward_latitude(-plane_slope.atan(), asymptote.asin());
        if !denominator.is_finite() || denominator == 0.0 || overlap || beyond_asymptote {
            return Err(CoordinateError::ConversionFailed(
                "AZP project: direction outside the visible region".into(),
            ));
        }
        let radius = (self.distance + 1.0) * cos_theta / denominator;
        Ok((radius * sin_phi, -radius * cos_phi / self.cos_tilt))
    }

    pub(super) fn deproject(&self, x: f64, y: f64) -> Result<(f64, f64), CoordinateError> {
        if self.distance == 0.0 && self.sin_tilt == 0.0 && self.cos_tilt == 1.0 {
            return deproject_tan(x, y);
        }
        let plane_y = y * self.cos_tilt;
        let radius = x.hypot(plane_y);
        if radius == 0.0 {
            return Ok((0.0, FRAC_PI_2));
        }
        // The sign of the perspective denominator selects the quadrant. Work
        // with its length directly so a zero denominator has a finite limit.
        let offset = self.distance + 1.0 + y * self.sin_tilt;
        let signed_radius = radius.copysign(offset);
        let ratio = self.distance * (signed_radius / radius.hypot(offset));
        let theta = planeward_latitude(
            offset.abs().atan2(signed_radius),
            ratio.clamp(-1.0, 1.0).asin(),
        );
        const TOLERANCE: f64 = 1e-13;
        if !ratio.is_finite() || ratio.abs() > 1.0 + TOLERANCE || theta < -FRAC_PI_2 - TOLERANCE {
            return Err(CoordinateError::ConversionFailed(
                "AZP deproject: pixel outside the projected sphere".into(),
            ));
        }
        Ok((x.atan2(-plane_y), theta.max(-FRAC_PI_2)))
    }
}
