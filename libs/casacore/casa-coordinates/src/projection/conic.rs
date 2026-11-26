// SPDX-License-Identifier: LGPL-3.0-or-later

use std::f64::consts::{FRAC_PI_2, PI};

use crate::direction::wcslib_sin_cos_degrees;
use crate::{CoordinateError, ProjectionType};

// Calabretta & Greisen (2002), section 5.4. The four conic projections use
// the same plane geometry and differ in their cone constant and radius law.
enum Radius {
    Perspective { scale: f64 },
    Equidistant,
    EqualArea { offset: f64 },
    Orthomorphic { scale: f64 },
}

pub(super) struct Conic {
    mean_latitude: f64,
    cone: f64,
    fiducial_radius: f64,
    radius: Radius,
}

fn bad_projection(message: &str) -> CoordinateError {
    CoordinateError::ConversionFailed(message.into())
}

impl Conic {
    pub(super) fn new(kind: ProjectionType, parameters: &[f64]) -> Result<Self, CoordinateError> {
        let sigma_degrees = parameters.first().copied().unwrap_or(f64::NAN);
        let delta_degrees = parameters.get(1).copied().unwrap_or_default();
        let sigma = sigma_degrees.to_radians();
        let delta = delta_degrees.to_radians();
        let (first, second) = (sigma_degrees - delta_degrees, sigma_degrees + delta_degrees);
        let (sin_sigma, cos_sigma) = wcslib_sin_cos_degrees(sigma_degrees);
        let (sin_delta, cos_delta) = wcslib_sin_cos_degrees(delta_degrees);
        let (cone, fiducial_radius, radius) = match kind {
            ProjectionType::COP => {
                if cos_delta == 0.0 {
                    return Err(bad_projection("Invalid COP parallel separation"));
                }
                (
                    sin_sigma,
                    cos_delta * cos_sigma / sin_sigma,
                    Radius::Perspective { scale: cos_delta },
                )
            }
            ProjectionType::COD => {
                let cone = if delta == 0.0 {
                    sin_sigma
                } else {
                    sin_sigma * sin_delta / delta
                };
                (cone, cos_delta * cos_sigma / cone, Radius::Equidistant)
            }
            ProjectionType::COE => {
                let (s1, _) = wcslib_sin_cos_degrees(first);
                let (s2, _) = wcslib_sin_cos_degrees(second);
                let cone = (s1 + s2) / 2.0;
                let offset = 1.0 + s1 * s2;
                (
                    cone,
                    (offset - 2.0 * cone * sin_sigma).sqrt() / cone,
                    Radius::EqualArea { offset },
                )
            }
            ProjectionType::COO => {
                let tangent = |latitude: f64| {
                    let (sine, cosine) = wcslib_sin_cos_degrees((90.0 - latitude) / 2.0);
                    sine / cosine
                };
                let (t1, t2) = (tangent(first), tangent(second));
                let (_, c1) = wcslib_sin_cos_degrees(first);
                let (_, c2) = wcslib_sin_cos_degrees(second);
                let cone = if first == second {
                    sin_sigma
                } else {
                    (c2 / c1).ln() / (t2 / t1).ln()
                };
                let scale = c1 / (cone * t1.powf(cone));
                if !scale.is_finite() || scale == 0.0 {
                    return Err(bad_projection("Invalid COO standard parallels"));
                }
                (
                    cone,
                    scale * tangent(sigma_degrees).powf(cone),
                    Radius::Orthomorphic { scale },
                )
            }
            _ => return Err(bad_projection("Unknown conic projection")),
        };
        if !cone.is_finite() || cone == 0.0 || !fiducial_radius.is_finite() {
            return Err(bad_projection("Invalid conic standard parallels"));
        }
        Ok(Self {
            mean_latitude: sigma,
            cone,
            fiducial_radius,
            radius,
        })
    }

    pub(super) fn project(&self, phi: f64, theta: f64) -> Result<(f64, f64), CoordinateError> {
        let radius = match self.radius {
            Radius::Perspective { scale } => {
                if (theta - self.mean_latitude).abs() == FRAC_PI_2 {
                    return Err(bad_projection("Latitude of divergence for COP"));
                }
                if theta.abs() == FRAC_PI_2 && theta.signum() == self.mean_latitude.signum() {
                    0.0
                } else {
                    let value = self.fiducial_radius - scale * (theta - self.mean_latitude).tan();
                    if value * self.cone < 0.0 || theta.abs() == FRAC_PI_2 {
                        return Err(bad_projection("Direction is beyond the COP cone"));
                    }
                    value
                }
            }
            Radius::Equidistant => self.fiducial_radius + self.mean_latitude - theta,
            Radius::EqualArea { offset } => {
                (offset - 2.0 * self.cone * theta.sin()).max(0.0).sqrt() / self.cone
            }
            Radius::Orthomorphic { scale } => {
                if theta == -FRAC_PI_2 {
                    if self.cone > 0.0 {
                        return Err(bad_projection("Latitude of divergence for COO"));
                    }
                    0.0
                } else {
                    scale * ((FRAC_PI_2 - theta) / 2.0).tan().powf(self.cone)
                }
            }
        };
        if !radius.is_finite() {
            return Err(bad_projection("Direction is outside the conic projection"));
        }
        let (sin_angle, cos_angle) = (self.cone * phi).sin_cos();
        Ok((
            radius * sin_angle,
            self.fiducial_radius - radius * cos_angle,
        ))
    }

    pub(super) fn deproject(&self, x: f64, y: f64) -> Result<(f64, f64), CoordinateError> {
        let relative_y = self.fiducial_radius - y;
        let r = x.hypot(relative_y).copysign(self.mean_latitude);
        let angle = if r == 0.0 {
            0.0
        } else {
            (x / r).atan2(relative_y / r)
        };
        let phi = angle / self.cone;
        let theta = match self.radius {
            Radius::Perspective { scale } => {
                self.mean_latitude + ((self.fiducial_radius - r) / scale).atan()
            }
            Radius::Equidistant => self.fiducial_radius + self.mean_latitude - r,
            Radius::EqualArea { offset } => {
                let sine = (offset - (self.cone * r).powi(2)) / (2.0 * self.cone);
                if sine.abs() > 1.0 + 1e-12 {
                    return Err(bad_projection("Pixel is outside the COE projection"));
                }
                sine.clamp(-1.0, 1.0).asin()
            }
            Radius::Orthomorphic { scale } => {
                if r == 0.0 && self.cone > 0.0 {
                    return Err(bad_projection("Pixel is outside the COO projection"));
                }
                FRAC_PI_2 - 2.0 * (r / scale).powf(1.0 / self.cone).atan()
            }
        };
        if phi.abs() <= PI + 1e-13 && theta.abs() <= FRAC_PI_2 + 1e-13 {
            Ok((phi.clamp(-PI, PI), theta.clamp(-FRAC_PI_2, FRAC_PI_2)))
        } else {
            Err(bad_projection("Pixel is outside the conic projection"))
        }
    }
}
