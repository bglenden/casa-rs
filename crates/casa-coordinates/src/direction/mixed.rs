// SPDX-License-Identifier: LGPL-3.0-or-later

use std::f64::consts::{FRAC_PI_2, PI, TAU};

use crate::CoordinateError;

use super::DirectionCoordinate;

const PIXEL_TOLERANCE: f64 = 1e-10;
const ANGLE_TOLERANCE: f64 = PIXEL_TOLERANCE * PI / 180.0;
pub(super) const DEFAULT_RANGES: [[f64; 2]; 2] = [[-PI, PI], [-FRAC_PI_2, FRAC_PI_2]];

#[derive(Clone, Copy)]
struct Sample {
    angle: f64,
    error: f64,
    pixel: [f64; 2],
}

fn invalid_world() -> CoordinateError {
    CoordinateError::ConversionFailed("wcs wcsmix_error: Invalid world coordinate".into())
}

fn signed_longitude(value: f64) -> f64 {
    let wrapped = value % TAU;
    if wrapped > PI {
        wrapped - TAU
    } else if wrapped < -PI {
        wrapped + TAU
    } else {
        wrapped
    }
}

impl DirectionCoordinate {
    pub(crate) fn reset_mix_ranges(&mut self) {
        self.mix_ranges = DEFAULT_RANGES;
    }

    pub(crate) fn retain_mix_ranges(&mut self, previous: &Self) {
        self.mix_ranges = previous.mix_ranges;
    }

    /// Casacore uses each positive image length to choose the midpoint,
    /// then extends the search interval around its projected sky position.
    pub(crate) fn set_mix_shape(
        &mut self,
        shape: [i64; 2],
        fixed_pixels: [Option<f64>; 2],
    ) -> Result<(), CoordinateError> {
        let center = [0, 1].map(|axis| {
            if shape[axis] > 0 {
                shape[axis] as f64 / 2.0
            } else {
                self.crpix[axis]
            }
        });
        let world = self.pixel_to_world(center)?;
        let width = [0, 1].map(|axis| {
            let half = if shape[axis] > 1 {
                shape[axis].saturating_add(shape[axis] / 2) / 2
            } else {
                10
            };
            self.cdelt[axis].abs() * half as f64
        });
        let longitude = width[0] / world[1].cos();
        self.mix_ranges = [
            [
                signed_longitude(world[0] - longitude),
                signed_longitude(world[0] + longitude),
            ],
            [world[1] - width[1], world[1] + width[1]],
        ];
        if fixed_pixels.iter().any(Option::is_some) {
            let fixed = [0, 1].map(|axis| fixed_pixels[axis].unwrap_or(self.crpix[axis]));
            let fixed_world = self.pixel_to_world(fixed)?;
            for (axis, fixed_pixel) in fixed_pixels.into_iter().enumerate() {
                if fixed_pixel.is_some() {
                    let width = 10.0 * self.cdelt[axis].abs();
                    self.mix_ranges[axis] = if axis == 0 {
                        let reference_latitude = self.pixel_to_world(self.crpix)?[1];
                        let width = width / reference_latitude.cos();
                        [
                            signed_longitude(fixed_world[0] - width),
                            signed_longitude(fixed_world[0] + width),
                        ]
                    } else {
                        [
                            (fixed_world[1] - width).max(-FRAC_PI_2),
                            (fixed_world[1] + width).min(FRAC_PI_2),
                        ]
                    };
                }
            }
        }
        Ok(())
    }

    /// Solve for the missing pixel component from the other pixel and a
    /// native sky coordinate. WCSLIB searches the missing sky component.
    pub(crate) fn pixels_from_mixed(
        &self,
        mut world: [f64; 2],
        pixels: [f64; 2],
        fixed_pixel_axis: usize,
    ) -> Result<[f64; 2], CoordinateError> {
        let target = pixels[fixed_pixel_axis];
        let [first, second] = self.mix_ranges[fixed_pixel_axis];
        let (lower, upper) = (first.min(second), first.max(second));
        let known_axis = 1 - fixed_pixel_axis;
        let known_world = world[known_axis];
        let project = |angle: f64, phi: f64, theta: f64| {
            let (x, y) = self
                .projection
                .project(phi, theta)
                .map_err(|_| invalid_world())?;
            let (px, py) = self
                .intermediate_to_pixel(x, y)
                .map_err(|_| invalid_world())?;
            let mut pixel = [px, py];
            let error = pixel[fixed_pixel_axis] - target;
            if !error.is_finite() {
                return Err(invalid_world());
            }
            pixel[fixed_pixel_axis] = target;
            Ok(Sample {
                angle,
                error,
                pixel,
            })
        };
        let mut evaluate = |angle: f64| {
            world[fixed_pixel_axis] = angle;
            // An image-centred interval can cross a celestial pole. The
            // rotation folds trial latitudes across it before projection.
            if !world[0].is_finite() || !world[1].is_finite() {
                return Err(invalid_world());
            }
            let (phi, theta) = self.celestial_to_native(world[0], world[1]);
            project(angle, phi, theta)
        };
        let low = evaluate(lower)?;
        if low.error.abs() < PIXEL_TOLERANCE {
            return Ok(low.pixel);
        }
        let high = evaluate(upper)?;
        if high.error.abs() < PIXEL_TOLERANCE {
            return Ok(high.pixel);
        }
        if let Some(pixel) = refine_crossing(low, high, &mut evaluate)? {
            return Ok(pixel);
        }
        if lower < upper {
            let mut step = ((upper - lower) / 10.0).min(1.0_f64.to_radians());
            for _ in 0..=5 {
                let mut previous = high;
                let mut best = high;
                loop {
                    let angle = (previous.angle - step).max(lower);
                    if angle >= previous.angle {
                        break;
                    }
                    let sample = evaluate(angle)?;
                    if sample.error.abs() < PIXEL_TOLERANCE {
                        return Ok(sample.pixel);
                    }
                    if let Some(pixel) = refine_crossing(sample, previous, &mut evaluate)? {
                        return Ok(pixel);
                    }
                    if sample.error.abs() < best.error.abs() {
                        best = sample;
                    }
                    previous = sample;
                }
                if let Some(pixel) = refine_minimum(best, lower, upper, step, &mut evaluate)? {
                    return Ok(pixel);
                }
                step *= 0.5;
            }
        }
        // A celestial coordinate at a native pole does not determine native
        // longitude. Some projections map that pole onto an edge or a line.
        for theta in [FRAC_PI_2, -FRAC_PI_2] {
            let (longitude, latitude) = self.native_to_celestial(0.0, theta);
            let pole = [longitude, latitude];
            let (difference, unknown) = if known_axis == 0 {
                ((known_world - pole[0]) % TAU, pole[1])
            } else {
                let longitude = pole[0] + ((lower - pole[0]) / TAU).ceil() * TAU;
                (known_world - pole[1], longitude)
            };
            if difference.abs() > ANGLE_TOLERANCE || unknown < lower || unknown > upper {
                continue;
            }
            let mut evaluate = |phi| project(phi, phi, theta);
            let mut previous = evaluate(-PI)?;
            if previous.error.abs() < PIXEL_TOLERANCE {
                return Ok(previous.pixel);
            }
            for degrees in -179..=180 {
                let sample = evaluate(f64::from(degrees).to_radians())?;
                if sample.error.abs() < PIXEL_TOLERANCE {
                    return Ok(sample.pixel);
                }
                if let Some(pixel) = refine_crossing(previous, sample, &mut evaluate)? {
                    return Ok(pixel);
                }
                previous = sample;
            }
        }
        Err(CoordinateError::ConversionFailed(
            "wcs wcsmix_error: No solution found in the specified interval".into(),
        ))
    }
}

fn refine_crossing(
    mut lower: Sample,
    mut upper: Sample,
    evaluate: &mut impl FnMut(f64) -> Result<Sample, CoordinateError>,
) -> Result<Option<[f64; 2]>, CoordinateError> {
    if lower.error.is_sign_negative() == upper.error.is_sign_negative() {
        return Ok(None);
    }
    for _ in 0..60 {
        let fraction = (lower.error / (lower.error - upper.error)).clamp(0.1, 0.9);
        let span = upper.angle - lower.angle;
        let sample = evaluate(lower.angle + fraction * span)?;
        if sample.error.abs() < PIXEL_TOLERANCE {
            return Ok(Some(sample.pixel));
        }
        if span < ANGLE_TOLERANCE {
            return Ok((sample.error.abs() < 100.0 * PIXEL_TOLERANCE).then_some(sample.pixel));
        }
        if sample.error.is_sign_negative() == lower.error.is_sign_negative() {
            lower = sample;
        } else {
            upper = sample;
        }
    }
    Ok(None)
}

fn refine_minimum(
    mut best: Sample,
    lower: f64,
    upper: f64,
    step: f64,
    evaluate: &mut impl FnMut(f64) -> Result<Sample, CoordinateError>,
) -> Result<Option<[f64; 2]>, CoordinateError> {
    if best.angle == lower || best.angle == upper {
        return Ok(None);
    }
    let mut left = (best.angle - step).max(lower);
    let mut right = (best.angle + step).min(upper);
    for _ in 0..60 {
        let part = (right - left) / 3.0;
        if part < ANGLE_TOLERANCE {
            break;
        }
        let a = evaluate(left + part)?;
        let b = evaluate(right - part)?;
        let candidate = if a.error.abs() < b.error.abs() {
            right = b.angle;
            a
        } else {
            left = a.angle;
            b
        };
        if candidate.error.abs() < best.error.abs() {
            best = candidate;
        }
    }
    Ok((best.error.abs() < PIXEL_TOLERANCE).then_some(best.pixel))
}
