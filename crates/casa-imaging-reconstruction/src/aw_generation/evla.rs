// SPDX-License-Identifier: LGPL-3.0-or-later
// Adapted from CASA BeamCalc.cc and BeamCalcParams.cc at
// 61020062cee290f5466cffed5ec5032e0c7a3434.
// Copyright (C) 1996-2002 Associated Universities, Inc.
// Original code licensed under LGPL-2.0-or-later; this adaptation uses LGPL-3.0-or-later.

use std::f64::consts::{FRAC_1_SQRT_2, FRAC_PI_2, TAU};

use casa_imaging_model::EvlaDishSurface;
use num_complex::Complex32;

use super::NativeAwGenerationError;

type Vector3 = [f64; 3];

/// Explicit EVLA dish surface, independent of CASA installation or file paths.
///
/// The model owner supplies validated radial samples, not precomputed CFs.
/// Reconstruction owns their numerical interpretation; it performs no model
/// data acquisition or implicit runtime-data discovery.
#[derive(Clone, Debug)]
pub struct EvlaApertureModel {
    surface: EvlaDishSurface,
    step: f64,
}

impl EvlaApertureModel {
    /// Bind an already validated, immutable physical dish surface.
    #[must_use]
    pub fn new(surface: EvlaDishSurface) -> Self {
        let step = 12.5 / (surface.samples().len() - 1) as f64;
        Self { surface, step }
    }

    /// Fill all four circular-feed Jones aperture planes in x-contiguous order.
    ///
    /// The caller supplies exactly four complete planes, ordered RR/RL/LR/LL.
    /// Values are integrated over the explicitly requested aperture subpixels.
    /// Geometry and ray propagation use f64; electric-field arithmetic and
    /// per-subpixel accumulation use CASA's Complex32 convention. This method
    /// allocates no per-ray or image-sized storage and performs no FFT or I/O.
    pub fn fill_aperture(
        &self,
        grid: EvlaApertureGrid,
        planes: &mut [Complex32],
    ) -> Result<(), NativeAwGenerationError> {
        let plane_length = grid.size * grid.size;
        if planes.len() != 4 * plane_length {
            return Err(NativeAwGenerationError::WorkspaceMismatch);
        }
        planes.fill(Complex32::default());
        let antenna = Antenna::new(self, grid.frequency_hz)?;
        let reference_path = antenna
            .trace(0.0, 0.00001)
            .ok_or(NativeAwGenerationError::InvalidNumerics)?
            .length(antenna.feed);
        let fields = antenna.feed_fields();
        let step = grid.cell_m / grid.subpixels as f64;
        let origin = -((grid.size / 2) as f64) * grid.cell_m - grid.cell_m / 2.0 + step / 2.0;
        let angle = f64::from(grid.parallactic_angle_rad as f32) + FRAC_PI_2;
        let (sin_pa, cos_pa) = angle.sin_cos();
        let count = grid.size * grid.subpixels;
        for j in 0..count {
            for i in 0..count {
                let u = origin + i as f64 * step;
                let v = origin + j as f64 * step;
                let x = -(cos_pa * u - sin_pa * v);
                let y = sin_pa * u + cos_pa * v;
                if let Some(values) = antenna.aperture_pixel(x, y, step, reference_path, fields)? {
                    // BeamCalc stores j on the first image axis, i on the second.
                    let pixel = j / grid.subpixels + grid.size * (i / grid.subpixels);
                    for (plane, value) in values.into_iter().enumerate() {
                        planes[plane * plane_length + pixel] += value;
                    }
                }
            }
        }
        if planes
            .iter()
            .any(|value| !value.re.is_finite() || !value.im.is_finite())
        {
            return Err(NativeAwGenerationError::InvalidNumerics);
        }
        Ok(())
    }

    fn dish(&self, radius: f64) -> (f64, f64) {
        if radius == 0.0 {
            return (self.surface.samples()[0][1], 0.0);
        }
        let index =
            ((radius / self.step + 0.5).floor() as usize).min(self.surface.samples().len() - 2);
        let x = radius - index as f64 * self.step;
        let [slope_a, slope_b, slope_c, height_sum] = if index == 0 {
            let [_, height, slope] = self.surface.samples()[1];
            [
                -slope,
                0.0,
                slope,
                2.0 * height + self.surface.samples()[0][1],
            ]
        } else {
            [
                self.surface.samples()[index - 1][2],
                self.surface.samples()[index][2],
                self.surface.samples()[index + 1][2],
                self.surface.samples()[index - 1][1]
                    + self.surface.samples()[index][1]
                    + self.surface.samples()[index + 1][1],
            ]
        };
        let square_step = self.step * self.step;
        let a = slope_b;
        let b = 0.5 * (slope_c - slope_a) / self.step;
        let c = 0.5 * (slope_c - 2.0 * slope_b + slope_a) / square_step;
        let d = (height_sum - b * square_step) / 3.0;
        (
            d + a * x + b * x * x / 2.0 + c * x * x * x / 3.0,
            a + b * x + c * x * x,
        )
    }
}

/// Validated square aperture grid used by the first EVLA provider.
#[derive(Clone, Copy, Debug)]
pub struct EvlaApertureGrid {
    size: usize,
    cell_m: f64,
    subpixels: usize,
    frequency_hz: f64,
    parallactic_angle_rad: f64,
}

impl EvlaApertureGrid {
    /// Describe the physical aperture sampling explicitly.
    pub fn new(
        size: usize,
        cell_m: f64,
        subpixels: usize,
        frequency_hz: f64,
        parallactic_angle_rad: f64,
    ) -> Result<Self, NativeAwGenerationError> {
        if size < 4
            || !size.is_multiple_of(2)
            || subpixels == 0
            || size
                .checked_mul(size)
                .and_then(|size| size.checked_mul(4))
                .is_none()
            || size.checked_mul(subpixels).is_none()
            || !cell_m.is_finite()
            || cell_m <= 0.0
            || !frequency_hz.is_finite()
            || !parallactic_angle_rad.is_finite()
            || !(0.9e9..=8.0e9).contains(&frequency_hz)
        {
            return Err(NativeAwGenerationError::InvalidGrid);
        }
        Ok(Self {
            size,
            cell_m,
            subpixels,
            frequency_hz,
            parallactic_angle_rad,
        })
    }

    /// Required number of Complex32 values, including all four Jones planes.
    #[must_use]
    pub const fn workspace_values(self) -> usize {
        4 * self.size * self.size
    }
}

struct Antenna<'a> {
    model: &'a EvlaApertureModel,
    feed: Vector3,
    feed_direction: Vector3,
    path_constant: f64,
    feed_scale: f64,
    hole_radius: f64,
    wavelength: f64,
    leg_foot_height: f64,
}

#[derive(Clone, Copy)]
struct Ray {
    sub: Vector3,
    dish: Vector3,
    aperture: Vector3,
}

impl Ray {
    fn length(self, feed: Vector3) -> f64 {
        norm(sub(feed, self.sub))
            + norm(sub(self.sub, self.dish))
            + norm(sub(self.dish, self.aperture))
    }
}

impl<'a> Antenna<'a> {
    fn new(
        model: &'a EvlaApertureModel,
        frequency_hz: f64,
    ) -> Result<Self, NativeAwGenerationError> {
        // Receiver selection uses the requested frequency; BeamCalc receives the
        // float-rounded frequency from VLACalcIlluminationConvFunc.
        let frequency_ghz = f64::from(frequency_hz as f32) / 1e9;
        let (feed, reference, taper, hole_radius) = if (0.9e9..=2e9).contains(&frequency_hz) {
            ([0.10026, 0.97019, 1.67640], 1.5, [10.0, 2.0, 0.0], 2.0)
        } else if (2e9..=4e9).contains(&frequency_hz) {
            (
                [-0.196123, -0.955439, 1.67640],
                3.0,
                [15.837, -0.004, 0.00023],
                3.87,
            )
        } else if (4e9..=8e9).contains(&frequency_hz) {
            (
                [-0.94300, -0.249152, 1.67640],
                6.0,
                [12.75, 0.375, 0.0],
                2.0,
            )
        } else {
            return Err(NativeAwGenerationError::UnsupportedFrequency);
        };
        let df = frequency_ghz - reference;
        let taper = taper[0] + taper[1] * df + taper[2] * df * df;
        let toward_sub = sub([0.0, 0.0, 8.47852], feed);
        Ok(Self {
            model,
            feed,
            feed_direction: unit(toward_sub),
            path_constant: 8.47852 + norm(toward_sub),
            feed_scale: TAU * taper.sqrt() * 0.1874 / 9.26_f64.to_radians().sin(),
            hole_radius,
            wavelength: 0.299792458 / frequency_ghz,
            // CASA initializes this before replacing the default leg foot by 7.55 m.
            leg_foot_height: model.dish(12.5 / 2.0).0,
        })
    }

    fn trace(&self, x: f64, y: f64) -> Option<Ray> {
        let r = (x * x + y * y).sqrt();
        let (height, slope) = self.model.dish(r);
        let slope = (2.0 * slope.atan()).tan();
        let w = 1.0 / (1.0 + slope * slope).sqrt();
        let direction = [-slope * (x / r) * w, -slope * (y / r) * w, w];
        let dish_start = [x, y, height];
        let feed_delta = sub(self.feed, dish_start);
        let path = self.path_constant + height;
        let distance = 0.5 * (dot(feed_delta, feed_delta) - path * path)
            / (-path + dot(direction, feed_delta));
        let subreflector = add(dish_start, scale(direction, distance));
        let to_feed = unit(sub(self.feed, subreflector));
        let sub_normal = unit(add(unit(sub(dish_start, subreflector)), to_feed));
        if !(-1.0..=0.0).contains(&sub_normal[2]) {
            return None;
        }
        let to_dish = sub(scale(sub_normal, 2.0 * dot(sub_normal, to_feed)), to_feed);
        let parabola = self.model.surface.samples().last()?[1] / (12.5 * 12.5);
        let a = parabola * (to_dish[0] * to_dish[0] + to_dish[1] * to_dish[1]);
        let b = 2.0 * parabola * (to_dish[0] * subreflector[0] + to_dish[1] * subreflector[1])
            - to_dish[2];
        let c = parabola * (subreflector[0] * subreflector[0] + subreflector[1] * subreflector[1])
            - subreflector[2];
        let mut t = 0.5 * ((b * b - 4.0 * a * c).sqrt() - b) / a;
        let mut dish = [0.0; 3];
        let mut dish_normal = [0.0; 3];
        for iteration in 0..=7 {
            dish = add(subreflector, scale(to_dish, t));
            let radius = (dish[0] * dish[0] + dish[1] * dish[1]).sqrt();
            let (height, slope) = self.model.dish(radius);
            dish[2] = height;
            let nz = 1.0 / (1.0 + slope * slope).sqrt();
            dish_normal = [
                -slope * (dish[0] / radius) * nz,
                -slope * (dish[1] / radius) * nz,
                nz,
            ];
            if iteration < 7 {
                t = dot(dish_normal, sub(dish, subreflector)) / dot(dish_normal, to_dish);
            }
        }
        let to_aperture = sub(to_dish, scale(dish_normal, 2.0 * dot(dish_normal, to_dish)));
        let t = (self.model.surface.samples().last()?[1] - dish[2]) / to_aperture[2];
        let aperture = add(dish, scale(to_aperture, t));
        Some(Ray {
            sub: subreflector,
            dish,
            aperture,
        })
    }

    fn blocked(&self, ray: Ray) -> bool {
        let radius_sq = ray.dish[0] * ray.dish[0] + ray.dish[1] * ray.dish[1];
        let direction = sub(
            if radius_sq < 7.55 * 7.55 {
                ray.aperture
            } else {
                ray.sub
            },
            ray.dish,
        );
        for leg in 0..4 {
            let angle = leg as f64 * FRAC_PI_2;
            let foot = [7.55 * angle.cos(), 7.55 * angle.sin(), self.leg_foot_height];
            let projection = foot[0] * ray.dish[0] + foot[1] * ray.dish[1];
            if projection / ((foot[0] * foot[0] + foot[1] * foot[1]) * radius_sq).sqrt() < 0.7 {
                continue;
            }
            let normal = unit(cross(direction, sub([0.0, 0.0, 10.93876], foot)));
            if dot(sub(ray.dish, foot), normal).abs() <= 0.27 / 2.0 {
                return true;
            }
        }
        false
    }

    fn feed_fields(&self) -> [[Complex32; 3]; 2] {
        let vertical = unit([self.feed_direction[1], -self.feed_direction[0], 0.0]);
        let horizontal = cross(vertical, self.feed_direction);
        let angle = horizontal[1].atan2(horizontal[0]);
        let cosine = angle.cos() as f32;
        let sine = angle.sin() as f32;
        [1.0, -1.0].map(|sign| {
            let p = Complex32::new(FRAC_1_SQRT_2 as f32, 0.0);
            let q = Complex32::new(0.0, (sign * FRAC_1_SQRT_2) as f32);
            let h = cosine * p + sine * q;
            let v = -sine * p + cosine * q;
            std::array::from_fn(|axis| horizontal[axis] as f32 * h + vertical[axis] as f32 * v)
        })
    }

    fn aperture_pixel(
        &self,
        x: f64,
        y: f64,
        step: f64,
        reference_path: f64,
        fields: [[Complex32; 3]; 2],
    ) -> Result<Option<[Complex32; 4]>, NativeAwGenerationError> {
        let radius_sq = x * x + y * y;
        if radius_sq > 12.5 * 12.5 || radius_sq < self.hole_radius * self.hole_radius {
            return Ok(None);
        }
        let mut location = [x, y];
        for _ in 0..6 {
            let Some(ray) = self.trace(location[0], location[1]) else {
                return Ok(None);
            };
            location[0] += x - ray.aperture[0];
            location[1] += y - ray.aperture[1];
        }
        let Some(ray) = self.trace(location[0], location[1]) else {
            return Ok(None);
        };
        if self.blocked(ray) {
            return Ok(None);
        }
        let epsilon = step / 4.0;
        let Some(ray_x) = self.trace(
            location[0] + if x < 0.0 { epsilon } else { -epsilon },
            location[1],
        ) else {
            return Ok(None);
        };
        let Some(ray_y) = self.trace(
            location[0],
            location[1] + if y < 0.0 { epsilon } else { -epsilon },
        ) else {
            return Ok(None);
        };
        let delta_x = sub(ray_x.aperture, ray.aperture);
        let delta_y = sub(ray_y.aperture, ray.aperture);
        let area = 0.5 * (delta_x[0] * delta_y[1] - delta_y[0] * delta_x[1]).abs();
        let center_direction = unit(sub(ray.sub, self.feed));
        let omega_x = sub(unit(sub(ray_x.sub, self.feed)), center_direction);
        let omega_y = sub(unit(sub(ray_y.sub, self.feed)), center_direction);
        let omega = 0.5 * norm(cross(omega_x, omega_y));
        let cosine = dot(self.feed_direction, center_direction);
        let feed_gain =
            (2.0 * -0.083 * self.feed_scale * self.feed_scale * (1.0 - cosine * cosine)).exp();
        let amplitude = (omega / area * step * step * feed_gain).sqrt();
        let phase = TAU * (ray.length(self.feed) - reference_path) / self.wavelength;
        if !amplitude.is_finite() || !phase.is_finite() {
            return Err(NativeAwGenerationError::InvalidNumerics);
        }
        let factor = Complex32::new(phase.cos() as f32, phase.sin() as f32) * amplitude as f32;
        let [right, left] = fields.map(|field| self.trace_polarization(ray, field));
        let i = Complex32::new(0.0, 1.0);
        let [exr, eyr] = [factor * right[0], factor * right[1]];
        let [exl, eyl] = [factor * left[0], factor * left[1]];
        Ok(Some([
            exr - i * eyr,
            exr + i * eyr,
            exl - i * eyl,
            exl + i * eyl,
        ]))
    }

    fn trace_polarization(&self, ray: Ray, mut field: [Complex32; 3]) -> [Complex32; 3] {
        let directions = [
            unit(sub(ray.sub, self.feed)),
            unit(sub(ray.dish, ray.sub)),
            unit(sub(ray.aperture, ray.dish)),
        ];
        for pair in directions.windows(2) {
            let normal = unit(sub(pair[0], pair[1]));
            let projection = normal[0] as f32 * field[0]
                + normal[1] as f32 * field[1]
                + normal[2] as f32 * field[2];
            field =
                std::array::from_fn(|axis| normal[axis] as f32 * projection * 2.0 - field[axis]);
        }
        field
    }
}

fn add(a: Vector3, b: Vector3) -> Vector3 {
    std::array::from_fn(|i| a[i] + b[i])
}
fn sub(a: Vector3, b: Vector3) -> Vector3 {
    std::array::from_fn(|i| a[i] - b[i])
}
fn scale(a: Vector3, scalar: f64) -> Vector3 {
    a.map(|v| v * scalar)
}
fn dot(a: Vector3, b: Vector3) -> f64 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}
fn norm(a: Vector3) -> f64 {
    dot(a, a).sqrt()
}
fn unit(a: Vector3) -> Vector3 {
    scale(a, 1.0 / norm(a))
}
fn cross(a: Vector3, b: Vector3) -> Vector3 {
    [
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parabola() -> EvlaApertureModel {
        EvlaApertureModel::new(
            EvlaDishSurface::new(
                (0..=1250)
                    .map(|i| {
                        let r = i as f64 / 100.0;
                        [r, 0.028 * r * r, 0.056 * r]
                    })
                    .collect(),
            )
            .unwrap(),
        )
    }

    #[test]
    fn t52_surface_interpolation_preserves_analytic_parabola() {
        let model = parabola();
        for radius in [0.0, 0.001, 0.034, 1.234, 7.55, 12.5, 12.501] {
            let (height, slope) = model.dish(radius);
            assert!((height - 0.028 * radius * radius).abs() < 1e-12);
            assert!((slope - 0.056 * radius).abs() < 1e-12);
        }
    }

    #[test]
    fn t52_surface_rejects_invalid_samples_and_identity_covers_content() {
        let model = parabola();
        let mut changed = model.surface.samples().to_vec();
        changed[100][1] += 0.001;
        assert_ne!(
            model.surface.content_identity(),
            EvlaDishSurface::new(changed).unwrap().content_identity()
        );
        let mut invalid = model.surface.samples().to_vec();
        invalid[12][0] += 0.005;
        assert!(matches!(
            EvlaDishSurface::new(invalid),
            Err(casa_imaging_model::NativeAwRequestError::InvalidSurface)
        ));
        assert!(EvlaDishSurface::from_surface_text("0 0 0\n1 NaN 1\n12.5 4 1").is_err());
    }

    #[test]
    fn t52_evla_aperture_is_finite_deterministic_and_blocked() {
        let model = parabola();
        let grid = EvlaApertureGrid::new(32, 1.0, 3, 3e9, 0.31).unwrap();
        let mut aperture = vec![Complex32::default(); grid.workspace_values()];
        model.fill_aperture(grid, &mut aperture).unwrap();
        let mut repeated = vec![Complex32::default(); grid.workspace_values()];
        model.fill_aperture(grid, &mut repeated).unwrap();
        assert_eq!(aperture, repeated);
        let center = 16 + 32 * 16;
        assert_eq!(aperture[center], Complex32::default());
        assert!(aperture.iter().any(|value| value.norm() > 0.01));
        assert!(
            aperture
                .iter()
                .all(|value| value.re.is_finite() && value.im.is_finite())
        );
        let rotated = EvlaApertureGrid::new(32, 1.0, 3, 3e9, 0.7).unwrap();
        model.fill_aperture(rotated, &mut repeated).unwrap();
        assert_ne!(aperture, repeated);
        assert_eq!(
            model.fill_aperture(grid, &mut []),
            Err(NativeAwGenerationError::WorkspaceMismatch)
        );
    }

    #[test]
    #[ignore = "requires explicitly supplied EVLA surface and frozen CASA aperture probe"]
    fn t52_evla_aperture_matches_frozen_casa() {
        let surface =
            std::fs::read_to_string(std::env::var("CASA_RS_T52_SURFACE").unwrap()).unwrap();
        let model = EvlaApertureModel::new(EvlaDishSurface::from_surface_text(&surface).unwrap());
        let bytes = std::fs::read(std::env::var("CASA_RS_T52_APERTURE_ORACLE").unwrap()).unwrap();
        let grid = EvlaApertureGrid::new(32, 1.0, 3, 3e9, 0.31).unwrap();
        assert_eq!(
            bytes.len(),
            grid.workspace_values() * size_of::<Complex32>()
        );
        let mut aperture = vec![Complex32::default(); grid.workspace_values()];
        model.fill_aperture(grid, &mut aperture).unwrap();
        let mut error_squared = 0.0_f64;
        let mut reference_squared = 0.0_f64;
        let mut maximum_error = 0.0_f64;
        let mut reference_peak = 0.0_f64;
        let mut support_mismatches = 0;
        for (value, bytes) in aperture.iter().zip(bytes.chunks_exact(8)) {
            let reference = Complex32::new(
                f32::from_le_bytes(bytes[..4].try_into().unwrap()),
                f32::from_le_bytes(bytes[4..].try_into().unwrap()),
            );
            assert!(reference.re.is_finite() && reference.im.is_finite());
            let error = f64::from((*value - reference).norm());
            let magnitude = f64::from(reference.norm());
            error_squared += error * error;
            reference_squared += magnitude * magnitude;
            maximum_error = maximum_error.max(error);
            reference_peak = reference_peak.max(magnitude);
            support_mismatches += usize::from(
                (*value == Complex32::default()) != (reference == Complex32::default()),
            );
        }
        let relative_l2 = (error_squared / reference_squared).sqrt();
        let scaled_maximum = maximum_error / reference_peak;
        eprintln!(
            "T52 aperture relative_l2={relative_l2:.12e} peak_scaled_max={scaled_maximum:.12e} support_mismatches={support_mismatches}"
        );
        // Budget below 100 f32 epsilons for the two reflections, feed rotation,
        // phase projection and nine-subpixel accumulation; no fitted scale/phase.
        assert!(
            relative_l2 <= 1e-5,
            "aperture relative L2 error {relative_l2}"
        );
        assert!(
            scaled_maximum <= 1e-5,
            "aperture element error {scaled_maximum}"
        );
        assert_eq!(support_mismatches, 0);
    }
}
