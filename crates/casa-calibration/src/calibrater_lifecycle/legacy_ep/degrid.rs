// SPDX-License-Identifier: LGPL-3.0-or-later
//! Sample the retained legacy EP Mueller plane and its pointing derivatives.

use std::f64::consts::PI;

use casa_types::Complex32;

use super::PreparedEpModel;

pub(super) struct PointingSample {
    taps: Vec<Tap>,
    phase: Complex32,
}

struct Tap {
    kernel: Complex32,
    sky: Complex32,
    gradient: [f32; 2],
}

impl PointingSample {
    pub(super) fn new(
        model: &PreparedEpModel,
        pol: usize,
        kernel_pol: usize,
        wavelengths: [f64; 2],
        angle: f64,
        phase: f64,
    ) -> Self {
        let side = model.side;
        let scale = model.cell_rad.map(|increment| increment * side as f64);
        // The legacy gridder rounds the one-based position in Float.
        let position = std::array::from_fn::<_, 2, _>(|axis| {
            (wavelengths[axis] * scale[axis] + (side / 2 + 1) as f64) as f32
        });
        let anchor = position.map(|value| value.round() as isize - 1);
        let sampling = model.products.sampling;
        let fine = std::array::from_fn::<_, 2, _>(|axis| {
            (((anchor[axis] + 1) as f32 - position[axis]) * sampling as f32).round() as isize
        });
        let support = model.products.support[0] as isize;
        let mut result = Self {
            taps: Vec::new(),
            phase: Complex32::new(phase.cos() as f32, phase.sin() as f32),
        };
        if anchor
            .iter()
            .any(|&value| value < support || value + support >= side as isize)
        {
            return result;
        }
        let kernel = &model.products.convolution;
        let kernel_side = kernel.shape()[0] as isize;
        let centre = kernel_side / 2;
        let (sin, cos) = (angle - model.products.reference_angle_rad).sin_cos();
        let fourier = &model.fourier[pol];
        result.taps.reserve_exact((2 * support + 1).pow(2) as usize);
        for y in -support..=support {
            let ky = (y * sampling as isize + fine[1]) as f64;
            for x in -support..=support {
                let kx = (x * sampling as isize + fine[0]) as f64;
                let u = (cos * kx + sin * ky).round() as isize;
                let v = (-sin * kx + cos * ky).round() as isize;
                let index = [centre + u, centre + v];
                if index.iter().any(|p| !(0..kernel_side).contains(p)) {
                    continue;
                }
                let kernel = kernel[[index[0] as usize, index[1] as usize, kernel_pol, 0]];
                let sky = fourier[(anchor[0] + x) as usize + side * (anchor[1] + y) as usize];
                // The one-based phase coordinates are offset from the kernel
                // lookup, and the derivative uses the stored visibility scale.
                let gradient = [u, v].map(|value| (value + 1) as f64 * PI / sampling as f64);
                result.taps.push(Tap {
                    kernel,
                    sky,
                    gradient: std::array::from_fn(|axis| (gradient[axis] / scale[axis]) as f32),
                });
            }
        }
        result
    }

    pub(super) fn evaluate(&self, offset: [f64; 2]) -> [Complex32; 3] {
        let mut norm = Complex32::default();
        let mut values = [Complex32::default(); 3];
        for tap in &self.taps {
            let angle = (f64::from(tap.gradient[0]) * offset[0]
                + f64::from(tap.gradient[1]) * offset[1]) as f32;
            let phase =
                Complex32::new(f64::from(angle).cos() as f32, f64::from(angle).sin() as f32);
            let weight = tap.kernel * phase;
            norm += weight;
            values[0] += weight * tap.sky;
            for (value, gradient) in values[1..].iter_mut().zip(tap.gradient) {
                *value += (Complex32::new(0., gradient) * weight) * tap.sky;
            }
        }
        if norm != Complex32::default() {
            values = values.map(|value| value * self.phase / norm);
        }
        values
    }
}
