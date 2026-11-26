// SPDX-License-Identifier: LGPL-3.0-or-later
//! Sample the retained legacy EP Mueller plane and its pointing derivatives.

use std::f64::consts::PI;
use std::sync::Arc;

use casa_core::Complex32;

use super::{EpCacheProducts, PreparedEpModel};

/// A visibility owns its grid position and shares the kernel and sky planes.
/// Retained storage is independent of the convolution footprint.
pub(super) struct PointingSample {
    products: Arc<EpCacheProducts>,
    anchor: [isize; 2],
    fine: [isize; 2],
    rotation: (f64, f64),
    phase: Complex32,
    pol: usize,
    kernel_pol: usize,
}

impl PointingSample {
    pub(super) fn new(
        model: &PreparedEpModel,
        products: &Arc<EpCacheProducts>,
        (pol, kernel_pol): (usize, usize),
        wavelengths: [f64; 2],
        angle: f64,
        phase: f64,
    ) -> Option<Self> {
        let side = model.side;
        let position = std::array::from_fn::<_, 2, _>(|axis| {
            (wavelengths[axis] * model.scale[axis] + (side / 2 + 1) as f64) as f32
        });
        // The legacy gridder rounds the one-based position in Float.
        let anchor = position.map(|value| value.round() as isize - 1);
        let sampling = products.sampling;
        let fine = std::array::from_fn(|axis| {
            (((anchor[axis] + 1) as f32 - position[axis]) * sampling as f32).round() as isize
        });
        let support = products.support[0] as isize;
        if anchor
            .iter()
            .any(|&value| value < support || value + support >= side as isize)
        {
            return None;
        }
        Some(Self {
            products: products.clone(),
            anchor,
            fine,
            rotation: (angle - products.reference_angle_rad).sin_cos(),
            phase: Complex32::new(phase.cos() as f32, phase.sin() as f32),
            pol,
            kernel_pol,
        })
    }

    pub(super) fn evaluate(&self, model: &PreparedEpModel, offset: [f64; 2]) -> [Complex32; 3] {
        let mut norm = Complex32::default();
        let mut values = [Complex32::default(); 3];
        let kernel = &self.products.convolution;
        let kernel_side = kernel.shape()[0] as isize;
        let centre = kernel_side / 2;
        let sampling = self.products.sampling;
        let support = self.products.support[0] as isize;
        let [ax, ay] = self.anchor;
        let (sin, cos) = self.rotation;
        let fourier = &model.fourier[self.pol];
        for y in -support..=support {
            let ky = (y * sampling as isize + self.fine[1]) as f64;
            let row = (ay + y) as usize * model.side;
            for x in -support..=support {
                let kx = (x * sampling as isize + self.fine[0]) as f64;
                let u = (cos * kx + sin * ky).round() as isize;
                let v = (-sin * kx + cos * ky).round() as isize;
                let index = [centre + u, centre + v];
                if index.iter().any(|p| !(0..kernel_side).contains(p)) {
                    continue;
                }
                let kernel = kernel[[index[0] as usize, index[1] as usize, self.kernel_pol, 0]];
                let sky = fourier[row + (ax + x) as usize];
                // The one-based phase coordinates are offset from the kernel lookup.
                let gradient = [u, v].map(|value| (value + 1) as f64 * PI / sampling as f64);
                let gradient = std::array::from_fn::<_, 2, _>(|axis| {
                    (gradient[axis] / model.scale[axis]) as f32
                });
                let angle = (f64::from(gradient[0]) * offset[0]
                    + f64::from(gradient[1]) * offset[1]) as f32;
                let (sin, cos) = f64::from(angle).sin_cos();
                let weight = kernel * Complex32::new(cos as f32, sin as f32);
                norm += weight;
                values[0] += weight * sky;
                for (value, gradient) in values[1..].iter_mut().zip(gradient) {
                    *value += (Complex32::new(0., gradient) * weight) * sky;
                }
            }
        }
        if norm != Complex32::default() {
            values = values.map(|value| value * self.phase / norm);
        }
        values
    }
}
