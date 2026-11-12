// SPDX-License-Identifier: LGPL-3.0-or-later
//! Parallactic-angle rotation epochs for an imported aperture buffer.

use std::sync::Arc;

/// Feed angles at the selected observation times and the rotation threshold.
#[derive(Clone, Debug, PartialEq)]
pub struct AwPaRotation {
    /// Minimum absolute change that triggers rotation, in radians.
    pub step_rad: f64,
    /// The selected times in increasing order, seconds since MJD 0, with
    /// the corresponding antenna-0 feed PA in Float radians.
    pub samples: Arc<[(f64, f32)]>,
}

impl AwPaRotation {
    /// The input is shared with the deployment and stays allocated.
    pub(super) fn samples_bytes(&self) -> usize {
        self.samples.len() * size_of::<(f64, f32)>() + 2 * size_of::<usize>()
    }

    /// In the single-buffer case, record each rotation epoch. The cached
    /// angle is retained until the next sample differs strictly more than
    /// the requested threshold (`AWConvFunc::prepareConvFunction`).
    pub(super) fn schedule(&self, base_deg: f64) -> Vec<(f64, f64)> {
        let base = f64::from(base_deg as f32).to_radians();
        let mut last = base;
        let mut rotations = Vec::new();
        for &(time, pa) in self.samples.iter() {
            let pa = f64::from(pa);
            if (last - pa).abs() > self.step_rad {
                rotations.push((time, base - pa));
                last = pa;
            }
        }
        rotations
    }
}

/// The inverse pixel transform for an axis-aligned linear CF plane.
pub(super) fn pixel_rotation(angle_rad: f64) -> [[f64; 2]; 2] {
    let (sin, cos) = angle_rad.sin_cos();
    [[cos, -sin], [sin, cos]]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rotation_samples_the_inverse_linear_coordinate_transform() {
        let [[xx, xy], [yx, yy]] = pixel_rotation(std::f64::consts::FRAC_PI_2);
        assert!(xx.abs() < 1.0e-15 && yy.abs() < 1.0e-15);
        assert_eq!([xy, yx], [-1.0, 1.0]);
    }

    #[test]
    fn rotation_epochs_use_the_previous_angle_and_rotate_the_base() {
        let rotation = AwPaRotation {
            step_rad: 0.125,
            samples: Arc::new([(1.0, 0.125), (2.0, 0.25), (3.0, 0.375), (4.0, 0.5)]),
        };
        assert_eq!(rotation.schedule(0.0), [(2.0, -0.25), (4.0, -0.5)]);
    }
}
