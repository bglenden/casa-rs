// SPDX-License-Identifier: LGPL-3.0-or-later
//! PBMath's VLA feed offsets and BeamSquint::getPointingDirection.
use super::{PrimaryBeam, VlaBand};

impl PrimaryBeam {
    /// CASA's VLA pointing displacement in RA/Dec tangent angles, radians.
    /// `reference_hz` selects the VLA band; the channel scales its offset.
    /// `parallactic_rad` includes the feed angle, and `right` chooses RR
    /// rather than LL. The returned shift uses MVDirection's true-angle
    /// convention. Patterns without active squint return zero.
    #[must_use]
    pub fn squint_displacement_rad(
        self,
        reference_hz: f64,
        channel_hz: f64,
        parallactic_rad: f64,
        right: bool,
    ) -> [f64; 2] {
        if !self.squints() {
            return [0.; 2];
        }
        let degrees: f64 = match VlaBand::of(reference_hz) {
            Some(VlaBand::L) => -135.,
            Some(VlaBand::C) => 25.,
            Some(VlaBand::X) => 82.,
            Some(VlaBand::U) => -25.,
            Some(VlaBand::K) => -6.,
            Some(VlaBand::Q) => 0.,
            _ => return [0.; 2],
        };
        let angle = f64::from(degrees.to_radians() as f32);
        let magnitude = f64::from(1.21_f32) * std::f64::consts::PI / (180. * 60.);
        let scale = f64::from((-1e9 / channel_hz) as f32) * if right { 1. } else { -1. };
        let (sx, sy) = (
            scale * magnitude * angle.cos(),
            scale * magnitude * angle.sin(),
        );
        let (sin, cos) = (-parallactic_rad).sin_cos();
        [-(sx * cos + sy * sin), -sx * sin + sy * cos]
    }
}
