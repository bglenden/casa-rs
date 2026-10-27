// SPDX-License-Identifier: LGPL-3.0-or-later
//! The position angle of one direction seen from another.

use super::direction::MDirection;

impl MDirection {
    /// The position angle of `target` seen from `self`, in radians: from
    /// the direction of `self`'s pole towards increasing longitude, in
    /// `(-π, π]`. Both directions are taken in their own longitude and
    /// latitude, so they should share a frame.
    ///
    /// A parallactic angle is the position angle of the celestial pole seen
    /// from the source, both in the observer's `AZEL`.
    ///
    /// Cf. casacore `MVDirection::positionAngle`, in its spherical form.
    #[must_use]
    pub fn position_angle(&self, target: &Self) -> f64 {
        let (origin_lon, origin_lat) = self.as_angles();
        let (target_lon, target_lat) = target.as_angles();
        let delta_lon = target_lon - origin_lon;
        let y = delta_lon.sin() * target_lat.cos();
        let x = origin_lat.cos() * target_lat.sin()
            - origin_lat.sin() * target_lat.cos() * delta_lon.cos();
        y.atan2(x)
    }
}
