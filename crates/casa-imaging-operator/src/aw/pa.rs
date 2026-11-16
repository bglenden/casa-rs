// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA AW buffer coverage for an imported or generated cache.

/// A cache header's PA after CASA's `CFCache::fillCFListFromDisk`
/// collects its Double degrees into a Float list and `nearestPA` reads
/// the radian value as Float.
#[inline]
#[must_use]
pub fn stored_pa_rad(degrees: f64) -> f32 {
    f64::from(degrees as f32).to_radians() as f32
}

/// First PA in the sorted cache strictly within `step_rad` of `pa_rad`,
/// following CASA's `CFStore2::nearestPA`. The angles and their
/// difference use Float radians. Angles do not wrap around the circle.
#[inline]
#[must_use]
pub fn first_covered_pa(pa_deg: &[f64], pa_rad: f32, step_rad: f32) -> Option<usize> {
    let first = pa_deg.partition_point(|&angle| pa_rad - stored_pa_rad(angle) >= step_rad);
    let angle = stored_pa_rad(*pa_deg.get(first)?);
    ((pa_rad - angle).abs() < step_rad).then_some(first)
}
