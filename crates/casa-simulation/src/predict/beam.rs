// SPDX-License-Identifier: LGPL-3.0-or-later
//! The primary beam `sm.setvp(dovp=True)` applies: CASA's default voltage
//! patterns, as `PBMath` gives them by telescope name and frequency
//! ([`PrimaryBeam`], from `casa-numerics`), unsquinted, which is what CASA
//! applies to the simulator's one-plane Stokes I model.

pub use casa_numerics::{BeamPattern, PrimaryBeam};
use ndarray::Array2;

use super::SkyModel;

/// `pattern`'s power on model plane `plane`, `[y][x]`, centred on the pixel
/// `centre`, at the plane's frequency (`BeamSkyJones::apply` squares the
/// voltage pattern).
///
/// The radius is `PBMath1D::apply`'s: each axis's squared offset in degrees
/// is formed in `Double` and kept as `Float`, their sum and its root are
/// `Float`, and the root times 60 · ν/GHz is kept as `Float` before the
/// lookup, so a radius at a table bin's edge falls as CASA's does.
pub(super) fn power_plane(
    pattern: &BeamPattern,
    model: &SkyModel,
    plane: usize,
    centre: [f64; 2],
) -> Array2<f32> {
    let [nx, ny] = model.shape();
    let frequency_hz = model.plane_frequency_hz(plane);
    let factor = 60.0 * frequency_hz / 1.0e9;
    let increment_deg = model.increment_rad.map(f64::to_degrees);
    let squared = |pixel: usize, axis: usize| {
        ((pixel as f64 - centre[axis]) * increment_deg[axis]).powi(2) as f32
    };
    let lookup = pattern.at_frequency(frequency_hz);
    Array2::from_shape_fn((ny, nx), |(y, x)| {
        let radius = (f64::from((squared(x, 0) + squared(y, 1)).sqrt()) * factor) as f32;
        lookup.power(f64::from(radius))
    })
}
