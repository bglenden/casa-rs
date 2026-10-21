// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA `GridFT`'s uvw rotation, for callers outside the imaging read path.

use crate::error::MsResult;

/// Rotate a row's raw `UVW` from its phase centre to an image's tangent
/// point, as CASA's `GridFT` does before it degrids or grids the row
/// (`FTMachine::rotateUVW`, with a `UVWMachine` that neither projects nor
/// treats the array as east–west). Both directions are J2000 `[ra, dec]` in
/// radians.
///
/// Returns the rotated uvw in metres and the phase shift in metres the
/// gridder applies (`−dphase`, the measurement operator's
/// `phase_shift_m`).
///
/// # Errors
///
/// A direction that does not convert.
pub fn gridft_uvw_between_j2000_directions(
    raw_uvw_m: [f64; 3],
    phase_centre_rad: [f64; 2],
    tangent_point_rad: [f64; 2],
) -> MsResult<([f64; 3], f64)> {
    super::engine::gridft_projection_between_j2000_directions(
        raw_uvw_m,
        phase_centre_rad,
        tangent_point_rad,
        false,
    )
}
