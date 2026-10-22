// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA `GridFT`'s uvw rotation, for callers outside the imaging read path.

use casa_types::measures::direction::{DirectionRef, MDirection};

use super::engine::CasaUvwMachine;
use crate::error::MsResult;

/// CASA `GridFT`'s rotation of rows from one phase centre to an image's
/// tangent point, built once and applied to every row
/// (`FTMachine::rotateUVW`, with a `UVWMachine` that neither projects nor
/// treats the array as east–west).
///
/// `GridFT` rotates every row, even when the two directions are the same:
/// `FTMachine::initMaps` resets the last field it saw, so the first row it
/// meets turns the rotation on. The rotation of two equal directions is the
/// identity up to rounding.
///
/// Cf. casacore `UVWMachine::convertUVW(Double&, Vector<Double>&)`.
#[derive(Debug, Clone, Copy)]
pub struct GridFtProjection {
    machine: CasaUvwMachine,
}

impl GridFtProjection {
    /// The rotation from rows phased on `phase_centre_rad` to an image whose
    /// tangent point is `tangent_point_rad`, both J2000 `[ra, dec]` in
    /// radians.
    #[must_use]
    pub fn between_j2000_directions(
        phase_centre_rad: [f64; 2],
        tangent_point_rad: [f64; 2],
    ) -> Self {
        let source = MDirection::from_angles(
            phase_centre_rad[0],
            phase_centre_rad[1],
            DirectionRef::J2000,
        );
        let target = MDirection::from_angles(
            tangent_point_rad[0],
            tangent_point_rad[1],
            DirectionRef::J2000,
        );
        Self {
            machine: CasaUvwMachine::new(&target, &source),
        }
    }

    /// A row's raw `UVW` in metres rotated to the tangent point, and the
    /// phase shift in metres the gridder applies (`−dphase`, the
    /// measurement operator's `phase_shift_m`).
    ///
    /// CASA's `UVWMachine` works in the opposite sign of u and v to the
    /// MeasurementSet's; the rotation is applied in its convention and the
    /// result returned in the MeasurementSet's.
    #[must_use]
    pub fn apply(&self, raw_uvw_m: [f64; 3]) -> ([f64; 3], f64) {
        let casa_input = [-raw_uvw_m[0], -raw_uvw_m[1], raw_uvw_m[2]];
        let (casa_output, casa_dphase_m) = self.machine.convert_uvw(casa_input);
        (
            [-casa_output[0], -casa_output[1], casa_output[2]],
            -casa_dphase_m,
        )
    }
}

/// Rotate a row's raw `UVW` from its phase centre to an image's tangent
/// point, as CASA's `GridFT` does before it degrids or grids the row
/// (`FTMachine::rotateUVW`, with a `UVWMachine` that neither projects nor
/// treats the array as east–west). Both directions are J2000 `[ra, dec]` in
/// radians. A caller rotating many rows between the same two directions
/// builds one [`GridFtProjection`] instead.
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
    Ok(
        GridFtProjection::between_j2000_directions(phase_centre_rad, tangent_point_rad)
            .apply(raw_uvw_m),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reusable projection is the per-row helper the imaging read path
    /// uses, bit for bit, and the rotation between equal directions keeps
    /// uvw to rounding with no phase shift.
    #[test]
    fn the_projection_is_the_per_row_rotation_bit_for_bit() {
        let phase_centre = [2.6179938779914944, -0.5235987755982988];
        let tangent = [2.6180011, -0.5236035];
        let projection = GridFtProjection::between_j2000_directions(phase_centre, tangent);
        for uvw in [
            [120.5, -48.25, 3.5],
            [-1_500.0, 732.125, -40.0],
            [0.0, 0.0, 0.0],
        ] {
            let engine = super::super::engine::gridft_projection_between_j2000_directions(
                uvw,
                phase_centre,
                tangent,
                false,
            )
            .expect("rotation");
            let projected = projection.apply(uvw);
            assert_eq!(projected.0.map(f64::to_bits), engine.0.map(f64::to_bits));
            assert_eq!(projected.1.to_bits(), engine.1.to_bits());
            assert_eq!(
                gridft_uvw_between_j2000_directions(uvw, phase_centre, tangent).expect("helper"),
                projected
            );
        }
        let same = GridFtProjection::between_j2000_directions(phase_centre, phase_centre);
        let (uvw, shift) = same.apply([120.5, -48.25, 3.5]);
        for (rotated, raw) in uvw.iter().zip([120.5, -48.25, 3.5]) {
            assert!((rotated - raw).abs() < 1e-12, "{rotated} against {raw}");
        }
        assert!(shift.abs() < 1e-12, "{shift}");
        assert_eq!(same.apply([0.0; 3]), ([0.0; 3], 0.0));
    }
}
