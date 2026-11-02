// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's `FTMachine` uvw reprojections: a row's raw UVW, phased on its
//! observation direction, carried to an image's tangent point, with the
//! phase shift the gridder applies.

use casa_types::measures::UvwMachine;
use casa_types::measures::direction::{DirectionRef, MDirection};

/// One of CASA's reprojections of rows from one phase centre to an image's
/// tangent point, built once and applied to every row:
///
/// - `GridFT`'s rotation ([`Self::between_directions`]): `FTMachine::rotateUVW`
///   with a `UVWMachine` that neither projects nor treats the array as
///   east–west;
/// - a faceted image's rotation ([`Self::faceted_between_directions`]): the
///   `project=true` machine, which also reprojects the rotated UV plane onto
///   the observation's tangent plane, as CASA builds every facet's;
/// - `MosaicFT`'s reprojection ([`Self::mosaic_between_directions`]):
///   `FTMachine::girarUVW`.
///
/// `GridFT` rotates every row, even when the two directions are the same:
/// `FTMachine::initMaps` resets the last field it saw, so the first row it
/// meets turns the rotation on. The rotation of two equal directions is the
/// identity up to rounding.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GridFtProjection {
    machine: Reprojection,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Reprojection {
    /// `FTMachine::rotateUVW`.
    Rotate(UvwMachine),
    /// `FTMachine::rotateUVW` with a `project=true` machine.
    Faceted(UvwMachine),
    /// `FTMachine::girarUVW`: a correlator-direction machine, then the
    /// image-centre machine.
    Girar {
        frame: UvwMachine,
        phase: UvwMachine,
    },
}

impl GridFtProjection {
    /// `GridFT`'s rotation from rows phased on `phase_centre` to an image
    /// whose tangent point is `tangent_point`, both J2000.
    #[must_use]
    pub fn between_directions(phase_centre: &MDirection, tangent_point: &MDirection) -> Self {
        Self {
            machine: Reprojection::Rotate(UvwMachine::new(tangent_point, phase_centre)),
        }
    }

    /// A faceted image's rotation from rows phased on `phase_centre` to the
    /// facet whose tangent point is `tangent_point`, both J2000: the
    /// rotation reprojected onto the observation's tangent plane, which is
    /// observable in the facet PSF.
    #[must_use]
    pub fn faceted_between_directions(
        phase_centre: &MDirection,
        tangent_point: &MDirection,
    ) -> Self {
        Self {
            machine: Reprojection::Faceted(UvwMachine::new(tangent_point, phase_centre)),
        }
    }

    /// `MosaicFT`'s reprojection of rows phased on `phase_centre` to an
    /// image whose tangent point is `tangent_point`, both J2000
    /// (`FTMachine::girarUVW`): the image-centre rotation forms the new u
    /// and v from the old u and v alone, so w does not reach them, and the
    /// phase shift is taken from the new u and v.
    #[must_use]
    pub fn mosaic_between_directions(
        phase_centre: &MDirection,
        tangent_point: &MDirection,
    ) -> Self {
        Self {
            machine: Reprojection::Girar {
                // `girarUVW` first runs an explicit-output machine from the
                // correlator direction to the in-field direction. Both are
                // the phase centre here, and CASA does not collapse this
                // machine to a no-op.
                frame: UvwMachine::new(phase_centre, phase_centre),
                phase: UvwMachine::new(tangent_point, phase_centre),
            },
        }
    }

    /// [`Self::between_directions`] for J2000 `[ra, dec]` in radians.
    #[must_use]
    pub fn between_j2000_directions(
        phase_centre_rad: [f64; 2],
        tangent_point_rad: [f64; 2],
    ) -> Self {
        let [source, target] = [phase_centre_rad, tangent_point_rad].map(j2000);
        Self::between_directions(&source, &target)
    }

    /// [`Self::mosaic_between_directions`] for J2000 `[ra, dec]` in radians.
    #[must_use]
    pub fn mosaic_between_j2000_directions(
        phase_centre_rad: [f64; 2],
        tangent_point_rad: [f64; 2],
    ) -> Self {
        let [source, target] = [phase_centre_rad, tangent_point_rad].map(j2000);
        Self::mosaic_between_directions(&source, &target)
    }

    /// A row's raw `UVW` in metres reprojected to the tangent point, and
    /// the phase shift in metres the gridder applies (`−dphase`, the
    /// measurement operator's `phase_shift_m`).
    ///
    /// CASA's `UVWMachine` works in the opposite sign of u and v to the
    /// MeasurementSet's (`negateUV`); the reprojection is applied in its
    /// convention and the result returned in the MeasurementSet's.
    #[must_use]
    pub fn apply(&self, raw_uvw_m: [f64; 3]) -> ([f64; 3], f64) {
        let casa_input = [-raw_uvw_m[0], -raw_uvw_m[1], raw_uvw_m[2]];
        let (casa_output, casa_dphase_m) = match self.machine {
            Reprojection::Rotate(machine) => machine.convert(casa_input),
            Reprojection::Faceted(machine) => machine.convert_projected(casa_input),
            Reprojection::Girar { frame, phase } => girar(frame, phase, casa_input),
        };
        (
            [-casa_output[0], -casa_output[1], casa_output[2]],
            -casa_dphase_m,
        )
    }
}

fn j2000([ra, dec]: [f64; 2]) -> MDirection {
    MDirection::from_angles(ra, dec, DirectionRef::J2000)
}

/// `FTMachine::girarUVW` on UVW in CASA's sign: UVW and `dphase`.
fn girar(frame: UvwMachine, phase: UvwMachine, casa_uvw_m: [f64; 3]) -> ([f64; 3], f64) {
    let (intermediate, mut dphase_m) = frame.convert(casa_uvw_m);
    let rot = phase.rotation();
    let output = [
        intermediate[0] * rot[0][0] + intermediate[1] * rot[1][0],
        intermediate[1] * rot[1][1] + intermediate[0] * rot[0][1],
        intermediate[0] * rot[0][2] + intermediate[1] * rot[1][2] + intermediate[2] * rot[2][2],
    ];
    // CASA's shipped synthesis library evaluates these expressions with
    // contraction disabled. Keep the source order: an explicit mul_add
    // matches the first row but not later ones.
    let phrot = phase.phase_rotation();
    dphase_m += phrot[0] * output[0] + phrot[1] * output[1];
    (output, dphase_m)
}

#[cfg(test)]
mod tests {
    use casa_types::measures::UvwMachine;
    use casa_types::measures::direction::{DirectionRef, MDirection};

    use super::GridFtProjection;

    /// Rotation between equal directions keeps uvw to rounding with no
    /// phase shift.
    #[test]
    fn rotation_between_equal_directions_keeps_uvw() {
        let phase_centre = [2.617_993_877_991_494_4, -0.523_598_775_598_298_8];
        let same = GridFtProjection::between_j2000_directions(phase_centre, phase_centre);
        let (uvw, shift) = same.apply([120.5, -48.25, 3.5]);
        for (rotated, raw) in uvw.iter().zip([120.5, -48.25, 3.5]) {
            assert!((rotated - raw).abs() < 1e-12, "{rotated} against {raw}");
        }
        assert!(shift.abs() < 1e-12, "{shift}");
        assert_eq!(same.apply([0.0; 3]), ([0.0; 3], 0.0));
    }

    /// `MosaicFT`'s reprojection gives CASA's `girarUVW` reference row, its
    /// phase as the gridder's `−dphase`, and departs from `GridFT`'s
    /// rotation by about `w · Δθ` in u.
    #[test]
    fn the_mosaic_projection_is_casas_girar_uvw() {
        let phase_centre = [3.149_884_015_678_791, -0.329_299_608_408_283];
        let tangent = [3.149_807_242_890_337, -0.329_299_608_769_085_2];
        let raw = [
            4.324_749_385_347_82,
            -121.757_651_483_641_07,
            -15.081_385_958_149_532,
        ];
        let mosaic = GridFtProjection::mosaic_between_j2000_directions(phase_centre, tangent);
        let (uvw, shift) = mosaic.apply(raw);
        for (got, want) in uvw.iter().zip([
            4.321_726_518_390_6,
            -121.757_758_815_853_26,
            -15.081_699_947_780_477,
        ]) {
            assert!((got - want).abs() < 1e-12, "{got} against {want}");
        }
        assert!(
            (shift - 0.000_314_029_427_521_723_44).abs() < 1e-12,
            "{shift}"
        );
        let (rotated, _) =
            GridFtProjection::between_j2000_directions(phase_centre, tangent).apply(raw);
        let w_share = (raw[2] * (tangent[0] - phase_centre[0]) * tangent[1].cos()).abs();
        assert!(
            (rotated[0] - uvw[0]).abs() > 0.5 * w_share,
            "{rotated:?} against {uvw:?}"
        );
    }

    /// The field of a GridFT chart and its phase centre, two VLA fields.
    const GRIDFT_SOURCE: [f64; 2] = [-1.053_851_618_969_825_7, 0.706_574_731_054_254_4];
    const GRIDFT_TARGET: [f64; 2] = [-1.058_214_942_099_811_3, 0.702_211_407_924_268_5];
    const GRIDFT_RAW: [f64; 3] = [
        27.073_056_790_908_41,
        -29.672_968_936_171_65,
        15.993_460_382_965_498,
    ];

    /// `GridFT`'s reprojection: its `negateUV`, `UVWMachine` and restored-UV
    /// equations at fixed values, the phase the adjoint's sign, opposite
    /// CASA's `locuvw` dphase because CASA forms `exp(-i dphase)`.
    #[test]
    fn the_gridft_projection_negates_uv_at_its_boundary() {
        let (uvw_m, phase_shift_m) =
            GridFtProjection::between_j2000_directions(GRIDFT_SOURCE, GRIDFT_TARGET)
                .apply(GRIDFT_RAW);
        for (got, want) in uvw_m.iter().zip([
            27.209_934_044_771_824,
            -29.526_408_975_072_36,
            16.032_371_216_641_046,
        ]) {
            assert!((got - want).abs() < 1.0e-12, "{got} against {want}");
        }
        assert!((phase_shift_m - -0.038_910_833_675_543_17).abs() < 1.0e-12);
    }

    /// A faceted chart's reprojection is casacore's
    /// `UVWMachine(out, in, EW=false, project=true)` after `GridFT`'s
    /// negate-UV boundary, with the adjoint's sign.
    #[test]
    fn a_faceted_projection_reprojects_the_uv_plane() {
        let [source, target] = [GRIDFT_SOURCE, GRIDFT_TARGET]
            .map(|[ra, dec]| MDirection::from_angles(ra, dec, DirectionRef::J2000));
        let (uvw_m, phase_shift_m) =
            GridFtProjection::faceted_between_directions(&source, &target).apply(GRIDFT_RAW);
        for (got, want) in uvw_m.iter().zip([
            27.126_332_109_240_163,
            -29.603_258_931_514_546,
            16.032_371_216_641_046,
        ]) {
            assert!((got - want).abs() < 1.0e-12, "{got} against {want}");
        }
        assert!((phase_shift_m - -0.038_910_833_675_545_334).abs() < 1.0e-12);
    }

    /// AWProject's offset field takes `girarUVW`'s UVW and phase: synthetic
    /// widefield values captured with casacore `UVWMachine` and CASA
    /// `FTMachine::girarUVW`'s row expressions, compiled with
    /// floating-point contraction disabled. The mosaic phase is the
    /// gridder's `−dphase`. GridFT's rotation departs from it.
    #[test]
    fn an_offset_aw_field_takes_girar_uvw_and_phase() {
        let source = [3.54, 0.29];
        let target = [3.55, 0.293];
        let raw = [-1_200.0, -2_000.0, 1_900.0];
        let (uvw, shift) =
            GridFtProjection::mosaic_between_j2000_directions(source, target).apply(raw);
        for (got, want) in uvw.iter().zip([
            -1_205.658_949_685_163_6,
            -1_996.516_890_818_620_8,
            1_882.388_540_906_844_6,
        ]) {
            assert!((got - want).abs() < 1.0e-10, "{got} against {want}");
        }
        assert!((shift - 17.514_874_069_400_992).abs() < 1.0e-10, "{shift}");
        let (gridft, gridft_shift) =
            GridFtProjection::between_j2000_directions(source, target).apply(raw);
        assert!((gridft[0] - -1_223.865_279_877_529_5).abs() < 1.0e-10);
        assert!((gridft_shift - 17.611_459_093_155_094).abs() < 1.0e-10);
        assert!((uvw[0] - gridft[0]).abs() > 18.0);
        assert!((shift - gridft_shift).abs() > 0.09);
    }

    /// The machines of a VLASS mosaic field match CASA's native bits.
    #[test]
    fn mosaic_machines_match_vlass_native_bits() {
        let source = MDirection::from_angles(
            -2.737_583_127_275_378,
            0.293_215_314_333_100_33,
            DirectionRef::J2000,
        );
        // The reference direction CASA's DirectionCoordinate returns after
        // its radians -> WCS degrees -> radians boundary for the frozen
        // VLASS field.
        let image = MDirection::from_angles(
            3.545_602_179_904_207_7,
            0.293_215_314_333_100_33,
            DirectionRef::J2000,
        );
        let rotation_bits = [
            [4_607_182_418_800_017_408, 0, 0],
            [0, 4_607_182_418_800_017_408, 4_363_988_038_922_010_624],
            [0, 4_363_988_038_922_010_624, 4_607_182_418_800_017_409],
        ];
        let frame = UvwMachine::new(&source, &source);
        let phase = UvwMachine::new(&image, &source);
        assert_eq!(
            frame.rotation().map(|row| row.map(f64::to_bits)),
            rotation_bits
        );
        assert_eq!(
            phase.rotation().map(|row| row.map(f64::to_bits)),
            rotation_bits
        );
        assert_eq!(frame.phase_rotation().map(f64::to_bits), [0, 0, 0]);
        assert_eq!(
            phase.phase_rotation().map(f64::to_bits),
            [
                4_379_056_720_170_678_802,
                13_570_113_653_343_543_584,
                13_578_075_827_820_232_864,
            ]
        );
        let (uvw_m, phase_shift_m) = GridFtProjection::mosaic_between_directions(&source, &image)
            .apply([
                -1_236.664_060_532_996_5,
                -2_033.447_768_019_716_7,
                1_892.803_548_834_091,
            ]);
        assert_eq!(
            uvw_m.map(f64::to_bits),
            [
                13_876_525_758_357_962_531,
                13_880_030_050_162_779_011,
                4_656_039_453_490_506_920,
            ]
        );
        assert_eq!((-phase_shift_m).to_bits(), 4_425_269_780_393_832_168);
    }
}
