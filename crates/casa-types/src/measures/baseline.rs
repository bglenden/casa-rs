// SPDX-License-Identifier: LGPL-3.0-or-later
//! Interferometer baselines to uvw coordinates, as casacore computes them.
//!
//! [`UvwFrame`] is casacore's `MBaseline` conversion from ITRF to J2000
//! followed by the `MVuvw` projection on a J2000 phase centre: the chain
//! `NewMSSimulator::calcAntUVW` and `MSUVWGenerator` use to give each antenna
//! its uvw. The conversion routes the baseline direction through HADEC, TOPO,
//! APP and JNAT, applying Earth rotation (local apparent sidereal time),
//! polar motion, diurnal aberration, precession and nutation (IAU 1976/1980),
//! annual aberration and solar light deflection. The baseline's length is
//! unchanged.
//!
//! An antenna's uvw is that of the baseline from the observatory to the
//! antenna (observatory minus antenna, casacore's `MVBaseline(first, second)`
//! convention). A row's uvw is the second antenna's minus the first's, so the
//! observatory term cancels.
//!
//! The epoch is the caller's. casacore's simulator reads a row's stored time
//! as UT1; this module converts whatever epoch it is given.

use std::sync::Arc;

use super::direction::{DirectionRef, MDirection};
use super::epoch::{EpochRef, MEpoch};
use super::error::MeasureError;
use super::frame::MeasFrame;
use super::position::MPosition;
use super::provider::MeasuresProvider;

/// The ITRF-to-uvw rotation of interferometer baselines at one epoch, for one
/// observatory and one J2000 phase centre.
///
/// Building a frame does the epoch-dependent work once; [`UvwFrame::baseline_uvw`]
/// then rotates and projects each baseline.
#[derive(Debug, Clone)]
pub struct UvwFrame {
    phase_center: MDirection,
    observatory_longitude_sin_cos: (f64, f64),
    polar_motion_rotation: [[f64; 3]; 3],
    diurnal_aberration_rotation: [[f64; 3]; 3],
    precession_nutation_inverse: [[f64; 3]; 3],
    inverse_aberration_rotation: [[f64; 3]; 3],
    inverse_solar_deflection_rotation: [[f64; 3]; 3],
}

impl UvwFrame {
    /// The frame at `epoch` for baselines at `observatory`, projected on the
    /// J2000 direction `phase_center_rad` (longitude, latitude in radians).
    ///
    /// # Errors
    ///
    /// An epoch or direction conversion the measures data cannot make, or a
    /// failed ephemeris lookup.
    pub fn new(
        phase_center_rad: [f64; 2],
        epoch: MEpoch,
        observatory: &MPosition,
        measures: Arc<dyn MeasuresProvider>,
    ) -> Result<Self, MeasureError> {
        let phase_center = MDirection::from_angles(
            phase_center_rad[0],
            phase_center_rad[1],
            DirectionRef::J2000,
        );
        let frame = MeasFrame::new()
            .with_epoch(epoch)
            .with_position(observatory.clone())
            .with_direction(phase_center.clone())
            .with_measures(measures);
        let longitude = observatory.longitude_rad();
        let observatory_longitude_sin_cos = longitude.sin_cos();
        let last = local_apparent_sidereal_time(&frame)?;
        let tdb_mjd = epoch_mjd(&frame, EpochRef::TDB)?;
        let (xp, yp) = polar_motion_rad(&frame, tdb_mjd)?;
        let polar_motion_rotation = polar_motion_euler(-xp, -yp, last);
        let radius = vector_norm(observatory.as_itrf());
        let v_c = diurnal_aberration_factor(radius);
        let aberration_direction =
            spherical_to_cartesian(last, observatory.geocentric_latitude_rad());
        let diurnal_aberration_shift = scale_vector(aberration_direction, -v_c);
        let app_phase_center = phase_center.convert_to(DirectionRef::APP, &frame)?;
        let tt = epoch_jd_pair(&frame, EpochRef::TT)?;
        let nutation = sofars::pnp::nutm80(tt.0, tt.1);
        let precession = sofars::pnp::pmat76(tt.0, tt.1);
        let precession_nutation_inverse =
            precession_nutation_inverse_matrix(&nutation, &precession);
        let phase_center_cosines = phase_center.cosines();
        let inverse_aberration_rotation = rotate_shift_matrix(
            inverse_aberration_shift(&phase_center, &frame)?,
            phase_center_cosines,
        );
        let inverse_solar_deflection_rotation = rotate_shift_matrix(
            inverse_solar_deflection_shift(&phase_center, &frame)?,
            phase_center_cosines,
        );

        Ok(Self {
            phase_center,
            observatory_longitude_sin_cos,
            polar_motion_rotation,
            diurnal_aberration_rotation: rotate_shift_matrix(
                diurnal_aberration_shift,
                app_phase_center.cosines(),
            ),
            precession_nutation_inverse,
            inverse_aberration_rotation,
            inverse_solar_deflection_rotation,
        })
    }

    /// The uvw, in metres, of the ITRF baseline vector `baseline_itrf_m`
    /// (first position minus second): its J2000 form projected on the phase
    /// centre, with w toward the source.
    pub fn baseline_uvw(&self, baseline_itrf_m: [f64; 3]) -> [f64; 3] {
        let baseline_len = vector_norm(baseline_itrf_m);
        if baseline_len == 0.0 {
            return [0.0, 0.0, 0.0];
        }

        let mut unit = scale_vector(baseline_itrf_m, 1.0 / baseline_len);
        unit = self.itrf_to_hadec(unit);
        unit = self.hadec_to_topo(unit);
        unit = rotate(&self.precession_nutation_inverse, &unit);
        unit = rotate(&self.inverse_aberration_rotation, &unit);
        unit = rotate(&self.inverse_solar_deflection_rotation, &unit);
        j2000_baseline_uvw(scale_vector(unit, baseline_len), &self.phase_center)
    }

    fn itrf_to_hadec(&self, vector: [f64; 3]) -> [f64; 3] {
        let (s, c) = self.observatory_longitude_sin_cos;
        let negated_y = -vector[1];
        [
            c * vector[0] - s * negated_y,
            s * vector[0] + c * negated_y,
            vector[2],
        ]
    }

    fn hadec_to_topo(&self, vector: [f64; 3]) -> [f64; 3] {
        let vector = rotate(
            &self.polar_motion_rotation,
            &[vector[0], -vector[1], vector[2]],
        );
        rotate(&self.diurnal_aberration_rotation, &vector)
    }
}

/// casacore's `MVuvw` projection: the uvw, in metres, of the J2000 baseline
/// vector `baseline_j2000_m` on the J2000 direction `phase_center`, with u
/// east, v north and w toward the source.
pub fn j2000_baseline_uvw(baseline_j2000_m: [f64; 3], phase_center: &MDirection) -> [f64; 3] {
    let (ra, dec) = phase_center.as_angles();
    let (sin_ra, cos_ra) = ra.sin_cos();
    let (sin_dec, cos_dec) = dec.sin_cos();
    let [x, y, z] = baseline_j2000_m;

    [
        -(sin_ra * x - cos_ra * y),
        -sin_dec * cos_ra * x - sin_dec * sin_ra * y + cos_dec * z,
        cos_dec * cos_ra * x + cos_dec * sin_ra * y + sin_dec * z,
    ]
}

fn ephemeris_error(what: &str) -> MeasureError {
    MeasureError::ModelError {
        model: "sofars epv00",
        reason: format!("{what} ephemeris lookup failed"),
    }
}

fn inverse_aberration_shift(
    phase_center: &MDirection,
    frame: &MeasFrame,
) -> Result<[f64; 3], MeasureError> {
    const C_AU_PER_DAY: f64 = 173.144_632_674_240_34;

    let tt = epoch_jd_pair(frame, EpochRef::TT)?;
    let (_, earth_bary) =
        sofars::eph::epv00(tt.0, tt.1).ok_or_else(|| ephemeris_error("aberration"))?;
    let velocity = scale_vector(earth_bary[1], 1.0 / C_AU_PER_DAY);
    let beta_inv = (1.0 - dot(velocity, velocity)).sqrt();
    let source = phase_center.cosines();
    let mut solution = subtract_vectors(source, velocity);

    loop {
        let dot_sv = dot(solution, velocity);
        let mut trial = scale_vector(
            add_vectors(
                scale_vector(solution, beta_inv),
                scale_vector(velocity, 1.0 + dot_sv / (1.0 + beta_inv)),
            ),
            1.0 / (1.0 + dot_sv),
        );
        trial = normalize(trial);
        let residual = subtract_vectors(trial, source);
        if vector_norm(residual) <= 1.0e-10 {
            break;
        }
        for idx in 0..3 {
            let component = velocity[idx];
            solution[idx] -= residual[idx]
                / (((beta_inv + component * component / (1.0 + beta_inv))
                    - component * trial[idx])
                    / (1.0 + dot_sv));
        }
    }

    Ok(subtract_vectors(solution, source))
}

fn inverse_solar_deflection_shift(
    phase_center: &MDirection,
    frame: &MeasFrame,
) -> Result<[f64; 3], MeasureError> {
    let tt = epoch_jd_pair(frame, EpochRef::TT)?;
    let (earth_helio, _) =
        sofars::eph::epv00(tt.0, tt.1).ok_or_else(|| ephemeris_error("solar-position"))?;
    let sun_vector = [-earth_helio[0][0], -earth_helio[0][1], -earth_helio[0][2]];
    let sun_distance = vector_norm(sun_vector);
    let sun = scale_vector(sun_vector, 1.0 / sun_distance);
    let source = phase_center.cosines();
    let mut dot_solution_sun = dot(source, sun);
    let strength = -1.974e-8 / sun_distance;
    let mut solution = source;

    loop {
        let correction = scale_vector(
            subtract_vectors(sun, scale_vector(solution, dot_solution_sun)),
            strength / (1.0 - dot_solution_sun),
        );
        for idx in 0..3 {
            let component = sun[idx];
            solution[idx] -= (correction[idx] + solution[idx] - source[idx])
                / (1.0
                    + (component * correction[idx]
                        - strength * (dot_solution_sun + component * solution[idx]))
                        / (1.0 - dot_solution_sun));
        }
        dot_solution_sun = dot(solution, sun);
        let residual = add_vectors(correction, subtract_vectors(solution, source));
        if vector_norm(residual) <= 1.0e-10 {
            break;
        }
    }

    Ok(subtract_vectors(solution, source))
}

/// The local apparent sidereal time, in radians and not reduced to a turn, at
/// the frame's epoch and position, as casacore's UT1-to-LAST conversion gives
/// it: GMST1 plus the equation of the equinoxes of its legacy `STANDARD`
/// nutation, plus the position's longitude.
///
/// # Errors
///
/// A frame without an epoch or a position, or an epoch it cannot convert to
/// UT1.
pub fn local_apparent_sidereal_time(frame: &MeasFrame) -> Result<f64, MeasureError> {
    let (ut1_a, ut1_b) = epoch_jd_pair(frame, EpochRef::UT1)?;
    let ut1_mjd = epoch_mjd(frame, EpochRef::UT1)?;
    let centuries = (ut1_mjd - 51_544.5) / 36_525.0;
    let gmst0_seconds =
        24_110.548_41 + 8_640_184.812_866 * centuries + 0.093_104 * centuries * centuries
            - 6.2e-6 * centuries * centuries * centuries;
    let gmst0_turns = (ut1_mjd + gmst0_seconds / 86_400.0 + 6_713.0).fract();
    let equation_of_equinoxes =
        sofars::vm::anpm(sofars::erst::gst94(ut1_a, ut1_b) - sofars::erst::gmst82(ut1_a, ut1_b));
    // Matches casacore's legacy `Nutation::STANDARD` equation of equinoxes
    // used by `MCEpoch::UT1_GAST`; SOFA `gst94` differs by about 0.1 ms.
    const CASACORE_STANDARD_NUTATION_GAST_OFFSET_RAD: f64 = 7.719e-9;
    let gast = gmst0_turns * std::f64::consts::TAU
        + equation_of_equinoxes
        + CASACORE_STANDARD_NUTATION_GAST_OFFSET_RAD;
    let position = frame
        .position()
        .ok_or(MeasureError::MissingFrameData { what: "position" })?;
    Ok(gast + position.longitude_rad())
}

fn epoch_jd_pair(frame: &MeasFrame, refer: EpochRef) -> Result<(f64, f64), MeasureError> {
    let epoch = frame
        .epoch()
        .ok_or(MeasureError::MissingFrameData { what: "epoch" })?;
    Ok(epoch.convert_to(refer, frame)?.value().as_jd_pair())
}

fn epoch_mjd(frame: &MeasFrame, refer: EpochRef) -> Result<f64, MeasureError> {
    let epoch = frame
        .epoch()
        .ok_or(MeasureError::MissingFrameData { what: "epoch" })?;
    Ok(epoch.convert_to(refer, frame)?.value().as_mjd())
}

fn polar_motion_rad(frame: &MeasFrame, epoch_mjd: f64) -> Result<(f64, f64), MeasureError> {
    const ARCSEC_TO_RAD: f64 = std::f64::consts::PI / (180.0 * 3600.0);
    match frame.polar_motion_for_mjd(epoch_mjd)? {
        Some((xp_arcsec, yp_arcsec)) => Ok((xp_arcsec * ARCSEC_TO_RAD, yp_arcsec * ARCSEC_TO_RAD)),
        None => Ok((0.0, 0.0)),
    }
}

fn polar_motion_euler(xp: f64, yp: f64, last: f64) -> [[f64; 3]; 3] {
    let mut matrix = [[0.0; 3]; 3];
    for (idx, row) in matrix.iter_mut().enumerate() {
        row[idx] = 1.0;
    }
    casacore_apply_single_euler(&mut matrix, xp, 2);
    casacore_apply_single_euler(&mut matrix, yp, 1);
    casacore_apply_single_euler(&mut matrix, last, 3);
    matrix
}

fn casacore_apply_single_euler(matrix: &mut [[f64; 3]; 3], angle: f64, axis: usize) {
    if angle == 0.0 || axis == 0 {
        return;
    }
    let mut single = [[0.0; 3]; 3];
    for (idx, row) in single.iter_mut().enumerate() {
        row[idx] = 1.0;
    }
    let i = axis % 3;
    let j = (i + 1) % 3;
    let (sin_angle, cos_angle) = angle.sin_cos();
    single[i][i] = cos_angle;
    single[j][j] = cos_angle;
    single[i][j] = -sin_angle;
    single[j][i] = sin_angle;

    let original = *matrix;
    for row in 0..3 {
        for col in 0..3 {
            matrix[row][col] = original[row][0] * single[0][col]
                + original[row][1] * single[1][col]
                + original[row][2] * single[2][col];
        }
    }
}

fn diurnal_aberration_factor(radius_m: f64) -> f64 {
    const C: f64 = 299_792_458.0;
    const SIDEREAL_RATIO: f64 = 1.002_737_909_35;
    (2.0 * std::f64::consts::PI * radius_m) / 86_400.0 * SIDEREAL_RATIO / C
}

fn rotate_shift_matrix(shift: [f64; 3], reference_direction: [f64; 3]) -> [[f64; 3]; 3] {
    let reference = normalize(reference_direction);
    let reference_long = reference[1].atan2(reference[0]);
    let reference_lat = reference[2].asin();

    let mut rot = [[0.0; 3]; 3];
    for (idx, row) in rot.iter_mut().enumerate() {
        row[idx] = 1.0;
    }
    casacore_apply_single_euler(&mut rot, -std::f64::consts::FRAC_PI_2 + reference_lat, 2);
    casacore_apply_single_euler(&mut rot, -reference_long, 3);

    let shifted_once = rotate(&rot, &shift);
    let shifted_long = shifted_once[1].atan2(shifted_once[0]);
    let mut long_rot = [[0.0; 3]; 3];
    for (idx, row) in long_rot.iter_mut().enumerate() {
        row[idx] = 1.0;
    }
    casacore_apply_single_euler(&mut long_rot, -shifted_long, 3);
    rot = multiply_matrices(&long_rot, &rot);

    let shifted_twice = rotate(&rot, &shift);
    let mut correction_rot = [[0.0; 3]; 3];
    for (idx, row) in correction_rot.iter_mut().enumerate() {
        row[idx] = 1.0;
    }
    casacore_apply_single_euler(&mut correction_rot, shifted_twice[0], 2);
    let corrected = multiply_matrices(&correction_rot, &rot);
    multiply_transpose_left(&rot, &corrected)
}

fn precession_nutation_inverse_matrix(
    nutation: &[[f64; 3]; 3],
    precession: &[[f64; 3]; 3],
) -> [[f64; 3]; 3] {
    let mut result = [[0.0; 3]; 3];
    for col in 0..3 {
        let mut basis = [0.0; 3];
        basis[col] = 1.0;
        let transformed = rotate_t(precession, &rotate_t(nutation, &basis));
        for row in 0..3 {
            result[row][col] = transformed[row];
        }
    }
    result
}

fn rotate(matrix: &[[f64; 3]; 3], vector: &[f64; 3]) -> [f64; 3] {
    [
        matrix[0][0] * vector[0] + matrix[0][1] * vector[1] + matrix[0][2] * vector[2],
        matrix[1][0] * vector[0] + matrix[1][1] * vector[1] + matrix[1][2] * vector[2],
        matrix[2][0] * vector[0] + matrix[2][1] * vector[1] + matrix[2][2] * vector[2],
    ]
}

fn rotate_t(matrix: &[[f64; 3]; 3], vector: &[f64; 3]) -> [f64; 3] {
    [
        matrix[0][0] * vector[0] + matrix[1][0] * vector[1] + matrix[2][0] * vector[2],
        matrix[0][1] * vector[0] + matrix[1][1] * vector[1] + matrix[2][1] * vector[2],
        matrix[0][2] * vector[0] + matrix[1][2] * vector[1] + matrix[2][2] * vector[2],
    ]
}

fn multiply_matrices(left: &[[f64; 3]; 3], right: &[[f64; 3]; 3]) -> [[f64; 3]; 3] {
    let mut result = [[0.0; 3]; 3];
    for row in 0..3 {
        for col in 0..3 {
            result[row][col] = left[row][0] * right[0][col]
                + left[row][1] * right[1][col]
                + left[row][2] * right[2][col];
        }
    }
    result
}

fn multiply_transpose_left(left: &[[f64; 3]; 3], right: &[[f64; 3]; 3]) -> [[f64; 3]; 3] {
    let mut result = [[0.0; 3]; 3];
    for row in 0..3 {
        for col in 0..3 {
            result[row][col] = left[0][row] * right[0][col]
                + left[1][row] * right[1][col]
                + left[2][row] * right[2][col];
        }
    }
    result
}

fn spherical_to_cartesian(lon: f64, lat: f64) -> [f64; 3] {
    let (sin_lon, cos_lon) = lon.sin_cos();
    let (sin_lat, cos_lat) = lat.sin_cos();
    [cos_lat * cos_lon, cos_lat * sin_lon, sin_lat]
}

fn vector_norm(vector: [f64; 3]) -> f64 {
    dot(vector, vector).sqrt()
}

fn normalize(vector: [f64; 3]) -> [f64; 3] {
    let norm = vector_norm(vector);
    if norm == 0.0 {
        vector
    } else {
        scale_vector(vector, 1.0 / norm)
    }
}

fn dot(left: [f64; 3], right: [f64; 3]) -> f64 {
    left[0] * right[0] + left[1] * right[1] + left[2] * right[2]
}

fn add_vectors(left: [f64; 3], right: [f64; 3]) -> [f64; 3] {
    [left[0] + right[0], left[1] + right[1], left[2] + right[2]]
}

fn subtract_vectors(left: [f64; 3], right: [f64; 3]) -> [f64; 3] {
    [left[0] - right[0], left[1] - right[1], left[2] - right[2]]
}

fn scale_vector(vector: [f64; 3], scale: f64) -> [f64; 3] {
    [vector[0] * scale, vector[1] * scale, vector[2] * scale]
}
