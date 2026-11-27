// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Apply rotation matrix to direction vector: result = R * v.
pub(super) fn rotate(r: &[[f64; 3]; 3], v: &[f64; 3]) -> [f64; 3] {
    let mut result = [0.0; 3];
    sofars::vm::rxp(r, v, &mut result);
    result
}

/// Apply transpose of rotation matrix: result = R^T * v.
pub(super) fn rotate_t(r: &[[f64; 3]; 3], v: &[f64; 3]) -> [f64; 3] {
    let mut result = [0.0; 3];
    sofars::vm::trxp(r, v, &mut result);
    result
}

// ---------------------------------------------------------------------------
// Constant rotation matrices
// ---------------------------------------------------------------------------

/// ICRS-to-J2000 frame tie rotation matrix (IAU 2000).
///
/// The frame tie between ICRS and the dynamical mean equator/equinox at
/// J2000.0 is a tiny rotation (~17 mas). We extract the frame bias matrix
/// `rb` from `sofars::pnp::bp00` evaluated at J2000.0, as casacore's
/// `MeasTable::ICRSToJ2000` uses `frameBias00`. The IAU 2006 matrix differs
/// by about 0.3 microarcseconds, observable in simulated FIELD directions.
///
/// v_J2000 = ICRS_TO_J2000 * v_ICRS
pub(super) fn icrs_to_j2000_matrix() -> [[f64; 3]; 3] {
    // bp00 returns (rb, rp, rbp) where rb is the frame bias.
    let (rb, _, _) = sofars::pnp::bp00(MJD_OFFSET, 51544.5);
    rb
}

// ---------------------------------------------------------------------------
// Epoch-dependent rotation matrices
// ---------------------------------------------------------------------------

/// Get TT JD pair from the frame epoch, converting if needed.
pub(super) fn get_tt_jd(frame: &MeasFrame, ctx: &ConvCtx) -> Result<(f64, f64), MeasureError> {
    ctx.get_tt_jd(frame)
}

/// Compute TT JD pair (uncached, used by ConvCtx).
pub(super) fn compute_tt_jd(frame: &MeasFrame) -> Result<(f64, f64), MeasureError> {
    let epoch = frame.epoch().ok_or(MeasureError::MissingFrameData {
        what: "epoch (for precession/nutation)",
    })?;

    if epoch.refer() == EpochRef::TT {
        return Ok(epoch.value().as_jd_pair());
    }

    let tt = epoch.convert_to(EpochRef::TT, frame)?;
    Ok(tt.value().as_jd_pair())
}

/// Precession matrix (including frame bias): GCRS/J2000 → JMEAN (mean equator of date).
///
/// Dispatches on the IAU model:
/// - [`Iau1976_1980`](IauModel::Iau1976_1980): `sofars::pnp::pmat76` (Lieske 1979)
/// - [`Iau2006_2000A`](IauModel::Iau2006_2000A): `sofars::pnp::pmat00` — IAU 2000
///   bias-precession matrix (GCRS → mean of date), matching C++ casacore's
///   `frameBias00 * Precession(IAU2000)`.
pub(super) fn precession_matrix(
    frame: &MeasFrame,
    ctx: &ConvCtx,
) -> Result<[[f64; 3]; 3], MeasureError> {
    ctx.get_precession(frame)
}

/// Nutation matrix: JMEAN → JTRUE (true equator of date).
///
/// Dispatches on the IAU model:
/// - [`Iau1976_1980`](IauModel::Iau1976_1980): `sofars::pnp::nutm80` (Wahr 1981)
/// - [`Iau2006_2000A`](IauModel::Iau2006_2000A): `sofars::pnp::num00a` (Mathews 2002)
pub(super) fn nutation_matrix(
    frame: &MeasFrame,
    ctx: &ConvCtx,
) -> Result<[[f64; 3]; 3], MeasureError> {
    ctx.get_nutation(frame)
}

/// The equatorial-to-ecliptic rotation about x by obliquity `eps`; its
/// transpose converts back.
pub(super) fn obliquity_rotation(eps: f64) -> [[f64; 3]; 3] {
    let mut r = [[0.0; 3]; 3];
    sofars::vm::ir(&mut r);
    sofars::vm::rx(eps, &mut r);
    r
}

/// The J2000-to-ECLIPTIC rotation (`MeasMath::deapplyECLIPtoJ2000`).
pub(super) fn j2000_ecliptic_rotation() -> [[f64; 3]; 3] {
    obliquity_rotation(casacore_frames::j2000_obliquity_rad())
}

/// Rotate equatorial to ecliptic coordinates by `eps`, or back.
pub(super) fn rotate_obliquity(eps: f64, cosines: &[f64; 3], to_ecliptic: bool) -> [f64; 3] {
    let m = obliquity_rotation(eps);
    if to_ecliptic {
        rotate(&m, cosines)
    } else {
        rotate_t(&m, cosines)
    }
}

/// The B1950 conversions' epoch term: the frame's UT1 epoch in Julian
/// centuries from J2000, or 0 without an epoch, as casacore's
/// `MeasMath::applyJ2000toB1950` takes it.
///
/// Without Earth-orientation data for the epoch, casacore's
/// `MeasTable::dUT1` returns 0, so UT1 is taken as UTC, with casacore's note
/// (`casacore_frames::note_missing_dut1`). The term moves a direction by
/// about 1e-15 rad per second of time. Every other UT1 conversion in casa-rs
/// still requires the data.
pub(super) fn b1950_epoch_centuries(frame: &MeasFrame, ctx: &ConvCtx) -> Result<f64, MeasureError> {
    let Some(epoch) = frame.epoch() else {
        return Ok(casacore_frames::b1950_epoch_centuries(None));
    };
    let ut1_mjd = match get_ut1_jd(frame, ctx) {
        Ok((ut_a, ut_b)) => (ut_a - MJD_OFFSET) + ut_b,
        Err(MeasureError::MissingFrameData { what }) if what == MISSING_DUT1 => {
            let ut1_mjd = epoch
                .convert_to(EpochRef::UT1, &frame.clone().with_dut1(0.0))?
                .value()
                .as_mjd();
            casacore_frames::note_missing_dut1(ut1_mjd);
            ut1_mjd
        }
        Err(error) => return Err(error),
    };
    Ok(casacore_frames::b1950_epoch_centuries(Some(ut1_mjd)))
}

/// Get the Earth's barycentric velocity in AU/day for aberration.
pub(super) fn earth_velocity_au_per_day(
    frame: &MeasFrame,
    ctx: &ConvCtx,
) -> Result<([f64; 3], f64), MeasureError> {
    ctx.get_earth_vel(frame)
}

/// Get GAST (Greenwich Apparent Sidereal Time) from the frame.
///
/// Under either IAU model this is casacore's `MCEpoch::UT1_GAST`, which
/// `MCFrame::getLASTr` uses for every HADEC conversion: the IAU 1982 GMST
/// plus the IAU 1980 equation of the equinoxes of `Nutation::STANDARD`,
/// without the 1994 complementary terms.
pub(super) fn get_gast(frame: &MeasFrame, ctx: &ConvCtx) -> Result<f64, MeasureError> {
    ctx.get_gast(frame)
}

/// Compute GAST (uncached, used by ConvCtx).
pub(super) fn compute_gast(frame: &MeasFrame, ctx: &ConvCtx) -> Result<f64, MeasureError> {
    let (ut_a, ut_b) = ctx.get_ut1_jd(frame)?;
    Ok(casacore_gast_rad(MjdHighPrec::from_jd_pair(ut_a, ut_b)))
}

/// Get the observer geodetic latitude from the frame position.
pub(super) fn get_latitude(frame: &MeasFrame) -> Result<f64, MeasureError> {
    let pos = frame.position().ok_or(MeasureError::MissingFrameData {
        what: "position (for observer latitude)",
    })?;
    // MCFrame uses zero geodetic latitude at the geocentre. SOFA's
    // degenerate Cartesian-to-geodetic convention chooses the pole there.
    Ok(if pos.as_itrf() == [0.; 3] {
        0.
    } else {
        pos.latitude_rad()
    })
}

/// Get the observer geocentric latitude from the frame position.
///
/// C++ casacore uses geocentric latitude (`asin(z/r)`) for AZEL conversions,
/// and geodetic latitude for AZELGEO. The difference is ~10 arcminutes.
pub(super) fn get_geocentric_latitude(frame: &MeasFrame) -> Result<f64, MeasureError> {
    let pos = frame.position().ok_or(MeasureError::MissingFrameData {
        what: "position (for observer geocentric latitude)",
    })?;
    Ok(pos.geocentric_latitude_rad())
}

/// Get UT1 JD pair from the frame epoch, converting if needed.
pub(super) fn get_ut1_jd(frame: &MeasFrame, ctx: &ConvCtx) -> Result<(f64, f64), MeasureError> {
    ctx.get_ut1_jd(frame)
}

/// Compute UT1 JD pair (uncached, used by ConvCtx).
pub(super) fn compute_ut1_jd(frame: &MeasFrame) -> Result<(f64, f64), MeasureError> {
    let epoch = frame.epoch().ok_or(MeasureError::MissingFrameData {
        what: "epoch (for Earth rotation)",
    })?;

    if epoch.refer() == EpochRef::UT1 {
        return Ok(epoch.value().as_jd_pair());
    }

    let ut1 = epoch.convert_to(EpochRef::UT1, frame)?;
    Ok(ut1.value().as_jd_pair())
}

/// Get polar motion (xp, yp) in radians from the frame's EOP table.
///
/// Returns a typed missing-data error when the explicit provider has no EOP
/// sample for the requested epoch; scientific conversions do not silently
/// substitute zero polar motion.
pub(super) fn get_polar_motion_rad(
    frame: &MeasFrame,
    epoch_mjd: f64,
) -> Result<(f64, f64), MeasureError> {
    const ARCSEC_TO_RAD: f64 = std::f64::consts::PI / (180.0 * 3600.0);
    match frame.polar_motion_for_mjd(epoch_mjd)? {
        Some((xp_arcsec, yp_arcsec)) => Ok((xp_arcsec * ARCSEC_TO_RAD, yp_arcsec * ARCSEC_TO_RAD)),
        None => Err(MeasureError::MissingFrameData {
            what: "polar-motion measures data for epoch",
        }),
    }
}

/// Get the geocentric radius in metres from the frame position.
pub(super) fn get_geocentric_radius(frame: &MeasFrame) -> Result<f64, MeasureError> {
    let pos = frame.position().ok_or(MeasureError::MissingFrameData {
        what: "position (for geocentric radius)",
    })?;
    let xyz = pos.as_itrf();
    Ok((xyz[0] * xyz[0] + xyz[1] * xyz[1] + xyz[2] * xyz[2]).sqrt())
}

/// Compute the diurnal aberration factor v/c for an observer.
///
/// Returns the ratio of the observer's rotational velocity to the speed of light.
/// C++ casacore computes: `(2π × radius) / SECinDAY × UTtoST(T) / c`.
/// The UTtoST ratio is approximately 1.00273790935 (sidereal/solar time ratio).
pub(super) fn diurnal_aberration_factor(radius_m: f64) -> f64 {
    const C: f64 = 299_792_458.0; // speed of light in m/s
    const SIDEREAL_RATIO: f64 = 1.002_737_909_35;
    (2.0 * std::f64::consts::PI * radius_m) / 86400.0 * SIDEREAL_RATIO / C
}

/// Compute the polar motion Euler matrix R = Ry(xp) × Rx(yp) × Rz(angle).
///
/// Returns the full 3×3 rotation matrix. This matches C++ casacore's
/// `MeasTable::polarMotion(tdb)` with the third Euler angle set to LAST.
pub(super) fn polar_motion_euler(xp: f64, yp: f64, last: f64) -> [[f64; 3]; 3] {
    // Ry(xp) × Rx(yp) × Rz(last)
    let (sx, cx) = xp.sin_cos();
    let (sy, cy) = yp.sin_cos();
    let (sl, cl) = last.sin_cos();

    // Row 0 of Ry(xp):  [cx, 0, sx]
    // Row 1 of Ry(xp):  [0, 1, 0]
    // Row 2 of Ry(xp):  [-sx, 0, cx]
    //
    // Rx(yp): [1, 0, 0; 0, cy, -sy; 0, sy, cy]
    // Rz(last): [cl, -sl, 0; sl, cl, 0; 0, 0, 1]
    //
    // Compute Rx(yp) × Rz(last) first, then Ry(xp) × result.
    let rxz = [
        [cl, -sl, 0.0],
        [cy * sl, cy * cl, -sy],
        [sy * sl, sy * cl, cy],
    ];
    // Ry(xp) × rxz
    [
        [
            cx * rxz[0][0] + sx * rxz[2][0],
            cx * rxz[0][1] + sx * rxz[2][1],
            cx * rxz[0][2] + sx * rxz[2][2],
        ],
        rxz[1],
        [
            -sx * rxz[0][0] + cx * rxz[2][0],
            -sx * rxz[0][1] + cx * rxz[2][1],
            -sx * rxz[0][2] + cx * rxz[2][2],
        ],
    ]
}

/// Compute the bias-precession-nutation matrix, dispatching on IAU model.
///
/// - IAU 1976/1980: `pnm80` (Lieske precession + Wahr nutation, no frame bias)
/// - IAU 2006/2000A: `pnm00a` (IAU 2000 precession + IAU 2000A nutation + frame bias),
///   matching C++ casacore's combined `frameBias00 * Precession(IAU2000) * Nutation(IAU2000A)`.
pub(super) fn bias_precession_nutation(frame: &MeasFrame, tt1: f64, tt2: f64) -> [[f64; 3]; 3] {
    match frame.iau_model() {
        IauModel::Iau1976_1980 => sofars::pnp::pnm80(tt1, tt2),
        IauModel::Iau2006_2000A => sofars::pnp::pnm00a(tt1, tt2),
    }
}

/// Return the frame epoch as a TDB MJD, matching the time argument used by
/// casacore's `Aberration` and `SolarPos` helpers.
pub(super) fn tdb_mjd(frame: &MeasFrame) -> Result<f64, MeasureError> {
    let epoch = frame.epoch().ok_or(MeasureError::MissingFrameData {
        what: "epoch (for apparent-place conversion)",
    })?;
    Ok(epoch.convert_to(EpochRef::TDB, frame)?.value().as_mjd())
}

/// Casacore `Aberration::STANDARD` Earth barycentric velocity as β = v/c in
/// J2000 Cartesian coordinates.
pub(crate) fn casacore_aberration_beta(frame: &MeasFrame) -> Result<[f64; 3], MeasureError> {
    let velocity_ms =
        frame.cached_earth_velocity_ms(|| Ok(earth_barycentric_velocity_ms(tdb_mjd(frame)?)))?;
    Ok(velocity_ms.map(|component| component / C_M_PER_S))
}

/// Apply the relativistic stellar-aberration transform used by casacore's
/// `MeasMath::applyAberration`. Passing `-beta` applies the exact inverse
/// Lorentz transform.
pub(super) fn apply_relativistic_aberration(direction: [f64; 3], beta: [f64; 3]) -> [f64; 3] {
    let beta_squared = beta
        .iter()
        .map(|component| component * component)
        .sum::<f64>();
    let gamma_inverse = (1.0 - beta_squared).sqrt();
    let dot = direction
        .iter()
        .zip(beta.iter())
        .map(|(direction, beta)| direction * beta)
        .sum::<f64>();
    let beta_factor = 1.0 + dot / (1.0 + gamma_inverse);
    let denominator = 1.0 + dot;
    let shifted = std::array::from_fn(|axis| {
        (gamma_inverse * direction[axis] + beta_factor * beta[axis]) / denominator
    });
    sofars::vm::pn(&shifted).1
}

/// Geocentric direction and distance of the Sun in the J2000-compatible SOFA
/// ephemeris axes. Casacore's deflection formula depends only weakly on the
/// sub-milliarcsecond ephemeris-series difference.
pub(super) fn geocentric_sun_position(frame: &MeasFrame) -> Result<SolarPosition, MeasureError> {
    let tdb_mjd = tdb_mjd(frame)?;
    SOLAR_POSITION_CACHE.with(|cache| {
        cache.get_or_compute(tdb_mjd, || {
            let (earth_heliocentric, _) = sofars::eph::epv00(MJD_OFFSET, tdb_mjd)
                .ok_or(MeasureError::SofarsError { code: -1 })?;
            let sun = [
                -earth_heliocentric[0][0],
                -earth_heliocentric[0][1],
                -earth_heliocentric[0][2],
            ];
            let (distance_au, unit) = sofars::vm::pn(&sun);
            Ok((unit, distance_au))
        })
    })
}

// Reuse the exact ephemeris input across antenna frames at the same epoch.
// One entry per thread bounds residency independently of MS size and avoids
// sharing mutable state between the admitted geometry workers.
pub(super) type SolarPosition = ([f64; 3], f64);

pub(super) struct SolarPositionCache {
    pub(super) latest: Cell<Option<(u64, SolarPosition)>>,
}

impl SolarPositionCache {
    pub(super) const fn new() -> Self {
        Self {
            latest: Cell::new(None),
        }
    }

    pub(super) fn get_or_compute(
        &self,
        tdb_mjd: f64,
        compute: impl FnOnce() -> Result<SolarPosition, MeasureError>,
    ) -> Result<SolarPosition, MeasureError> {
        let key = tdb_mjd.to_bits();
        if let Some((previous, position)) = self.latest.get()
            && previous == key
        {
            return Ok(position);
        }
        let position = compute()?;
        self.latest.set(Some((key, position)));
        Ok(position)
    }
}

thread_local! {
    pub(super) static SOLAR_POSITION_CACHE: SolarPositionCache = const { SolarPositionCache::new() };
}

/// Casacore `MeasMath::applySolarPos`: convert a J2000 mean direction to the
/// corresponding natural direction before annual aberration.
pub(super) fn apply_solar_deflection(
    direction: [f64; 3],
    frame: &MeasFrame,
) -> Result<[f64; 3], MeasureError> {
    const SOLAR_SEMIDIAMETER_RAD_AT_ONE_AU: f64 = 0.004_652_472_638;
    const TWO_GM_SUN_OVER_C2_AU: f64 = 1.974e-8;

    let (sun_direction, sun_distance_au) = geocentric_sun_position(frame)?;
    let dot = direction
        .iter()
        .zip(sun_direction.iter())
        .map(|(direction, sun)| direction * sun)
        .sum::<f64>();
    let limb_tolerance = 1.0 - (SOLAR_SEMIDIAMETER_RAD_AT_ONE_AU / sun_distance_au).cos();
    if (dot - 1.0).abs() <= limb_tolerance {
        return Ok(direction);
    }

    let scale = (-TWO_GM_SUN_OVER_C2_AU / sun_distance_au) / (1.0 - dot);
    let shifted = std::array::from_fn(|axis| {
        direction[axis] + (sun_direction[axis] - dot * direction[axis]) * scale
    });
    Ok(sofars::vm::pn(&shifted).1)
}

/// Casacore `MeasMath::deapplySolarPos`: invert the very small solar
/// deflection. Fixed-point correction converges quadratically here because
/// the forward displacement is O(1e-8) away from the solar limb.
pub(super) fn remove_solar_deflection(
    natural_direction: [f64; 3],
    frame: &MeasFrame,
) -> Result<[f64; 3], MeasureError> {
    let mut mean_direction = natural_direction;
    for _ in 0..5 {
        let predicted_natural = apply_solar_deflection(mean_direction, frame)?;
        let corrected = std::array::from_fn(|axis| {
            mean_direction[axis] + natural_direction[axis] - predicted_natural[axis]
        });
        mean_direction = sofars::vm::pn(&corrected).1;
    }
    Ok(mean_direction)
}

/// Get the observer longitude from the frame position.
pub(super) fn get_longitude(frame: &MeasFrame) -> Result<f64, MeasureError> {
    let pos = frame.position().ok_or(MeasureError::MissingFrameData {
        what: "position (for observer longitude)",
    })?;
    Ok(pos.longitude_rad())
}
