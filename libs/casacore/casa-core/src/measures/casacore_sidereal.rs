// SPDX-License-Identifier: LGPL-3.0-or-later
//! casacore's sidereal epochs (`MCEpoch`): UT1 to and from GMST1 and GAST,
//! and GMST1/GAST to and from local sidereal time, in casacore's sidereal day
//! count.
//!
//! casacore counts a sidereal epoch from UT1 plus the IAU 1982 GMST at 0h, in
//! days, plus 6713 days: UT1 MJD 51544.5 is GMST1 58257.779. The fraction is
//! the sidereal time of day; the whole days count sidereal days (#850).
//!
//! Both IAU models use casacore's IAU 1976/1980 formulas here.
//! `MCEpoch::UT1_GAST` takes `Nutation::STANDARD` under either model, which
//! the apparent sidereal time of every HADEC, AZEL and ITRF conversion
//! inherits. casacore's IAU 2000 `UT1_GMST1` adds the TT day fraction to its
//! GMST, so its GMST1 and LMST are up to half a day off and its inverse does
//! not invert; casa-rs does not follow it (`docs/CASA (C++) bugs.md`).

use super::casacore_nutation::standard_equation_of_equinoxes_rad;
use super::epoch::MjdHighPrec;

const MJD2000: f64 = 51_544.5;
const JULIAN_CENTURY_DAYS: f64 = 36_525.0;
const SECONDS_PER_DAY: f64 = 86_400.0;

/// The sidereal day count's offset in days (`MCEpoch`'s `mve6713`).
const SIDEREAL_DAY_OFFSET: f64 = 6_713.0;

/// `MeasTable::GMST0`: the IAU 1982 GMST at 0h UT1, in seconds, as a
/// polynomial evaluated at the UT1 epoch itself.
fn gmst0_seconds(ut1_mjd: f64) -> f64 {
    let t = (ut1_mjd - MJD2000) / JULIAN_CENTURY_DAYS;
    24_110.548_41 + t * (8_640_184.812_866 + t * (0.093_104 + t * -6.2e-6))
}

/// `MeasTable::GMUT0`: the inverse polynomial, UT1 − GMST1 in units of
/// `SECinDAY / JDCEN` days, evaluated at the GMST1 epoch.
fn gmut0(gmst1_mjd: f64) -> f64 {
    let t = (gmst1_mjd - MJD2000 - SIDEREAL_DAY_OFFSET) / JULIAN_CENTURY_DAYS;
    -0.658_308_450_562_548_7
        + t * (-235.909_469_167_107_52 + t * (-0.000_002_528_225_535_979_72 + t * 1.679e-10))
}

/// Add `days` as casacore's `MVEpoch` does, its whole and fractional parts
/// separately, so a 6713-day offset costs no precision.
fn add_days(epoch: MjdHighPrec, days: f64) -> MjdHighPrec {
    let whole = days.floor();
    MjdHighPrec::new(epoch.day() + whole, epoch.frac() + (days - whole))
}

/// UT1 to GMST1 (`MCEpoch::UT1_GMST1`, IAU 1976/1980).
pub(super) fn ut1_to_gmst1(ut1: MjdHighPrec) -> MjdHighPrec {
    let gmst = add_days(ut1, gmst0_seconds(ut1.as_mjd()) / SECONDS_PER_DAY);
    add_days(gmst, SIDEREAL_DAY_OFFSET)
}

/// GMST1 to UT1 (`MCEpoch::GMST1_UT1`, IAU 1976/1980): casacore's inverse
/// polynomial, which leaves up to about 1 µs of the round trip.
pub(super) fn gmst1_to_ut1(gmst1: MjdHighPrec) -> MjdHighPrec {
    let ut1 = add_days(
        gmst1,
        gmut0(gmst1.as_mjd()) * JULIAN_CENTURY_DAYS / SECONDS_PER_DAY,
    );
    add_days(ut1, -SIDEREAL_DAY_OFFSET)
}

/// UT1 to GAST (`MCEpoch::UT1_GAST`): GMST1 plus casacore's IAU 1980
/// equation of the equinoxes at the UT1 epoch.
pub(super) fn ut1_to_gast(ut1: MjdHighPrec) -> MjdHighPrec {
    let equation_of_equinoxes = standard_equation_of_equinoxes_rad(ut1.as_mjd());
    add_days(
        ut1_to_gmst1(ut1),
        equation_of_equinoxes / std::f64::consts::TAU,
    )
}

/// GAST to UT1 (`MCEpoch::GAST_UT1`): remove the equation of the equinoxes
/// at a first UT1 guess, then invert GMST1.
pub(super) fn gast_to_ut1(gast: MjdHighPrec) -> MjdHighPrec {
    let gast_mjd = gast.as_mjd();
    let ut1_guess =
        gast_mjd + gmut0(gast_mjd) * JULIAN_CENTURY_DAYS / SECONDS_PER_DAY - SIDEREAL_DAY_OFFSET;
    let equation_of_equinoxes = standard_equation_of_equinoxes_rad(ut1_guess);
    gmst1_to_ut1(add_days(
        gast,
        -equation_of_equinoxes / std::f64::consts::TAU,
    ))
}

/// Greenwich to local sidereal time (`MCEpoch::GMST1_LMST`,
/// `MCEpoch::GAST_LAST`), or back for a negated longitude.
pub(super) fn add_longitude(sidereal: MjdHighPrec, longitude_rad: f64) -> MjdHighPrec {
    add_days(sidereal, longitude_rad / std::f64::consts::TAU)
}

/// The Greenwich apparent sidereal time of a UT1 epoch as an angle in
/// [0, 2π), as `MCFrame::getLASTr` takes it before adding the longitude.
pub(super) fn gast_rad(ut1: MjdHighPrec) -> f64 {
    ut1_to_gast(ut1).frac() * std::f64::consts::TAU
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn j2000_is_casacore_s_sidereal_day_58257() {
        let gmst1 = ut1_to_gmst1(MjdHighPrec::from_mjd(MJD2000));
        assert_eq!(gmst1.day(), 58_257.0);
        // 18h 41m 50.54841s, the IAU 1982 GMST at J2000.0.
        let expected = 0.5 + 24_110.548_41 / SECONDS_PER_DAY;
        assert!((gmst1.frac() - expected).abs() < 1e-15, "{}", gmst1.frac());
    }

    #[test]
    fn inverses_round_trip_to_casacore_s_microsecond() {
        for mjd in [51_544.5, 58_849.906_25, 61_181.25] {
            let ut1 = MjdHighPrec::from_mjd(mjd);
            for back in [
                gmst1_to_ut1(ut1_to_gmst1(ut1)),
                gast_to_ut1(ut1_to_gast(ut1)),
            ] {
                let error_s = (back - ut1).abs() * SECONDS_PER_DAY;
                assert!(error_s < 2e-6, "MJD {mjd}: {error_s} s");
            }
        }
    }

    #[test]
    fn longitude_moves_only_the_sidereal_time() {
        let gast = ut1_to_gast(MjdHighPrec::from_mjd(MJD2000));
        let last = add_longitude(gast, -1.878_283_2);
        let back = add_longitude(last, 1.878_283_2);
        assert!((back - gast).abs() < 1e-15);
    }
}
