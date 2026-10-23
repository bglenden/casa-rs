// SPDX-License-Identifier: LGPL-3.0-or-later
//! casacore C++ laws for the celestial frame conversions and the sidereal
//! epochs (#850): galactic, supergalactic, FK4 B1950, the three ecliptics and
//! ICRS, and UT1/UTC to and from GMST1, LMST, GAST and LAST with casacore's
//! sidereal day count.
#![cfg(all(feature = "cpp-interop-tests", has_casacore_cpp))]

use casa_measures_data::MeasuresRuntime;
use casa_test_support::measures_interop::MeasuresOracle;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::{EpochRef, IauModel, MEpoch, MPosition, MeasFrame, MeasuresProvider};
use std::sync::{Arc, OnceLock};

const SECONDS_PER_DAY: f64 = 86_400.0;
const MICROARCSEC_PER_RAD: f64 = 180.0 / std::f64::consts::PI * 3600.0 * 1.0e6;

/// The bound for every direction law, a thousand times below 1 mas. The laws
/// compare casacore's direction vector, which `MCDirection` leaves
/// unnormalized after its 10-digit galactic and FK4 matrices. The latitude
/// casacore reports, `asin(z)` of that vector, carries a further
/// (|v| − 1)·tan(b), which casa-rs does not reproduce (`docs/CASA (C++)
/// bugs.md`); the laws print it.
const MAX_DIRECTION_SEPARATION_MICROARCSEC: f64 = 1.0;

/// The bound for every sidereal epoch law, day count included. A double MJD
/// near 58,000 resolves 1.1 µs, so this is a few units of that.
const MAX_EPOCH_DIFFERENCE_SECONDS: f64 = 5.0e-6;

const VLA_LON: f64 = -1.878_283_2;
const VLA_LAT: f64 = 0.595_370_3;
const VLA_H: f64 = 2124.0;

/// J2000.0, and 2026-05-21 06:00 UTC.
const EPOCHS_MJD: [f64; 2] = [51_544.5, 61_181.25];

fn measures_runtime() -> Arc<MeasuresRuntime> {
    static RUNTIME: OnceLock<Arc<MeasuresRuntime>> = OnceLock::new();
    Arc::clone(RUNTIME.get_or_init(|| {
        Arc::new(
            MeasuresRuntime::open_discovered(Default::default()).expect("test measures runtime"),
        )
    }))
}

/// Directions on a grid in longitude and latitude, the poles' neighbourhood
/// included.
fn directions() -> Vec<(f64, f64)> {
    let mut directions = Vec::new();
    for lon in [0.0, 0.7, 1.9, 3.1, 4.4, 5.8] {
        for lat in [-1.57, -1.1, -0.4, 0.0, 0.3, 0.9, 1.4, 1.5706] {
            directions.push((lon, lat));
        }
    }
    directions
}

/// The angle between two vectors of any length.
fn vector_separation(a: [f64; 3], b: [f64; 3]) -> f64 {
    let cross = [
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    ];
    let dot = a[0] * b[0] + a[1] * b[1] + a[2] * b[2];
    cross.iter().map(|c| c * c).sum::<f64>().sqrt().atan2(dot)
}

/// The direction casacore reports for its vector: `MVDirection::getLong()`
/// and `getLat()`, the latter `asin(z)` without normalizing.
fn casacore_reported(xyz: [f64; 3]) -> [f64; 3] {
    let (lon, lat) = (xyz[1].atan2(xyz[0]), xyz[2].asin());
    [lat.cos() * lon.cos(), lat.cos() * lon.sin(), lat.sin()]
}

fn frame(epoch_mjd: Option<f64>, model: IauModel) -> MeasFrame {
    let frame = MeasFrame::new()
        .with_measures(measures_runtime())
        .with_iau_model(model);
    match epoch_mjd {
        Some(mjd) => frame.with_epoch(MEpoch::from_mjd(mjd, EpochRef::UTC)),
        None => frame,
    }
}

/// The largest separations, in µas, between casa-rs and casacore converting
/// every grid direction from `from` to `to` in one frame: from casacore's
/// direction vector, and from the angles casacore reports.
fn max_direction_separation(
    from: DirectionRef,
    to: DirectionRef,
    epoch_mjd: Option<f64>,
    model: IauModel,
) -> (f64, f64) {
    let frame = frame(epoch_mjd, model);
    let oracle_epoch = epoch_mjd.unwrap_or(0.0);
    let (mut worst, mut worst_reported): (f64, f64) = (0.0, 0.0);
    for (lon, lat) in directions() {
        let rust = MDirection::from_angles(lon, lat, from)
            .convert_to(to, &frame)
            .unwrap_or_else(|error| panic!("casa-rs {from}->{to}: {error}"))
            .cosines();
        let convert = match model {
            IauModel::Iau1976_1980 => MeasuresOracle::direction_convert_cosines,
            IauModel::Iau2006_2000A => MeasuresOracle::direction_convert_cosines_iau2000a,
        };
        let cpp = convert((lon, lat), from.as_str(), to.as_str(), oracle_epoch)
            .unwrap_or_else(|error| panic!("casacore {from}->{to}: {error}"));
        worst = worst.max(vector_separation(rust, cpp) * MICROARCSEC_PER_RAD);
        let reported = vector_separation(rust, casacore_reported(cpp)) * MICROARCSEC_PER_RAD;
        worst_reported = worst_reported.max(reported);
    }
    (worst, worst_reported)
}

fn assert_routes(routes: &[(DirectionRef, DirectionRef)], epochs: &[Option<f64>], model: IauModel) {
    let mut failures = Vec::new();
    for &(a, b) in routes {
        for &epoch in epochs {
            for (from, to) in [(a, b), (b, a)] {
                let (worst, reported) = max_direction_separation(from, to, epoch, model);
                eprintln!(
                    "{model:?} {from}->{to} epoch={epoch:?}: max sep {worst:.4} uas \
                     (reported angles {reported:.4} uas)"
                );
                if worst > MAX_DIRECTION_SEPARATION_MICROARCSEC {
                    failures.push(format!("{from}->{to} epoch={epoch:?}: {worst:.4} uas"));
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{model:?} routes beyond {MAX_DIRECTION_SEPARATION_MICROARCSEC} uas:\n{}",
        failures.join("\n")
    );
}

/// Routes that need no epoch, also run without one: galactic, supergalactic,
/// ecliptic and ICRS are fixed rotations; B1950 then takes casacore's
/// default epoch.
#[test]
fn fixed_frames_match_casacore_with_and_without_an_epoch() {
    use DirectionRef::*;
    let routes = [
        (J2000, GALACTIC),
        (GALACTIC, SUPERGAL),
        (J2000, SUPERGAL),
        (J2000, ECLIPTIC),
        (GALACTIC, ECLIPTIC),
        (J2000, ICRS),
    ];
    let epochs = [None, Some(EPOCHS_MJD[0]), Some(EPOCHS_MJD[1])];
    assert_routes(&routes, &epochs, IauModel::Iau1976_1980);
}

/// FK4 B1950 through casacore's rotation, E-terms and epoch term, directly
/// from and to galactic as casacore routes it.
#[test]
fn b1950_matches_casacore_with_and_without_an_epoch() {
    use DirectionRef::*;
    let routes = [
        (J2000, B1950),
        (GALACTIC, B1950),
        (SUPERGAL, B1950),
        (B1950, ECLIPTIC),
        (B1950, ICRS),
    ];
    let epochs = [None, Some(EPOCHS_MJD[0]), Some(EPOCHS_MJD[1])];
    assert_routes(&routes, &epochs, IauModel::Iau1976_1980);
}

/// At epochs the IERS tables do not cover (1941 and 2058), casacore's dUT1
/// is 0, so its B1950 epoch term takes UT1 as UTC; casa-rs does the same
/// instead of failing for want of Earth-orientation data.
#[test]
fn b1950_without_earth_orientation_data_matches_casacore() {
    use DirectionRef::*;
    let epochs = [30_000.5, 73_000.5];
    for mjd in epochs {
        let covered = measures_runtime().eop_values(mjd).expect("read EOP");
        assert!(covered.is_none(), "the EOP table covers MJD {mjd}");
    }
    assert_routes(
        &[(J2000, B1950), (B1950, ICRS)],
        &epochs.map(Some),
        IauModel::Iau1976_1980,
    );
}

/// The ecliptics of date, under each IAU model. Under IAU 2000 the routes
/// start from the equator of date: casacore's IAU 2000 JMEAN-to-J2000 applies
/// frame bias and precession in the wrong order, 0.14 mas off its own inverse
/// in 2026, which casa-rs does not follow (`docs/CASA (C++) bugs.md`).
#[test]
fn ecliptics_of_date_match_casacore() {
    use DirectionRef::*;
    let epochs = EPOCHS_MJD.map(Some);
    let routes = [
        (J2000, MECLIPTIC),
        (J2000, TECLIPTIC),
        (JMEAN, MECLIPTIC),
        (JTRUE, TECLIPTIC),
    ];
    assert_routes(&routes, &epochs, IauModel::Iau1976_1980);
    let iau2000_routes = [(JMEAN, MECLIPTIC), (JTRUE, TECLIPTIC)];
    assert_routes(&iau2000_routes, &epochs, IauModel::Iau2006_2000A);
}

/// TOPO is APP for a sky direction, as casacore's APP-to-TOPO shifts only a
/// direction with a distance. casa-rs once put an hour angle there.
#[test]
fn topocentric_matches_casacore() {
    use DirectionRef::*;
    let epochs = EPOCHS_MJD.map(Some);
    assert_routes(
        &[(J2000, TOPO), (APP, TOPO)],
        &epochs,
        IauModel::Iau1976_1980,
    );
}

/// casacore's full value for one epoch conversion at the VLA.
fn casacore_epoch(mjd: f64, from: EpochRef, to: EpochRef, model: IauModel) -> f64 {
    match model {
        IauModel::Iau1976_1980 => MeasuresOracle::epoch_convert_with_frame(
            mjd,
            from.as_str(),
            to.as_str(),
            VLA_LON,
            VLA_LAT,
            VLA_H,
            0.0,
        ),
        IauModel::Iau2006_2000A => MeasuresOracle::epoch_convert_iau2000a(
            mjd,
            from.as_str(),
            to.as_str(),
            [VLA_LON, VLA_LAT, VLA_H],
        ),
    }
    .unwrap_or_else(|error| panic!("casacore {from}->{to}: {error}"))
}

/// One sidereal conversion through each implementation, full value.
fn epoch_difference_seconds(mjd: f64, from: EpochRef, to: EpochRef, model: IauModel) -> f64 {
    let frame = MeasFrame::new()
        .with_measures(measures_runtime())
        .with_iau_model(model)
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H));
    let rust = MEpoch::from_mjd(mjd, from)
        .convert_to(to, &frame)
        .unwrap_or_else(|error| panic!("casa-rs {from}->{to}: {error}"))
        .value()
        .as_mjd();
    let cpp = casacore_epoch(mjd, from, to, model);
    eprintln!("{model:?} {from}->{to} at {mjd}: casa-rs {rust:.9} casacore {cpp:.9}");
    (rust - cpp).abs() * SECONDS_PER_DAY
}

/// casacore counts sidereal epochs in sidereal days: UT1 51544.5 is GMST1
/// 58257.779. Both directions, both models, with the day count. Under IAU
/// 2000 only the apparent times are compared: casacore's IAU 2000 GMST1 adds
/// the TT day fraction (GMST1 58258.280 at UT1 51544.5) and its inverse does
/// not invert, which casa-rs does not follow (`docs/CASA (C++) bugs.md`).
#[test]
fn sidereal_epochs_match_casacore_with_the_day_count() {
    use EpochRef::*;
    let mean_and_apparent = [(UT1, GMST1), (UT1, GAST), (UTC, LMST), (UTC, LAST)];
    let apparent = [(UT1, GAST), (UTC, LAST)];
    let mut failures = Vec::new();
    for (model, routes) in [
        (IauModel::Iau1976_1980, &mean_and_apparent[..]),
        (IauModel::Iau2006_2000A, &apparent[..]),
    ] {
        for solar_mjd in [EPOCHS_MJD[0], EPOCHS_MJD[1], 58_849.906_25] {
            for &(from, to) in routes {
                let forward = epoch_difference_seconds(solar_mjd, from, to, model);
                let sidereal_mjd = casacore_epoch(solar_mjd, from, to, model);
                let backward = epoch_difference_seconds(sidereal_mjd, to, from, model);
                for (label, difference) in [("forward", forward), ("backward", backward)] {
                    if difference > MAX_EPOCH_DIFFERENCE_SECONDS {
                        failures.push(format!(
                            "{model:?} {from}<->{to} at {solar_mjd} {label}: {difference:.3e} s"
                        ));
                    }
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "sidereal epochs beyond {MAX_EPOCH_DIFFERENCE_SECONDS} s:\n{}",
        failures.join("\n")
    );
}
