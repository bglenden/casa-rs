// SPDX-License-Identifier: LGPL-3.0-or-later
//! Integration tests for the measures module.

use casacore_types::measures::direction::{DirectionRef, MDirection};
use casacore_types::measures::doppler::{DopplerRef, MDoppler};
use casacore_types::measures::frequency::{FrequencyRef, MFrequency};
use casacore_types::measures::{
    EpochRef, MEpoch, MPosition, MeasFrame, MeasureError, MjdHighPrec, PositionRef,
    direction_from_record, direction_to_record, doppler_from_record, doppler_to_record,
    epoch_from_record, epoch_to_record, frequency_from_record, frequency_to_record,
    position_from_record, position_to_record,
};

const SECONDS_PER_DAY: f64 = 86_400.0;

// J2000.0 epoch: 2000 January 1.5 TT = MJD 51544.5
const J2000_MJD: f64 = 51544.5;

fn close(a: f64, b: f64, tol: f64) -> bool {
    (a - b).abs() < tol
}

// ==========================================================================
// MjdHighPrec
// ==========================================================================

#[test]
fn mjd_normalization_negative_frac() {
    let m = MjdHighPrec::new(51545.0, -0.3);
    assert!(close(m.day(), 51544.0, 1e-15));
    assert!(close(m.frac(), 0.7, 1e-15));
}

#[test]
fn mjd_precision_better_than_single_f64() {
    // Two dates 0.1 microsecond apart — hard to distinguish in a single f64
    // for large MJDs, but the (day, frac) representation keeps them separate.
    let a = MjdHighPrec::new(99999.0, 0.5);
    let b = MjdHighPrec::new(99999.0, 0.5 + 1e-12); // ~0.086 μs
    let diff_days = b - a;
    assert!(close(diff_days, 1e-12, 1e-15), "diff = {diff_days}");
}

// ==========================================================================
// EpochRef parsing
// ==========================================================================

#[test]
fn epoch_ref_all_12_roundtrip() {
    for r in EpochRef::ALL {
        let parsed: EpochRef = r.as_str().parse().unwrap();
        assert_eq!(parsed, r);
    }
}

#[test]
fn epoch_ref_synonyms() {
    assert_eq!("IAT".parse::<EpochRef>().unwrap(), EpochRef::TAI);
    assert_eq!("TDT".parse::<EpochRef>().unwrap(), EpochRef::TT);
    assert_eq!("ET".parse::<EpochRef>().unwrap(), EpochRef::TT);
    assert_eq!("UT".parse::<EpochRef>().unwrap(), EpochRef::UT1);
    assert_eq!("GMST".parse::<EpochRef>().unwrap(), EpochRef::GMST1);
}

#[test]
fn epoch_ref_case_insensitive() {
    assert_eq!("utc".parse::<EpochRef>().unwrap(), EpochRef::UTC);
    assert_eq!("Tai".parse::<EpochRef>().unwrap(), EpochRef::TAI);
}

// ==========================================================================
// Epoch conversions
// ==========================================================================

#[test]
fn utc_to_tai_j2000() {
    // At J2000.0, TAI−UTC = 32 seconds (leap seconds accumulated by 1999-01-01).
    let utc = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = MeasFrame::new();
    let tai = utc.convert_to(EpochRef::TAI, &frame).unwrap();

    let diff_s = (tai.value().as_mjd() - utc.value().as_mjd()) * SECONDS_PER_DAY;
    assert!(
        close(diff_s, 32.0, 0.01),
        "TAI−UTC = {diff_s}s, expected 32s"
    );
}

#[test]
fn tai_to_tt() {
    // TT = TAI + 32.184s
    let tai = MEpoch::from_mjd(J2000_MJD, EpochRef::TAI);
    let frame = MeasFrame::new();
    let tt = tai.convert_to(EpochRef::TT, &frame).unwrap();

    let diff_s = (tt.value().as_mjd() - tai.value().as_mjd()) * SECONDS_PER_DAY;
    assert!(
        close(diff_s, 32.184, 0.001),
        "TT−TAI = {diff_s}s, expected 32.184s"
    );
}

#[test]
fn utc_to_tt_chained() {
    // UTC → TAI → TT: total offset = 32 + 32.184 = 64.184s
    let utc = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = MeasFrame::new();
    let tt = utc.convert_to(EpochRef::TT, &frame).unwrap();

    let diff_s = (tt.value().as_mjd() - utc.value().as_mjd()) * SECONDS_PER_DAY;
    assert!(
        close(diff_s, 64.184, 0.01),
        "TT−UTC = {diff_s}s, expected 64.184s"
    );
}

#[test]
fn tt_to_tdb_small_periodic() {
    // TDB−TT is a small periodic term, maximum ~1.7ms.
    let tt = MEpoch::from_mjd(J2000_MJD, EpochRef::TT);
    let frame = MeasFrame::new();
    let tdb = tt.convert_to(EpochRef::TDB, &frame).unwrap();

    let diff_s = (tdb.value().as_mjd() - tt.value().as_mjd()) * SECONDS_PER_DAY;
    assert!(
        diff_s.abs() < 0.002,
        "TDB−TT = {diff_s}s, expected |diff| < 2ms"
    );
}

#[test]
fn tt_to_tcg() {
    // TCG runs faster than TT by the rate LG = 6.969290134e-10.
    // Over time since J2000, TCG−TT accumulates.
    let tt = MEpoch::from_mjd(J2000_MJD, EpochRef::TT);
    let frame = MeasFrame::new();
    let tcg = tt.convert_to(EpochRef::TCG, &frame).unwrap();

    // At J2000, the difference should be very small (sub-second).
    let diff_s = (tcg.value().as_mjd() - tt.value().as_mjd()) * SECONDS_PER_DAY;
    assert!(
        diff_s.abs() < 1.0,
        "TCG−TT at J2000 = {diff_s}s, expected < 1s"
    );
}

#[test]
fn tdb_to_tcb() {
    let tdb = MEpoch::from_mjd(J2000_MJD, EpochRef::TDB);
    let frame = MeasFrame::new();
    let tcb = tdb.convert_to(EpochRef::TCB, &frame).unwrap();

    // TCB runs faster than TDB. At J2000, there's an accumulated difference.
    let diff_s = (tcb.value().as_mjd() - tdb.value().as_mjd()) * SECONDS_PER_DAY;
    // The difference depends on epoch — just check it's reasonable.
    assert!(diff_s.abs() < 100.0, "TCB−TDB at J2000 = {diff_s}s");
}

#[test]
fn roundtrip_utc_tai_tt_tdb_and_back() {
    let utc = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = MeasFrame::new();

    let tai = utc.convert_to(EpochRef::TAI, &frame).unwrap();
    let tt = tai.convert_to(EpochRef::TT, &frame).unwrap();
    let tdb = tt.convert_to(EpochRef::TDB, &frame).unwrap();
    let tt2 = tdb.convert_to(EpochRef::TT, &frame).unwrap();
    let tai2 = tt2.convert_to(EpochRef::TAI, &frame).unwrap();
    let utc2 = tai2.convert_to(EpochRef::UTC, &frame).unwrap();

    let diff_s = (utc2.value().as_mjd() - utc.value().as_mjd()) * SECONDS_PER_DAY;
    assert!(
        diff_s.abs() < 1e-6,
        "Roundtrip error = {diff_s}s, expected < 1μs"
    );
}

#[test]
fn ut1_to_utc_with_dut1() {
    let ut1 = MEpoch::from_mjd(J2000_MJD, EpochRef::UT1);
    let dut1 = 0.3; // UT1−UTC = +0.3s (example value)
    let frame = MeasFrame::new().with_dut1(dut1);

    let utc = ut1.convert_to(EpochRef::UTC, &frame).unwrap();
    let diff_s = (ut1.value().as_mjd() - utc.value().as_mjd()) * SECONDS_PER_DAY;
    assert!(
        close(diff_s, dut1, 1e-3),
        "UT1−UTC = {diff_s}s, expected {dut1}s"
    );
}

#[test]
fn utc_to_ut1_inverse() {
    let utc = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let dut1 = -0.15;
    let frame = MeasFrame::new().with_dut1(dut1);

    let ut1 = utc.convert_to(EpochRef::UT1, &frame).unwrap();
    let utc2 = ut1.convert_to(EpochRef::UTC, &frame).unwrap();

    let diff_s = (utc2.value().as_mjd() - utc.value().as_mjd()) * SECONDS_PER_DAY;
    assert!(diff_s.abs() < 1e-12, "UTC roundtrip error = {diff_s}s");
}

#[test]
fn ut1_without_dut1_is_error() {
    let ut1 = MEpoch::from_mjd(J2000_MJD, EpochRef::UT1);
    let frame = MeasFrame::new(); // no dUT1 set
    let result = ut1.convert_to(EpochRef::UTC, &frame);
    assert!(matches!(result, Err(MeasureError::MissingFrameData { .. })));
}

#[test]
fn ut1_to_gmst1() {
    // UT1 → GMST1 requires dUT1 (for the UT1→UTC→TAI→TT chain inside gmst06).
    let vla = MPosition::new_itrf(-1601185.4, -5041977.5, 3554875.9);
    let ut1 = MEpoch::from_mjd(J2000_MJD, EpochRef::UT1);
    let frame = MeasFrame::new().with_dut1(0.3).with_position(vla);

    let gmst = ut1.convert_to(EpochRef::GMST1, &frame).unwrap();
    // GMST should be a fractional-day value — check it's in [0, 1)
    assert!(
        gmst.value().frac() >= 0.0 && gmst.value().frac() < 1.0,
        "GMST frac = {}",
        gmst.value().frac()
    );
}

#[test]
fn gmst1_to_lmst_with_position() {
    let vla = MPosition::new_itrf(-1601185.4, -5041977.5, 3554875.9);
    let ut1 = MEpoch::from_mjd(J2000_MJD, EpochRef::UT1);
    let frame = MeasFrame::new().with_dut1(0.3).with_position(vla.clone());

    let gmst = ut1.convert_to(EpochRef::GMST1, &frame).unwrap();
    let lmst = gmst.convert_to(EpochRef::LMST, &frame).unwrap();

    // LMST = GMST + longitude/(2π)
    let lon_turns = vla.longitude_rad() / (2.0 * std::f64::consts::PI);
    let expected_frac = gmst.value().frac() + lon_turns;
    // Normalize to [0, 1)
    let expected_frac = expected_frac - expected_frac.floor();
    let actual_frac = lmst.value().frac();
    let actual_frac = actual_frac - actual_frac.floor();

    assert!(
        close(actual_frac, expected_frac, 1e-10),
        "LMST frac = {actual_frac}, expected {expected_frac}"
    );
}

#[test]
fn ut1_to_gast() {
    let vla = MPosition::new_itrf(-1601185.4, -5041977.5, 3554875.9);
    let ut1 = MEpoch::from_mjd(J2000_MJD, EpochRef::UT1);
    let frame = MeasFrame::new().with_dut1(0.3).with_position(vla);

    let gast = ut1.convert_to(EpochRef::GAST, &frame).unwrap();
    // GAST should be a fractional-day value — check it's in [0, 1)
    assert!(
        gast.value().frac() >= 0.0 && gast.value().frac() < 1.0,
        "GAST frac = {}",
        gast.value().frac()
    );
    // GAST should differ from GMST by the equation of equinoxes (~seconds)
    let gmst = ut1.convert_to(EpochRef::GMST1, &frame).unwrap();
    let diff_s = (gast.value().frac() - gmst.value().frac()).abs() * SECONDS_PER_DAY;
    assert!(
        diff_s < 2.0,
        "GAST-GMST = {diff_s}s, expected < 2s (equation of equinoxes)"
    );
}

#[test]
fn gast_to_last_with_position() {
    let vla = MPosition::new_itrf(-1601185.4, -5041977.5, 3554875.9);
    let ut1 = MEpoch::from_mjd(J2000_MJD, EpochRef::UT1);
    let frame = MeasFrame::new().with_dut1(0.3).with_position(vla.clone());

    let gast = ut1.convert_to(EpochRef::GAST, &frame).unwrap();
    let last = gast.convert_to(EpochRef::LAST, &frame).unwrap();

    // LAST = GAST + longitude/(2π)
    let lon_turns = vla.longitude_rad() / (2.0 * std::f64::consts::PI);
    let expected_frac = gast.value().frac() + lon_turns;
    let expected_frac = expected_frac - expected_frac.floor();
    let actual_frac = last.value().frac();
    let actual_frac = actual_frac - actual_frac.floor();

    assert!(
        close(actual_frac, expected_frac, 1e-10),
        "LAST frac = {actual_frac}, expected {expected_frac}"
    );
}

#[test]
fn gmst1_to_ut1_roundtrip() {
    let vla = MPosition::new_itrf(-1601185.4, -5041977.5, 3554875.9);
    let ut1 = MEpoch::from_mjd(J2000_MJD, EpochRef::UT1);
    let frame = MeasFrame::new().with_dut1(0.3).with_position(vla);

    let gmst = ut1.convert_to(EpochRef::GMST1, &frame).unwrap();
    let ut1_back = gmst.convert_to(EpochRef::UT1, &frame).unwrap();

    let diff_s = (ut1_back.value().as_mjd() - ut1.value().as_mjd()).abs() * SECONDS_PER_DAY;
    assert!(
        diff_s < 1e-3,
        "GMST1→UT1 roundtrip error = {diff_s}s, expected < 1ms"
    );
}

#[test]
fn identity_conversion() {
    let utc = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = MeasFrame::new();
    let utc2 = utc.convert_to(EpochRef::UTC, &frame).unwrap();
    assert!((utc2.value().as_mjd() - utc.value().as_mjd()).abs() < 1e-15);
}

// ==========================================================================
// Position conversions
// ==========================================================================

#[test]
fn itrf_to_wgs84_vla() {
    // VLA site coordinates
    let vla = MPosition::new_itrf(-1601185.4, -5041977.5, 3554875.9);
    let wgs = vla.convert_to(PositionRef::WGS84).unwrap();

    // Expected: roughly lon=-107.6°, lat=34.1°
    let lon_deg = wgs.values()[0].to_degrees();
    let lat_deg = wgs.values()[1].to_degrees();

    assert!(
        close(lon_deg, -107.6, 0.2),
        "lon = {lon_deg}°, expected ~-107.6°"
    );
    assert!(
        close(lat_deg, 34.1, 0.2),
        "lat = {lat_deg}°, expected ~34.1°"
    );
}

#[test]
fn wgs84_to_itrf_roundtrip() {
    let wgs = MPosition::new_wgs84(-107.6_f64.to_radians(), 34.1_f64.to_radians(), 2124.0);
    let itrf = wgs.convert_to(PositionRef::ITRF).unwrap();
    let wgs2 = itrf.convert_to(PositionRef::WGS84).unwrap();

    for i in 0..3 {
        assert!(
            (wgs2.values()[i] - wgs.values()[i]).abs() < 1e-6,
            "coord {i}: {} vs {}",
            wgs2.values()[i],
            wgs.values()[i]
        );
    }
}

#[test]
fn equator_prime_meridian() {
    // Point on the equator at prime meridian, sea level
    let wgs = MPosition::new_wgs84(0.0, 0.0, 0.0);
    let itrf = wgs.convert_to(PositionRef::ITRF).unwrap();

    // x should be approximately Earth's equatorial radius (~6378137 m)
    // y and z should be ~0
    assert!(
        close(itrf.values()[0], 6_378_137.0, 1.0),
        "x = {}",
        itrf.values()[0]
    );
    assert!(itrf.values()[1].abs() < 1.0, "y = {}", itrf.values()[1]);
    assert!(itrf.values()[2].abs() < 1.0, "z = {}", itrf.values()[2]);
}

#[test]
fn north_pole() {
    let wgs = MPosition::new_wgs84(0.0, std::f64::consts::FRAC_PI_2, 0.0);
    let itrf = wgs.convert_to(PositionRef::ITRF).unwrap();

    // x, y should be ~0; z should be approximately Earth's polar radius (~6356752 m)
    assert!(itrf.values()[0].abs() < 1.0, "x = {}", itrf.values()[0]);
    assert!(itrf.values()[1].abs() < 1.0, "y = {}", itrf.values()[1]);
    assert!(
        close(itrf.values()[2], 6_356_752.0, 2.0),
        "z = {}",
        itrf.values()[2]
    );
}

// ==========================================================================
// Record serialization
// ==========================================================================

#[test]
fn epoch_record_roundtrip() {
    for ref_type in [EpochRef::UTC, EpochRef::TAI, EpochRef::TT, EpochRef::TDB] {
        let epoch = MEpoch::from_mjd(J2000_MJD, ref_type);
        let rec = epoch_to_record(&epoch);
        let decoded = epoch_from_record(&rec).unwrap();

        assert_eq!(decoded.refer(), ref_type);
        assert!(
            close(decoded.value().as_mjd(), J2000_MJD, 1e-12),
            "MJD roundtrip failed for {ref_type}"
        );
    }
}

#[test]
fn position_record_roundtrip_itrf() {
    let pos = MPosition::new_itrf(-1601185.4, -5041977.5, 3554875.9);
    let rec = position_to_record(&pos);
    let decoded = position_from_record(&rec).unwrap();

    assert_eq!(decoded.refer(), PositionRef::ITRF);
    let orig = pos.values();
    let dec = decoded.values();
    for i in 0..3 {
        assert!(
            close(orig[i], dec[i], 1.0),
            "ITRF coord {i}: {:.3} vs {:.3}",
            orig[i],
            dec[i]
        );
    }
}

#[test]
fn position_record_roundtrip_wgs84() {
    let pos = MPosition::new_wgs84(-107.6_f64.to_radians(), 34.1_f64.to_radians(), 2124.0);
    let rec = position_to_record(&pos);
    let decoded = position_from_record(&rec).unwrap();

    assert_eq!(decoded.refer(), PositionRef::WGS84);
    // WGS84 record stores (lon, lat, radius) not (lon, lat, height), so
    // the decoded height will differ from input. Check lon/lat match.
    assert!(close(decoded.values()[0], pos.values()[0], 1e-6));
    assert!(close(decoded.values()[1], pos.values()[1], 1e-6));
}

#[test]
fn epoch_record_cpp_compatible_field_names() {
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let rec = epoch_to_record(&epoch);

    // Verify expected field names match C++ MeasureHolder format
    assert!(rec.get("type").is_some(), "missing 'type' field");
    assert!(rec.get("refer").is_some(), "missing 'refer' field");
    assert!(rec.get("m0").is_some(), "missing 'm0' field");
}

#[test]
fn position_record_cpp_compatible_field_names() {
    let pos = MPosition::new_itrf(1.0, 2.0, 3.0);
    let rec = position_to_record(&pos);

    assert!(rec.get("type").is_some());
    assert!(rec.get("refer").is_some());
    assert!(rec.get("m0").is_some());
    assert!(rec.get("m1").is_some());
    assert!(rec.get("m2").is_some());
}

// ==========================================================================
// MEpoch from_quantity
// ==========================================================================

#[test]
fn epoch_from_quantity() {
    let q = casacore_types::quanta::Quantity::new(J2000_MJD, "d").unwrap();
    let epoch = MEpoch::from_quantity(&q, EpochRef::UTC).unwrap();
    assert!(close(epoch.value().as_mjd(), J2000_MJD, 1e-12));
}

#[test]
fn epoch_from_quantity_wrong_units() {
    let q = casacore_types::quanta::Quantity::new(1.0, "m").unwrap();
    let result = MEpoch::from_quantity(&q, EpochRef::UTC);
    assert!(matches!(
        result,
        Err(MeasureError::NonConformantUnit { .. })
    ));
}

// ==========================================================================
// Helper: angular separation between two unit-vector directions
// ==========================================================================

fn angular_sep(a: &[f64; 3], b: &[f64; 3]) -> f64 {
    let dot = a[0] * b[0] + a[1] * b[1] + a[2] * b[2];
    dot.clamp(-1.0, 1.0).acos()
}

// ==========================================================================
// MDoppler conversions
// ==========================================================================

#[test]
fn doppler_radio_to_ratio() {
    // RADIO(0.5) means v/c = 0.5, so ratio = 1 - 0.5 = 0.5
    let d = MDoppler::new(0.5, DopplerRef::RADIO);
    let frame = MeasFrame::new();
    let ratio = d.convert_to(DopplerRef::RATIO, &frame).unwrap();
    assert!(close(ratio.value(), 0.5, 1e-12), "got {}", ratio.value());
}

#[test]
fn doppler_z_to_ratio() {
    // Z(1.0) → ratio = 1/(1+z) = 0.5
    let d = MDoppler::new(1.0, DopplerRef::Z);
    let frame = MeasFrame::new();
    let ratio = d.convert_to(DopplerRef::RATIO, &frame).unwrap();
    assert!(close(ratio.value(), 0.5, 1e-12), "got {}", ratio.value());
}

#[test]
fn doppler_beta_to_ratio() {
    // BETA(0.6) → ratio = √((1-0.6)/(1+0.6)) = √(0.4/1.6) = √0.25 = 0.5
    let d = MDoppler::new(0.6, DopplerRef::BETA);
    let frame = MeasFrame::new();
    let ratio = d.convert_to(DopplerRef::RATIO, &frame).unwrap();
    let expected = ((1.0_f64 - 0.6) / (1.0 + 0.6)).sqrt();
    assert!(
        close(ratio.value(), expected, 1e-12),
        "got {}, expected {}",
        ratio.value(),
        expected
    );
}

#[test]
fn doppler_gamma_to_ratio() {
    // GAMMA(2.0) → β = √(1 - 1/γ²) = √(1 - 0.25) = √0.75
    // ratio = √((1-β)/(1+β))
    let gamma = 2.0_f64;
    let beta = (1.0 - 1.0 / (gamma * gamma)).sqrt();
    let expected_ratio = ((1.0 - beta) / (1.0 + beta)).sqrt();

    let d = MDoppler::new(gamma, DopplerRef::GAMMA);
    let frame = MeasFrame::new();
    let ratio = d.convert_to(DopplerRef::RATIO, &frame).unwrap();
    assert!(
        close(ratio.value(), expected_ratio, 1e-12),
        "got {}, expected {}",
        ratio.value(),
        expected_ratio
    );
}

#[test]
fn doppler_radio_z_roundtrip() {
    let d = MDoppler::new(0.3, DopplerRef::RADIO);
    let frame = MeasFrame::new();
    let z = d.convert_to(DopplerRef::Z, &frame).unwrap();
    let back = z.convert_to(DopplerRef::RADIO, &frame).unwrap();
    assert!(
        close(back.value(), 0.3, 1e-12),
        "roundtrip: got {}, expected 0.3",
        back.value()
    );
}

#[test]
fn doppler_all_through_ratio_roundtrip() {
    let frame = MeasFrame::new();
    let test_values = [
        (DopplerRef::RADIO, 0.3),
        (DopplerRef::Z, 0.5),
        (DopplerRef::BETA, 0.4),
        (DopplerRef::GAMMA, 1.5),
    ];
    for (ref_type, val) in test_values {
        let d = MDoppler::new(val, ref_type);
        let ratio = d.convert_to(DopplerRef::RATIO, &frame).unwrap();
        let back = ratio.convert_to(ref_type, &frame).unwrap();
        assert!(
            close(back.value(), val, 1e-12),
            "{ref_type}: roundtrip {val} → {} → {}",
            ratio.value(),
            back.value()
        );
    }
}

#[test]
fn doppler_identity() {
    let d = MDoppler::new(0.42, DopplerRef::RADIO);
    let frame = MeasFrame::new();
    let same = d.convert_to(DopplerRef::RADIO, &frame).unwrap();
    assert!(
        close(same.value(), 0.42, 1e-15),
        "identity: got {}",
        same.value()
    );
}

#[test]
fn doppler_record_roundtrip() {
    let frame = MeasFrame::new();
    for (ref_type, val) in [(DopplerRef::RADIO, 0.3), (DopplerRef::Z, 1.0)] {
        let d = MDoppler::new(val, ref_type);
        let rec = doppler_to_record(&d);
        let decoded = doppler_from_record(&rec).unwrap();
        assert_eq!(decoded.refer(), ref_type);
        assert!(
            close(decoded.value(), val, 1e-12),
            "{ref_type}: {val} roundtrip got {}",
            decoded.value()
        );
        let _ = d.convert_to(DopplerRef::RATIO, &frame).unwrap(); // smoke test
    }
}

// ==========================================================================
// MDirection conversions
// ==========================================================================

#[test]
fn direction_j2000_to_galactic() {
    // Galactic center (l=0, b=0) in J2000 ≈ RA 266.4°, Dec -28.9°
    let gc = MDirection::from_angles(0.0, 0.0, DirectionRef::GALACTIC);
    let frame = MeasFrame::new();
    let j2000 = gc.convert_to(DirectionRef::J2000, &frame).unwrap();

    let (ra, dec) = j2000.as_angles();
    let ra_deg = ra.to_degrees().rem_euclid(360.0);
    let dec_deg = dec.to_degrees();

    assert!(
        close(ra_deg, 266.4, 1.0),
        "RA = {ra_deg}°, expected ~266.4°"
    );
    assert!(
        close(dec_deg, -28.9, 1.0),
        "Dec = {dec_deg}°, expected ~-28.9°"
    );
}

#[test]
fn direction_galactic_roundtrip() {
    let dir = MDirection::from_angles(1.23, 0.45, DirectionRef::J2000);
    let frame = MeasFrame::new();
    let gal = dir.convert_to(DirectionRef::GALACTIC, &frame).unwrap();
    let back = gal.convert_to(DirectionRef::J2000, &frame).unwrap();

    let sep = angular_sep(&dir.cosines(), &back.cosines());
    assert!(
        sep < 1e-13,
        "J2000→GAL→J2000 angular sep = {sep} rad, expected < 1e-13"
    );
}

#[test]
fn direction_icrs_j2000_close() {
    // ICRS and J2000 differ by < 20 mas (frame tie rotation ~17 mas)
    let dir = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new();
    let icrs = dir.convert_to(DirectionRef::ICRS, &frame).unwrap();

    let sep = angular_sep(&dir.cosines(), &icrs.cosines());
    let sep_mas = sep.to_degrees() * 3_600_000.0;
    assert!(
        sep_mas < 20.0,
        "J2000 vs ICRS = {sep_mas} mas, expected < 20 mas"
    );
}

#[test]
fn direction_supergal_roundtrip() {
    let dir = MDirection::from_angles(0.5, 0.3, DirectionRef::GALACTIC);
    let frame = MeasFrame::new();
    let sg = dir.convert_to(DirectionRef::SUPERGAL, &frame).unwrap();
    let back = sg.convert_to(DirectionRef::GALACTIC, &frame).unwrap();

    let sep = angular_sep(&dir.cosines(), &back.cosines());
    assert!(
        sep < 1e-7,
        "GAL→SUPERGAL→GAL angular sep = {sep} rad, expected < 1e-7"
    );
}

#[test]
fn direction_azel_azelsw_flip() {
    use std::f64::consts::PI;
    // AZEL(az, el) → AZELSW should have az+π (modulo 2π)
    let az = 1.0;
    let el = 0.5;
    let dir = MDirection::from_angles(az, el, DirectionRef::AZEL);
    let frame = MeasFrame::new();
    let sw = dir.convert_to(DirectionRef::AZELSW, &frame).unwrap();

    let (sw_az, sw_el) = sw.as_angles();
    let expected_az = (az + PI).rem_euclid(2.0 * PI);
    let actual_az = sw_az.rem_euclid(2.0 * PI);

    assert!(
        close(actual_az, expected_az, 1e-12),
        "AZELSW az = {actual_az}, expected {expected_az}"
    );
    assert!(
        close(sw_el, el, 1e-12),
        "AZELSW el = {sw_el}, expected {el}"
    );
}

#[test]
fn direction_j2000_jmean_needs_epoch() {
    let dir = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new(); // no epoch
    let result = dir.convert_to(DirectionRef::JMEAN, &frame);
    assert!(
        matches!(result, Err(MeasureError::MissingFrameData { .. })),
        "expected MissingFrameData, got {result:?}"
    );
}

#[test]
fn direction_jmean_jtrue_roundtrip() {
    let dir = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = MeasFrame::new().with_epoch(epoch);

    let jmean = dir.convert_to(DirectionRef::JMEAN, &frame).unwrap();
    let jtrue = jmean.convert_to(DirectionRef::JTRUE, &frame).unwrap();
    let back = jtrue
        .convert_to(DirectionRef::JMEAN, &frame)
        .unwrap()
        .convert_to(DirectionRef::J2000, &frame)
        .unwrap();

    let sep = angular_sep(&dir.cosines(), &back.cosines());
    assert!(
        sep < 1e-10,
        "J2000→JMEAN→JTRUE→JMEAN→J2000 angular sep = {sep} rad, expected < 1e-10"
    );
}

#[test]
fn direction_ecliptic_roundtrip() {
    let dir = MDirection::from_angles(2.0, -0.3, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = MeasFrame::new().with_epoch(epoch);

    let ecl = dir.convert_to(DirectionRef::ECLIPTIC, &frame).unwrap();
    let back = ecl.convert_to(DirectionRef::J2000, &frame).unwrap();

    let sep = angular_sep(&dir.cosines(), &back.cosines());
    assert!(
        sep < 1e-6,
        "J2000→ECLIPTIC→J2000 angular sep = {sep} rad, expected < 1e-6"
    );
}

#[test]
fn direction_record_roundtrip() {
    let dir = MDirection::from_angles(1.5, -0.2, DirectionRef::J2000);
    let rec = direction_to_record(&dir);
    let decoded = direction_from_record(&rec).unwrap();

    assert_eq!(decoded.refer(), DirectionRef::J2000);
    let sep = angular_sep(&dir.cosines(), &decoded.cosines());
    assert!(
        sep < 1e-12,
        "direction record roundtrip angular sep = {sep} rad"
    );
}

// ==========================================================================
// MFrequency conversions
// ==========================================================================

#[test]
fn frequency_lsrk_bary_shift() {
    // LSRK → BARY for a known direction; shift should be < 1 MHz at 1.42 GHz
    let freq = MFrequency::new(1.42e9, FrequencyRef::LSRK);
    let dir = MDirection::from_angles(0.0, 0.0, DirectionRef::J2000);
    let frame = MeasFrame::new().with_direction(dir);

    let bary = freq.convert_to(FrequencyRef::BARY, &frame).unwrap();
    let shift = (bary.hz() - freq.hz()).abs();
    assert!(
        shift < 1e6,
        "LSRK→BARY shift = {shift} Hz, expected < 1 MHz"
    );
}

#[test]
fn frequency_bary_lgroup() {
    // BARY → LGROUP: max shift from local group motion (~308 km/s)
    let freq = MFrequency::new(1.42e9, FrequencyRef::BARY);
    let dir = MDirection::from_angles(0.0, 0.0, DirectionRef::J2000);
    let frame = MeasFrame::new().with_direction(dir);

    let lgroup = freq.convert_to(FrequencyRef::LGROUP, &frame).unwrap();
    // 308 km/s → fractional shift ~1e-3, so max ~1.5 MHz at 1.42 GHz
    let shift = (lgroup.hz() - freq.hz()).abs();
    assert!(
        shift < 2e6,
        "BARY→LGROUP shift = {shift} Hz, expected < 2 MHz"
    );
}

#[test]
fn frequency_bary_cmb() {
    // BARY → CMB: CMB dipole ~369.5 km/s
    let freq = MFrequency::new(1.42e9, FrequencyRef::BARY);
    let dir = MDirection::from_angles(0.0, 0.0, DirectionRef::J2000);
    let frame = MeasFrame::new().with_direction(dir);

    let cmb = freq.convert_to(FrequencyRef::CMB, &frame).unwrap();
    // Max fractional shift ~1.2e-3
    let shift = (cmb.hz() - freq.hz()).abs();
    assert!(shift < 2e6, "BARY→CMB shift = {shift} Hz, expected < 2 MHz");
}

#[test]
fn frequency_identity() {
    let freq = MFrequency::new(1.42e9, FrequencyRef::LSRK);
    let frame = MeasFrame::new();
    let same = freq.convert_to(FrequencyRef::LSRK, &frame).unwrap();
    assert!(
        close(same.hz(), 1.42e9, 1e-6),
        "identity: got {}",
        same.hz()
    );
}

#[test]
fn frequency_lsrk_bary_roundtrip() {
    let freq = MFrequency::new(1.42e9, FrequencyRef::LSRK);
    let dir = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new().with_direction(dir);

    let bary = freq.convert_to(FrequencyRef::BARY, &frame).unwrap();
    let back = bary.convert_to(FrequencyRef::LSRK, &frame).unwrap();

    let diff = (back.hz() - freq.hz()).abs();
    assert!(
        diff < 1.0,
        "LSRK→BARY→LSRK roundtrip error = {diff} Hz, expected < 1 Hz"
    );
}

#[test]
fn frequency_rest_needs_radial_velocity() {
    let freq = MFrequency::new(1.42e9, FrequencyRef::REST);
    let dir = MDirection::from_angles(0.0, 0.0, DirectionRef::J2000);
    let frame = MeasFrame::new().with_direction(dir);

    let result = freq.convert_to(FrequencyRef::LSRK, &frame);
    assert!(
        matches!(result, Err(MeasureError::MissingFrameData { .. })),
        "expected MissingFrameData, got {result:?}"
    );
}

#[test]
fn frequency_rest_to_lsrk_with_rv() {
    use casacore_types::measures::radial_velocity::{MRadialVelocity, RadialVelocityRef};
    let rv = MRadialVelocity::new(1_000_000.0, RadialVelocityRef::LSRK); // 1000 km/s
    let dir = MDirection::from_angles(0.0, 0.0, DirectionRef::J2000);
    let frame = MeasFrame::new()
        .with_direction(dir)
        .with_radial_velocity(rv);

    let f_rest = MFrequency::new(1.42e9, FrequencyRef::REST);
    let f_lsrk = f_rest.convert_to(FrequencyRef::LSRK, &frame).unwrap();
    // Receding source → observed frequency lower
    assert!(f_lsrk.hz() < f_rest.hz());

    // Roundtrip
    let back = f_lsrk.convert_to(FrequencyRef::REST, &frame).unwrap();
    assert!(
        (back.hz() - f_rest.hz()).abs() < 1.0,
        "REST roundtrip: {} vs {}",
        back.hz(),
        f_rest.hz()
    );
}

#[test]
fn frequency_record_roundtrip() {
    let freq = MFrequency::new(1.42e9, FrequencyRef::LSRK);
    let rec = frequency_to_record(&freq);
    let decoded = frequency_from_record(&rec).unwrap();

    assert_eq!(decoded.refer(), FrequencyRef::LSRK);
    assert!(
        close(decoded.hz(), 1.42e9, 1e-6),
        "frequency record roundtrip: got {} Hz",
        decoded.hz()
    );
}

// ==========================================================================
// MRadialVelocity conversions
// ==========================================================================

#[test]
fn radial_velocity_lsrk_to_bary() {
    use casacore_types::measures::radial_velocity::{MRadialVelocity, RadialVelocityRef};
    let rv = MRadialVelocity::new(100_000.0, RadialVelocityRef::LSRK);
    let dir = MDirection::from_angles(0.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new().with_direction(dir);

    let bary = rv.convert_to(RadialVelocityRef::BARY, &frame).unwrap();
    // LSRK→BARY shift should be < 20 km/s
    assert!(
        (bary.ms() - rv.ms()).abs() < 25_000.0,
        "shift = {} m/s",
        bary.ms() - rv.ms()
    );
}

#[test]
fn radial_velocity_roundtrip() {
    use casacore_types::measures::radial_velocity::{MRadialVelocity, RadialVelocityRef};
    let rv = MRadialVelocity::new(50_000.0, RadialVelocityRef::LSRK);
    let dir = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new().with_direction(dir);

    let bary = rv.convert_to(RadialVelocityRef::BARY, &frame).unwrap();
    let back = bary.convert_to(RadialVelocityRef::LSRK, &frame).unwrap();
    assert!(
        (back.ms() - rv.ms()).abs() < 0.01,
        "roundtrip error: {} m/s",
        (back.ms() - rv.ms()).abs()
    );
}

#[test]
fn radial_velocity_record_roundtrip() {
    use casacore_types::measures::radial_velocity::{MRadialVelocity, RadialVelocityRef};
    let rv = MRadialVelocity::new(100_000.0, RadialVelocityRef::LSRK);
    let rec = casacore_types::measures::radial_velocity_to_record(&rv);
    let decoded = casacore_types::measures::radial_velocity_from_record(&rec).unwrap();
    assert_eq!(decoded.refer(), RadialVelocityRef::LSRK);
    assert!(close(decoded.ms(), 100_000.0, 1e-6));
}

// ==========================================================================
// B1950 direction
// ==========================================================================

#[test]
fn direction_b1950_roundtrip() {
    let dir = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new();
    let b1950 = dir.convert_to(DirectionRef::B1950, &frame).unwrap();
    let back = b1950.convert_to(DirectionRef::J2000, &frame).unwrap();
    let sep = angular_sep(&dir.cosines(), &back.cosines());
    assert!(
        sep < 1e-6,
        "B1950 roundtrip angular sep = {sep} rad, expected < 1e-6"
    );
}

// ==========================================================================
// ITRF direction
// ==========================================================================

#[test]
fn direction_j2000_to_itrf_needs_epoch() {
    let dir = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new();
    let result = dir.convert_to(DirectionRef::ITRF, &frame);
    assert!(matches!(result, Err(MeasureError::MissingFrameData { .. })));
}

#[test]
fn direction_j2000_itrf_roundtrip() {
    let dir = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    // ITRF conversion routes through HADEC, which needs observer position
    let obs = MPosition::new_wgs84(-1.878_283_2, 0.595_370_3, 2124.0);
    let frame = MeasFrame::new()
        .with_epoch(epoch)
        .with_position(obs)
        .with_dut1(0.3);
    let itrf = dir.convert_to(DirectionRef::ITRF, &frame).unwrap();
    let back = itrf.convert_to(DirectionRef::J2000, &frame).unwrap();
    let sep = angular_sep(&dir.cosines(), &back.cosines());
    assert!(
        sep < 1e-10,
        "ITRF roundtrip angular sep = {sep} rad, expected < 1e-10"
    );
}
