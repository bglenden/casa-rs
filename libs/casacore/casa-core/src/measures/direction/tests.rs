// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[test]
fn casa_direction_frame_names_resolve_aliases_and_ambiguous_prefixes() {
    assert_eq!(casa_direction_frame_name("GAL"), Some("GALACTIC"));
    assert_eq!(casa_direction_frame_name("j2"), Some("J2000"));
    assert_eq!(casa_direction_frame_name("AZELNE"), Some("AZEL"));
    assert_eq!(casa_direction_frame_name("B1950_VLA"), Some("B1950_VLA"));
    assert_eq!(casa_direction_frame_name("J"), None);
    assert_eq!(casa_direction_frame_name("not-a-frame"), None);
}

use crate::measures::epoch::MEpoch;
use crate::measures::position::MPosition;
use crate::measures::provider::test_measures;

#[test]
fn relabeling_a_direction_keeps_its_cosines_exactly() {
    let mut direction = MDirection::from_angles(1.234, -0.321, DirectionRef::J2000);
    let before = direction.cosines().map(f64::to_bits);
    direction.set_reference(DirectionRef::GALACTIC);
    assert_eq!(direction.cosines().map(f64::to_bits), before);
    assert_eq!(direction.refer(), DirectionRef::GALACTIC);
}

fn close(a: f64, b: f64, tol: f64) -> bool {
    (a - b).abs() < tol
}

fn runtime_frame() -> MeasFrame {
    MeasFrame::new().with_measures(test_measures())
}

fn wsrt_frame() -> MeasFrame {
    let measures = test_measures();
    let position = MPosition::from_observatory_name("WSRT", measures.as_ref())
        .unwrap()
        .expect("WSRT test observatory");
    MeasFrame::new()
        .with_measures(measures)
        .with_epoch(MEpoch::from_mjd(55418.55, EpochRef::UTC))
        .with_position(position)
}

#[test]
fn direction_ref_parse_all() {
    // Test non-deferred refs
    for r in [
        DirectionRef::J2000,
        DirectionRef::GALACTIC,
        DirectionRef::ICRS,
        DirectionRef::ECLIPTIC,
        DirectionRef::HADEC,
        DirectionRef::AZEL,
    ] {
        let parsed: DirectionRef = r.as_str().parse().unwrap();
        assert_eq!(parsed, r);
    }
}

#[test]
fn direction_ref_aliases() {
    assert_eq!(
        "GAL".parse::<DirectionRef>().unwrap(),
        DirectionRef::GALACTIC
    );
    assert_eq!(
        "ECL".parse::<DirectionRef>().unwrap(),
        DirectionRef::ECLIPTIC
    );
}

#[test]
fn from_angles_roundtrip() {
    let lon = 1.5_f64;
    let lat = 0.7_f64;
    let d = MDirection::from_angles(lon, lat, DirectionRef::J2000);
    let (lon2, lat2) = d.as_angles();
    assert!(close(lon2, lon, 1e-14));
    assert!(close(lat2, lat, 1e-14));
}

#[test]
fn formats_equatorial_angles_for_display() {
    let direction = MDirection::from_angles(
        -0.25 * std::f64::consts::TAU,
        (-12.5_f64).to_radians(),
        DirectionRef::J2000,
    );
    assert_eq!(direction.format_right_ascension(6), "18:00:00.000000");
    assert_eq!(direction.format_declination(5), "-12.30.00.00000");
    assert_eq!(
        direction.format_right_ascension_labeled(6),
        "18:00:00.000000 hms"
    );
    assert_eq!(
        direction.format_declination_labeled(5),
        "-12.30.00.00000 dms"
    );
    assert_eq!(
        format_right_ascension(-0.25 * std::f64::consts::TAU, 6),
        "18:00:00.000000"
    );
    assert_eq!(
        format_declination((-12.5_f64).to_radians(), 5),
        "-12.30.00.00000"
    );
    assert_eq!(
        format_right_ascension_labeled(-0.25 * std::f64::consts::TAU, 6),
        "18:00:00.000000 hms"
    );
    assert_eq!(
        format_declination_labeled((-12.5_f64).to_radians(), 5),
        "-12.30.00.00000 dms"
    );
    assert_eq!(
        right_ascension_increment_seconds(-1e-4).to_string(),
        "-1.3750987083139758 s"
    );
    assert_eq!(
        angular_increment_arcseconds(-1e-4).to_string(),
        "-20.626480624709636 arcsec"
    );
    assert_eq!(
        declination_increment_arcseconds(1e-4).to_string(),
        "20.626480624709636 arcsec"
    );
}

#[test]
fn galactic_center_to_j2000() {
    // Galactic center (l=0, b=0) → RA ≈ 266.4°, Dec ≈ -28.9°
    let gc = MDirection::from_angles(0.0, 0.0, DirectionRef::GALACTIC);
    let frame = MeasFrame::new();
    let j = gc.convert_to(DirectionRef::J2000, &frame).unwrap();
    let (ra, dec) = j.as_angles();
    assert!(
        close(ra.to_degrees(), 266.4, 1.0),
        "RA = {}",
        ra.to_degrees()
    );
    assert!(
        close(dec.to_degrees(), -28.9, 1.0),
        "Dec = {}",
        dec.to_degrees()
    );
}

#[test]
fn j2000_galactic_roundtrip() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new();
    let gal = d.convert_to(DirectionRef::GALACTIC, &frame).unwrap();
    let back = gal.convert_to(DirectionRef::J2000, &frame).unwrap();
    let sep = sofars::vm::sepp(&d.cosines(), &back.cosines());
    // casacore's 10-digit matrix is orthogonal to about 7e-11.
    assert!(sep < 2e-10, "roundtrip sep = {} rad", sep);
}

#[test]
fn j2000_icrs_small_offset() {
    // The frame tie is ~17 mas. Check roundtrip.
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new();
    let icrs = d.convert_to(DirectionRef::ICRS, &frame).unwrap();
    let back = icrs.convert_to(DirectionRef::J2000, &frame).unwrap();
    let sep = sofars::vm::sepp(&d.cosines(), &back.cosines());
    assert!(sep < 1e-12, "roundtrip sep = {} rad", sep);
}

#[test]
fn icrs_calibrator_direction_matches_casa_frame_bias() {
    // CASA 6.7.6.14 me.measure(me.direction('ICRS', '10h01m00',
    // '34d30m00'), 'J2000'), the SIM-23 calibrator metadata control.
    let direction = MDirection::from_angles(
        150.25_f64.to_radians(),
        34.5_f64.to_radians(),
        DirectionRef::ICRS,
    );
    let converted = direction
        .convert_to(DirectionRef::J2000, &MeasFrame::new())
        .unwrap();
    let (ra, dec) = converted.as_angles();
    assert!((ra - 2.622_357_224_702_305_7).abs() < 5e-15);
    assert!((dec - 0.602_138_645_476_905_5).abs() < 5e-15);
}

#[test]
fn galactic_supergal_roundtrip() {
    let d = MDirection::from_angles(0.8, 0.3, DirectionRef::GALACTIC);
    let frame = MeasFrame::new();
    let sg = d.convert_to(DirectionRef::SUPERGAL, &frame).unwrap();
    let back = sg.convert_to(DirectionRef::GALACTIC, &frame).unwrap();
    let sep = sofars::vm::sepp(&d.cosines(), &back.cosines());
    assert!(sep < 1e-14, "roundtrip sep = {} rad", sep);
}

#[test]
fn azel_azelsw_roundtrip() {
    let d = MDirection::from_angles(0.5, 0.3, DirectionRef::AZEL);
    let frame = MeasFrame::new();
    let sw = d.convert_to(DirectionRef::AZELSW, &frame).unwrap();
    let back = sw.convert_to(DirectionRef::AZEL, &frame).unwrap();
    let sep = sofars::vm::sepp(&d.cosines(), &back.cosines());
    assert!(sep < 1e-15, "roundtrip sep = {} rad", sep);
}

#[test]
fn b1950_j2000_roundtrip() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new();
    let b1950 = d.convert_to(DirectionRef::B1950, &frame).unwrap();
    let back = b1950.convert_to(DirectionRef::J2000, &frame).unwrap();
    let sep = sofars::vm::sepp(&d.cosines(), &back.cosines());
    assert!(sep < 1e-6, "B1950 roundtrip sep = {} rad", sep);
}

#[test]
fn j2000_jmean_needs_epoch() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new(); // no epoch
    let result = d.convert_to(DirectionRef::JMEAN, &frame);
    assert!(matches!(result, Err(MeasureError::MissingFrameData { .. })));
}

#[test]
fn j2000_jmean_roundtrip() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(51544.5, EpochRef::TT);
    let frame = MeasFrame::new().with_epoch(epoch);
    let jm = d.convert_to(DirectionRef::JMEAN, &frame).unwrap();
    let back = jm.convert_to(DirectionRef::J2000, &frame).unwrap();
    let sep = sofars::vm::sepp(&d.cosines(), &back.cosines());
    assert!(sep < 1e-12, "roundtrip sep = {} rad", sep);
}

#[test]
fn j2000_jnat_applies_solar_deflection_and_roundtrips() {
    let direction = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = wsrt_frame();
    let natural = direction.convert_to(DirectionRef::JNAT, &frame).unwrap();
    let displacement = sofars::vm::sepp(&direction.cosines(), &natural.cosines());
    assert!(
        displacement > 1.0e-10,
        "solar deflection was unexpectedly absent: {displacement} rad"
    );
    let roundtrip = natural.convert_to(DirectionRef::J2000, &frame).unwrap();
    let error = sofars::vm::sepp(&direction.cosines(), &roundtrip.cosines());
    assert!(error < 1.0e-14, "J2000/JNAT roundtrip = {error} rad");
}

#[test]
fn solar_position_cache_reuses_only_the_exact_latest_epoch() {
    let cache = SolarPositionCache::new();
    let calls = Cell::new(0);
    let epoch = 59_000.5_f64;
    let adjacent = f64::from_bits(epoch.to_bits() + 1);
    for (time, expected_calls) in [
        (epoch, 1),
        (epoch, 1),
        (adjacent, 2),
        (adjacent, 2),
        (epoch, 3),
    ] {
        let position = cache
            .get_or_compute(time, || {
                calls.set(calls.get() + 1);
                Ok(([time, 0.0, 1.0], time))
            })
            .unwrap();
        assert_eq!(position, ([time, 0.0, 1.0], time));
        assert_eq!(calls.get(), expected_calls);
    }
    assert!(size_of::<SolarPositionCache>() <= 64);
}

#[test]
fn solar_position_cache_propagates_errors_without_retaining_them() {
    let cache = SolarPositionCache::new();
    let epoch = 59_000.5;
    let position = ([0.0, 1.0, 0.0], 1.0);
    cache.get_or_compute(epoch, || Ok(position)).unwrap();
    let calls = Cell::new(0);
    for _ in 0..2 {
        assert!(matches!(
            cache.get_or_compute(epoch + 1.0, || {
                calls.set(calls.get() + 1);
                Err(MeasureError::SofarsError { code: -1 })
            }),
            Err(MeasureError::SofarsError { code: -1 })
        ));
    }
    assert_eq!(calls.get(), 2);
    assert_eq!(
        cache
            .get_or_compute(epoch, || panic!("successful entry was evicted by an error"))
            .unwrap(),
        position
    );
}

#[test]
fn solar_position_reuse_matches_uncached_ephemeris_across_frames() {
    for epoch in [
        51_544.5,
        59_000.5,
        59_000.500_000_000_01,
        61_000.25,
        51_544.5,
    ] {
        for reference in [EpochRef::UTC, EpochRef::TT, EpochRef::TDB] {
            let frame = runtime_frame().with_epoch(MEpoch::from_mjd(epoch, reference));
            let time = tdb_mjd(&frame).unwrap();
            let (earth, _) = sofars::eph::epv00(MJD_OFFSET, time).unwrap();
            let (distance, unit) = sofars::vm::pn(&earth[0].map(|value| -value));
            for model in [IauModel::Iau1976_1980, IauModel::Iau2006_2000A] {
                for position in [[1_000.0, 2_000.0, 3_000.0], [6_000.0, 5_000.0, 4_000.0]] {
                    let antenna_frame = frame
                        .clone()
                        .with_position(MPosition::new_itrf(position[0], position[1], position[2]))
                        .with_iau_model(model);
                    assert_eq!(
                        geocentric_sun_position(&antenna_frame).unwrap(),
                        (unit, distance)
                    );
                }
            }
        }
    }
    let frame = MeasFrame::new();
    assert!(matches!(
        geocentric_sun_position(&frame),
        Err(MeasureError::MissingFrameData { .. })
    ));
}

#[test]
fn solar_position_cache_is_thread_local() {
    SOLAR_POSITION_CACHE.with(|cache| {
        cache
            .get_or_compute(59_000.5, || Ok(([1.0, 0.0, 0.0], 1.0)))
            .unwrap();
    });
    std::thread::spawn(|| {
        SOLAR_POSITION_CACHE.with(|cache| assert!(cache.latest.get().is_none()));
    })
    .join()
    .unwrap();
}

#[test]
fn a_dynamical_output_preserves_topocentric_parallax() {
    let direction = MDirection::from_angles(1.0, 0.5, DirectionRef::TOPO);
    let frame = wsrt_frame();
    let distance_m = 384.4e6;
    let j2000 = direction
        .convert_with_parallax(DirectionRef::J2000, &frame, distance_m)
        .unwrap();
    let moon = direction
        .convert_with_parallax(DirectionRef::MOON, &frame, distance_m)
        .unwrap();
    assert_eq!(moon.refer(), DirectionRef::MOON);
    assert_eq!(moon.cosines(), j2000.cosines());
    assert_ne!(
        j2000.cosines(),
        direction
            .convert_to(DirectionRef::J2000, &frame)
            .unwrap()
            .cosines(),
    );
}

#[test]
fn j2000_app_roundtrip_uses_casacore_apparent_place_path() {
    let direction = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = wsrt_frame();
    let apparent = direction.convert_to(DirectionRef::APP, &frame).unwrap();
    let roundtrip = apparent.convert_to(DirectionRef::J2000, &frame).unwrap();
    let error = sofars::vm::sepp(&direction.cosines(), &roundtrip.cosines());
    assert!(error < 1.0e-12, "J2000/APP roundtrip = {error} rad");
}

#[test]
fn iau1980_gast_omits_the_1994_complementary_terms_like_casacore() {
    let frame = MeasFrame::new()
        .with_measures(test_measures())
        .with_epoch(MEpoch::from_mjd(58_574.453_934_895_835, EpochRef::UTC))
        .with_dut1(-0.119_625_683_873_891_83);
    let ctx = ConvCtx::new();
    let gast = compute_gast(&frame, &ctx).unwrap();
    let casa_gast = 6.159_758_319_092_965_5;
    let error = sofars::vm::anpm(gast - casa_gast).abs();
    assert!(error < 5.0e-10, "casacore GAST error = {error} rad");

    let (ut1_a, ut1_b) = compute_ut1_jd(&frame).unwrap();
    let sofa_1994 = sofars::erst::gst94(ut1_a, ut1_b);
    let complementary_term = sofars::vm::anpm(sofa_1994 - gast).abs();
    assert!(
        complementary_term > 1.0e-8,
        "1994 complementary terms unexpectedly absent: {complementary_term} rad"
    );
}

#[test]
fn j2000_ecliptic_roundtrip() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new();
    let ecl = d.convert_to(DirectionRef::ECLIPTIC, &frame).unwrap();
    let back = ecl.convert_to(DirectionRef::J2000, &frame).unwrap();
    let sep = sofars::vm::sepp(&d.cosines(), &back.cosines());
    assert!(sep < 1e-14, "roundtrip sep = {} rad", sep);
}

#[test]
fn identity_conversion() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new();
    let same = d.convert_to(DirectionRef::J2000, &frame).unwrap();
    assert_eq!(d.cosines(), same.cosines());
}

#[test]
fn casacore_code_roundtrip() {
    for r in DirectionRef::ALL {
        let code = r.casacore_code();
        assert_eq!(DirectionRef::from_casacore_code(code), Some(r));
    }
}

#[test]
fn casacore_code_known_values() {
    assert_eq!(DirectionRef::J2000.casacore_code(), 0);
    assert_eq!(DirectionRef::B1950.casacore_code(), 4);
    assert_eq!(DirectionRef::GALACTIC.casacore_code(), 8);
    assert_eq!(DirectionRef::JNAT.casacore_code(), 14);
    assert_eq!(DirectionRef::ICRS.casacore_code(), 21);
}

#[test]
fn casacore_codes_include_dynamical_labels_and_reject_gaps() {
    assert_eq!(DirectionRef::from_casacore_code(-1), None);
    assert_eq!(DirectionRef::from_casacore_code(22), None);
    // MDirection.h defines dynamical labels at codes 32 through 42.
    assert_eq!(
        DirectionRef::from_casacore_code(32),
        Some(DirectionRef::MERCURY)
    );
    assert_eq!(
        DirectionRef::from_casacore_code(42),
        Some(DirectionRef::COMET)
    );
    assert_eq!(DirectionRef::from_casacore_code(43), None);
}

#[test]
fn source_name_casa_matches_builtin_j2000_position() {
    let direction = MDirection::from_source_name("CasA", &runtime_frame()).unwrap();
    assert_eq!(direction.refer(), DirectionRef::J2000);
    let (lon, lat) = direction.as_angles();
    assert!(close(lon, 6.123_487_680_622_104, 1e-15));
    assert!(close(lat, 1.026_515_399_560_464_8, 1e-15));
}

#[test]
fn source_name_catalog_entry_uses_runtime_sources_table() {
    let direction = MDirection::from_source_name("0002-478", &runtime_frame()).unwrap();
    assert_eq!(direction.refer(), DirectionRef::ICRS);
    let (lon, lat) = direction.as_angles();
    assert!(close(lon, 0.020_046_3, 1e-6));
    assert!(close(lat, -0.830_872, 1e-6));
}

#[test]
fn source_name_zenith_is_local_up() {
    let direction = MDirection::from_source_name("ZENITH", &MeasFrame::new()).unwrap();
    assert_eq!(direction.refer(), DirectionRef::AZEL);
    let (lon, lat) = direction.as_angles();
    assert!(close(lon, 0.0, 1e-15));
    assert!(close(lat, std::f64::consts::FRAC_PI_2, 1e-15));
}

#[test]
fn source_name_sun_requires_epoch() {
    let err = MDirection::from_source_name("SUN", &MeasFrame::new()).unwrap_err();
    assert!(matches!(err, MeasureError::MissingFrameData { .. }));
}

#[test]
fn source_name_rejects_invalid_sun_suffix() {
    let err = rise_set_times_from_name("SUN-XYZ", &MeasFrame::new()).unwrap_err();
    assert!(matches!(err, MeasureError::InvalidSourceName { .. }));
}

#[test]
fn source_name_parser_covers_supported_suffixes_and_empty_input() {
    let frame = runtime_frame();
    let sun = parse_named_direction("SUN-CT", &frame).unwrap();
    assert!(matches!(
        sun.kind,
        NamedDirectionKind::SolarSystem(SolarSystemBody::Sun)
    ));
    assert!(close(
        sun.rise_set_horizon_rad,
        (-6.0_f64).to_radians(),
        1e-15
    ));

    let moon = parse_named_direction("MOON-LR", &frame).unwrap();
    assert!(matches!(
        moon.kind,
        NamedDirectionKind::SolarSystem(SolarSystemBody::Moon)
    ));
    assert!(close(
        moon.rise_set_horizon_rad,
        (-0.333_f64).to_radians(),
        1e-15
    ));

    let mercury = parse_named_direction("MERCURY", &frame).unwrap();
    assert!(matches!(
        mercury.kind,
        NamedDirectionKind::SolarSystem(SolarSystemBody::Mercury)
    ));

    assert!(matches!(
        parse_named_direction("", &frame),
        Err(MeasureError::UnknownSourceName { .. })
    ));
}

#[test]
fn fixed_source_rise_set_returns_ordered_times() {
    let frame = wsrt_frame();

    let riseset = rise_set_times_from_name("CasA", &frame).unwrap();
    assert!(riseset.rise_mjd >= 55418.0);
    assert!(riseset.set_mjd > riseset.rise_mjd);
}

#[test]
fn moving_source_direction_is_finite_with_frame() {
    let frame = wsrt_frame();

    let direction = MDirection::from_source_name("SUN", &frame).unwrap();
    let (lon, lat) = direction.as_angles();
    assert!(lon.is_finite());
    assert!(lat.is_finite());
}

#[test]
fn moving_planet_names_and_pluto_error_are_exercised() {
    let frame = wsrt_frame();

    for body in [
        "MERCURY", "VENUS", "MARS", "JUPITER", "SATURN", "URANUS", "NEPTUNE",
    ] {
        let direction = MDirection::from_source_name(body, &frame).unwrap();
        let (lon, lat) = direction.as_angles();
        assert!(lon.is_finite(), "{body}");
        assert!(lat.is_finite(), "{body}");
    }

    let err = MDirection::from_source_name("PLUTO", &frame).unwrap_err();
    assert!(matches!(err, MeasureError::NotYetImplemented { .. }));
}

#[test]
fn rise_set_method_and_to_j2000_cover_additional_branches() {
    let source = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    assert!(matches!(
        source.rise_set_times(&MeasFrame::new()),
        Err(MeasureError::MissingFrameData { .. })
    ));

    let frame = wsrt_frame();
    let rise_set = source.rise_set_times(&frame).unwrap();
    assert!(rise_set.set_mjd > rise_set.rise_mjd);

    let j2000 = source.to_j2000(&frame).unwrap();
    assert_eq!(j2000, source.cosines());
    let displayed = source.to_string();
    assert!(displayed.contains("Direction J2000"));
    assert!(displayed.contains("lon="));
}
