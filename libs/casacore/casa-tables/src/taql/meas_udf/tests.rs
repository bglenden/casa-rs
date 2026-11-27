// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use casa_core::quanta::Quantity;
fn s(val: &str) -> ExprValue {
    ExprValue::String(val.to_string())
}
fn f(val: f64) -> ExprValue {
    ExprValue::Float(val)
}
fn q(val: f64, unit: &str) -> ExprValue {
    ExprValue::Quantity(Quantity::new(val, unit).unwrap())
}
fn extract_datetime_array(val: &ExprValue) -> [f64; 2] {
    match val {
        ExprValue::Array(arr) => {
            assert_eq!(arr.shape, vec![2]);
            match (&arr.data[0], &arr.data[1]) {
                (ExprValue::DateTime(a), ExprValue::DateTime(b)) => [*a, *b],
                other => panic!("expected DateTime pair, got {other:?}"),
            }
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

// ── Epoch ────────────────────────────────────────────────────────

#[test]
fn epoch_utc_to_tai() {
    // J2000.0 = MJD 51544.5 in UTC; TAI is 32 leap seconds ahead.
    let result = call_meas_function("meas.epoch", &[s("TAI"), f(51544.5)]).unwrap();
    let mjd = match result {
        ExprValue::Float(v) => v,
        other => panic!("expected Float, got {other:?}"),
    };
    let offset_s = (mjd - 51544.5) * 86400.0;
    assert!(
        (offset_s - 32.0).abs() < 0.01,
        "UTC→TAI offset should be ~32s, got {offset_s}"
    );
}

#[test]
fn epoch_explicit_source() {
    let result = call_meas_function("meas.epoch", &[s("TAI"), f(51544.5), s("UTC")]).unwrap();
    let mjd = match result {
        ExprValue::Float(v) => v,
        other => panic!("expected Float, got {other:?}"),
    };
    let offset_s = (mjd - 51544.5) * 86400.0;
    assert!((offset_s - 32.0).abs() < 0.01);
}

#[test]
fn epoch_tai_roundtrip() {
    // TAI→UTC→TAI should be identity.
    let tai_mjd = 51544.5 + 32.0 / 86400.0;
    let utc = call_meas_function("meas.epoch", &[s("UTC"), f(tai_mjd), s("TAI")]).unwrap();
    let utc_mjd = match utc {
        ExprValue::Float(v) => v,
        other => panic!("expected Float, got {other:?}"),
    };
    assert!(
        (utc_mjd - 51544.5).abs() < 1e-10,
        "roundtrip failed: {utc_mjd}"
    );
}

#[test]
fn last_with_position() {
    let epoch = MEpoch::from_mjd(50217.625, EpochRef::UTC);
    let frame = MeasFrame::new()
        .with_measures(Arc::new(TestMeasuresProvider))
        .with_position(MPosition::new_wgs84(
            6.60417_f64.to_radians(),
            52.8_f64.to_radians(),
            10.0,
        ));
    let expected = epoch
        .convert_to(EpochRef::LAST, &frame)
        .unwrap()
        .value()
        .frac()
        * SECONDS_PER_DAY;

    let result = call_meas_function(
        "meas.last",
        &[
            f(50217.625),
            f(6.60417_f64.to_radians()),
            f(52.8_f64.to_radians()),
            f(10.0),
            s("WGS84"),
        ],
    )
    .unwrap();
    let actual = match result {
        ExprValue::Float(v) => v,
        other => panic!("expected Float, got {other:?}"),
    };
    assert!((actual - expected).abs() < 1e-6);
}

#[test]
fn lst_alias() {
    let result = call_meas_function(
        "meas.lst",
        &[
            f(50217.625),
            f(6.60417_f64.to_radians()),
            f(52.8_f64.to_radians()),
            f(10.0),
            s("WGS84"),
        ],
    )
    .unwrap();
    assert!(matches!(result, ExprValue::Float(_)));
}

// ── Direction ────────────────────────────────────────────────────

#[test]
fn dir_j2000_to_galactic() {
    // Galactic center ≈ (l=0, b=0) maps to J2000 ≈ (RA=266.4°, Dec=-28.9°)
    // Convert J2000 (0,0) to GALACTIC — just check it produces a valid result.
    let result =
        call_meas_function("meas.dir", &[s("GALACTIC"), f(0.0), f(0.0), s("J2000")]).unwrap();
    match result {
        ExprValue::Array(arr) => {
            assert_eq!(arr.shape, vec![2]);
            assert_eq!(arr.data.len(), 2);
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[test]
fn j2000_shortcut() {
    // Convert galactic center to J2000 via shortcut.
    let result = call_meas_function("meas.j2000", &[f(0.0), f(0.0), s("GALACTIC")]).unwrap();
    let (lon, lat) = extract_dir(&result);
    // Galactic center in J2000: RA ≈ 4.65 rad (266.4°), Dec ≈ -0.505 rad (-28.9°)
    assert!((lon - 4.65).abs() < 0.02, "expected RA≈4.65 rad, got {lon}");
    assert!(
        (lat - (-0.505)).abs() < 0.02,
        "expected Dec≈-0.505 rad, got {lat}"
    );
}

#[test]
fn galactic_shortcut() {
    // RA=0, Dec=0 in J2000 → galactic
    let result = call_meas_function("meas.galactic", &[f(0.0), f(0.0)]).unwrap();
    let (lon, lat) = extract_dir(&result);
    // J2000 (0,0) → Galactic ≈ (l≈96.3°, b≈-60.2°) = (1.681, -1.050) rad
    assert!(
        (lon - 1.681).abs() < 0.02,
        "expected l≈1.681 rad, got {lon}"
    );
    assert!(
        (lat - (-1.050)).abs() < 0.02,
        "expected b≈-1.050 rad, got {lat}"
    );
}

#[test]
fn dircos_returns_unit_vector() {
    let lon = 1.0;
    let lat = 0.5;
    let result =
        call_meas_function("meas.dircos", &[s("J2000"), f(lon), f(lat), s("J2000")]).unwrap();
    let cosines = extract_array3(&result);
    let clat = lat.cos();
    let expected = [lon.cos() * clat, lon.sin() * clat, lat.sin()];
    for i in 0..3 {
        assert!((cosines[i] - expected[i]).abs() < 1e-12);
    }
}

#[test]
fn app_hadec_and_azel_shortcuts_work() {
    let pos = MPosition::new_wgs84(-1.878_283_2, 0.595_370_3, 2124.0)
        .convert_to(PositionRef::ITRF)
        .unwrap();
    let [px, py, pz] = pos.values();
    let args = &[
        f(0.185_948_8),
        f(0.722_777_4),
        s("J2000"),
        f(51544.5),
        f(px),
        f(py),
        f(pz),
    ];
    let _ = extract_dir(&call_meas_function("meas.app", args).unwrap());
    let _ = extract_dir(&call_meas_function("meas.hadec", args).unwrap());
    let _ = extract_dir(&call_meas_function("meas.azel", args).unwrap());
}

#[test]
fn help_without_topic_lists_sections() {
    let result = call_meas_function("meas.help", &[]).unwrap();
    let text = match result {
        ExprValue::String(text) => text,
        other => panic!("expected String, got {other:?}"),
    };
    assert!(text.contains("Position conversion functions:"));
    assert!(text.contains("Frequency conversion functions:"));
    assert!(text.contains("Doppler conversion functions:"));
    assert!(text.contains("MEAS.RISET"));
    assert!(text.contains(MEAS_HELP_URL));
}

#[test]
fn help_frequency_topic_includes_rest_and_shift_aliases() {
    let result = call_meas_function("meas.help", &[s("freq")]).unwrap();
    let text = match result {
        ExprValue::String(text) => text,
        other => panic!("expected String, got {other:?}"),
    };
    assert!(text.contains("MEAS.FREQ"));
    assert!(text.contains("MEAS.REST"));
    assert!(text.contains("MEAS.SHIFTFREQ"));
    assert!(text.contains("Known frequency types:"));
}

#[test]
fn help_earthmag_topic_mentions_quantity_support() {
    let result = call_meas_function("meas.help", &[s("em")]).unwrap();
    let text = match result {
        ExprValue::String(text) => text,
        other => panic!("expected String, got {other:?}"),
    };
    assert!(text.contains("MEAS.EM"));
    assert!(text.contains("MEAS.IGRF"));
    assert!(text.contains("Known EarthMagnetic types:"));
    assert!(text.contains("angle, angle, length scalars"));
    assert!(text.contains("1 'km/s'"));
}

#[test]
fn help_unknown_topic_reports_error_text() {
    let result = call_meas_function("meas.help", &[s("bogus")]).unwrap();
    let text = match result {
        ExprValue::String(text) => text,
        other => panic!("expected String, got {other:?}"),
    };
    assert!(text.contains("bogus is an unknown meas subtype"));
    assert!(!text.contains(MEAS_HELP_URL));
}

#[test]
fn named_direction_sources_work_in_shortcuts() {
    let result = call_meas_function("meas.j2000", &[s("CasA")]).unwrap();
    let (lon, lat) = extract_dir(&result);
    assert!((lon - 6.123_487_680_622_104).abs() < 1e-12);
    assert!((lat - 1.026_515_399_560_464_8).abs() < 1e-12);

    let zenith = call_meas_function("meas.azel", &[s("ZENITH")]).unwrap();
    let (az, el) = extract_dir(&zenith);
    assert!(az.abs() < 1e-12);
    assert!((el - std::f64::consts::FRAC_PI_2).abs() < 1e-12);
}

#[test]
fn riseset_returns_datetime_pair_for_named_source() {
    let result = call_meas_function("meas.riseset", &[s("CasA"), f(55418.55), s("WSRT")]).unwrap();
    let [rise, set] = extract_datetime_array(&result);
    assert!(rise >= 55418.0);
    assert!(set > rise);
}

#[test]
fn earthmag_xyz_and_length_outputs_work() {
    let pos = MPosition::new_wgs84(-1.878_283_2, 0.595_370_3, 2124.0)
        .convert_to(PositionRef::ITRF)
        .unwrap();
    let [px, py, pz] = pos.values();

    let xyz = call_meas_function(
        "meas.emxyz",
        &[
            s("APP"),
            f(-8.460_923_183_69e-9),
            f(-8.036_417_537_78e-10),
            f(5.269_434_391_97e-9),
            s("B1950"),
            f(51544.5),
            f(px),
            f(py),
            f(pz),
        ],
    )
    .unwrap();
    assert!(extract_array3(&xyz).iter().all(|v| v.is_finite()));

    let length = call_meas_function(
        "meas.emlen",
        &[
            s("APP"),
            f(-8.460_923_183_69e-9),
            f(-8.036_417_537_78e-10),
            f(5.269_434_391_97e-9),
            s("B1950"),
            f(51544.5),
            f(px),
            f(py),
            f(pz),
        ],
    )
    .unwrap();
    let length = match length {
        ExprValue::Float(v) => v,
        other => panic!("expected Float, got {other:?}"),
    };
    assert!(length.is_finite());
    assert!(length > 0.0);
}

#[test]
fn earthmag_angle_quantity_scalars_round_trip() {
    let angles = call_meas_function(
        "meas.emang",
        &[q(0.3, "rad"), q(-0.2, "rad"), q(5.0e4, "nT")],
    )
    .unwrap();
    let length = call_meas_function(
        "meas.emlen",
        &[q(0.3, "rad"), q(-0.2, "rad"), q(5.0e4, "nT")],
    )
    .unwrap();

    let arr = match angles {
        ExprValue::Array(arr) => arr,
        other => panic!("expected angle array, got {other:?}"),
    };
    assert_eq!(arr.shape, vec![2]);
    let lon = arr.data[0].to_float().unwrap();
    let lat = arr.data[1].to_float().unwrap();
    assert!((lon - 0.3).abs() < 1e-12);
    assert!((lat + 0.2).abs() < 1e-12);
    match length {
        ExprValue::Float(v) => assert!((v - 5.0e4).abs() < 1e-9),
        other => panic!("expected length float, got {other:?}"),
    }
}

#[test]
fn igrf_outputs_work_for_zenith() {
    let xyz = call_meas_function(
        "meas.igrfxyz",
        &[
            f(0.0),
            f(0.0),
            f(std::f64::consts::FRAC_PI_2),
            s("AZEL"),
            f(51544.5),
            s("VLA"),
        ],
    )
    .unwrap();
    assert!(extract_array3(&xyz).iter().all(|v| v.is_finite()));

    let angles = call_meas_function(
        "meas.igrfang",
        &[
            f(0.0),
            f(0.0),
            f(std::f64::consts::FRAC_PI_2),
            s("AZEL"),
            f(51544.5),
            s("VLA"),
        ],
    )
    .unwrap();
    assert!(extract_array2(&angles).iter().all(|v| v.is_finite()));

    let los = call_meas_function(
        "meas.igrflos",
        &[
            f(0.0),
            f(0.0),
            f(std::f64::consts::FRAC_PI_2),
            s("AZEL"),
            f(51544.5),
            s("VLA"),
        ],
    )
    .unwrap();
    let los = match los {
        ExprValue::Float(v) => v,
        other => panic!("expected Float, got {other:?}"),
    };
    assert!(los.is_finite());

    let long = call_meas_function(
        "meas.igrflong",
        &[
            f(0.0),
            f(0.0),
            f(std::f64::consts::FRAC_PI_2),
            s("AZEL"),
            f(51544.5),
            s("VLA"),
        ],
    )
    .unwrap();
    let long = match long {
        ExprValue::Float(v) => v,
        other => panic!("expected Float, got {other:?}"),
    };
    assert!(long.is_finite());
}

#[test]
fn ecliptic_and_supergalactic_aliases_work() {
    let _ = extract_dir(&call_meas_function("meas.ecl", &[f(1.0), f(0.5)]).unwrap());
    let _ = extract_dir(&call_meas_function("meas.ecliptic", &[f(1.0), f(0.5)]).unwrap());
    let _ = extract_dir(&call_meas_function("meas.gal", &[f(1.0), f(0.5)]).unwrap());
    let _ = extract_dir(&call_meas_function("meas.sgal", &[f(1.0), f(0.5)]).unwrap());
    let _ = extract_dir(&call_meas_function("meas.supergal", &[f(1.0), f(0.5)]).unwrap());
    let _ = extract_dir(&call_meas_function("meas.supergalactic", &[f(1.0), f(0.5)]).unwrap());
}

#[test]
fn itrf_direction_aliases_work() {
    let pos = MPosition::new_wgs84(-1.878_283_2, 0.595_370_3, 2124.0)
        .convert_to(PositionRef::ITRF)
        .unwrap();
    let [px, py, pz] = pos.values();
    let args = &[
        f(0.185_948_8),
        f(0.722_777_4),
        s("J2000"),
        f(51544.5),
        f(px),
        f(py),
        f(pz),
    ];
    let _ = extract_dir(&call_meas_function("meas.itrfd", args).unwrap());
    let _ = extract_dir(&call_meas_function("meas.itrfdir", args).unwrap());
    let _ = extract_dir(&call_meas_function("meas.itrfdirection", args).unwrap());
}

#[test]
fn dir_roundtrip_j2000_galactic() {
    let orig_lon = 1.0;
    let orig_lat = 0.5;
    // J2000 → GALACTIC
    let gal = call_meas_function(
        "meas.dir",
        &[s("GALACTIC"), f(orig_lon), f(orig_lat), s("J2000")],
    )
    .unwrap();
    let (glon, glat) = extract_dir(&gal);
    // GALACTIC → J2000
    let j2k =
        call_meas_function("meas.dir", &[s("J2000"), f(glon), f(glat), s("GALACTIC")]).unwrap();
    let (rlon, rlat) = extract_dir(&j2k);
    assert!(
        (rlon - orig_lon).abs() < 1e-10,
        "lon roundtrip: {rlon} vs {orig_lon}"
    );
    assert!(
        (rlat - orig_lat).abs() < 1e-10,
        "lat roundtrip: {rlat} vs {orig_lat}"
    );
}

// ── Position ─────────────────────────────────────────────────────

#[test]
fn pos_itrf_to_wgs84() {
    // VLA ≈ ITRF (-1601185, -5041977, 3554876) metres
    let result = call_meas_function(
        "meas.pos",
        &[
            s("WGS84"),
            f(-1601185.0),
            f(-5041977.0),
            f(3554876.0),
            s("ITRF"),
        ],
    )
    .unwrap();
    match &result {
        ExprValue::Array(arr) => {
            assert_eq!(arr.shape, vec![3]);
            let lon = match &arr.data[0] {
                ExprValue::Float(v) => *v,
                _ => panic!("expected float"),
            };
            let lat = match &arr.data[1] {
                ExprValue::Float(v) => *v,
                _ => panic!("expected float"),
            };
            // VLA ≈ lon -107.6° (-1.879 rad), lat 34.1° (0.595 rad)
            assert!(
                (lon - (-1.879)).abs() < 0.01,
                "expected lon≈-1.879, got {lon}"
            );
            assert!((lat - 0.595).abs() < 0.01, "expected lat≈0.595, got {lat}");
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[test]
fn pos_wgs84_to_itrf_roundtrip() {
    let lon = -1.879_f64;
    let lat = 0.595_f64;
    let height = 2100.0_f64;
    // WGS84 → ITRF
    let itrf = call_meas_function(
        "meas.pos",
        &[s("ITRF"), f(lon), f(lat), f(height), s("WGS84")],
    )
    .unwrap();
    let vals = extract_array3(&itrf);
    // ITRF → WGS84
    let wgs84 = call_meas_function(
        "meas.pos",
        &[s("WGS84"), f(vals[0]), f(vals[1]), f(vals[2]), s("ITRF")],
    )
    .unwrap();
    let back = extract_array3(&wgs84);
    assert!((back[0] - lon).abs() < 1e-8, "lon roundtrip failed");
    assert!((back[1] - lat).abs() < 1e-8, "lat roundtrip failed");
    assert!((back[2] - height).abs() < 1.0, "height roundtrip failed");
}

#[test]
fn itrf_extractors_match_spherical_record_components() {
    let source = MPosition::new_wgs84(-1.879, 0.595, 2100.0)
        .convert_to(PositionRef::ITRF)
        .unwrap();
    let xyz = source.values();
    let expected = source.as_spherical();

    let ll =
        call_meas_function("meas.itrfll", &[f(xyz[0]), f(xyz[1]), f(xyz[2]), s("ITRF")]).unwrap();
    let h =
        call_meas_function("meas.itrfh", &[f(xyz[0]), f(xyz[1]), f(xyz[2]), s("ITRF")]).unwrap();
    let llh = call_meas_function(
        "meas.itrfllh",
        &[f(xyz[0]), f(xyz[1]), f(xyz[2]), s("ITRF")],
    )
    .unwrap();

    let ll_vals = extract_array2(&ll);
    let h_val = match h {
        ExprValue::Float(v) => v,
        other => panic!("expected Float, got {other:?}"),
    };
    let llh_vals = extract_array3(&llh);

    assert!((ll_vals[0] - expected.0).abs() < 1e-12);
    assert!((ll_vals[1] - expected.1).abs() < 1e-12);
    assert!((h_val - expected.2).abs() < 1e-6);
    assert!((llh_vals[0] - expected.0).abs() < 1e-12);
    assert!((llh_vals[1] - expected.1).abs() < 1e-12);
    assert!((llh_vals[2] - expected.2).abs() < 1e-6);
}

#[test]
fn wgs_extractors_match_meas_pos() {
    let result = call_meas_function(
        "meas.wgsllh",
        &[f(-1601185.0), f(-5041977.0), f(3554876.0), s("ITRF")],
    )
    .unwrap();
    let ll = call_meas_function(
        "meas.wgsll",
        &[f(-1601185.0), f(-5041977.0), f(3554876.0), s("ITRF")],
    )
    .unwrap();
    let h = call_meas_function(
        "meas.wgsh",
        &[f(-1601185.0), f(-5041977.0), f(3554876.0), s("ITRF")],
    )
    .unwrap();
    let expected = extract_array3(
        &call_meas_function(
            "meas.pos",
            &[
                s("WGS84"),
                f(-1601185.0),
                f(-5041977.0),
                f(3554876.0),
                s("ITRF"),
            ],
        )
        .unwrap(),
    );
    let ll_vals = extract_array2(&ll);
    let h_val = match h {
        ExprValue::Float(v) => v,
        other => panic!("expected Float, got {other:?}"),
    };
    let llh_vals = extract_array3(&result);
    assert!((ll_vals[0] - expected[0]).abs() < 1e-12);
    assert!((ll_vals[1] - expected[1]).abs() < 1e-12);
    assert!((h_val - expected[2]).abs() < 1e-6);
    assert_eq!(llh_vals, expected);
}

#[test]
fn wgsxyz_alias_matches_casacore_raw_value_convention() {
    let lon = -1.879;
    let lat = 0.595;
    let height = 2100.0;
    let expected = casacore_wgs_xyz_values(&MPosition::new_wgs84(lon, lat, height));
    let result =
        call_meas_function("meas.wgsxyz", &[f(lon), f(lat), f(height), s("WGS84")]).unwrap();
    let alias = call_meas_function("meas.wgs", &[f(lon), f(lat), f(height), s("WGS84")]).unwrap();
    let vals = extract_array3(&result);
    let alias_vals = extract_array3(&alias);
    for i in 0..3 {
        assert!((vals[i] - expected[i]).abs() < 1e-12);
        assert!((alias_vals[i] - expected[i]).abs() < 1e-12);
    }
}

// ── Doppler ──────────────────────────────────────────────────────

#[test]
fn doppler_radio_to_z() {
    // radio = 0.5 → z = 1/(1-0.5) - 1 = 1.0
    let result = call_meas_function("meas.doppler", &[s("Z"), f(0.5), s("RADIO")]).unwrap();
    let z = match result {
        ExprValue::Float(v) => v,
        other => panic!("expected Float, got {other:?}"),
    };
    assert!((z - 1.0).abs() < 1e-10, "expected z≈1.0, got {z}");
}

#[test]
fn doppler_default_source() {
    // Default source is RADIO. Convert 0.0 to Z → 0.0
    let result = call_meas_function("meas.doppler", &[s("Z"), f(0.0)]).unwrap();
    let z = match result {
        ExprValue::Float(v) => v,
        other => panic!("expected Float, got {other:?}"),
    };
    assert!(z.abs() < 1e-10, "expected z≈0, got {z}");
}

// ── Frequency ────────────────────────────────────────────────────

#[test]
fn freq_same_frame() {
    // LSRK→LSRK should be identity.
    let result = call_meas_function("meas.freq", &[s("LSRK"), f(1.4e9), s("LSRK")]).unwrap();
    let hz = match result {
        ExprValue::Float(v) => v,
        other => panic!("expected Float, got {other:?}"),
    };
    assert!(
        (hz - 1.4e9).abs() < 1.0,
        "same-frame should be identity, got {hz}"
    );
}

#[test]
fn freq_rest_with_radvel_matches_rest_helper() {
    let rest = call_meas_function(
        "meas.rest",
        &[f(1.4e9), s("LSRK"), f(50_000.0), s("LSRK"), f(1.0), f(0.5)],
    )
    .unwrap();
    let via_freq = call_meas_function(
        "meas.freq",
        &[
            s("REST"),
            f(1.4e9),
            s("LSRK"),
            f(50_000.0),
            s("LSRK"),
            f(1.0),
            f(0.5),
        ],
    )
    .unwrap();
    let rest_hz = match rest {
        ExprValue::Float(v) => v,
        other => panic!("expected Float, got {other:?}"),
    };
    let via_freq_hz = match via_freq {
        ExprValue::Float(v) => v,
        other => panic!("expected Float, got {other:?}"),
    };
    assert!((rest_hz - via_freq_hz).abs() < 1e-6);
}

#[test]
fn rest_and_shift_with_doppler_radio() {
    let rest = call_meas_function("meas.rest", &[f(2.0e8), s("LSRK"), f(0.5)]).unwrap();
    let shift = call_meas_function("meas.shift", &[f(2.0e8), s("LSRK"), f(0.5)]).unwrap();
    let rest_hz = match rest {
        ExprValue::Float(v) => v,
        other => panic!("expected Float, got {other:?}"),
    };
    let shift_hz = match shift {
        ExprValue::Float(v) => v,
        other => panic!("expected Float, got {other:?}"),
    };
    assert!((rest_hz - 4.0e8).abs() < 1e-3);
    assert!((shift_hz - 1.0e8).abs() < 1e-3);
}

// ── Radial velocity ──────────────────────────────────────────────

#[test]
fn radvel_same_frame() {
    // LSRK→LSRK should be identity.
    let result = call_meas_function("meas.radvel", &[s("LSRK"), f(1000.0), s("LSRK")]).unwrap();
    let ms = match result {
        ExprValue::Float(v) => v,
        other => panic!("expected Float, got {other:?}"),
    };
    assert!(
        (ms - 1000.0).abs() < 0.01,
        "same-frame should be identity, got {ms}"
    );
}

// ── NULL propagation ─────────────────────────────────────────────

#[test]
fn null_propagation() {
    let result = call_meas_function("meas.epoch", &[s("TAI"), ExprValue::Null]).unwrap();
    assert!(result.is_null());

    let result = call_meas_function("meas.dir", &[s("GALACTIC"), ExprValue::Null, f(0.0)]).unwrap();
    assert!(result.is_null());
}

// ── Error cases ──────────────────────────────────────────────────

#[test]
fn wrong_arity() {
    let err = call_meas_function("meas.epoch", &[s("TAI")]);
    assert!(matches!(err, Err(TaqlError::ArgumentCount { .. })));
}

#[test]
fn invalid_ref_string() {
    let err = call_meas_function("meas.epoch", &[s("BOGUS"), f(51544.5)]);
    assert!(matches!(err, Err(TaqlError::TypeError { .. })));
}

#[test]
fn unknown_meas_function() {
    let err = call_meas_function("meas.nonexistent", &[]);
    assert!(matches!(err, Err(TaqlError::UnknownFunction { .. })));
}

// ── B1950 shortcut ───────────────────────────────────────────────

#[test]
fn b1950_shortcut() {
    // Just verify it works without error and returns 2-element array.
    let result = call_meas_function("meas.b1950", &[f(0.0), f(0.0)]).unwrap();
    let (_lon, _lat) = extract_dir(&result);
}

// ── Alias coverage ───────────────────────────────────────────────

#[test]
fn direction_alias() {
    let result = call_meas_function(
        "meas.direction",
        &[s("GALACTIC"), f(0.0), f(0.0), s("J2000")],
    )
    .unwrap();
    let _ = extract_dir(&result);
}

#[test]
fn position_alias() {
    let result = call_meas_function(
        "meas.position",
        &[s("ITRF"), f(-1.879), f(0.595), f(2100.0), s("WGS84")],
    )
    .unwrap();
    let _ = extract_array3(&result);
}

#[test]
fn frequency_alias() {
    let result = call_meas_function("meas.frequency", &[s("LSRK"), f(1.4e9), s("LSRK")]).unwrap();
    assert!(matches!(result, ExprValue::Float(_)));
}

#[test]
fn redshift_alias() {
    let result = call_meas_function("meas.redshift", &[s("Z"), f(0.0), s("RADIO")]).unwrap();
    assert!(matches!(result, ExprValue::Float(_)));
}

#[test]
fn radialvelocity_alias() {
    let result =
        call_meas_function("meas.radialvelocity", &[s("LSRK"), f(0.0), s("LSRK")]).unwrap();
    assert!(matches!(result, ExprValue::Float(_)));
}

// ── Helpers ──────────────────────────────────────────────────────

fn extract_dir(val: &ExprValue) -> (f64, f64) {
    match val {
        ExprValue::Array(arr) => {
            assert_eq!(arr.shape, vec![2]);
            let lon = match &arr.data[0] {
                ExprValue::Float(v) => *v,
                other => panic!("expected Float, got {other:?}"),
            };
            let lat = match &arr.data[1] {
                ExprValue::Float(v) => *v,
                other => panic!("expected Float, got {other:?}"),
            };
            (lon, lat)
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

fn extract_array3(val: &ExprValue) -> [f64; 3] {
    match val {
        ExprValue::Array(arr) => {
            assert_eq!(arr.shape, vec![3]);
            let mut out = [0.0; 3];
            for (i, v) in arr.data.iter().enumerate() {
                out[i] = match v {
                    ExprValue::Float(f) => *f,
                    other => panic!("expected Float, got {other:?}"),
                };
            }
            out
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

fn extract_array2(val: &ExprValue) -> [f64; 2] {
    match val {
        ExprValue::Array(arr) => {
            assert_eq!(arr.shape, vec![2]);
            let mut out = [0.0; 2];
            for (i, v) in arr.data.iter().enumerate() {
                out[i] = match v {
                    ExprValue::Float(f) => *f,
                    other => panic!("expected Float, got {other:?}"),
                };
            }
            out
        }
        other => panic!("expected Array, got {other:?}"),
    }
}
