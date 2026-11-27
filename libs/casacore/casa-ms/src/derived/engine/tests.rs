// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use crate::builder::MeasurementSetBuilder;
use crate::schema;
use crate::test_helpers::default_value_for_def;
use casa_core::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
use casa_measures::table_measures::default_direction_ref_map;
use casa_measures::table_measures::{MeasureType, TableMeasDesc};
use casa_tables::ColumnSchema;
use ndarray::{ArrayD, ShapeBuilder};

/// VLA approximate ITRF position.
const VLA_X: f64 = -1601185.4;
const VLA_Y: f64 = -5041977.5;
const VLA_Z: f64 = 3554875.9;

fn make_engine() -> MsCalEngine {
    // Two antennas near VLA, 100m apart in X
    let pos0 = MPosition::new_itrf(VLA_X, VLA_Y, VLA_Z);
    let pos1 = MPosition::new_itrf(VLA_X + 100.0, VLA_Y, VLA_Z);

    // Field at RA=0, Dec=+45°
    let dir = MDirection::from_angles(0.0, std::f64::consts::FRAC_PI_4, DirectionRef::J2000);

    MsCalEngine::from_parts(
        vec![pos0.clone(), pos1],
        vec![dir],
        pos0,
        casa_test_support::deterministic_measures_provider(),
    )
}

#[test]
fn mjd_seconds_scaling_matches_casacore_quantity_rounding() {
    let epoch = MEpoch::from_mjd_seconds(5_060_832_843.375, EpochRef::UTC);
    assert_eq!(epoch.value().day(), 58_574.0);
    assert_eq!(epoch.value().frac(), 0.454_205_729_161_913_04);
}

#[test]
fn evla_observatory_fallback_matches_current_casa_position() {
    let measures = casa_test_support::deterministic_measures_provider();
    let position =
        MPosition::casa_observatory(" EVLA ", measures.as_ref()).expect("known EVLA position");
    let (longitude_rad, latitude_rad, radius_m) = position.as_spherical();
    assert!((longitude_rad - -1.878_288_434_411_257_6).abs() < 1.0e-15);
    assert!((latitude_rad - 0.591_675_343_072_337_6).abs() < 1.0e-15);
    assert!((radius_m - 6_373_580.000_000_001).abs() < 1.0e-6);
}

fn add_vla_antenna(ms: &mut MeasurementSet) {
    ms.antenna_mut()
        .unwrap()
        .add_antenna(
            "VLA01",
            "N01",
            "GROUND-BASED",
            "ALT-AZ",
            [VLA_X, VLA_Y, VLA_Z],
            [0.0; 3],
            25.0,
        )
        .unwrap();
}

fn add_field_with_direction(ms: &mut MeasurementSet, direction: MDirection, time_mjd_sec: f64) {
    let (lon, lat) = direction.as_angles();
    let phase_dir = ArrayValue::Float64(
        ArrayD::from_shape_vec(ndarray::IxDyn(&[2, 1]).f(), vec![lon, lat]).unwrap(),
    );
    let field_fields: Vec<RecordField> = schema::field::REQUIRED_COLUMNS
        .iter()
        .map(|col| match col.name {
            "NAME" => RecordField::new(
                col.name,
                Value::Scalar(ScalarValue::String("TEST".to_string())),
            ),
            "TIME" => RecordField::new(col.name, Value::Scalar(ScalarValue::Float64(time_mjd_sec))),
            "NUM_POLY" => RecordField::new(col.name, Value::Scalar(ScalarValue::Int32(0))),
            "DELAY_DIR" | "PHASE_DIR" | "REFERENCE_DIR" => {
                RecordField::new(col.name, Value::Array(phase_dir.clone()))
            }
            _ => RecordField::new(col.name, default_value_for_def(col)),
        })
        .collect();
    ms.subtable_mut(schema::SubtableId::Field)
        .unwrap()
        .add_row(RecordValue::new(field_fields))
        .unwrap();
}

#[test]
fn engine_from_ms() {
    let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
    {
        let mut ant = ms.antenna_mut().unwrap();
        ant.add_antenna(
            "VLA01",
            "N01",
            "GROUND-BASED",
            "ALT-AZ",
            [VLA_X, VLA_Y, VLA_Z],
            [0.0; 3],
            25.0,
        )
        .unwrap();
    }
    let engine =
        MsCalEngine::with_measures(&ms, casa_test_support::deterministic_measures_provider())
            .unwrap();
    assert_eq!(engine.num_antennas(), 1);
    assert_eq!(engine.num_fields(), 0);
}

#[test]
fn engine_uses_main_time_measinfo_reference() {
    let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
    TableMeasDesc::new_fixed("TIME", MeasureType::Epoch, "TAI")
        .write(ms.main_table_mut())
        .unwrap();
    let engine =
        MsCalEngine::with_measures(&ms, casa_test_support::deterministic_measures_provider())
            .unwrap();
    assert_eq!(engine.time_reference(), EpochRef::TAI);
}

#[test]
fn engine_accepts_fortran_order_phase_dir() {
    let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
    {
        let mut ant = ms.antenna_mut().unwrap();
        ant.add_antenna(
            "VLA01",
            "N01",
            "GROUND-BASED",
            "ALT-AZ",
            [VLA_X, VLA_Y, VLA_Z],
            [0.0; 3],
            25.0,
        )
        .unwrap();
    }

    let phase_dir = ArrayValue::Float64(
        ArrayD::from_shape_vec(ndarray::IxDyn(&[2, 1]).f(), vec![0.25, 0.5]).unwrap(),
    );
    let field_fields: Vec<RecordField> = schema::field::REQUIRED_COLUMNS
        .iter()
        .map(|col| match col.name {
            "NAME" => RecordField::new(
                col.name,
                Value::Scalar(ScalarValue::String("TEST".to_string())),
            ),
            "NUM_POLY" => RecordField::new(col.name, Value::Scalar(ScalarValue::Int32(0))),
            "DELAY_DIR" | "PHASE_DIR" | "REFERENCE_DIR" => {
                RecordField::new(col.name, Value::Array(phase_dir.clone()))
            }
            _ => RecordField::new(col.name, default_value_for_def(col)),
        })
        .collect();
    ms.subtable_mut(schema::SubtableId::Field)
        .unwrap()
        .add_row(RecordValue::new(field_fields))
        .unwrap();

    let engine =
        MsCalEngine::with_measures(&ms, casa_test_support::deterministic_measures_provider())
            .unwrap();
    assert_eq!(engine.num_fields(), 1);
    let (ra, dec) = engine.field_dir(0).unwrap().as_angles();
    assert!((ra - 0.25).abs() < 1e-12);
    assert!((dec - 0.5).abs() < 1e-12);
}

#[test]
fn field_direction_converts_fixed_dynamic_reference_to_j2000() {
    let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
    add_vla_antenna(&mut ms);

    let time_mjd_sec = 59_000.5 * 86_400.0;
    let j2000 = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let measures = casa_test_support::deterministic_measures_provider();
    let frame = MeasFrame::new()
        .with_measures(measures.clone())
        .with_epoch(MEpoch::from_mjd(time_mjd_sec / 86_400.0, EpochRef::UTC))
        .with_position(MPosition::new_itrf(VLA_X, VLA_Y, VLA_Z));
    let azel = j2000.convert_to(DirectionRef::AZEL, &frame).unwrap();
    add_field_with_direction(&mut ms, azel, time_mjd_sec);
    TableMeasDesc::new_fixed("PHASE_DIR", MeasureType::Direction, "AZEL")
        .write(ms.subtable_mut(schema::SubtableId::Field).unwrap())
        .unwrap();

    let engine = MsCalEngine::with_measures(&ms, measures).unwrap();
    let recovered = engine.field_direction_j2000(0).unwrap();
    let (ra, dec) = recovered.as_angles();
    assert!((ra - 1.0).abs() < 1e-9, "ra={ra}");
    assert!((dec - 0.5).abs() < 1e-9, "dec={dec}");
}

#[test]
fn field_direction_supports_variable_int_references() {
    let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
    add_vla_antenna(&mut ms);

    let time_mjd_sec = 59_000.5 * 86_400.0;
    let j2000 = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let measures = casa_test_support::deterministic_measures_provider();
    let frame = MeasFrame::new()
        .with_measures(measures.clone())
        .with_epoch(MEpoch::from_mjd(time_mjd_sec / 86_400.0, EpochRef::UTC))
        .with_position(MPosition::new_itrf(VLA_X, VLA_Y, VLA_Z));
    let azel = j2000.convert_to(DirectionRef::AZEL, &frame).unwrap();
    add_field_with_direction(&mut ms, azel, time_mjd_sec);

    let field = ms.subtable_mut(schema::SubtableId::Field).unwrap();
    field
        .add_column(
            ColumnSchema::scalar("PhaseDir_Ref", PrimitiveType::Int32),
            Some(Value::Scalar(ScalarValue::Int32(
                DirectionRef::AZEL.casacore_code(),
            ))),
        )
        .unwrap();
    let (types, codes) = default_direction_ref_map();
    TableMeasDesc::new_variable_int(
        "PHASE_DIR",
        MeasureType::Direction,
        "PhaseDir_Ref",
        types,
        codes,
    )
    .unwrap()
    .write(field)
    .unwrap();

    let engine = MsCalEngine::with_measures(&ms, measures).unwrap();
    let recovered = engine.field_direction_j2000(0).unwrap();
    let (ra, dec) = recovered.as_angles();
    assert!((ra - 1.0).abs() < 1e-9, "ra={ra}");
    assert!((dec - 0.5).abs() < 1e-9, "dec={dec}");
}

#[test]
fn reproject_raw_uvw_matches_casa_fixvis_reference_values() {
    let engine = MsCalEngine::from_parts(
        vec![MPosition::new_itrf(VLA_X, VLA_Y, VLA_Z)],
        vec![
            MDirection::from_angles(
                -1.058_214_942_099_811_3,
                0.702_211_407_924_268_5,
                DirectionRef::J2000,
            ),
            MDirection::from_angles(
                -1.058_214_942_099_811_3,
                0.706_574_731_054_254_4,
                DirectionRef::J2000,
            ),
        ],
        MPosition::new_itrf(VLA_X, VLA_Y, VLA_Z),
        casa_test_support::deterministic_measures_provider(),
    );
    let (uvw_m, phase_shift_m) = engine
        .reproject_raw_uvw_between_fields([24.4234, -31.0309, 17.6013], 1, 0)
        .unwrap();
    // Reference values captured from CASA `fixvis`; this direct transform
    // is intentionally distinct from GridFT's negateUV boundary.
    assert!((uvw_m[0] - 24.4234).abs() < 1.0e-12, "u={}", uvw_m[0]);
    assert!(
        (uvw_m[1] - -30.953_804_692_483_43).abs() < 1.0e-12,
        "v={}",
        uvw_m[1]
    );
    assert!(
        (uvw_m[2] - 17.736_529_862_393_91).abs() < 1.0e-12,
        "w={}",
        uvw_m[2]
    );
    assert!(
        (phase_shift_m - 0.135_229_862_393_907_52).abs() < 1.0e-12,
        "phase_shift_m={phase_shift_m}"
    );
}

#[test]
fn reproject_raw_uvw_matches_casa_fixvis_ra_offset_reference_values() {
    let engine = MsCalEngine::from_parts(
        vec![MPosition::new_itrf(VLA_X, VLA_Y, VLA_Z)],
        vec![
            MDirection::from_angles(
                -1.058_214_942_099_811_3,
                0.702_211_407_924_268_5,
                DirectionRef::J2000,
            ),
            MDirection::from_angles(
                -1.053_851_618_969_825_7,
                0.702_211_407_924_268_5,
                DirectionRef::J2000,
            ),
        ],
        MPosition::new_itrf(VLA_X, VLA_Y, VLA_Z),
        casa_test_support::deterministic_measures_provider(),
    );
    let (uvw_m, phase_shift_m) = engine
        .reproject_raw_uvw_between_fields(
            [
                27.073_056_790_908_41,
                -29.672_968_936_171_65,
                15.993_460_382_965_498,
            ],
            1,
            0,
        )
        .unwrap();
    assert!(
        (uvw_m[0] - 27.042_446_437_092_313).abs() < 1.0e-12,
        "u={}",
        uvw_m[0]
    );
    assert!(
        (uvw_m[1] - -29.749_226_002_013_575).abs() < 1.0e-12,
        "v={}",
        uvw_m[1]
    );
    assert!(
        (uvw_m[2] - 15.903_330_075_501_767).abs() < 1.0e-12,
        "w={}",
        uvw_m[2]
    );
    assert!(
        (phase_shift_m - -0.090_130_307_463_740_37).abs() < 1.0e-12,
        "phase_shift_m={phase_shift_m}"
    );
}

#[test]
fn hour_angle_is_finite() {
    let engine = make_engine();
    // MJD 59000 noon UTC
    let time = 59000.5 * 86400.0;
    let ha = engine.hour_angle(time, 0, 0).unwrap();
    assert!(ha.is_finite(), "HA should be finite, got {ha}");
}

#[test]
fn hour_angle_observatory_is_finite() {
    let engine = make_engine();
    let time = 59000.5 * 86400.0;
    let ha = engine.hour_angle_observatory(time, 0).unwrap();
    assert!(ha.is_finite(), "observatory HA should be finite, got {ha}");
}

#[test]
fn parallactic_angle_is_finite() {
    let engine = make_engine();
    let time = 59000.5 * 86400.0;
    let pa = engine.parallactic_angle(time, 0, 0).unwrap();
    assert!(pa.is_finite(), "PA should be finite, got {pa}");
}

#[test]
fn parallactic_angle_observatory_is_finite() {
    let engine = make_engine();
    let time = 59000.5 * 86400.0;
    let pa = engine.parallactic_angle_observatory(time, 0).unwrap();
    assert!(pa.is_finite(), "observatory PA should be finite, got {pa}");
}

#[test]
fn azel_elevation_positive_for_visible_source() {
    let engine = make_engine();
    // Try several times to find one where the source is up
    let mut found_visible = false;
    for hour in 0..24 {
        let time = (59000.0 + hour as f64 / 24.0) * 86400.0;
        let result = engine.azel(time, 0, 0);
        if let Ok((_az, el)) = result
            && el > 0.0
        {
            found_visible = true;
            break;
        }
    }
    assert!(
        found_visible,
        "Source at Dec=+45° should be visible from VLA at some hour"
    );
}

#[test]
fn observatory_azel_elevation_positive_for_visible_source() {
    let engine = make_engine();
    let mut found_visible = false;
    for hour in 0..24 {
        let time = (59000.0 + hour as f64 / 24.0) * 86400.0;
        let result = engine.azel_observatory(time, 0);
        if let Ok((_az, el)) = result
            && el > 0.0
        {
            found_visible = true;
            break;
        }
    }
    assert!(
        found_visible,
        "Source at Dec=+45° should be visible from the observatory at some hour"
    );
}

#[test]
fn last_monotonically_increases() {
    let engine = make_engine();
    let t0 = 59000.0 * 86400.0;
    let dt = 600.0; // 10 minutes
    let last0 = engine.last(t0, 0).unwrap();
    let last1 = engine.last(t0 + dt, 0).unwrap();
    // LAST should increase (modulo 2π)
    // Over 10 minutes, LAST should increase by ~2.5 minutes of sidereal time
    let diff = (last1 - last0).rem_euclid(2.0 * std::f64::consts::PI);
    assert!(
        diff > 0.0 && diff < std::f64::consts::PI,
        "LAST should increase monotonically, diff={diff}"
    );
}

#[test]
fn uvw_j2000_is_finite() {
    let engine = make_engine();
    let time = 59000.5 * 86400.0;
    let uvw = engine.uvw_j2000(time, 0, 0, 1).unwrap();
    assert!(uvw[0].is_finite());
    assert!(uvw[1].is_finite());
    assert!(uvw[2].is_finite());
    // Baseline is ~100m, so UVW magnitudes should be of that order
    let mag = (uvw[0] * uvw[0] + uvw[1] * uvw[1] + uvw[2] * uvw[2]).sqrt();
    assert!(
        mag > 1.0 && mag < 200.0,
        "UVW magnitude should be ~100m for a 100m baseline, got {mag}"
    );
}

#[test]
fn hadec_returns_angles() {
    let engine = make_engine();
    let time = 59000.5 * 86400.0;
    let (ha, dec) = engine.hadec(time, 0, 0).unwrap();
    assert!(ha.is_finite());
    assert!(dec.is_finite());
    // Dec should be close to +45° (the field direction)
    assert!(
        (dec - std::f64::consts::FRAC_PI_4).abs() < 0.1,
        "Dec should be ~45°, got {}°",
        dec.to_degrees()
    );
}

#[test]
fn engine_public_methods_cover_observatory_and_invalid_index_paths() {
    let engine = make_engine();
    let time = 59000.5 * 86400.0;

    assert_eq!(engine.num_antennas(), 2);
    assert_eq!(engine.num_fields(), 1);
    assert!(engine.field_direction_j2000(0).is_ok());
    assert!(engine.spectral_frame_observatory(time, 0).is_ok());
    assert!(engine.hour_angle(time, 0, 0).unwrap().is_finite());
    assert!(engine.hour_angle_observatory(time, 0).unwrap().is_finite());
    assert!(engine.parallactic_angle(time, 0, 0).unwrap().is_finite());
    assert!(
        engine
            .parallactic_angle_observatory(time, 0)
            .unwrap()
            .is_finite()
    );
    assert!(engine.azel(time, 0, 0).unwrap().0.is_finite());
    assert!(engine.azel_observatory(time, 0).unwrap().0.is_finite());
    assert!(engine.last(time, 0).unwrap().is_finite());
    assert!(engine.last_observatory(time).unwrap().is_finite());
    assert!(engine.hadec(time, 0, 0).unwrap().0.is_finite());
    assert!(engine.uvw_j2000(time, 0, 0, 1).unwrap()[0].is_finite());
    assert!(engine.field_direction_j2000(1).is_err());
    assert!(engine.hour_angle(time, 0, 9).is_err());
}
