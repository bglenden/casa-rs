// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use crate::direction::DirectionCoordinate;
use crate::linear::LinearCoordinate;
use crate::projection::{Projection, ProjectionType};
use crate::spectral::SpectralCoordinate;
use crate::stokes::{StokesCoordinate, StokesType};
use crate::tabular::TabularCoordinate;

use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::epoch::MEpoch;
use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use casa_types::measures::position::MPosition;

fn direction() -> DirectionCoordinate {
    DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [1.0, 0.5],
        [-1e-5, 1e-5],
        [10.0, 20.0],
    )
}

#[test]
fn a_replaced_direction_moves_its_reference_pixel_and_increments() {
    let mut cs = CoordinateSystem::new();
    cs.add_coordinate(direction());
    cs.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
    let before = cs.to_world(&[10.0, 20.0, 0.0]).unwrap();
    let changed = direction()
        .with_reference_pixel([30.0, 40.0])
        .with_increment([-2.5e-6, 2.5e-6]);
    let old = cs.replace_coordinate(0, changed);
    assert_eq!(old.reference_pixel(), vec![10.0, 20.0]);
    // The reference value now sits at the new reference pixel, and a
    // pixel step is a quarter of what it was.
    assert_eq!(cs.to_world(&[30.0, 40.0, 0.0]).unwrap(), before);
    let step = cs.to_world(&[31.0, 40.0, 0.0]).unwrap();
    let old_step = {
        let mut old_cs = CoordinateSystem::new();
        old_cs.add_coordinate(direction());
        old_cs.to_world(&[10.25, 20.0]).unwrap()
    };
    assert!((step[0] - old_step[0]).abs() < 1e-12);
}

#[test]
#[should_panic(expected = "the same axes")]
fn a_replacing_coordinate_keeps_the_axes() {
    let mut cs = CoordinateSystem::new();
    cs.add_coordinate(direction());
    cs.replace_coordinate(0, StokesCoordinate::new(vec![StokesType::I]));
}
use casa_types::measures::{EpochRef, MeasFrame, PositionRef};
use casa_types::{ArrayD, ArrayValue, RecordField, ScalarValue};
use ndarray::IxDyn;

fn quantity_record(value: f64, unit: &str) -> RecordValue {
    RecordValue::new(vec![
        RecordField::new("value", Value::Scalar(ScalarValue::Float64(value))),
        RecordField::new("unit", Value::Scalar(ScalarValue::String(unit.into()))),
    ])
}

fn measure_epoch_record(mjd: f64, refer: &str) -> RecordValue {
    RecordValue::new(vec![
        RecordField::new("type", Value::Scalar(ScalarValue::String("epoch".into()))),
        RecordField::new("refer", Value::Scalar(ScalarValue::String(refer.into()))),
        RecordField::new("m0", Value::Record(quantity_record(mjd, "d"))),
    ])
}

fn measure_position_record(refer: &str, lon: f64, lat: f64, radius: f64) -> RecordValue {
    RecordValue::new(vec![
        RecordField::new(
            "type",
            Value::Scalar(ScalarValue::String("position".into())),
        ),
        RecordField::new("refer", Value::Scalar(ScalarValue::String(refer.into()))),
        RecordField::new("m0", Value::Record(quantity_record(lon, "rad"))),
        RecordField::new("m1", Value::Record(quantity_record(lat, "rad"))),
        RecordField::new("m2", Value::Record(quantity_record(radius, "m"))),
    ])
}

fn matrix_record(values: &[f64], shape: &[usize]) -> Value {
    Value::Array(ArrayValue::Float64(
        ArrayD::from_shape_vec(IxDyn(shape), values.to_vec()).unwrap(),
    ))
}

fn casa_direction_record() -> RecordValue {
    RecordValue::new(vec![
        RecordField::new("system", Value::Scalar(ScalarValue::String("J2000".into()))),
        RecordField::new(
            "projection",
            Value::Scalar(ScalarValue::String("SIN".into())),
        ),
        RecordField::new(
            "projection_parameters",
            Value::Array(ArrayValue::from_f64_vec(vec![0.0, 0.0])),
        ),
        RecordField::new(
            "crval",
            Value::Array(ArrayValue::from_f64_vec(vec![4.02298, 0.08843])),
        ),
        RecordField::new(
            "crpix",
            Value::Array(ArrayValue::from_f64_vec(vec![128.0, 128.0])),
        ),
        RecordField::new(
            "cdelt",
            Value::Array(ArrayValue::from_f64_vec(vec![-7.27221e-05, 7.27221e-05])),
        ),
        RecordField::new("pc", matrix_record(&[1.0, 0.0, 0.0, 1.0], &[2, 2])),
        RecordField::new(
            "axes",
            Value::Array(ArrayValue::from_string_vec(vec![
                "Right Ascension".into(),
                "Declination".into(),
            ])),
        ),
        RecordField::new(
            "units",
            Value::Array(ArrayValue::from_string_vec(vec![
                "rad".into(),
                "rad".into(),
            ])),
        ),
        RecordField::new(
            "conversionSystem",
            Value::Scalar(ScalarValue::String("J2000".into())),
        ),
        RecordField::new("longpole", Value::Scalar(ScalarValue::Float64(180.0))),
        RecordField::new("latpole", Value::Scalar(ScalarValue::Float64(5.06667))),
    ])
}

fn casa_stokes_record() -> RecordValue {
    RecordValue::new(vec![
        RecordField::new(
            "axes",
            Value::Array(ArrayValue::from_string_vec(vec!["Stokes".into()])),
        ),
        RecordField::new(
            "stokes",
            Value::Array(ArrayValue::from_string_vec(vec!["I".into()])),
        ),
        RecordField::new("crval", Value::Array(ArrayValue::from_f64_vec(vec![1.0]))),
        RecordField::new("crpix", Value::Array(ArrayValue::from_f64_vec(vec![0.0]))),
        RecordField::new("cdelt", Value::Array(ArrayValue::from_f64_vec(vec![1.0]))),
        RecordField::new("pc", matrix_record(&[1.0], &[1, 1])),
    ])
}

fn casa_spectral_wcs_record() -> RecordValue {
    let wcs = RecordValue::new(vec![
        RecordField::new("crval", Value::Scalar(ScalarValue::Float64(1.41279e9))),
        RecordField::new("crpix", Value::Scalar(ScalarValue::Float64(0.0))),
        RecordField::new("cdelt", Value::Scalar(ScalarValue::Float64(24_414.1))),
        RecordField::new("pc", Value::Scalar(ScalarValue::Float64(1.0))),
        RecordField::new("ctype", Value::Scalar(ScalarValue::String("FREQ".into()))),
    ]);
    RecordValue::new(vec![
        RecordField::new("version", Value::Scalar(ScalarValue::Int32(2))),
        RecordField::new("system", Value::Scalar(ScalarValue::String("LSRK".into()))),
        RecordField::new("restfreq", Value::Scalar(ScalarValue::Float64(1.42041e9))),
        RecordField::new(
            "restfreqs",
            Value::Array(ArrayValue::from_f64_vec(vec![1.42041e9])),
        ),
        RecordField::new("velType", Value::Scalar(ScalarValue::Int32(0))),
        RecordField::new("nativeType", Value::Scalar(ScalarValue::Int32(0))),
        RecordField::new("velUnit", Value::Scalar(ScalarValue::String("km/s".into()))),
        RecordField::new("waveUnit", Value::Scalar(ScalarValue::String("mm".into()))),
        RecordField::new(
            "formatUnit",
            Value::Scalar(ScalarValue::String(String::new())),
        ),
        RecordField::new("wcs", Value::Record(wcs)),
        RecordField::new("unit", Value::Scalar(ScalarValue::String("Hz".into()))),
        RecordField::new(
            "name",
            Value::Scalar(ScalarValue::String("Frequency".into())),
        ),
    ])
}

fn casa_spectral_tabular_record() -> RecordValue {
    let tabular = RecordValue::new(vec![
        RecordField::new(
            "crval",
            Value::Array(ArrayValue::from_f64_vec(vec![1.15022e11])),
        ),
        RecordField::new("crpix", Value::Array(ArrayValue::from_f64_vec(vec![0.0]))),
        RecordField::new(
            "cdelt",
            Value::Array(ArrayValue::from_f64_vec(vec![6.25005e6])),
        ),
        RecordField::new("pc", matrix_record(&[1.0], &[1, 1])),
        RecordField::new(
            "axes",
            Value::Array(ArrayValue::from_string_vec(vec!["Frequency".into()])),
        ),
        RecordField::new(
            "units",
            Value::Array(ArrayValue::from_string_vec(vec!["Hz".into()])),
        ),
        RecordField::new(
            "pixelvalues",
            Value::Array(ArrayValue::from_f64_vec(Vec::new())),
        ),
        RecordField::new(
            "worldvalues",
            Value::Array(ArrayValue::from_f64_vec(Vec::new())),
        ),
    ]);
    RecordValue::new(vec![
        RecordField::new("system", Value::Scalar(ScalarValue::String("LSRD".into()))),
        RecordField::new("restfreq", Value::Scalar(ScalarValue::Float64(1.15271e11))),
        RecordField::new(
            "restfreqs",
            Value::Array(ArrayValue::from_f64_vec(vec![1.15271e11])),
        ),
        RecordField::new("velType", Value::Scalar(ScalarValue::Int32(0))),
        RecordField::new("velUnit", Value::Scalar(ScalarValue::String("km/s".into()))),
        RecordField::new(
            "formatUnit",
            Value::Scalar(ScalarValue::String(String::new())),
        ),
        RecordField::new("tabular", Value::Record(tabular)),
    ])
}

fn casa_spectral_tabular_record_with_conversion() -> RecordValue {
    let mut record = casa_spectral_tabular_record();
    let conversion = RecordValue::new(vec![
        RecordField::new(
            "direction",
            Value::Record(RecordValue::new(vec![
                RecordField::new(
                    "type",
                    Value::Scalar(ScalarValue::String("direction".into())),
                ),
                RecordField::new("refer", Value::Scalar(ScalarValue::String("J2000".into()))),
                RecordField::new(
                    "m0",
                    Value::Record(quantity_record(-2.8940293347227444, "rad")),
                ),
                RecordField::new(
                    "m1",
                    Value::Record(quantity_record(0.3784702849718404, "rad")),
                ),
            ])),
        ),
        RecordField::new(
            "position",
            Value::Record(measure_position_record(
                "WGS84",
                -2.1200320498502676,
                0.7123949192959743,
                1021.0,
            )),
        ),
        RecordField::new(
            "epoch",
            Value::Record(measure_epoch_record(50_919.14846423176, "UTC")),
        ),
        RecordField::new("system", Value::Scalar(ScalarValue::String("LSRK".into()))),
    ]);
    record.upsert("conversion", Value::Record(conversion));
    record
}

fn casa_spectral_non_linear_tabular_record() -> RecordValue {
    let tabular = RecordValue::new(vec![
        RecordField::new(
            "crval",
            Value::Array(ArrayValue::from_f64_vec(vec![1.15022e11])),
        ),
        RecordField::new("crpix", Value::Array(ArrayValue::from_f64_vec(vec![0.0]))),
        RecordField::new(
            "cdelt",
            Value::Array(ArrayValue::from_f64_vec(vec![6.25005e6])),
        ),
        RecordField::new("pc", matrix_record(&[1.0], &[1, 1])),
        RecordField::new(
            "axes",
            Value::Array(ArrayValue::from_string_vec(vec!["Frequency".into()])),
        ),
        RecordField::new(
            "units",
            Value::Array(ArrayValue::from_string_vec(vec!["Hz".into()])),
        ),
        RecordField::new(
            "pixelvalues",
            Value::Array(ArrayValue::from_f64_vec(vec![0.0, 1.0, 3.0, 4.0])),
        ),
        RecordField::new(
            "worldvalues",
            Value::Array(ArrayValue::from_f64_vec(vec![
                1.15022e11,
                1.1502825e11,
                1.1504075e11,
                1.15047e11,
            ])),
        ),
    ]);
    RecordValue::new(vec![
        RecordField::new("system", Value::Scalar(ScalarValue::String("LSRD".into()))),
        RecordField::new("restfreq", Value::Scalar(ScalarValue::Float64(1.15271e11))),
        RecordField::new(
            "restfreqs",
            Value::Array(ArrayValue::from_f64_vec(vec![1.15271e11])),
        ),
        RecordField::new("velType", Value::Scalar(ScalarValue::Int32(0))),
        RecordField::new("velUnit", Value::Scalar(ScalarValue::String("km/s".into()))),
        RecordField::new(
            "formatUnit",
            Value::Scalar(ScalarValue::String(String::new())),
        ),
        RecordField::new("tabular", Value::Record(tabular)),
    ])
}

fn casa_pointing_center_record(longitude_rad: f64, latitude_rad: f64) -> RecordValue {
    RecordValue::new(vec![
        RecordField::new(
            "value",
            Value::Array(ArrayValue::from_f64_vec(vec![longitude_rad, latitude_rad])),
        ),
        RecordField::new("initial", Value::Scalar(ScalarValue::Bool(false))),
    ])
}

fn casa_style_coords_with_wcs_spectral() -> RecordValue {
    RecordValue::new(vec![
        RecordField::new(
            "telescope",
            Value::Scalar(ScalarValue::String("VLA".into())),
        ),
        RecordField::new(
            "observer",
            Value::Scalar(ScalarValue::String("TEST".into())),
        ),
        RecordField::new(
            "obsdate",
            Value::Record(measure_epoch_record(49_820.4, "TAI")),
        ),
        RecordField::new(
            "telescopeposition",
            Value::Record(measure_position_record(
                "ITRF", -1.87829, 0.591675, 6.37358e6,
            )),
        ),
        RecordField::new(
            "pointingcenter",
            Value::Record(casa_pointing_center_record(4.02298, 0.08843)),
        ),
        RecordField::new("direction0", Value::Record(casa_direction_record())),
        RecordField::new(
            "worldmap0",
            Value::Array(ArrayValue::from_i32_vec(vec![0, 1])),
        ),
        RecordField::new(
            "pixelmap0",
            Value::Array(ArrayValue::from_i32_vec(vec![0, 1])),
        ),
        RecordField::new("stokes1", Value::Record(casa_stokes_record())),
        RecordField::new("worldmap1", Value::Array(ArrayValue::from_i32_vec(vec![2]))),
        RecordField::new("pixelmap1", Value::Array(ArrayValue::from_i32_vec(vec![2]))),
        RecordField::new("spectral2", Value::Record(casa_spectral_wcs_record())),
        RecordField::new("worldmap2", Value::Array(ArrayValue::from_i32_vec(vec![3]))),
        RecordField::new("pixelmap2", Value::Array(ArrayValue::from_i32_vec(vec![3]))),
    ])
}

fn casa_style_coords_with_tabular_spectral() -> RecordValue {
    RecordValue::new(vec![
        RecordField::new(
            "telescope",
            Value::Scalar(ScalarValue::String("BIMA".into())),
        ),
        RecordField::new(
            "observer",
            Value::Scalar(ScalarValue::String(String::new())),
        ),
        RecordField::new(
            "obsdate",
            Value::Record(measure_epoch_record(50_919.1, "UTC")),
        ),
        RecordField::new(
            "pointingcenter",
            Value::Record(casa_pointing_center_record(
                -2.8940293347227444,
                0.3784702849718404,
            )),
        ),
        RecordField::new("direction0", Value::Record(casa_direction_record())),
        RecordField::new(
            "worldmap0",
            Value::Array(ArrayValue::from_i32_vec(vec![0, 1])),
        ),
        RecordField::new(
            "pixelmap0",
            Value::Array(ArrayValue::from_i32_vec(vec![0, 1])),
        ),
        RecordField::new("stokes1", Value::Record(casa_stokes_record())),
        RecordField::new("worldmap1", Value::Array(ArrayValue::from_i32_vec(vec![2]))),
        RecordField::new("pixelmap1", Value::Array(ArrayValue::from_i32_vec(vec![2]))),
        RecordField::new("spectral2", Value::Record(casa_spectral_tabular_record())),
        RecordField::new("worldmap2", Value::Array(ArrayValue::from_i32_vec(vec![3]))),
        RecordField::new("pixelmap2", Value::Array(ArrayValue::from_i32_vec(vec![3]))),
    ])
}

fn casa_style_coords_with_non_linear_tabular_spectral() -> RecordValue {
    RecordValue::new(vec![
        RecordField::new(
            "telescope",
            Value::Scalar(ScalarValue::String("BIMA".into())),
        ),
        RecordField::new(
            "observer",
            Value::Scalar(ScalarValue::String(String::new())),
        ),
        RecordField::new(
            "obsdate",
            Value::Record(measure_epoch_record(50_919.1, "UTC")),
        ),
        RecordField::new(
            "pointingcenter",
            Value::Record(casa_pointing_center_record(
                -2.8940293347227444,
                0.3784702849718404,
            )),
        ),
        RecordField::new("direction0", Value::Record(casa_direction_record())),
        RecordField::new(
            "worldmap0",
            Value::Array(ArrayValue::from_i32_vec(vec![0, 1])),
        ),
        RecordField::new(
            "pixelmap0",
            Value::Array(ArrayValue::from_i32_vec(vec![0, 1])),
        ),
        RecordField::new("stokes1", Value::Record(casa_stokes_record())),
        RecordField::new("worldmap1", Value::Array(ArrayValue::from_i32_vec(vec![2]))),
        RecordField::new("pixelmap1", Value::Array(ArrayValue::from_i32_vec(vec![2]))),
        RecordField::new(
            "spectral2",
            Value::Record(casa_spectral_non_linear_tabular_record()),
        ),
        RecordField::new("worldmap2", Value::Array(ArrayValue::from_i32_vec(vec![3]))),
        RecordField::new("pixelmap2", Value::Array(ArrayValue::from_i32_vec(vec![3]))),
    ])
}

fn casa_style_coords_with_tabular_spectral_conversion() -> RecordValue {
    RecordValue::new(vec![
        RecordField::new(
            "telescope",
            Value::Scalar(ScalarValue::String("BIMA".into())),
        ),
        RecordField::new(
            "observer",
            Value::Scalar(ScalarValue::String(String::new())),
        ),
        RecordField::new(
            "obsdate",
            Value::Record(measure_epoch_record(50_919.14846423176, "UTC")),
        ),
        RecordField::new(
            "pointingcenter",
            Value::Record(casa_pointing_center_record(
                -2.8940293347227444,
                0.3784702849718404,
            )),
        ),
        RecordField::new("direction0", Value::Record(casa_direction_record())),
        RecordField::new(
            "worldmap0",
            Value::Array(ArrayValue::from_i32_vec(vec![0, 1])),
        ),
        RecordField::new(
            "pixelmap0",
            Value::Array(ArrayValue::from_i32_vec(vec![0, 1])),
        ),
        RecordField::new("stokes1", Value::Record(casa_stokes_record())),
        RecordField::new("worldmap1", Value::Array(ArrayValue::from_i32_vec(vec![2]))),
        RecordField::new("pixelmap1", Value::Array(ArrayValue::from_i32_vec(vec![2]))),
        RecordField::new(
            "spectral2",
            Value::Record(casa_spectral_tabular_record_with_conversion()),
        ),
        RecordField::new("worldmap2", Value::Array(ArrayValue::from_i32_vec(vec![3]))),
        RecordField::new("pixelmap2", Value::Array(ArrayValue::from_i32_vec(vec![3]))),
    ])
}

fn make_typical_system() -> CoordinateSystem {
    let mut cs = CoordinateSystem::new();

    // Direction-like: 2-axis linear as placeholder
    let dir = LinearCoordinate::new(
        2,
        vec!["Right Ascension".into(), "Declination".into()],
        vec!["rad".into(), "rad".into()],
    );
    cs.add_coordinate(dir);

    // Spectral
    let spec = SpectralCoordinate::new(FrequencyRef::LSRK, 1.42e9, 1e6, 0.0, 1.42e9);
    cs.add_coordinate(spec);

    // Stokes
    let stokes = StokesCoordinate::new(vec![
        StokesType::I,
        StokesType::Q,
        StokesType::U,
        StokesType::V,
    ]);
    cs.add_coordinate(stokes);

    cs
}

#[test]
fn axis_counts() {
    let cs = make_typical_system();
    assert_eq!(cs.n_coordinates(), 3);
    assert_eq!(cs.n_pixel_axes(), 4); // 2 + 1 + 1
    assert_eq!(cs.n_world_axes(), 4);
}

#[test]
fn find_coordinate() {
    let cs = make_typical_system();
    assert_eq!(cs.find_coordinate(CoordinateType::Linear), Some(0));
    assert_eq!(cs.find_coordinate(CoordinateType::Spectral), Some(1));
    assert_eq!(cs.find_coordinate(CoordinateType::Stokes), Some(2));
    assert_eq!(cs.find_coordinate(CoordinateType::Direction), None);
    assert_eq!(cs.find_coordinate(CoordinateType::Tabular), None);
}

#[test]
fn to_world_full() {
    let cs = make_typical_system();
    // pixel: [dir_x, dir_y, spec_chan, stokes_idx]
    let pixel = vec![0.0, 0.0, 0.0, 0.0];
    let world = cs.to_world(&pixel).unwrap();
    assert_eq!(world.len(), 4);
    // Linear: world = pixel (default)
    assert!((world[0]).abs() < 1e-12);
    assert!((world[1]).abs() < 1e-12);
    // Spectral: crval at pixel 0
    assert!((world[2] - 1.42e9).abs() < 1.0);
    // Stokes: I = 1
    assert!((world[3] - 1.0).abs() < 1e-12);
}

#[test]
fn roundtrip() {
    let cs = make_typical_system();
    let pixel = vec![10.0, 20.0, 50.0, 2.0];
    let world = cs.to_world(&pixel).unwrap();
    let back = cs.to_pixel(&world).unwrap();
    for i in 0..4 {
        assert!(
            (back[i] - pixel[i]).abs() < 1e-8,
            "axis {i}: {} vs {}",
            back[i],
            pixel[i]
        );
    }
}

#[test]
fn dimension_mismatch() {
    let cs = make_typical_system();
    assert!(cs.to_world(&[1.0, 2.0]).is_err());
    assert!(cs.to_pixel(&[1.0]).is_err());
}

#[test]
fn empty_system() {
    let cs = CoordinateSystem::new();
    assert_eq!(cs.n_coordinates(), 0);
    assert_eq!(cs.n_pixel_axes(), 0);
    assert_eq!(cs.n_world_axes(), 0);
    let world = cs.to_world(&[]).unwrap();
    assert!(world.is_empty());
}

#[test]
fn obs_info_access() {
    let obs = ObsInfo::new("ALMA").with_observer("Test");
    let cs = CoordinateSystem::new().with_obs_info(obs);
    assert_eq!(cs.obs_info().telescope, "ALMA");
    assert_eq!(cs.obs_info().observer, "Test");
}

#[test]
fn obs_info_mut() {
    let mut cs = CoordinateSystem::new();
    cs.obs_info_mut().telescope = "VLA".into();
    assert_eq!(cs.obs_info().telescope, "VLA");
}

#[test]
fn to_record_has_coordinates() {
    let cs = make_typical_system();
    let rec = cs.to_record();
    assert!(rec.get("linear0").is_some());
    assert!(rec.get("spectral1").is_some());
    assert!(rec.get("stokes2").is_some());
    assert!(rec.get("worldmap0").is_some());
    assert!(rec.get("pixelmap2").is_some());
}

#[test]
fn from_record_empty() {
    let rec = RecordValue::default();
    let cs = CoordinateSystem::from_record(&rec).unwrap();
    assert_eq!(cs.n_coordinates(), 0);
}

#[test]
fn from_record_with_obsinfo() {
    let mut rec = RecordValue::default();
    let mut obs_rec = RecordValue::default();
    obs_rec.upsert(
        "telescope",
        Value::Scalar(ScalarValue::String("MeerKAT".into())),
    );
    rec.upsert("obsinfo", Value::Record(obs_rec));
    let cs = CoordinateSystem::from_record(&rec).unwrap();
    assert_eq!(cs.obs_info().telescope, "MeerKAT");
}

#[test]
fn coordinate_access() {
    let cs = make_typical_system();
    assert_eq!(cs.coordinate(0).coordinate_type(), CoordinateType::Linear);
    assert_eq!(cs.coordinate(1).coordinate_type(), CoordinateType::Spectral);
    assert_eq!(cs.coordinate(2).coordinate_type(), CoordinateType::Stokes);
}

#[test]
fn clone_preserves_coordinates() {
    let cs = make_typical_system();
    let cloned = cs.clone();
    assert_eq!(cloned.n_coordinates(), 3);
    assert_eq!(
        cloned.coordinate(0).coordinate_type(),
        CoordinateType::Linear,
    );
}

#[test]
fn default_is_empty() {
    let cs = CoordinateSystem::default();
    assert_eq!(cs.n_coordinates(), 0);
}

#[test]
fn from_record_roundtrips_mixed_system() {
    let mut cs = CoordinateSystem::new();
    let dir = DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [0.0, std::f64::consts::FRAC_PI_4],
        [-1e-4, 1e-4],
        [256.0, 256.0],
    );
    cs.add_coordinate(dir);
    cs.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        1.42e9,
        1.0e6,
        0.0,
        1.42040575e9,
    ));
    cs.add_coordinate(StokesCoordinate::new(vec![StokesType::I, StokesType::Q]));
    cs.add_coordinate(TabularCoordinate::new(
        vec![0.0, 1.0, 2.0],
        vec![100.0, 200.0, 300.0],
        "Velocity",
        "km/s",
    ));

    let restored = CoordinateSystem::from_record(&cs.to_record()).unwrap();

    assert_eq!(restored.n_coordinates(), 4);
    assert_eq!(
        restored.coordinate(0).coordinate_type(),
        CoordinateType::Direction
    );
    assert_eq!(
        restored.coordinate(1).coordinate_type(),
        CoordinateType::Spectral
    );
    assert_eq!(
        restored.coordinate(2).coordinate_type(),
        CoordinateType::Stokes
    );
    assert_eq!(
        restored.coordinate(3).coordinate_type(),
        CoordinateType::Tabular
    );
    assert_eq!(restored.n_pixel_axes(), 5);

    let pixel = vec![256.5, 255.5, 4.0, 1.0, 1.5];
    let world = restored.to_world(&pixel).unwrap();
    let back = restored.to_pixel(&world).unwrap();
    assert_eq!(back.len(), pixel.len());
    for (index, (restored_value, original)) in back.iter().zip(pixel.iter()).enumerate() {
        assert!(
            (restored_value - original).abs() < 1e-6,
            "axis {index}: {} vs {}",
            restored_value,
            original
        );
    }
}

#[test]
fn from_record_rejects_unknown_coordinate_type() {
    let mut rec = RecordValue::default();
    rec.upsert("ncoordinates", Value::Scalar(ScalarValue::Int32(2)));
    let mut unknown = RecordValue::default();
    unknown.upsert(
        "coordinate_type",
        Value::Scalar(ScalarValue::String("Unknown".into())),
    );
    rec.upsert("coordinate0", Value::Record(unknown));
    rec.upsert(
        "coordinate1",
        Value::Record(LinearCoordinate::new(1, vec!["X".into()], vec!["m".into()]).to_record()),
    );

    assert!(matches!(
        CoordinateSystem::from_record(&rec),
        Err(CoordinateError::InvalidRecord(_))
    ));
}

#[test]
fn from_record_rejects_invalid_coordinate_record() {
    let mut rec = RecordValue::default();
    rec.upsert("ncoordinates", Value::Scalar(ScalarValue::Int32(2)));
    let mut broken = RecordValue::default();
    broken.upsert(
        "coordinate_type",
        Value::Scalar(ScalarValue::String("Spectral".into())),
    );
    rec.upsert("coordinate0", Value::Record(broken));
    rec.upsert(
        "coordinate1",
        Value::Record(StokesCoordinate::new(vec![StokesType::I]).to_record()),
    );

    assert!(matches!(
        CoordinateSystem::from_record(&rec),
        Err(CoordinateError::InvalidRecord(_))
    ));
}

#[test]
fn from_record_parses_casa_style_wcs_coords() {
    let cs = CoordinateSystem::from_record(&casa_style_coords_with_wcs_spectral()).unwrap();

    assert_eq!(cs.obs_info().telescope, "VLA");
    assert_eq!(cs.obs_info().observer, "TEST");
    assert_eq!(cs.obs_info().date.as_ref().unwrap().refer(), EpochRef::TAI);
    assert_eq!(
        cs.obs_info().telescope_position.as_ref().unwrap().refer(),
        PositionRef::ITRF
    );
    assert_eq!(cs.obs_info().pointing_center_rad, [4.02298, 0.08843]);
    assert!(!cs.obs_info().pointing_center_initial);
    assert_eq!(cs.n_coordinates(), 3);
    assert_eq!(
        cs.coordinate(0).coordinate_type(),
        CoordinateType::Direction
    );
    assert_eq!(cs.coordinate(1).coordinate_type(), CoordinateType::Stokes);
    assert_eq!(cs.coordinate(2).coordinate_type(), CoordinateType::Spectral);

    let world = cs.to_world(&[128.0, 128.0, 0.0, 0.0]).unwrap();
    assert_eq!(world.len(), 4);
    assert!((world[0] - 4.02298).abs() < 1e-8);
    assert!((world[1] - 0.08843).abs() < 1e-8);
    assert!((world[2] - 1.0).abs() < 1e-8);
    assert!((world[3] - 1.41279e9).abs() < 1.0);
}

#[test]
fn from_record_parses_casa_style_tabular_spectral_coords() {
    let cs = CoordinateSystem::from_record(&casa_style_coords_with_tabular_spectral()).unwrap();

    assert_eq!(cs.obs_info().telescope, "BIMA");
    assert_eq!(cs.obs_info().date.as_ref().unwrap().refer(), EpochRef::UTC);
    assert_eq!(
        cs.obs_info().pointing_center_rad,
        [-2.8940293347227444, 0.3784702849718404]
    );
    assert!(!cs.obs_info().pointing_center_initial);
    assert_eq!(cs.n_coordinates(), 3);
    assert_eq!(
        cs.coordinate(0).coordinate_type(),
        CoordinateType::Direction
    );
    assert_eq!(cs.coordinate(1).coordinate_type(), CoordinateType::Stokes);
    assert_eq!(cs.coordinate(2).coordinate_type(), CoordinateType::Spectral);

    let world = cs.to_world(&[128.0, 128.0, 0.0, 0.0]).unwrap();
    assert_eq!(world.len(), 4);
    assert!((world[3] - 1.15022e11).abs() < 10.0);
}

#[test]
fn from_record_parses_non_linear_casa_style_tabular_spectral_coords() {
    let cs = CoordinateSystem::from_record(&casa_style_coords_with_non_linear_tabular_spectral())
        .unwrap();

    assert_eq!(cs.obs_info().telescope, "BIMA");
    assert_eq!(cs.n_coordinates(), 3);
    assert_eq!(cs.coordinate(2).coordinate_type(), CoordinateType::Spectral);

    let world = cs.to_world(&[128.0, 128.0, 0.0, 2.0]).unwrap();
    assert_eq!(world.len(), 4);
    // Casacore reconstructs a spectral lookup on consecutive channels.
    assert!((world[3] - 1.1504075e11).abs() < 10.0);

    let pixel = cs.to_pixel(&[4.02298, 0.08843, 1.0, 1.1504075e11]).unwrap();
    assert!((pixel[3] - 2.0).abs() < 1e-10);
}

#[test]
fn from_record_applies_casa_style_spectral_conversion_state() {
    let cs = CoordinateSystem::from_record(&casa_style_coords_with_tabular_spectral_conversion())
        .unwrap();

    let expected = MFrequency::new(1.15022e11, FrequencyRef::LSRD)
        .convert_to(
            FrequencyRef::LSRK,
            &MeasFrame::new()
                .with_direction(MDirection::from_angles(
                    -2.8940293347227444,
                    0.3784702849718404,
                    DirectionRef::J2000,
                ))
                .with_position(MPosition::new_wgs84(
                    -2.1200320498502676,
                    0.7123949192959743,
                    1021.0,
                ))
                .with_epoch(MEpoch::from_mjd(50_919.14846423176, EpochRef::UTC)),
        )
        .unwrap()
        .hz();

    let world = cs.to_world(&[128.0, 128.0, 0.0, 0.0]).unwrap();
    assert_eq!(world.len(), 4);
    assert!((world[3] - expected).abs() < 1.0);

    let pixel = cs.to_pixel(&[4.02298, 0.08843, 1.0, expected]).unwrap();
    assert!((pixel[3] - 0.0).abs() < 1e-6);
}

#[test]
fn coupled_linear_slice_uses_independent_hidden_pixel_and_world_values() {
    let mut original = CoordinateSystem::new();
    original.add_coordinate(
        LinearCoordinate::new(2, vec![], vec![])
            .with_pc_matrix(ndarray::array![[1.0, 0.5], [0.25, 1.0]]),
    );
    let mut record = original.to_record();
    record.upsert(
        "pixelmap0",
        Value::Array(ArrayValue::from_i32_vec(vec![-1, 0])),
    );
    record.upsert(
        "pixelreplace0",
        Value::Array(ArrayValue::from_f64_vec(vec![2.0, 0.0])),
    );
    record.upsert(
        "worldmap0",
        Value::Array(ArrayValue::from_i32_vec(vec![1, 0])),
    );
    let pixel_only = CoordinateSystem::from_record(&record).unwrap();
    assert_eq!(pixel_only.pixel_axes(0), [None, Some(0)]);
    assert_eq!(pixel_only.world_axes(0), [Some(1), Some(0)]);
    assert_eq!(pixel_only.world_axis_to_pixel(1), None);
    assert_eq!(pixel_only.pixel_axis_to_world(0), Some(0));
    assert_eq!(pixel_only.to_world(&[3.0]).unwrap(), [3.5, 3.5]);
    assert_eq!(pixel_only.to_pixel(&[3.5, 3.5]).unwrap(), [3.0]);

    record.upsert(
        "worldmap0",
        Value::Array(ArrayValue::from_i32_vec(vec![-1, 0])),
    );
    record.upsert(
        "worldreplace0",
        Value::Array(ArrayValue::from_f64_vec(vec![10.0, 0.0])),
    );
    let slice = CoordinateSystem::from_record(&record).unwrap();
    assert_eq!(slice.to_world(&[3.0]).unwrap(), [3.5]);
    let world = ndarray::array![[3.5, 10.0]];
    let batch = slice.to_pixel_columns(world.view()).unwrap();
    for (column, expected) in [8.0 / 7.0, 60.0 / 7.0].into_iter().enumerate() {
        let scalar = slice.to_pixel(&[world[[0, column]]]).unwrap()[0];
        assert!((scalar - expected).abs() < 1e-12);
        assert_eq!(batch[[0, column]], scalar);
    }
    for key in ["pixelmap0", "worldmap0", "pixelreplace0", "worldreplace0"] {
        assert_eq!(slice.to_record().get(key), record.get(key));
    }
}

#[test]
fn repeated_axis_removals_keep_duplicate_signed_markers() {
    let mut original = CoordinateSystem::new();
    original.add_coordinate(LinearCoordinate::new(3, vec![], vec![]));
    let mut record = original.to_record();
    for key in ["pixelmap0", "worldmap0"] {
        record.upsert(key, Value::Array(ArrayValue::from_i32_vec(vec![-1, -1, 0])));
    }
    let restored = CoordinateSystem::from_record(&record).unwrap();
    assert_eq!(restored.pixel_axes(0), [None, None, Some(0)]);
    assert_eq!(restored.to_world(&[3.0]).unwrap(), [3.0]);
    assert_eq!(
        restored.to_record().get("pixelmap0"),
        record.get("pixelmap0")
    );
    record.remove("pixelreplace0");
    assert!(CoordinateSystem::from_record(&record).is_err());
}

#[test]
fn transpose_and_removals_keep_local_model_and_signed_markers() {
    let mut cs = CoordinateSystem::new();
    cs.add_coordinate(LinearCoordinate::new(3, vec![], vec![]));
    cs.transpose_axes(&[2, 0, 1], &[0, 2, 1]).unwrap();
    assert_eq!(
        cs.to_world(&[11.0, 23.0, 35.0]).unwrap(),
        [23.0, 11.0, 35.0]
    );
    cs.remove_pixel_axis(0, 4.0).unwrap();
    cs.remove_world_axis(1, 5.0).unwrap();
    cs.remove_world_axis(0, 6.0).unwrap();
    assert_eq!(cs.pixel_axes(0), [None, Some(0), None]);
    assert_eq!(cs.world_axes(0), [None, Some(0), None]);
    assert_eq!(cs.to_world(&[9.0]).unwrap(), [9.0]);
    let record = cs.to_record();
    assert_eq!(
        crate::record_utils::get_required_vec_i32(&record, "pixelmap0").unwrap(),
        [-1, 0, -1]
    );
    assert_eq!(
        crate::record_utils::get_required_vec_f64(&record, "pixelreplace0").unwrap(),
        [4.0, 0.0, 6.0]
    );
    assert_eq!(
        crate::record_utils::get_required_vec_i32(&record, "worldmap0").unwrap(),
        [-2, 0, -1]
    );
    assert!(cs.transpose_axes(&[1], &[0]).is_err());
    assert_eq!(cs.to_record(), record);
}

#[test]
fn direction_replacements_keep_record_units_and_follow_each_edit_kind() {
    let direction = DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [0.2, -0.3],
        [-0.01, 0.01],
        [2.0, 3.0],
    )
    .with_world_units(["deg", "deg"])
    .unwrap();
    let mut original = CoordinateSystem::new();
    original.add_coordinate(direction.clone());
    let replacement = 12.0_f64;
    original.remove_world_axis(0, replacement).unwrap();
    let world = -0.29;
    let expected = direction
        .world_to_pixel([replacement.to_radians(), world])
        .unwrap()[1];
    let mut record = original.to_record();
    let restored = CoordinateSystem::from_record(&record).unwrap();
    assert!((restored.to_pixel(&[world]).unwrap()[0] - expected).abs() < 1e-12);
    let before = record.get("worldreplace0").cloned();
    let (mut edited, _) =
        CoordinateSystem::set_world_axis_unit_in_record(&mut record, 0, "rad").unwrap();
    assert_eq!(record.get("worldreplace0"), before.as_ref());
    let (reference_edit, _) =
        CoordinateSystem::set_world_reference_value_in_record(&mut record, 0, -0.31).unwrap();
    assert_eq!(
        reference_edit.to_record().get("worldreplace0"),
        before.as_ref()
    );
    edited.replace_coordinate(0, direction.with_world_units(["arcmin", "rad"]).unwrap());
    let saved = edited.to_record();
    let replacements = crate::record_utils::get_required_vec_f64(&saved, "worldreplace0").unwrap();
    assert!((replacements[0] - 720.0).abs() < 1e-12);
    assert!((edited.to_pixel(&[world]).unwrap()[0] - expected).abs() < 1e-12);
}

#[test]
fn decoded_contiguous_coordinates_keep_the_slice_transform_path() {
    let original = make_typical_system();
    let restored = CoordinateSystem::from_record(&original.to_record()).unwrap();
    assert!(restored.layout.as_ref().unwrap().contiguous);
}

#[test]
fn replacement_preserves_the_old_default_values_before_first_serialization() {
    let initial =
        LinearCoordinate::new(1, vec![], vec!["m".into()]).with_reference_value(vec![10.0]);
    let replacement = initial.clone().with_reference_value(vec![20.0]);
    let mut built = CoordinateSystem::new();
    built.add_coordinate(initial);
    let mut restored = CoordinateSystem::from_record(&built.to_record()).unwrap();
    for system in [&mut built, &mut restored] {
        system.replace_coordinate(0, replacement.clone());
        let record = system.to_record();
        assert_eq!(
            crate::record_utils::get_required_vec_f64(&record, "worldreplace0").unwrap(),
            [10.0]
        );
        assert_eq!(system.to_world(&[0.0]).unwrap(), [20.0]);
    }
}
