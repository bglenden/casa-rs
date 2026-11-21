// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[test]
fn rebuilding_a_projection_keeps_the_removed_direction_axis() {
    let mut direction = DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [0.4, 0.6],
        [-1e-3, 1e-3],
        [0.0, 0.0],
    );
    direction.remove_record_axis(0, 13.0, 0.4).unwrap();
    let rebuilt = direction
        .with_rebuilt_projection(Projection::new(ProjectionType::TAN))
        .unwrap();
    assert_eq!(rebuilt.n_pixel_axes(), 1);
    assert_eq!(rebuilt.n_world_axes(), 1);
    let removed = rebuilt.removed_axis().unwrap();
    assert_eq!(removed.index, 0);
    assert_eq!(removed.pixel, 13.0);
    assert_eq!(removed.world_rad, 0.4);
}
use crate::projection::ProjectionType;

const TOL: f64 = 1e-10;

/// An analytic projection whose reference lies on its native equator takes its
/// reference pixel to its reference direction south
/// of the celestial equator too, where WCSLIB's default native pole
/// longitude is 180° and two native poles solve the rotation
/// (Calabretta & Greisen 2002, eq. 8; LATPOLE picks the northern one).
#[test]
fn a_native_equator_reference_stays_on_its_pixel() {
    for kind in [
        ProjectionType::CAR,
        ProjectionType::SFL,
        ProjectionType::MER,
        ProjectionType::AIT,
        ProjectionType::PAR,
        ProjectionType::MOL,
        ProjectionType::PCO,
        ProjectionType::TSC,
        ProjectionType::QSC,
    ] {
        for latitude in [-1.2, -0.4, -0.05, -1e-9, 0.0, 1e-9, 0.05, 0.4, 1.2] {
            let coord = DirectionCoordinate::new(
                DirectionRef::J2000,
                Projection::new(kind),
                [5.2, latitude],
                [-1e-4, 1e-4],
                [50.0, 50.0],
            );
            let [longitude, centre] = coord.pixel_to_world([50.0, 50.0]).unwrap();
            assert!(
                (longitude - 5.2).abs() < 1e-12 && (centre - latitude).abs() < 1e-12,
                "{kind:?} at {latitude}: {longitude} {centre}"
            );
            let pixel = coord.world_to_pixel([5.2 + 1e-3, latitude + 2e-3]).unwrap();
            let back = coord.pixel_to_world(pixel).unwrap();
            assert!(
                (back[0] - 5.2 - 1e-3).abs() < 1e-12 && (back[1] - latitude - 2e-3).abs() < 1e-12,
                "{kind:?} at {latitude}: {back:?}"
            );
        }
    }
}

fn make_sin_coord() -> DirectionCoordinate {
    let proj = Projection::new(ProjectionType::SIN);
    // Reference point at RA=0, Dec=+45 degrees
    let crval = [0.0, std::f64::consts::FRAC_PI_4];
    let cdelt = [-1e-4, 1e-4]; // ~20 arcsec pixels
    let crpix = [512.0, 512.0];
    DirectionCoordinate::new(DirectionRef::J2000, proj, crval, cdelt, crpix)
}

#[test]
fn reference_pixel_gives_reference_value() {
    let coord = make_sin_coord();
    let world = coord.to_world(&[512.0, 512.0]).unwrap();
    assert!(
        (world[0] - coord.crval[0]).abs() < TOL
            || (world[0] - coord.crval[0] - 2.0 * PI).abs() < TOL,
        "lon mismatch: {} vs {}",
        world[0],
        coord.crval[0]
    );
    assert!(
        (world[1] - coord.crval[1]).abs() < TOL,
        "lat mismatch: {} vs {}",
        world[1],
        coord.crval[1]
    );
}

#[test]
fn vlass_4096_zero_pixel_matches_casa_wcslib() {
    let coord = DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [3.5456021799042077, 0.29321531433310033],
        [-2.9088820866572157e-6, 2.9088820866572157e-6],
        [2048.0, 2048.0],
    );

    assert_eq!(
        coord.to_world(&[0.0, 0.0]).unwrap(),
        vec![3.5518141381294104, 0.2872525403597405]
    );
}

#[test]
fn vlass_12150_zero_pixel_matches_casa_wcslib() {
    let coord = DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [3.5456021799042077, 0.29321531433310033],
        [-2.9088820866572157e-6, 2.9088820866572157e-6],
        [6075.0, 6075.0],
    );

    let world = coord.to_world(&[0.0, 0.0]).unwrap();
    let expected = [3.56396716862884, 0.2754960264910762];
    assert_eq!(world[0], expected[0]);
    assert!(
        world[1].to_bits().abs_diff(expected[1].to_bits()) <= 1,
        "latitude differs by more than one ULP: {} vs {}",
        world[1],
        expected[1]
    );
}

#[test]
fn roundtrip_near_reference() {
    let coord = make_sin_coord();
    let pixel = [520.0, 530.0];
    let world = coord.to_world(&pixel).unwrap();
    let back = coord.to_pixel(&world).unwrap();
    assert!(
        (back[0] - pixel[0]).abs() < 1e-6,
        "px roundtrip: {} vs {}",
        back[0],
        pixel[0]
    );
    assert!(
        (back[1] - pixel[1]).abs() < 1e-6,
        "py roundtrip: {} vs {}",
        back[1],
        pixel[1]
    );
}

/// The issue607 standard-MFS VLA image's coordinate (SIN, 0.35″ cells).
fn make_vla_sin_coord() -> DirectionCoordinate {
    DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [4.712391234768306, -0.4014237887039714],
        [-1.696847883883376e-6, 1.696847883883376e-6],
        [256.0, 256.0],
    )
}

#[test]
fn reference_value_gives_reference_pixel_exactly() {
    // `asin` of the native latitude's sine put this 0.0088 pixel off
    // in latitude, and with it an analytic primary beam's centre.
    let coord = make_vla_sin_coord();
    let pixel = coord.to_pixel(&coord.crval).unwrap();
    assert!(
        (pixel[0] - 256.0).abs() < 1e-9 && (pixel[1] - 256.0).abs() < 1e-9,
        "reference value maps to {pixel:?}"
    );
}

#[test]
fn roundtrip_within_a_pixel_of_the_reference() {
    let coord = make_vla_sin_coord();
    for pixel in [[256.25, 255.75], [255.5, 256.5], [256.0, 256.01]] {
        let world = coord.to_world(&pixel).unwrap();
        let back = coord.to_pixel(&world).unwrap();
        assert!(
            (back[0] - pixel[0]).abs() < 1e-8 && (back[1] - pixel[1]).abs() < 1e-8,
            "{pixel:?} round-trips to {back:?}"
        );
    }
}

#[test]
fn roundtrip_tan() {
    let proj = Projection::new(ProjectionType::TAN);
    let crval = [1.0, 0.5];
    let cdelt = [-1e-4, 1e-4];
    let crpix = [256.0, 256.0];
    let coord = DirectionCoordinate::new(DirectionRef::J2000, proj, crval, cdelt, crpix);

    let pixel = [270.0, 280.0];
    let world = coord.to_world(&pixel).unwrap();
    let back = coord.to_pixel(&world).unwrap();
    assert!((back[0] - pixel[0]).abs() < 1e-6);
    assert!((back[1] - pixel[1]).abs() < 1e-6);
}

#[test]
fn roundtrip_car() {
    let proj = Projection::new(ProjectionType::CAR);
    // Use positive declination so longpole=0 (simple case)
    let crval = [3.0, 0.3];
    let cdelt = [-1e-3, 1e-3];
    let crpix = [100.0, 100.0];
    let coord = DirectionCoordinate::new(DirectionRef::J2000, proj, crval, cdelt, crpix);

    let pixel = [110.0, 90.0];
    let world = coord.to_world(&pixel).unwrap();
    let back = coord.to_pixel(&world).unwrap();
    assert!(
        (back[0] - pixel[0]).abs() < 1e-6,
        "CAR px roundtrip: {} vs {}",
        back[0],
        pixel[0]
    );
    assert!(
        (back[1] - pixel[1]).abs() < 1e-6,
        "CAR py roundtrip: {} vs {}",
        back[1],
        pixel[1]
    );
}

// NOTE: CAR with negative declination (delta_0 < theta_0 = 0) requires
// longpole = pi and a more complex native-pole computation that introduces
// coordinate inversions. This case is deferred to a future iteration.

#[test]
fn dimension_mismatch() {
    let coord = make_sin_coord();
    assert!(coord.to_world(&[1.0]).is_err());
    assert!(coord.to_pixel(&[1.0, 2.0, 3.0]).is_err());
}

#[test]
fn trait_methods() {
    let coord = make_sin_coord();
    assert_eq!(coord.coordinate_type(), CoordinateType::Direction);
    assert_eq!(coord.n_pixel_axes(), 2);
    assert_eq!(coord.n_world_axes(), 2);
    assert_eq!(coord.axis_names(), vec!["Right Ascension", "Declination"]);
    assert_eq!(coord.axis_units(), vec!["rad", "rad"]);
}

#[test]
fn galactic_axis_names() {
    let proj = Projection::new(ProjectionType::SIN);
    let coord = DirectionCoordinate::new(
        DirectionRef::GALACTIC,
        proj,
        [0.0, 0.0],
        [1e-4, 1e-4],
        [0.0, 0.0],
    );
    assert_eq!(coord.axis_names(), vec!["Longitude", "Latitude"]);
}

/// A coordinate stored in arcminutes (CASA's `fromshape` default)
/// computes in radians, presents and saves itself in arcminutes, as
/// casacore's does.
#[test]
fn stored_world_units_are_kept() {
    let arcmin = PI / 180.0 / 60.0;
    let stored = DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [0.0, 0.0],
        [-arcmin, arcmin],
        [2.0, 2.0],
    )
    .with_world_units(["'", "'"])
    .unwrap();
    let record = stored.to_record();
    assert_eq!(get_optional_vec_f64(&record, "cdelt").unwrap(), [-1.0, 1.0]);
    assert_eq!(
        get_optional_vec_string(&record, "units").unwrap(),
        ["'", "'"]
    );
    let restored = DirectionCoordinate::from_record(&record).unwrap();
    assert_eq!(restored.world_units(), &["'".to_string(), "'".to_string()]);
    let increment = restored.increment();
    assert!((increment[0] + arcmin).abs() < 1e-18 && (increment[1] - arcmin).abs() < 1e-18);
    assert_eq!(
        restored.in_world_units([increment[0], increment[1]]),
        [-1.0, 1.0]
    );
    assert!(stored.clone().with_world_units(["Hz", "'"]).is_err());
}

#[test]
fn to_record_has_projection() {
    let coord = make_sin_coord();
    let rec = coord.to_record();
    assert!(rec.get("projection").is_some());
    assert!(rec.get("system").is_some());
}

#[test]
fn record_roundtrip() {
    let proj = Projection::with_parameters(ProjectionType::SIN, vec![0.25, -0.5]);
    let pc = Array2::from_shape_vec((2, 2), vec![0.9, -0.1, 0.2, 1.1]).unwrap();
    let coord = DirectionCoordinate::new(
        DirectionRef::GALACTIC,
        proj,
        [1.0, 0.5],
        [-1e-4, 2e-4],
        [128.0, 64.0],
    )
    .with_pc_matrix(pc.clone())
    .with_longpole(0.25)
    .with_latpole(1.2);

    let restored = DirectionCoordinate::from_record(&coord.to_record()).unwrap();

    assert_eq!(restored.direction_ref(), DirectionRef::GALACTIC);
    assert_eq!(restored.reference_value(), vec![1.0, 0.5]);
    assert_eq!(restored.reference_pixel(), vec![128.0, 64.0]);
    assert_eq!(restored.increment(), vec![-1e-4, 2e-4]);
    assert_eq!(restored.pc_matrix(), &pc);
    assert_eq!(restored.projection().projection_type(), ProjectionType::SIN);
    assert_eq!(restored.projection().parameters(), &[0.25, -0.5]);
    assert!((restored.longpole() - 0.25).abs() < TOL);
    assert!((restored.latpole() - 1.2).abs() < TOL);
}

#[test]
fn b1950_vla_uses_the_casa_name_in_saved_direction_frames() {
    for (frame, conversion) in [
        (DirectionRef::B1950Vla, DirectionRef::J2000),
        (DirectionRef::J2000, DirectionRef::B1950Vla),
    ] {
        let coordinate = DirectionCoordinate::new(
            frame,
            Projection::new(ProjectionType::SIN),
            [0.2, 0.5],
            [-1e-4, 1e-4],
            [0.0, 0.0],
        )
        .with_conversion_direction_ref(conversion);
        let restored = DirectionCoordinate::from_record(&coordinate.to_record()).unwrap();
        assert_eq!(restored.direction_ref(), frame);
        assert_eq!(restored.conversion_direction_ref(), conversion);
    }
}

/// Supported zenithal images reopen with their coordinates intact.
/// Unsupported parameters fail at open, before regridding.
#[test]
fn restricted_zenithal_records_accept_supported_parameters() {
    for (kind, allowed, rejected) in [
        (
            ProjectionType::AZP,
            vec![vec![0.0, 0.0], vec![0.0, 0.0, 90.0], vec![2.0, 30.0]],
            vec![vec![-1.0, 0.0], vec![0.0, 90.0]],
        ),
        (
            ProjectionType::SZP,
            vec![vec![0.0, 0.0, 90.0], vec![2.0, 30.0, 45.0]],
            vec![vec![-1.0, 0.0, 90.0], vec![0.0, f64::NAN, 45.0]],
        ),
        (
            ProjectionType::AIR,
            // WCSLIB defaults an omitted AIR latitude to 90 degrees.
            vec![
                vec![],
                vec![90.0],
                vec![0.0, 0.0],
                vec![0.0, 0.0, 90.0],
                vec![45.0],
                vec![120.0],
            ],
            vec![vec![-90.0], vec![300.0]],
        ),
    ] {
        let record_with = |parameters| {
            DirectionCoordinate::new(
                DirectionRef::J2000,
                Projection::with_parameters(kind, parameters),
                [1.0, 0.5],
                [-1e-4, 1e-4],
                [128.0, 64.0],
            )
            .to_record()
        };
        for parameters in allowed {
            let restored =
                DirectionCoordinate::from_record(&record_with(parameters.clone())).unwrap();
            assert_eq!(restored.projection().projection_type(), kind);
            assert_eq!(restored.projection().parameters(), parameters);
        }
        for parameters in rejected {
            assert!(matches!(
                DirectionCoordinate::from_record(&record_with(parameters)),
                Err(CoordinateError::ConversionFailed(message)) if message.contains(kind.name())
            ));
        }
    }
}

/// A rotated image CASA 6.7.6 made (`coordsys.setlineartransform` with
/// casacore's matrix [[1 - 1.25e-9, 5e-5], [-5e-5, 1 - 1.25e-9]]) reads
/// to CASA's own world coordinates (`coordsys.toworld`).
#[test]
fn a_casacore_record_reads_to_casas_world_coordinates() {
    let arcsec = PI / 648_000.0;
    let mut record = DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [0.6, -0.4],
        [-0.2 * arcsec, 0.2 * arcsec],
        [16.0, 17.0],
    )
    .to_record();
    let (c, s) = (0.999_999_998_75, 5e-5);
    // casacore's matrix [[c, s], [-s, c]], column by column.
    let matrix = ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![c, -s, s, c]).unwrap();
    record.upsert("pc", Value::Array(ArrayValue::Float64(matrix)));
    let coord = DirectionCoordinate::from_record(&record).unwrap();
    assert_eq!(coord.pc_matrix(), &ndarray::array![[c, -s], [s, c]]);
    for (pixel, casa) in [
        (
            [0.0, 0.0],
            [0.600_016_842_882_085_7, -0.400_016_484_389_964_9],
        ),
        (
            [32.0, 0.0],
            [0.599_983_155_328_274, -0.400_016_482_838_550_3],
        ),
        (
            [0.0, 34.0],
            [0.600_016_844_436_951_4, -0.399_983_517_059_678_9],
        ),
        (
            [10.5, 3.25],
            [0.600_005_789_316_842, -0.400_013_332_636_851_1],
        ),
    ] {
        let world = coord.to_world(&pixel).unwrap();
        for (ours, casa) in world.iter().zip(casa) {
            assert!(
                (ours - casa).abs() < 1e-13,
                "{pixel:?}: {world:?} against CASA's {casa}"
            );
        }
    }
    let written = coord.to_record();
    let Some(Value::Array(ArrayValue::Float64(matrix))) = written.get("pc") else {
        panic!("a pc matrix");
    };
    assert_eq!(
        matrix.iter().copied().collect::<Vec<_>>(),
        vec![c, s, -s, c]
    );
}

#[test]
fn casa_record_units_are_converted_to_radians() {
    let mut record = RecordValue::default();
    record.upsert("system", Value::Scalar(ScalarValue::String("J2000".into())));
    record.upsert(
        "projection",
        Value::Scalar(ScalarValue::String("SIN".into())),
    );
    record.upsert(
        "crval",
        Value::Array(ArrayValue::from_f64_vec(vec![120.0, 45.0])),
    );
    record.upsert(
        "crpix",
        Value::Array(ArrayValue::from_f64_vec(vec![0.0, 0.0])),
    );
    record.upsert(
        "cdelt",
        Value::Array(ArrayValue::from_f64_vec(vec![-1.0, 1.0])),
    );
    record.upsert(
        "units",
        Value::Array(ArrayValue::from_string_vec(vec![
            "deg".into(),
            "deg".into(),
        ])),
    );
    record.upsert(
        "axes",
        Value::Array(ArrayValue::from_string_vec(vec![
            "Right Ascension".into(),
            "Declination".into(),
        ])),
    );
    record.upsert(
        "conversionSystem",
        Value::Scalar(ScalarValue::String("J2000".into())),
    );
    record.upsert("longpole", Value::Scalar(ScalarValue::Float64(180.0)));
    record.upsert("latpole", Value::Scalar(ScalarValue::Float64(90.0)));

    let coord = DirectionCoordinate::from_record(&record).unwrap();
    assert!((coord.reference_value()[0] - 120f64.to_radians()).abs() < TOL);
    assert!((coord.reference_value()[1] - 45f64.to_radians()).abs() < TOL);
    assert!((coord.increment()[0] - (-1f64).to_radians()).abs() < TOL);
    assert!((coord.increment()[1] - 1f64.to_radians()).abs() < TOL);
    assert!((coord.longpole() - PI).abs() < TOL);
    assert!((coord.latpole() - FRAC_PI_2).abs() < TOL);
}

#[test]
fn casa_record_serialization_uses_legacy_fields() {
    let coord = make_sin_coord().with_longpole(PI).with_latpole(FRAC_PI_2);
    let rec = coord.to_record();
    assert_eq!(
        rec.get("system"),
        Some(&Value::Scalar(ScalarValue::String("J2000".into())))
    );
    assert_eq!(
        rec.get("conversionSystem"),
        Some(&Value::Scalar(ScalarValue::String("J2000".into())))
    );
    assert_eq!(
        rec.get("projection_parameters"),
        Some(&Value::Array(ArrayValue::from_f64_vec(Vec::new())))
    );
    assert_eq!(
        rec.get("units"),
        Some(&Value::Array(ArrayValue::from_string_vec(vec![
            "rad".into(),
            "rad".into()
        ])))
    );
    assert_eq!(
        rec.get("longpole"),
        Some(&Value::Scalar(ScalarValue::Float64(180.0)))
    );
    assert_eq!(
        rec.get("latpole"),
        Some(&Value::Scalar(ScalarValue::Float64(90.0)))
    );
}
