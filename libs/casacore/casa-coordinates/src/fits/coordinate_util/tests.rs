// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use crate::CoordinateType;
use crate::fits::header::FitsHeader;

#[test]
fn zpn_header_preserves_high_polynomial_coefficients() {
    let mut coefficients = vec![0.0; 21];
    coefficients[1] = 1.0;
    coefficients[20] = 0.1;
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::with_parameters(ProjectionType::ZPN, coefficients),
        [0.0, std::f64::consts::FRAC_PI_2],
        [0.1, 0.1],
        [0.0, 0.0],
    ));
    let shape = [16, 16];
    let header = try_to_fits_header(&coordinates, &shape).unwrap();
    assert_eq!(header.get_float("PV2_20"), Some(0.1));
    let restored = from_fits_header(&header, &shape).unwrap();
    // At one radian from the zenith, r = z + 0.1 z^20 is 1.1 rad.
    let world = restored.to_world(&[0.0, -11.0]).unwrap();
    assert!((world[1] - (std::f64::consts::FRAC_PI_2 - 1.0)).abs() < 1e-12);
}

#[test]
fn fits_readback_keeps_a_rotated_direction_and_a_separately_placed_frequency() {
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(
        DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [1.2, -0.3],
            [-1.0e-3, 1.0e-3],
            [1.0, 2.0],
        )
        .with_pc_matrix(ndarray::array![[1.0, 0.3], [-0.2, 1.0]]),
    );
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        1.4e9,
        1.0e6,
        3.0,
        1.42e9,
    ));
    coordinates.transpose_axes(&[2, 0, 1], &[0, 1, 2]).unwrap();
    let shape = [5, 6, 7];
    let header = try_to_fits_header(&coordinates, &shape).unwrap();
    let restored = from_fits_header(&header, &shape).unwrap();
    for pixel in [[1.0, 2.0, 3.0], [3.5, 4.0, 1.5], [0.0, 1.25, 6.0]] {
        let expected = coordinates.to_world(&pixel).unwrap();
        let actual = restored.to_world(&pixel).unwrap();
        for (expected, actual) in expected.into_iter().zip(actual) {
            assert!((expected - actual).abs() <= 1e-12 * expected.abs().max(1.0));
        }
    }
}

/// Helper: build a typical 4-axis coordinate system (RA+Dec+Freq+Stokes).
fn make_4axis_cs() -> CoordinateSystem {
    let mut cs = CoordinateSystem::new();

    let proj = Projection::new(ProjectionType::SIN);
    let dir = DirectionCoordinate::new(
        DirectionRef::J2000,
        proj,
        [3.5, 0.8],    // RA ~200 deg, Dec ~46 deg
        [-1e-4, 1e-4], // ~20 arcsec pixels
        [512.0, 512.0],
    );
    cs.add_coordinate(dir);

    let spec = SpectralCoordinate::new(FrequencyRef::LSRK, 1.42040575e9, 1e6, 128.0, 1.42040575e9);
    cs.add_coordinate(spec);

    let stokes = StokesCoordinate::new(vec![
        StokesType::I,
        StokesType::Q,
        StokesType::U,
        StokesType::V,
    ]);
    cs.add_coordinate(stokes);

    let obs = ObsInfo::new("ALMA")
        .with_observer("Test Observer")
        .with_date(casa_core::measures::epoch::MEpoch::from_mjd(
            59_000.5,
            casa_core::measures::EpochRef::TAI,
        ));
    *cs.obs_info_mut() = obs;

    cs
}

#[test]
fn legacy_tutorial_frequency_matches_casacore() {
    use crate::Coordinate;
    // Actual CASA 6.7.6.14 ia.open of the Guide's ppdisk672_GHz_50pc.fits:
    // spectral2 is LSRK/Hz, with these reference, increment and rest values.
    let mut header = FitsHeader::new();
    header.set("CTYPE1", FitsValue::String("FREQ-LSR".into()));
    header.set("CUNIT1", FitsValue::String("HZ".into()));
    header.set("CRVAL1", FitsValue::Float(672_000_001_103.0));
    header.set("CDELT1", FitsValue::Float(32_000_000_029.8));
    header.set("CRPIX1", FitsValue::Float(1.0));
    header.set("RESTFREQ", FitsValue::Float(1_420_405_758.37));
    let restored = from_fits_header(&header, &[1]).unwrap();
    let CoordinateModel::Spectral(spectral) = restored.coordinate(0) else {
        panic!("not spectral")
    };
    assert_eq!(spectral.frequency_ref(), FrequencyRef::LSRK);
    assert_eq!(spectral.axis_units(), vec!["Hz".to_owned()]);
    assert_eq!(spectral.reference_value(), vec![672_000_001_103.0]);
    assert_eq!(spectral.increment(), vec![32_000_000_029.8]);
    assert_eq!(spectral.rest_frequency(), 1_420_405_758.37);
}

#[test]
fn roundtrip_4axis() {
    let cs1 = make_4axis_cs();
    let shape = [1024, 1024, 256, 4];
    let header = to_fits_header(&cs1, &shape);
    let cs2 = from_fits_header(&header, &shape).unwrap();

    assert_eq!(cs2.n_pixel_axes(), 4);
    assert_eq!(cs2.n_coordinates(), 3);

    // Check direction coordinate
    let dir_idx = cs2.find_coordinate(CoordinateType::Direction).unwrap();
    let dir = cs2.coordinate(dir_idx);
    let crval = dir.reference_value();
    assert!(
        (crval[0] - 3.5).abs() < 1e-8,
        "RA crval: {} vs 3.5",
        crval[0]
    );
    assert!(
        (crval[1] - 0.8).abs() < 1e-8,
        "Dec crval: {} vs 0.8",
        crval[1]
    );

    // Check spectral
    let spec_idx = cs2.find_coordinate(CoordinateType::Spectral).unwrap();
    let spec = cs2.coordinate(spec_idx);
    let spec_crval = spec.reference_value();
    assert!(
        (spec_crval[0] - 1.42040575e9).abs() < 1.0,
        "freq crval: {}",
        spec_crval[0]
    );

    // Check stokes
    let stokes_idx = cs2.find_coordinate(CoordinateType::Stokes).unwrap();
    let stokes = cs2.coordinate(stokes_idx);
    // Stokes I = code 1 at pixel 0
    let w = stokes.to_world(&[0.0]).unwrap();
    assert_eq!(w[0] as i32, 1);
    // Stokes V = code 4 at pixel 3
    let w = stokes.to_world(&[3.0]).unwrap();
    assert_eq!(w[0] as i32, 4);

    // Obs info, with the date in its own time scale (#1017).
    assert_eq!(cs2.obs_info().telescope, "ALMA");
    assert_eq!(cs2.obs_info().observer, "Test Observer");
    let date = cs2.obs_info().date.as_ref().expect("observation date");
    assert_eq!(date.refer(), casa_core::measures::EpochRef::TAI);
    assert_eq!(date.value().as_mjd(), 59_000.5);
}

#[test]
fn parse_realistic_header() {
    let cards = [
        "NAXIS   =                    4",
        "NAXIS1  =                 1024",
        "NAXIS2  =                 1024",
        "NAXIS3  =                  256",
        "NAXIS4  =                    4",
        "CTYPE1  = 'RA---SIN'",
        "CRVAL1  =   2.005000000000000E+02",
        "CRPIX1  =   5.130000000000000E+02",
        "CDELT1  =  -5.729577951308232E-03",
        "CUNIT1  = 'deg     '",
        "CTYPE2  = 'DEC--SIN'",
        "CRVAL2  =   4.583662361046586E+01",
        "CRPIX2  =   5.130000000000000E+02",
        "CDELT2  =   5.729577951308232E-03",
        "CUNIT2  = 'deg     '",
        "CTYPE3  = 'FREQ    '",
        "CRVAL3  =   1.420405750000000E+09",
        "CRPIX3  =   1.290000000000000E+02",
        "CDELT3  =   1.000000000000000E+06",
        "CUNIT3  = 'Hz      '",
        "CTYPE4  = 'STOKES  '",
        "CRVAL4  =   1.000000000000000E+00",
        "CRPIX4  =   1.000000000000000E+00",
        "CDELT4  =   1.000000000000000E+00",
        "RADESYS = 'FK5     '",
        "EQUINOX =   2.000000000000000E+03",
        "SPECSYS = 'LSRK    '",
        "RESTFRQ =   1.420405750000000E+09",
        "TELESCOP= 'VLA     '",
        "OBSERVER= 'Jane    '",
    ];

    let h = FitsHeader::from_cards(&cards);
    let shape = [1024, 1024, 256, 4];
    let cs = from_fits_header(&h, &shape).unwrap();

    assert_eq!(cs.n_pixel_axes(), 4);
    assert_eq!(cs.n_coordinates(), 3);

    // Direction
    let dir_idx = cs.find_coordinate(CoordinateType::Direction).unwrap();
    let dir = cs.coordinate(dir_idx);
    let crval = dir.reference_value();
    // 200.5 degrees in radians
    assert!(
        (crval[0] - 200.5 * DEG_TO_RAD).abs() < 1e-6,
        "RA: {} vs {}",
        crval[0],
        200.5 * DEG_TO_RAD
    );

    // Spectral
    let spec_idx = cs.find_coordinate(CoordinateType::Spectral).unwrap();
    let spec = cs.coordinate(spec_idx);
    assert!((spec.reference_value()[0] - 1.420405750e9).abs() < 1.0,);

    // Stokes
    let stokes_idx = cs.find_coordinate(CoordinateType::Stokes).unwrap();
    let stokes = cs.coordinate(stokes_idx);
    assert_eq!(stokes.to_world(&[0.0]).unwrap()[0] as i32, 1); // I
    assert_eq!(stokes.to_world(&[3.0]).unwrap()[0] as i32, 4); // V

    // Obs info
    assert_eq!(cs.obs_info().telescope, "VLA");
    assert_eq!(cs.obs_info().observer, "Jane");
}

#[test]
fn direction_sin_roundtrip() {
    let mut cs = CoordinateSystem::new();
    let proj = Projection::new(ProjectionType::SIN);
    let dir = DirectionCoordinate::new(
        DirectionRef::J2000,
        proj,
        [1.0, 0.5],
        [-2e-5, 2e-5],
        [256.0, 256.0],
    );
    cs.add_coordinate(dir);

    let shape = [512, 512];
    let header = to_fits_header(&cs, &shape);
    let cs2 = from_fits_header(&header, &shape).unwrap();

    let dir2 = cs2.coordinate(0);
    let crval = dir2.reference_value();
    assert!((crval[0] - 1.0).abs() < 1e-10);
    assert!((crval[1] - 0.5).abs() < 1e-10);

    let cdelt = dir2.increment();
    assert!((cdelt[0] - (-2e-5)).abs() < 1e-15);
    assert!((cdelt[1] - 2e-5).abs() < 1e-15);
}

#[test]
fn b1950_vla_uses_casas_fits_equinox() {
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::B1950Vla,
        Projection::new(ProjectionType::SIN),
        [1.0, 0.5],
        [-1e-4, 1e-4],
        [2.0, 2.0],
    ));
    let shape = [5, 5];
    let exported = to_fits_header(&coordinates, &shape);
    assert_eq!(exported.get_string("RADESYS"), Some("FK4"));
    assert_eq!(exported.get_float("EQUINOX"), Some(1979.9));

    for (card, system, epoch_card, supported) in [
        (Some("RADESYS"), "FK4", "EQUINOX", true),
        (Some("RADESYS"), "FK4-NO-E", "EQUINOX", true),
        (Some("RADECSYS"), "FK4", "EPOCH", true),
        (None, "", "EPOCH", true),
        (Some("RADESYS"), "FK5", "EQUINOX", false),
    ] {
        let mut header = FitsHeader::new();
        for keyword in exported
            .iter()
            .filter(|keyword| !matches!(keyword.name.as_str(), "RADESYS" | "EQUINOX"))
        {
            header.set(&keyword.name, keyword.value.clone());
        }
        if let Some(card) = card {
            header.set(card, FitsValue::String(system.into()));
        }
        header.set(epoch_card, FitsValue::Float(1979.9));
        if epoch_card == "EQUINOX" {
            header.set("EPOCH", FitsValue::Float(1950.0));
        }
        let restored = from_fits_header(&header, &shape);
        if !supported {
            assert!(
                restored
                    .unwrap_err()
                    .to_string()
                    .contains("FK5 with equinox 1979.9")
            );
            continue;
        }
        let restored = restored.unwrap();
        let CoordinateModel::Direction(direction) = restored.coordinate(0) else {
            panic!("expected direction coordinate");
        };
        assert_eq!(direction.direction_ref(), DirectionRef::B1950Vla);
    }
    let mut conflicting = exported;
    conflicting.set("RADESYS", FitsValue::String("FK4-NO-E".into()));
    conflicting.set("EQUINOX", FitsValue::Float(1975.0));
    assert!(from_fits_header(&conflicting, &shape).is_err());
}

#[test]
fn azp_fits_parameters_follow_the_latitude_axis() {
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::with_parameters(ProjectionType::AZP, vec![2.0, 30.0]),
        [1.0, 0.5],
        [-2e-3, 2e-3],
        [64.0, 64.0],
    ));
    let shape = [1, 128, 128];
    let header = to_fits_header(&coordinates, &shape);
    assert_eq!(header.get_float("PV3_1"), Some(2.0));
    assert_eq!(header.get_float("PV3_2"), Some(30.0));
    let restored = from_fits_header(&header, &shape).unwrap();
    let index = restored.find_coordinate(CoordinateType::Direction).unwrap();
    let CoordinateModel::Direction(direction) = restored.coordinate(index) else {
        panic!("direction coordinate");
    };
    assert_eq!(direction.projection().parameters(), &[2.0, 30.0]);
}

#[test]
fn spectral_roundtrip() {
    let mut cs = CoordinateSystem::new();
    let spec = SpectralCoordinate::new(FrequencyRef::BARY, 1.5e9, -500e3, 64.0, 1.42040575e9);
    cs.add_coordinate(spec);

    let shape = [128];
    let header = to_fits_header(&cs, &shape);

    assert_eq!(header.get_string("SPECSYS"), Some("BARYCENT"));

    let cs2 = from_fits_header(&header, &shape).unwrap();
    let spec2 = cs2.coordinate(0);

    assert!((spec2.reference_value()[0] - 1.5e9).abs() < 1.0,);
    assert!((spec2.increment()[0] - (-500e3)).abs() < 0.1,);
    assert!((spec2.reference_pixel()[0] - 64.0).abs() < 1e-10,);
}

#[test]
fn undefined_spectral_frame_does_not_claim_a_fits_reference_frame() {
    let mut cs = CoordinateSystem::new();
    cs.add_coordinate(
        SpectralCoordinate::new(FrequencyRef::TOPO, 1.5e9, 1e6, 0., 1.4e9).with_undefined_frame(),
    );
    let header = to_fits_header(&cs, &[8]);
    assert!(header.get("SPECSYS").is_none());
    assert_eq!(header.get_float("RESTFRQ"), Some(1.4e9));
}

#[test]
fn spectral_reference_frames_use_casa_fits_names_and_roundtrip() {
    for (reference, name) in [
        (FrequencyRef::LSRK, "LSRK"),
        (FrequencyRef::LSRD, "LSRD"),
        (FrequencyRef::BARY, "BARYCENT"),
        (FrequencyRef::TOPO, "TOPOCENT"),
        (FrequencyRef::GEO, "GEOCENTR"),
        (FrequencyRef::GALACTO, "GALACTOC"),
        (FrequencyRef::LGROUP, "LOCALGRP"),
        (FrequencyRef::CMB, "CMBDIPOL"),
        (FrequencyRef::REST, "SOURCE"),
    ] {
        let mut cs = CoordinateSystem::new();
        cs.add_coordinate(SpectralCoordinate::new(reference, 1.5e9, 1e6, 0., 1.4e9));
        let header = to_fits_header(&cs, &[8]);
        assert_eq!(header.get_string("SPECSYS"), Some(name));
        let restored = from_fits_header(&header, &[8]).unwrap();
        let CoordinateModel::Spectral(spectral) = restored.coordinate(0) else {
            panic!("not spectral")
        };
        assert_eq!(spectral.frequency_ref(), reference);
    }
}

#[test]
fn missing_crval_error() {
    let cards = [
        "NAXIS   =                    1",
        "CTYPE1  = 'FREQ    '",
        "CDELT1  =   1.000000000000000E+06",
    ];
    let h = FitsHeader::from_cards(&cards);
    let result = from_fits_header(&h, &[256]);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("CRVAL"), "error: {err}");
}

#[test]
fn unsupported_projection_error() {
    let cards = [
        "NAXIS   =                    2",
        "CTYPE1  = 'RA---BON'",
        "CRVAL1  =   0.0",
        "CDELT1  =  -1.0E-03",
        "CRPIX1  =   1.0",
        "CTYPE2  = 'DEC--BON'",
        "CRVAL2  =   0.0",
        "CDELT2  =   1.0E-03",
        "CRPIX2  =   1.0",
    ];
    let h = FitsHeader::from_cards(&cards);
    let result = from_fits_header(&h, &[256, 256]);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("BON"), "error: {err}");
}

#[test]
fn cd_matrix_handling() {
    // Use CD matrix instead of CDELT + PC
    let cards = [
        "NAXIS   =                    2",
        "CTYPE1  = 'RA---TAN'",
        "CRVAL1  =   1.800000000000000E+02",
        "CRPIX1  =   5.130000000000000E+02",
        "CTYPE2  = 'DEC--TAN'",
        "CRVAL2  =   4.500000000000000E+01",
        "CRPIX2  =   5.130000000000000E+02",
        "CD1_1   =  -1.000000000000000E-03",
        "CD1_2   =   0.000000000000000E+00",
        "CD2_1   =   0.000000000000000E+00",
        "CD2_2   =   1.000000000000000E-03",
        "RADESYS = 'FK5     '",
    ];

    let h = FitsHeader::from_cards(&cards);
    let cs = from_fits_header(&h, &[1024, 1024]).unwrap();
    assert_eq!(cs.n_pixel_axes(), 2);

    let dir = cs.coordinate(0);
    let cdelt = dir.increment();
    // CD1_1 = -1e-3 deg -> radians
    assert!(
        (cdelt[0] - (-1e-3 * DEG_TO_RAD)).abs() < 1e-15,
        "cdelt[0]: {} vs {}",
        cdelt[0],
        -1e-3 * DEG_TO_RAD
    );
}

#[test]
fn pc_matrix_roundtrip() {
    // Direction with a non-identity PC matrix
    let mut cs = CoordinateSystem::new();
    let proj = Projection::new(ProjectionType::TAN);
    let rot = 0.1_f64; // small rotation
    let pc =
        ndarray::Array2::from_shape_vec((2, 2), vec![rot.cos(), -rot.sin(), rot.sin(), rot.cos()])
            .unwrap();
    let dir = DirectionCoordinate::new(
        DirectionRef::J2000,
        proj,
        [3.0, 0.7],
        [-1e-4, 1e-4],
        [256.0, 256.0],
    )
    .with_pc_matrix(pc.clone());
    cs.add_coordinate(dir);

    let shape = [512, 512];
    let header = to_fits_header(&cs, &shape);

    // Verify PC keywords were emitted
    assert!(header.get_float("PC1_1").is_some());
    assert!(header.get_float("PC1_2").is_some());

    let cs2 = from_fits_header(&header, &shape).unwrap();
    let dir2 = cs2.coordinate(0);

    // Verify roundtrip via pixel conversion
    let pixel = [270.0, 280.0];
    let world1 = cs.coordinate(0).to_world(&pixel).unwrap();
    let world2 = dir2.to_world(&pixel).unwrap();
    assert!(
        (world1[0] - world2[0]).abs() < 1e-8,
        "lon: {} vs {}",
        world1[0],
        world2[0]
    );
    assert!(
        (world1[1] - world2[1]).abs() < 1e-8,
        "lat: {} vs {}",
        world1[1],
        world2[1]
    );
}

#[test]
fn parses_direction_units_in_radians() {
    let cards = [
        "NAXIS   =                    2",
        "NAXIS1  =                   32",
        "NAXIS2  =                   32",
        "CTYPE1  = 'RA---SIN'",
        "CRVAL1  =   1.230000000000000E+00",
        "CRPIX1  =   1.700000000000000E+01",
        "CDELT1  =  -1.000000000000000E-06",
        "CUNIT1  = 'rad     '",
        "CTYPE2  = 'DEC--SIN'",
        "CRVAL2  =  -4.500000000000000E-01",
        "CRPIX2  =   1.700000000000000E+01",
        "CDELT2  =   1.000000000000000E-06",
        "CUNIT2  = 'rad     '",
    ];

    let header = FitsHeader::from_cards(&cards);
    let cs = from_fits_header(&header, &[32, 32]).unwrap();
    let dir = cs.coordinate(cs.find_coordinate(CoordinateType::Direction).unwrap());

    assert!((dir.reference_value()[0] - 1.23).abs() < 1e-12);
    assert!((dir.reference_value()[1] + 0.45).abs() < 1e-12);
    assert!((dir.increment()[0] + 1e-6).abs() < 1e-18);
    assert!((dir.increment()[1] - 1e-6).abs() < 1e-18);
}

#[test]
fn crota2_handling() {
    // Legacy CROTA2 convention
    let cards = [
        "NAXIS   =                    2",
        "CTYPE1  = 'RA---SIN'",
        "CRVAL1  =   0.000000000000000E+00",
        "CRPIX1  =   5.130000000000000E+02",
        "CDELT1  =  -1.000000000000000E-03",
        "CTYPE2  = 'DEC--SIN'",
        "CRVAL2  =   4.500000000000000E+01",
        "CRPIX2  =   5.130000000000000E+02",
        "CDELT2  =   1.000000000000000E-03",
        "CROTA2  =   5.000000000000000E+00",
    ];

    let h = FitsHeader::from_cards(&cards);
    let cs = from_fits_header(&h, &[1024, 1024]).unwrap();
    assert_eq!(cs.n_pixel_axes(), 2);

    // The rotation should be present in the PC matrix
    let dir = cs.coordinate(0);
    let pixel = [520.0, 530.0];
    let world = dir.to_world(&pixel).unwrap();
    // Just verify it doesn't crash and the pixel is near the reference
    assert!(world[0].is_finite());
    assert!(world[1].is_finite());
}

#[test]
fn unknown_ctype_becomes_linear() {
    let cards = [
        "NAXIS   =                    1",
        "CTYPE1  = 'VOPT    '",
        "CRVAL1  =   1.000000000000000E+03",
        "CRPIX1  =   1.000000000000000E+00",
        "CDELT1  =   1.000000000000000E+01",
        "CUNIT1  = 'km/s    '",
    ];
    let h = FitsHeader::from_cards(&cards);
    let cs = from_fits_header(&h, &[128]).unwrap();

    assert_eq!(cs.n_coordinates(), 1);
    assert_eq!(cs.coordinate(0).coordinate_type(), CoordinateType::Linear);
    assert!((cs.coordinate(0).reference_value()[0] - 1000.0).abs() < 1e-10,);
}

#[test]
fn galactic_direction_roundtrip() {
    let mut cs = CoordinateSystem::new();
    let proj = Projection::new(ProjectionType::CAR);
    let dir = DirectionCoordinate::new(
        DirectionRef::GALACTIC,
        proj,
        [0.5, 0.3],
        [-1e-3, 1e-3],
        [100.0, 100.0],
    );
    cs.add_coordinate(dir);

    let shape = [200, 200];
    let header = to_fits_header(&cs, &shape);

    // Verify CTYPE uses GLON/GLAT
    assert_eq!(header.get_string("CTYPE1"), Some("GLON-CAR"));
    assert_eq!(header.get_string("CTYPE2"), Some("GLAT-CAR"));

    let cs2 = from_fits_header(&header, &shape).unwrap();
    let dir2 = cs2.coordinate(0);
    let crval = dir2.reference_value();
    assert!((crval[0] - 0.5).abs() < 1e-8);
    assert!((crval[1] - 0.3).abs() < 1e-8);
}

#[test]
fn non_equatorial_fits_axes_are_not_overridden_by_radesys() {
    for (frame, lon, lat) in [
        (DirectionRef::GALACTIC, "GLON-SIN", "GLAT-SIN"),
        (DirectionRef::SUPERGAL, "SLON-SIN", "SLAT-SIN"),
        (DirectionRef::ECLIPTIC, "ELON-SIN", "ELAT-SIN"),
    ] {
        let mut system = CoordinateSystem::new();
        system.add_coordinate(DirectionCoordinate::new(
            frame,
            Projection::new(ProjectionType::SIN),
            [0.5, 0.3],
            [-1e-3, 1e-3],
            [2.0, 2.0],
        ));
        let mut header = to_fits_header(&system, &[4, 4]);
        assert_eq!(header.get_string("CTYPE1"), Some(lon));
        assert_eq!(header.get_string("CTYPE2"), Some(lat));
        assert_eq!(header.get_string("RADESYS"), None);
        header.set("RADESYS", FitsValue::String("ICRS".into()));
        let restored = from_fits_header(&header, &[4, 4]).unwrap();
        let CoordinateModel::Direction(direction) = restored.coordinate(0) else {
            panic!("direction")
        };
        assert_eq!(direction.direction_ref(), frame);
    }
}

#[test]
fn empty_system_roundtrip() {
    let cs = CoordinateSystem::new();
    let header = to_fits_header(&cs, &[]);
    assert_eq!(header.get_int("NAXIS"), Some(0));

    let cs2 = from_fits_header(&header, &[]).unwrap();
    assert_eq!(cs2.n_pixel_axes(), 0);
}

#[test]
fn stokes_roundtrip_circular() {
    let mut cs = CoordinateSystem::new();
    let stokes = StokesCoordinate::new(vec![
        StokesType::RR,
        StokesType::RL,
        StokesType::LR,
        StokesType::LL,
    ]);
    cs.add_coordinate(stokes);

    let shape = [4];
    let header = to_fits_header(&cs, &shape);
    let cs2 = from_fits_header(&header, &shape).unwrap();

    let st = cs2.coordinate(0);
    assert_eq!(st.to_world(&[0.0]).unwrap()[0] as i32, 5); // RR
    assert_eq!(st.to_world(&[3.0]).unwrap()[0] as i32, 8); // LL
}

#[test]
fn extended_stokes_codes_match_casa_fits_cards() {
    // CRVAL and CDELT read from CASA 6.7.6.14 ia.tofits products.
    for (values, reference, increment) in [
        (vec![StokesType::RX], 113.0, 1.0),
        (vec![StokesType::RX, StokesType::RY], 113.0, 1.0),
        (vec![StokesType::I, StokesType::RX], 1.0, 112.0),
        (vec![StokesType::RCircular], 125.0, 1.0),
        (vec![StokesType::Undefined], 100.0, 1.0),
    ] {
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(StokesCoordinate::new(values.clone()));
        let shape = [values.len()];
        let mut header = to_fits_header(&coordinates, &shape);
        assert_eq!(header.get_float("CRVAL1"), Some(reference));
        assert_eq!(header.get_float("CDELT1"), Some(increment));
        let restored = from_fits_header(&header, &shape).unwrap();
        let CoordinateModel::Stokes(stokes) = restored.coordinate(0) else {
            panic!("expected the FITS Stokes axis");
        };
        assert_eq!(stokes.stokes(), values);
        header.set("CRVAL1", FitsValue::Float(13.0));
        let restored = from_fits_header(&header, &[1]).unwrap();
        let CoordinateModel::Stokes(stokes) = restored.coordinate(0) else {
            panic!("expected the FITS Stokes axis");
        };
        assert_eq!(stokes.stokes(), [StokesType::Undefined]);
    }
}
