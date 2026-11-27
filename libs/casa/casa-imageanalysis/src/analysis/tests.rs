// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use casa_coordinates::{
    DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate, StokesCoordinate,
    StokesType,
};
use casa_core::Complex32;
use casa_core::measures::direction::DirectionRef;
use casa_core::measures::frequency::FrequencyRef;
use casa_images::ImageType;
use fitsio::FitsFile;

/// An imstat request for a classic run with one selection.
fn imstat_request(
    imagename: &Path,
    box_pixels: Option<&str>,
    region: Option<&str>,
) -> ImstatRequest {
    ImstatRequest {
        imagename: imagename.to_path_buf(),
        box_pixels: box_pixels.map(str::to_string),
        region: region.map(str::to_string),
        ..ImstatRequest::default()
    }
}

/// The global statistics of a classic run.
fn imstat_global(request: &ImstatRequest) -> ImageStatisticsSummary {
    match imstat(request).unwrap() {
        ImstatOutcome::Global(summary) => summary,
        other => panic!("expected global statistics: {other:?}"),
    }
}

#[test]
fn parse_box_is_inclusive() {
    assert_eq!(parse_box("100,100,150,150").unwrap(), [100, 100, 150, 150]);
}

#[test]
fn parse_indices_expands_casa_range() {
    assert_eq!(
        parse_image_channel_selection("4~6,8", 10).unwrap(),
        vec![4, 5, 6, 8]
    );
    assert_eq!(
        parse_image_channel_selection("0~8^2", 10).unwrap(),
        vec![0, 2, 4, 6, 8]
    );
}

#[test]
fn image_analysis_task_dispatch_roundtrips_real_image_products() {
    let temp = tempfile::tempdir().unwrap();
    let source_path = temp.path().join("source.image");
    let moment_path = temp.path().join("moment.image");
    let fits_path = temp.path().join("source.fits");
    let imported_path = temp.path().join("imported.image");
    create_spectral_test_image(&source_path);

    let header = imhead(&source_path).unwrap();
    assert_eq!(header.shape, vec![2, 2, 3]);
    assert_eq!(header.units, "Jy/beam");
    assert_eq!(header.object_name, "NGC Test");
    assert_eq!(header.axes.len(), 3);
    assert!(header.restoring_beam.is_some());

    let imstat_result = imstat(&ImstatRequest {
        imagename: source_path.clone(),
        box_pixels: Some("0,0,1,1".to_string()),
        chans: Some("1~2".to_string()),
        includepix: Some([5.0, 12.0]),
        ..ImstatRequest::default()
    })
    .unwrap();
    let ImstatOutcome::Global(stats) = imstat_result else {
        panic!("expected a global imstat result");
    };
    assert_eq!(stats.blc, vec![0, 0, 1]);
    assert_eq!(stats.trc, vec![1, 1, 2]);
    assert_eq!(stats.npts, 8.0);
    assert_eq!(stats.units, "Jy/beam");
    let source = PagedImage::<f32>::open(&source_path).unwrap();
    // casacore integrates the two selected channels' flux density over
    // one channel's radio velocity width: 2 MHz at a 372 GHz rest
    // frequency.
    let channel_km_s = SPEED_OF_LIGHT_KM_S * 2.0e6 / 372.0e9;
    let expected_flux = stats.sum * direction_pixel_area(source.coordinates()).unwrap()
        / source
            .image_info()
            .unwrap()
            .beam_set
            .single_beam()
            .unwrap()
            .area()
        * channel_km_s;
    assert!((stats.flux.unwrap() - expected_flux).abs() < 1e-12 * expected_flux);

    let moment = immoments(&ImmomentsRequest {
        imagename: source_path.clone(),
        outfile: moment_path.clone(),
        moments: vec![0],
        chans: Some("0~2".to_string()),
        includepix: Some([2.0, 12.0]),
        mask: None,
        overwrite: false,
        ..ImmomentsRequest::default()
    })
    .unwrap();
    assert_eq!(moment.shape, vec![2, 2, 1]);
    assert_eq!(moment.products[0].units, "Jy/beam.km/s");
    assert_eq!(moment.products[0].valid_profiles, 4);
    let moment_image = PagedImage::<f32>::open(&moment_path).unwrap();
    // CASA creates mask0 even when every profile is valid.
    assert_eq!(moment_image.default_mask_name().as_deref(), Some("mask0"));
    for (moment_number, expected_units) in
        [(-1, "Jy/beam"), (1, "km/s"), (2, "km/s"), (3, "Jy/beam")]
    {
        let output_path = temp.path().join(format!("moment{moment_number}.image"));
        let summary = immoments(&ImmomentsRequest {
            imagename: source_path.clone(),
            outfile: output_path,
            moments: vec![moment_number],
            chans: Some("0~2".to_string()),
            includepix: Some([2.0, 12.0]),
            mask: None,
            overwrite: false,
            ..ImmomentsRequest::default()
        })
        .unwrap();
        assert_eq!(summary.products[0].moment, moment_number);
        assert_eq!(summary.products[0].units, expected_units);
        assert_eq!(summary.products[0].valid_profiles, 4);
    }

    let mask_path = temp.path().join("source.pb");
    create_spectral_mask_image(&mask_path);
    let masked_moment_path = temp.path().join("masked-moment.image");
    let masked_moment = immoments(&ImmomentsRequest {
        imagename: source_path.clone(),
        outfile: masked_moment_path,
        moments: vec![0],
        chans: Some("0~2".to_string()),
        includepix: None,
        mask: Some(format!("{}>0.3", mask_path.display())),
        overwrite: false,
        ..ImmomentsRequest::default()
    })
    .unwrap();
    assert_eq!(masked_moment.products[0].valid_profiles, 3);

    let pv_path = temp.path().join("pv.image");
    let pv_error = impv(&ImpvRequest {
        imagename: source_path.clone(),
        outfile: pv_path.clone(),
        start: Some(PvPosition::Pixel([0.0, 0.0])),
        end: Some(PvPosition::Pixel([1.0, 1.0])),
        chans: Some("1~2".into()),
        ..ImpvRequest::default()
    })
    .unwrap_err();
    assert!(pv_error.to_string().contains("square direction pixels"));
    assert!(!pv_path.exists());

    let subimage_path = temp.path().join("sub.image");
    let subimage = imsubimage(&ImsubimageRequest {
        imagename: source_path.clone(),
        outfile: subimage_path.clone(),
        box_pixels: Some("0,0,0,1".to_string()),
        chans: Some("1~2".to_string()),
        ..ImsubimageRequest::default()
    })
    .unwrap();
    assert_eq!(subimage.shape, vec![1, 2, 2]);
    let subimage_image = PagedImage::<f32>::open(&subimage_path).unwrap();
    assert_eq!(subimage_image.shape(), &[1, 2, 2]);

    let product_path = temp.path().join("product.image");
    let math = immath(&ImmathRequest {
        imagename: vec![subimage_path.clone(), subimage_path.clone()],
        outfile: product_path.clone(),
        expr: "IM0 * IM1".to_string(),
        overwrite: false,
        ..ImmathRequest::default()
    })
    .unwrap();
    assert_eq!(math.shape, vec![1, 2, 2]);
    assert_eq!(math.valid_pixels, 4);
    let product_image = PagedImage::<f32>::open(&product_path).unwrap();
    assert_eq!(product_image.shape(), &[1, 2, 2]);

    let scaled_path = temp.path().join("scaled.image");
    let scaled = immath(&ImmathRequest {
        imagename: vec![subimage_path.clone()],
        outfile: scaled_path.clone(),
        expr: "1.222e6*IM0/1.579^2/(29.30*29.03)".to_string(),
        overwrite: false,
        ..ImmathRequest::default()
    })
    .unwrap();
    assert_eq!(scaled.shape, vec![1, 2, 2]);
    assert_eq!(scaled.valid_pixels, 4);
    let scaled_image = PagedImage::<f32>::open(&scaled_path).unwrap();
    let subimage_input = subimage_image.get_slice(&[0, 0, 0], &[1, 1, 1]).unwrap();
    let scaled_data = scaled_image.get_slice(&[0, 0, 0], &[1, 1, 1]).unwrap();
    let factor = 1.222e6 / 1.579_f64.powi(2) / (29.30 * 29.03);
    let expected = f64::from(subimage_input[[0, 0, 0]]) * factor;
    assert!((f64::from(scaled_data[[0, 0, 0]]) - expected).abs() < 1.0e-3);
    let put_header = imhead_put(&scaled_path, "bunit", "K").unwrap();
    assert_eq!(put_header.units, "K");
    assert_eq!(PagedImage::<f32>::open(&scaled_path).unwrap().units(), "K");

    let pbcor_path = temp.path().join("pbcor.image");
    let pbcor = impbcor(&ImpbcorRequest {
        imagename: source_path.clone(),
        pbimage: mask_path.clone().into(),
        outfile: pbcor_path.clone(),
        mode: "divide".to_string(),
        cutoff: 0.3,
        box_selection: None,
        region: None,
        chans: None,
        stokes: None,
        mask: None,
        stretch: false,
        overwrite: false,
    })
    .unwrap();
    assert_eq!(pbcor.shape, vec![2, 2, 3]);
    assert_eq!(pbcor.units, "Jy/beam");
    assert_eq!(pbcor.valid_pixels, 9);
    let pbcor_image = PagedImage::<f32>::open(&pbcor_path).unwrap();
    assert_eq!(pbcor_image.default_mask_name(), Some("mask0".to_string()));
    let pbcor_data = pbcor_image.get_slice(&[1, 0, 0], &[1, 1, 1]).unwrap();
    let source_data = source.get_slice(&[1, 0, 0], &[1, 1, 1]).unwrap();
    assert!((pbcor_data[[0, 0, 0]] - source_data[[0, 0, 0]]).abs() < 1.0e-6);

    exportfits(&ExportFitsRequest {
        imagename: source_path.clone(),
        fitsimage: fits_path.clone(),
        velocity: true,
        overwrite: false,
        ..ExportFitsRequest::default()
    })
    .unwrap();
    assert!(fits_path.exists());

    importfits(&ImportFitsRequest {
        fitsimage: fits_path.clone(),
        imagename: imported_path.clone(),
        ..ImportFitsRequest::default()
    })
    .unwrap();
    let imported = PagedImage::<f32>::open(&imported_path).unwrap();
    assert_eq!(imported.shape(), &[2, 2, 3]);
    assert_eq!(imported.units(), "Jy/beam");
}

#[test]
fn image_analysis_rejects_unsupported_pixels_and_invalid_selections() {
    let temp = tempfile::tempdir().unwrap();
    let real_path = temp.path().join("real.image");
    let complex_path = temp.path().join("complex.image");
    let output_path = temp.path().join("moment.image");
    create_direction_only_test_image(&real_path);
    create_complex_test_image(&complex_path);

    let complex_header = imhead(&complex_path).unwrap();
    assert_eq!(complex_header.pixel_type, "Complex32");
    assert_eq!(complex_header.units, "Jy");
    assert_eq!(complex_header.axes.len(), 2);

    assert!(matches!(
        imstat(&imstat_request(&complex_path, None, None)),
        Err(ImageError::InvalidMetadata(message))
            if message.contains("real-valued images")
    ));
    assert!(matches!(
        immoments(&ImmomentsRequest {
            imagename: complex_path.clone(),
            outfile: output_path.clone(),
            moments: vec![0],
            chans: None,
            includepix: None,
            mask: None,
            overwrite: false,
            ..ImmomentsRequest::default()
        }),
        Err(ImageError::InvalidMetadata(message))
            if message.contains("real-valued images")
    ));
    assert!(matches!(
        export_fits(&complex_path, temp.path().join("complex.fits"), false, false),
        Err(ImageError::InvalidMetadata(message))
            if message.contains("real-valued images")
    ));

    assert!(matches!(
        imstat(&imstat_request(&real_path, Some("0,0,4,4"), None)),
        Err(ImageError::CasaSelection(message))
            if message.contains("trc in box spec")
    ));
    assert!(matches!(
        imstat(&ImstatRequest {
            imagename: real_path.clone(),
            chans: Some("0".to_string()),
            ..ImstatRequest::default()
        }),
        Err(ImageError::InvalidMetadata(message))
            if message.contains("spectral axis")
    ));
    assert!(matches!(
    imstat(&imstat_request(
                    &real_path,
                    Some("0,0,1,1"),
                    Some("box:0,0,1,1"),
                )),
                Err(ImageError::InvalidMetadata(message))
                    if message.contains("either box or region")
            ));
    assert!(matches!(
        parse_box("2,0,1,1"),
        Err(ImageError::InvalidMetadata(message))
            if message.contains("inclusive upper bounds")
    ));
    assert!(matches!(
        parse_image_channel_selection("3~1", 4),
        Err(ImageError::InvalidMetadata(message))
            if message.contains("end precedes start")
    ));
    assert!(matches!(
        parse_image_channel_selection("0,9", 4),
        Err(ImageError::ShapeMismatch { .. })
    ));
    assert_eq!(
        parse_image_channel_selection("2,1,2", 4).unwrap(),
        vec![1, 2]
    );

    assert!(matches!(
        immoments(&ImmomentsRequest {
            imagename: real_path.clone(),
            outfile: output_path.clone(),
            moments: vec![12],
            chans: None,
            includepix: None,
            mask: None,
            overwrite: false,
            ..ImmomentsRequest::default()
        }),
        Err(ImageError::InvalidMetadata(message))
            if message.contains("moment numbers -1 through 11")
    ));
    assert!(matches!(
        immoments(&ImmomentsRequest {
            imagename: real_path,
            outfile: output_path,
            moments: vec![0],
            chans: None,
            includepix: None,
            mask: None,
            overwrite: false,
            ..ImmomentsRequest::default()
        }),
        Err(ImageError::InvalidMetadata(message))
            if message.contains("spectral axis")
    ));
}

#[test]
fn imstat_accepts_inline_and_crtf_pixel_box_regions() {
    let temp = tempfile::tempdir().unwrap();
    let image_path = temp.path().join("real.image");
    let region_path = temp.path().join("source.crtf");
    create_direction_only_test_image(&image_path);

    let inline = imstat_global(&imstat_request(
        &image_path,
        None,
        Some("box[[0pix,0pix],[0pix,0pix]]"),
    ));
    assert_eq!(inline.npts, 1.0);
    assert_eq!(inline.sum, 1.0);

    fs::write(
        &region_path,
        "#CRTFv0 CASA Region Text Format version 0\nbox[[0pix,0pix],[1pix,0pix]]\n",
    )
    .unwrap();

    let from_file = imstat_global(&imstat_request(
        &image_path,
        None,
        Some(region_path.to_str().unwrap()),
    ));
    assert_eq!(from_file.npts, 2.0);
    assert_eq!(from_file.sum, 3.0);
    assert_eq!(from_file.blc, vec![0, 0]);
    assert_eq!(from_file.trc, vec![1, 0]);
}

#[test]
fn imstat_accepts_crtf_world_box_regions() {
    let temp = tempfile::tempdir().unwrap();
    let image_path = temp.path().join("real.image");
    let region_path = temp.path().join("source.crtf");
    create_direction_only_test_image(&image_path);

    let image = PagedImage::<f32>::open(&image_path).unwrap();
    let first = image.coordinates().to_world(&[0.0, 0.0]).unwrap();
    let second = image.coordinates().to_world(&[1.0, 0.0]).unwrap();
    fs::write(
        &region_path,
        format!(
            "#CRTFv0 CASA Region Text Format version 0\nbox[[{:.15}rad,{:.15}rad],[{:.15}rad,{:.15}rad]]\n",
            first[0], first[1], second[0], second[1]
        ),
    )
    .unwrap();

    let from_file = imstat_global(&imstat_request(
        &image_path,
        None,
        Some(region_path.to_str().unwrap()),
    ));
    assert_eq!(from_file.npts, 2.0);
    assert_eq!(from_file.sum, 3.0);
    assert_eq!(from_file.blc, vec![0, 0]);
    assert_eq!(from_file.trc, vec![1, 0]);
}

#[test]
fn imstat_accepts_crtf_pixel_polygon_regions() {
    let temp = tempfile::tempdir().unwrap();
    let image_path = temp.path().join("real.image");
    let region_path = temp.path().join("source.crtf");
    create_direction_grid_test_image(&image_path, &[3, 3]);
    fs::write(
        &region_path,
        "#CRTFv0 CASA Region Text Format version 0\npoly [[0pix,0pix], [2pix,0pix], [0pix,2pix]]\n",
    )
    .unwrap();

    let from_file = imstat_global(&imstat_request(
        &image_path,
        None,
        Some(region_path.to_str().unwrap()),
    ));

    assert_eq!(from_file.blc, vec![0, 0]);
    assert_eq!(from_file.trc, vec![2, 2]);
    assert_eq!(from_file.npts, 3.0);
    assert_eq!(from_file.sum, 14.0);
}

#[test]
fn imstat_accepts_crtf_world_polygon_regions() {
    let temp = tempfile::tempdir().unwrap();
    let image_path = temp.path().join("real.image");
    let region_path = temp.path().join("source.crtf");
    create_direction_grid_test_image(&image_path, &[3, 3]);
    let image = PagedImage::<f32>::open(&image_path).unwrap();
    let a = image.coordinates().to_world(&[0.0, 0.0]).unwrap();
    let b = image.coordinates().to_world(&[2.0, 0.0]).unwrap();
    let c = image.coordinates().to_world(&[0.0, 2.0]).unwrap();
    fs::write(
        &region_path,
        format!(
            "#CRTFv0 CASA Region Text Format version 0\npoly [[{:.15}rad,{:.15}rad], [{:.15}rad,{:.15}rad], [{:.15}rad,{:.15}rad]]\n",
            a[0], a[1], b[0], b[1], c[0], c[1]
        ),
    )
    .unwrap();

    let from_file = imstat_global(&imstat_request(
        &image_path,
        None,
        Some(region_path.to_str().unwrap()),
    ));

    assert_eq!(from_file.blc, vec![0, 0]);
    assert_eq!(from_file.trc, vec![2, 2]);
    assert_eq!(from_file.npts, 3.0);
    assert_eq!(from_file.sum, 14.0);
}

#[test]
fn crtf_parsers_allow_optional_keyword_whitespace_with_word_boundary() {
    assert_eq!(
        crtf_polygon_parts("poly[[0pix,0pix],[2pix,0pix],[2pix,2pix],[0pix,2pix]]").unwrap(),
        Some(vec![
            ["0pix".to_string(), "0pix".to_string()],
            ["2pix".to_string(), "0pix".to_string()],
            ["2pix".to_string(), "2pix".to_string()],
            ["0pix".to_string(), "2pix".to_string()],
        ])
    );
    assert_eq!(
        crtf_box_parts("box [[1pix, 2pix], [3pix, 4pix]]").unwrap(),
        Some([
            "1pix".to_string(),
            "2pix".to_string(),
            "3pix".to_string(),
            "4pix".to_string(),
        ])
    );
    assert_eq!(crtf_polygon_parts("my_poly[0].crtf").unwrap(), None);
    assert_eq!(crtf_box_parts("my_box[[0]].crtf").unwrap(), None);
}

#[test]
fn fits_import_export_roundtrip_preserves_pixels_wcs_units_beam_and_object() {
    let temp = tempfile::tempdir().unwrap();
    let source_path = temp.path().join("source.image");
    let first_fits = temp.path().join("first.fits");
    let imported_path = temp.path().join("imported.image");
    let second_fits = temp.path().join("second.fits");

    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [1.2, -0.4],
        [-2.0e-6, 3.0e-6],
        [1.0, 0.0],
    ));
    coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I, StokesType::Q]));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        372.0e9,
        2.0e6,
        2.0,
        372.0e9,
    ));

    // Observed at 2020-05-31 12:00 TAI: the date keeps its time scale
    // (#1017).
    coordinates.obs_info_mut().date = Some(casa_core::measures::epoch::MEpoch::from_mjd(
        59_000.5,
        casa_core::measures::EpochRef::TAI,
    ));

    let shape = vec![3, 2, 2, 5];
    let mut source = PagedImage::<f32>::create(shape.clone(), coordinates, &source_path).unwrap();
    let data = ArrayD::from_shape_vec(
        IxDyn(&shape).f(),
        (0..shape.iter().product::<usize>())
            .map(|value| value as f32 + 0.25)
            .collect(),
    )
    .unwrap();
    source.put_slice(&data, &[0, 0, 0, 0]).unwrap();
    source.set_units("Jy/beam").unwrap();
    source
        .set_image_info(&ImageInfo {
            beam_set: ImageBeamSet::new(GaussianBeam::new(
                1.7e-4_f64.to_radians(),
                1.2e-4_f64.to_radians(),
                (-34.5_f64).to_radians(),
            )),
            image_type: ImageType::Intensity,
            object_name: "TW Hya".to_string(),
        })
        .unwrap();
    source.save().unwrap();

    export_fits(&source_path, &first_fits, false, false).unwrap();
    import_fits(&first_fits, &imported_path, false).unwrap();
    export_fits(&imported_path, &second_fits, false, false).unwrap();

    let imported = PagedImage::<f32>::open(&imported_path).unwrap();
    assert_eq!(imported.shape(), &[3, 2, 5, 2]);
    let imported_pixels = imported.get().unwrap();
    for x in 0..3 {
        for y in 0..2 {
            for st in 0..2 {
                for ch in 0..5 {
                    assert_eq!(imported_pixels[[x, y, ch, st]], data[[x, y, st, ch]]);
                }
            }
        }
    }
    assert_eq!(imported.units(), "Jy/beam");
    let info = imported.image_info().unwrap();
    assert_eq!(info.object_name, "TW Hya");
    let beam = info.beam_set.single_beam().unwrap();
    assert!((beam.major_in("deg").unwrap() - 1.7e-4).abs() < 1.0e-12);
    assert!((beam.minor_in("deg").unwrap() - 1.2e-4).abs() < 1.0e-12);
    assert!((beam.position_angle_in("deg").unwrap() + 34.5).abs() < 1.0e-12);
    let date = imported.coordinates().obs_info().date.as_ref().unwrap();
    assert_eq!(date.refer(), casa_core::measures::EpochRef::TAI);
    assert_eq!(date.value().as_mjd(), 59_000.5);
    // As CASA 6.7.6's exportfits writes the date: ISO, in its own scale.
    assert_eq!(
        read_fits_key_as_string(&first_fits, "DATE-OBS"),
        "2020-05-31T12:00:00.000000"
    );
    assert_eq!(read_fits_key_as_string(&first_fits, "TIMESYS"), "TAI");

    assert_eq!(
        read_fits_pixels(&first_fits),
        read_fits_pixels(&second_fits)
    );
    for key in [
        "NAXIS", "NAXIS1", "NAXIS2", "NAXIS3", "NAXIS4", "BUNIT", "BMAJ", "BMIN", "BPA", "OBJECT",
        "RADESYS", "CTYPE1", "CTYPE2", "CTYPE3", "CTYPE4", "CRVAL1", "CRVAL2", "CRVAL3", "CRVAL4",
        "CRPIX1", "CRPIX2", "CRPIX3", "CRPIX4", "CDELT1", "CDELT2", "CDELT3", "CDELT4", "CUNIT1",
        "CUNIT2", "CUNIT3", "CUNIT4", "RESTFRQ", "SPECSYS", "DATE-OBS", "TIMESYS",
    ] {
        assert_fits_key_matches(&first_fits, &second_fits, key);
    }
}

/// A 2 by 2 FITS sky image of float pixels whose header holds `cards`,
/// each written as given, after the required ones and a SIN direction.
fn write_fits_with_cards(path: &Path, cards: &[&str]) {
    let mut header = vec![
        "SIMPLE  =                    T",
        "BITPIX  =                  -32",
        "NAXIS   =                    2",
        "NAXIS1  =                    2",
        "NAXIS2  =                    2",
        "CTYPE1  = 'RA---SIN'",
        "CTYPE2  = 'DEC--SIN'",
        "CRVAL1  =                 68.8",
        "CRVAL2  =                -22.9",
        "CDELT1  =           -0.0002778",
        "CDELT2  =            0.0002778",
        "CRPIX1  =                  1.0",
        "CRPIX2  =                  1.0",
        "CUNIT1  = 'deg'",
        "CUNIT2  = 'deg'",
    ];
    header.extend_from_slice(cards);
    header.push("END");
    let mut bytes = header
        .iter()
        .flat_map(|card| format!("{card:<80}").into_bytes())
        .collect::<Vec<u8>>();
    bytes.resize(bytes.len().div_ceil(2880) * 2880, b' ');
    for value in [1.0_f32, 2.0, 3.0, 4.0] {
        bytes.extend(value.to_be_bytes());
    }
    bytes.resize(bytes.len().div_ceil(2880) * 2880, 0);
    fs::write(path, bytes).unwrap();
}

#[test]
fn importfits_takes_the_date_past_unusable_date_cards() {
    // An MJD-OBS with no value (FITS's way to say it is unknown) or one
    // CFITSIO cannot read as a number leaves DATE-OBS to give the date,
    // as CASA 6.7.6 imports such headers; it once stopped the import
    // (#1020 review). A TIMESYS with no value is UTC, and a DATE-OBS with
    // none gives no date.
    use casa_core::measures::EpochRef;
    let temp = tempfile::tempdir().unwrap();
    let date = "DATE-OBS= '2000-01-01T02:57:46.666569'";
    let no_mjd = "MJD-OBS =                      / date unavailable";
    let cases: [(&[&str], Option<EpochRef>); 5] = [
        (&[date, "TIMESYS = 'UTC'", no_mjd], Some(EpochRef::UTC)),
        (
            &[date, "TIMESYS = 'TAI'", "MJD-OBS = 'garbage'"],
            Some(EpochRef::TAI),
        ),
        (
            &[date, "TIMESYS =                      / unknown"],
            Some(EpochRef::UTC),
        ),
        (&["DATE-OBS=                      / unknown"], None),
        (&[], None),
    ];
    for (index, (cards, expected)) in cases.into_iter().enumerate() {
        let fits = temp.path().join(format!("{index}.fits"));
        let image_path = temp.path().join(format!("{index}.image"));
        write_fits_with_cards(&fits, cards);
        import_fits(&fits, &image_path, false)
            .unwrap_or_else(|error| panic!("case {index}: {error}"));
        let image = PagedImage::<f32>::open(&image_path).unwrap();
        let date = image.coordinates().obs_info().date.as_ref();
        match expected {
            Some(reference) => {
                let date = date.unwrap_or_else(|| panic!("case {index}: no date"));
                assert_eq!(date.refer(), reference, "case {index}");
                let seconds = (date.value().as_mjd() - 51_544.123_456_789) * 86_400.0;
                assert!(seconds.abs() < 1e-5, "case {index}: {seconds} s");
            }
            None => assert!(date.is_none(), "case {index}"),
        }
    }
}

#[test]
fn fits_import_export_overwrite_policy_preserves_double_images() {
    let temp = tempfile::tempdir().unwrap();
    let source_path = temp.path().join("source.image");
    let fits_path = temp.path().join("source.fits");
    let imported_path = temp.path().join("imported.image");
    create_double_spectral_test_image(&source_path);

    export_fits(&source_path, &fits_path, false, false).unwrap();
    assert!(matches!(
        export_fits(&source_path, &fits_path, false, false),
        Err(ImageError::Io(message)) if message.contains("already exists")
    ));
    let export_summary = export_fits(&source_path, &fits_path, false, true).unwrap();
    assert_eq!(export_summary.shape, vec![2, 2, 2]);

    import_fits(&fits_path, &imported_path, false).unwrap();
    assert!(matches!(
        import_fits(&fits_path, &imported_path, false),
        Err(ImageError::Io(message)) if message.contains("already exists")
    ));
    let import_summary = import_fits(&fits_path, &imported_path, true).unwrap();
    assert_eq!(import_summary.pixel_type, "Float64");
    assert_eq!(import_summary.units, "K");
    assert_eq!(import_summary.object_name, "Double Test");

    let imported = PagedImage::<f64>::open(&imported_path).unwrap();
    assert_eq!(imported.shape(), &[2, 2, 2]);
    assert_eq!(imported.units(), "K");

    let header = imhead(&imported_path).unwrap();
    assert_eq!(header.pixel_type, "Float64");
    assert_eq!(header.object_name, "Double Test");
    let stats = imstat_global(&ImstatRequest {
        imagename: imported_path.clone(),
        box_pixels: Some("0,0,1,1".to_string()),
        chans: Some("0~1".to_string()),
        includepix: Some([1.0, 8.0]),
        ..ImstatRequest::default()
    });
    assert_eq!(stats.npts, 7.0);
    assert_eq!(stats.units, "K");
}

#[test]
fn exportfits_writes_default_mask_as_nan_and_velocity_wcs() {
    let temp = tempfile::tempdir().unwrap();
    let source_path = temp.path().join("masked.image");
    let fits_path = temp.path().join("masked.fits");

    let rest_frequency = 372.0e9;
    let frequency_increment = -620_000.0;
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [1.2, -0.4],
        [-2.0e-6, 3.0e-6],
        [0.0, 0.0],
    ));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        rest_frequency,
        frequency_increment,
        0.0,
        rest_frequency,
    ));
    let shape = vec![2, 2, 3];
    let mut image = PagedImage::<f32>::create(shape.clone(), coordinates, &source_path).unwrap();
    let data = ArrayD::from_shape_vec(
        IxDyn(&shape).f(),
        (0..shape.iter().product::<usize>())
            .map(|value| value as f32)
            .collect(),
    )
    .unwrap();
    image.put_slice(&data, &[0, 0, 0]).unwrap();
    image.make_mask("mask0", true, true).unwrap();
    let mut mask = ArrayD::from_elem(IxDyn(&shape).f(), true);
    mask[[0, 0, 0]] = false;
    mask[[1, 1, 2]] = false;
    image.put_mask("mask0", &mask).unwrap();
    image.save().unwrap();

    export_fits(&source_path, &fits_path, true, false).unwrap();

    let pixels = read_fits_pixels(&fits_path);
    assert_eq!(pixels.iter().filter(|value| value.is_nan()).count(), 2);
    assert_eq!(read_fits_key_as_string(&fits_path, "CTYPE3"), "VRAD");
    assert_eq!(read_fits_key_as_string(&fits_path, "CUNIT3"), "m/s");
    let crval3 = read_fits_key_as_f64(&fits_path, "CRVAL3");
    let cdelt3 = read_fits_key_as_f64(&fits_path, "CDELT3");
    assert!(crval3.abs() < 1.0e-9);
    let expected_increment = -SPEED_OF_LIGHT_M_S * frequency_increment / rest_frequency;
    assert!((cdelt3 - expected_increment).abs() < 1.0e-6);
}

#[test]
fn optimized_moment_collapse_matches_reference_for_masked_axis() {
    let shape = vec![3, 2, 4];
    let input = ArrayD::from_shape_vec(
        IxDyn(&shape).f(),
        (0..shape.iter().product::<usize>())
            .map(|value| value as f32 / 10.0)
            .collect(),
    )
    .unwrap();
    let mask = ArrayD::from_shape_vec(
        IxDyn(&shape).f(),
        (0..shape.iter().product::<usize>())
            .map(|value| value % 3 != 0)
            .collect(),
    )
    .unwrap();
    let coords = vec![-2.0, -1.0, 0.0, 1.0];
    for moment in [-1, 0, 1, 2, 3] {
        let actual = immoments::collapse_moment(
            &input,
            Some(&mask),
            2,
            &immoments::ProfileCoordinates::fixed(&coords),
            0.25,
            immoments::PixelClip::from_include(Some([0.4, 1.8])),
            moment,
        );
        let expected = collapse_moment_reference(
            &input,
            Some(&mask),
            2,
            &coords,
            0.25,
            Some([0.4, 1.8]),
            moment,
        )
        .unwrap();
        assert_eq!(actual, expected);
    }
}

fn create_spectral_test_image(path: &Path) {
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [1.2, -0.4],
        [-2.0e-6, 3.0e-6],
        [0.0, 0.0],
    ));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        372.0e9,
        2.0e6,
        0.0,
        372.0e9,
    ));
    let shape = vec![2, 2, 3];
    let mut image = PagedImage::<f32>::create(shape.clone(), coordinates, path).unwrap();
    let data = ArrayD::from_shape_vec(
        IxDyn(&shape).f(),
        (1..=shape.iter().product::<usize>())
            .map(|value| value as f32)
            .collect(),
    )
    .unwrap();
    image.put_slice(&data, &[0, 0, 0]).unwrap();
    image.set_units("Jy/beam").unwrap();
    image
        .set_image_info(&ImageInfo {
            beam_set: ImageBeamSet::new(GaussianBeam::new(
                1.7e-4_f64.to_radians(),
                1.2e-4_f64.to_radians(),
                (-34.5_f64).to_radians(),
            )),
            image_type: ImageType::Intensity,
            object_name: "NGC Test".to_string(),
        })
        .unwrap();
    image.save().unwrap();
}

fn create_spectral_mask_image(path: &Path) {
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [1.2, -0.4],
        [-2.0e-6, 3.0e-6],
        [0.0, 0.0],
    ));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        372.0e9,
        2.0e6,
        0.0,
        372.0e9,
    ));
    let shape = vec![2, 2, 3];
    let mut image = PagedImage::<f32>::create(shape.clone(), coordinates, path).unwrap();
    let mut data = ArrayD::<f32>::ones(IxDyn(&shape).f());
    for chan in 0..shape[2] {
        data[[0, 0, chan]] = 0.0;
    }
    image.put_slice(&data, &[0, 0, 0]).unwrap();
    image.save().unwrap();
}

fn create_double_spectral_test_image(path: &Path) {
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [0.0, 0.0],
        [-1.0e-6, 1.0e-6],
        [0.0, 0.0],
    ));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        115.0e9,
        1.0e6,
        0.0,
        115.0e9,
    ));
    let shape = vec![2, 2, 2];
    let mut image = PagedImage::<f64>::create(shape.clone(), coordinates, path).unwrap();
    let data = ArrayD::from_shape_vec(
        IxDyn(&shape).f(),
        (0..shape.iter().product::<usize>())
            .map(|value| value as f64 + 0.125)
            .collect(),
    )
    .unwrap();
    image.put_slice(&data, &[0, 0, 0]).unwrap();
    image.set_units("K").unwrap();
    image
        .set_image_info(&ImageInfo {
            beam_set: ImageBeamSet::default(),
            image_type: ImageType::Intensity,
            object_name: "Double Test".to_string(),
        })
        .unwrap();
    image.save().unwrap();
}

fn create_direction_only_test_image(path: &Path) {
    let shape = vec![2, 2];
    let mut image =
        PagedImage::<f32>::create(shape.clone(), direction_coordinates(), path).unwrap();
    let data = ArrayD::from_shape_vec(IxDyn(&shape).f(), vec![1.0, 2.0, 3.0, 4.0]).unwrap();
    image.put_slice(&data, &[0, 0]).unwrap();
    image.save().unwrap();
}

fn create_direction_grid_test_image(path: &Path, shape: &[usize]) {
    let mut image =
        PagedImage::<f32>::create(shape.to_vec(), direction_coordinates(), path).unwrap();
    let data = ArrayD::from_shape_fn(IxDyn(shape).f(), |idx| (idx[0] + idx[1] * 10 + 1) as f32);
    image.put_slice(&data, &[0, 0]).unwrap();
    image.save().unwrap();
}

fn create_complex_test_image(path: &Path) {
    let shape = vec![2, 2];
    let mut image =
        PagedImage::<Complex32>::create(shape.clone(), direction_coordinates(), path).unwrap();
    let data = ArrayD::from_shape_vec(
        IxDyn(&shape).f(),
        vec![
            Complex32::new(1.0, 0.5),
            Complex32::new(2.0, -0.5),
            Complex32::new(3.0, 1.5),
            Complex32::new(4.0, -1.5),
        ],
    )
    .unwrap();
    image.put_slice(&data, &[0, 0]).unwrap();
    image.set_units("Jy").unwrap();
    image.save().unwrap();
}

fn direction_coordinates() -> CoordinateSystem {
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [1.2, -0.4],
        [-2.0e-6, 3.0e-6],
        [0.0, 0.0],
    ));
    coordinates
}

fn read_fits_pixels(path: &Path) -> Vec<f32> {
    let mut fits = FitsFile::open(path).unwrap();
    let hdu = fits.primary_hdu().unwrap();
    hdu.read_image::<Vec<f32>>(&mut fits).unwrap()
}

fn assert_fits_key_matches(lhs: &Path, rhs: &Path, key: &str) {
    let lhs_value = read_fits_key_as_string(lhs, key);
    let rhs_value = read_fits_key_as_string(rhs, key);
    assert_eq!(lhs_value, rhs_value, "FITS key {key}");
}

fn read_fits_key_as_string(path: &Path, key: &str) -> String {
    let mut fits = FitsFile::open(path).unwrap();
    let hdu = fits.primary_hdu().unwrap();
    if let Ok(value) = hdu.read_key::<String>(&mut fits, key) {
        return value.trim_end().to_string();
    }
    if let Ok(value) = hdu.read_key::<i64>(&mut fits, key) {
        return value.to_string();
    }
    hdu.read_key::<f64>(&mut fits, key).unwrap().to_string()
}

fn read_fits_key_as_f64(path: &Path, key: &str) -> f64 {
    let mut fits = FitsFile::open(path).unwrap();
    let hdu = fits.primary_hdu().unwrap();
    hdu.read_key::<f64>(&mut fits, key).unwrap()
}
