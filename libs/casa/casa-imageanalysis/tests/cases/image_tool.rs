// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::{RecordValue, quanta::Quantity};
use casa_imageanalysis::analysis::{
    HistogramOptions, ImageCropOptions, ImagePadOptions, ImageRegridTarget, SmoothingKernel,
    SubimageRegion,
};
use casa_imageanalysis::tool::add_axes::{AddDegAxesRequest, add_degenerate_axes};
use casa_imageanalysis::tool::attachment::{AttachedImage, no_external_open, remove_file};
use casa_imageanalysis::tool::beam_ops::{
    FullSetBeamRequest, deconvolve_component_beam, rotate_beam_quantity, set_restoring_beam_full,
};
use casa_imageanalysis::tool::beams::{
    beam_area_record, common_beam_record, restoring_beam_record,
};
use casa_imageanalysis::tool::concatenate::concatenate_casa_images;
use casa_imageanalysis::tool::conformance::check_conformance;
use casa_imageanalysis::tool::convertflux::convert_flux_quantity;
use casa_imageanalysis::tool::coordinate_conversion::{to_pixel, to_world};
use casa_imageanalysis::tool::coords::default_record_for_shape;
use casa_imageanalysis::tool::create::{
    FromShapeRequest, PixelArrayInput, create_from_array, create_from_shape,
};
use casa_imageanalysis::tool::imagecalc::image_calc;
use casa_imageanalysis::tool::lifecycle::ImageToolState;
use casa_imageanalysis::tool::locking::{has_lock, lock_image, unlock_image};
use casa_imageanalysis::tool::mask_expression::calc_mask;
use casa_imageanalysis::tool::masks::mask_handler_casa;
use casa_imageanalysis::tool::metadata::{
    brightness_unit, image_history, image_shape, image_type, misc_info, set_brightness_unit,
    set_history_casa, set_misc_info,
};
use casa_imageanalysis::tool::ops_edit::{add_noise, set_pixels_and_mask};
use casa_imageanalysis::tool::ops_fit::{bounding_box, histograms};
use casa_imageanalysis::tool::ops_spatial::{crop_image, pad_image};
use casa_imageanalysis::tool::ops_transform::{collapse_image, regrid_attached, smooth_axis};
use casa_imageanalysis::tool::pixels::{GetChunkRequest, PixelChunk, get_chunk, put_chunk};
use casa_imageanalysis::tool::point::{PixelScalar, pixel_value_casa};
use casa_imageanalysis::tool::region_read::{GetRegionRequest, get_region};
use casa_imageanalysis::tool::serialization::{image_from_record, image_to_record};
use casa_imageanalysis::tool::slice::{GetSliceRequest, extract_slice};
use casa_imageanalysis::tool::statistics::{StatisticsRequest, image_statistics};
use casa_imageanalysis::tool::subimage::{SubimageRequest, extract_subimage};
use casa_imageanalysis::tool::summary::{SummaryOptions, summarize_image};
use casa_imageanalysis::tool::transpose::{TransposeOrder, transpose_attached};
use casa_images::{GaussianBeam, ImagePixelType};
use ndarray::{ArrayD, IxDyn};

fn create_test_cube(
    dir: &std::path::Path,
    name: &str,
) -> (
    std::path::PathBuf,
    ArrayD<f32>,
    ImageToolState,
    AttachedImage,
) {
    let img_path = dir.join(name);
    let data = ArrayD::from_shape_fn(IxDyn(&[8, 8, 1, 4]), |idx| {
        (idx[0] + idx[1] * 2 + idx[3] * 3) as f32 + 1.0
    });
    let (handle, _) = create_from_array(
        img_path.to_str().unwrap(),
        PixelArrayInput::Float(data.clone()),
        None,
        false,
        false,
        true,
        true,
        &no_external_open,
    )
    .unwrap();
    drop(handle);

    let mut state = ImageToolState::new();
    assert!(!state.is_open());
    state.open(&img_path, true).unwrap();
    assert!(state.is_open());
    let attached = AttachedImage::open(&img_path).unwrap();
    (img_path, data, state, attached)
}

#[test]
fn pure_rust_image_tool_lifecycle_beams_and_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let (_img_path, _data, state, attached) = create_test_cube(dir.path(), "base.im");
    assert_eq!(state.name(true).unwrap(), "base.im");
    assert_eq!(image_shape(&attached), vec![8, 8, 1, 4]);
    assert_eq!(image_type(&attached), "PagedImage");

    assert!(lock_image(&attached, "write", 1).unwrap());
    assert_eq!(has_lock(&attached, Some("write")).unwrap(), vec![true]);

    set_brightness_unit(&attached, "Jy/beam", true).unwrap();
    assert_eq!(brightness_unit(&attached), "Jy/beam");
    let mut misc = casa_core::RecordValue::default();
    misc.upsert(
        "observer",
        casa_core::Value::Scalar(casa_core::ScalarValue::String("unit-test".into())),
    );
    set_misc_info(&attached, misc, true).unwrap();
    assert!(misc_info(&attached).unwrap().get("observer").is_some());
    set_history_casa(&attached, "test", vec!["custom note".to_owned()]).unwrap();
    assert!(!image_history(&attached).unwrap().is_empty());

    // Set and query restoring beam + rotate + deconvolve + convertflux
    let empty_rec = RecordValue::default();
    set_restoring_beam_full(
        &attached,
        &FullSetBeamRequest {
            quantities: Some([
                Quantity::new(4.0, "arcsec").unwrap(),
                Quantity::new(2.0, "arcsec").unwrap(),
                Quantity::new(30.0, "deg").unwrap(),
            ]),
            beam: &empty_rec,
            beam_history: "{}",
            input_histories: ["'4arcsec'", "'2arcsec'", "'15deg'"],
            remove: false,
            log: true,
            channel: -1,
            polarization: -1,
            imagename: "",
            imagename_path: None,
            has_axis_args: true,
        },
        true,
    )
    .unwrap();
    let rb = restoring_beam_record(
        &attached.beam_view(),
        || attached.lock().coordinate_record(),
        -1,
        -1,
        "list",
    )
    .unwrap();
    assert!(rb.get("major").is_some());
    rotate_beam_quantity(
        &attached,
        &Quantity::new(15.0, "deg").unwrap(),
        true,
        "'15deg'",
    )
    .unwrap();
    let cb = common_beam_record(&attached.lock(), &attached.beam_view()).unwrap();
    assert!(cb.get("major").is_some());
    assert!(cb.get("minor").is_some());
    let pa = match cb.get("pa") {
        Some(casa_core::Value::Record(rec)) => match (rec.get("value"), rec.get("unit")) {
            (
                Some(casa_core::Value::Scalar(casa_core::ScalarValue::Float64(val))),
                Some(
                    casa_core::Value::TableRef(unit)
                    | casa_core::Value::Scalar(casa_core::ScalarValue::String(unit)),
                ),
            ) if unit == "deg" => *val,
            other => panic!("unexpected pa record {other:?}"),
        },
        other => panic!("unexpected commonbeam pa {other:?}"),
    };
    assert!((pa - 45.0).abs() < 1e-6);
    let cb_beam = GaussianBeam::from_record(&rb).unwrap();
    let area_rec = beam_area_record(&attached.lock(), &attached.beam_view(), -1, -1).unwrap();
    assert!(area_rec.get("arcsec2").is_some());
    let dec = deconvolve_component_beam(
        GaussianBeam::new(
            (8.0 / 3600.0_f64).to_radians(),
            (6.0 / 3600.0_f64).to_radians(),
            45.0_f64.to_radians(),
        ),
        cb_beam,
    )
    .unwrap();
    assert!(dec.fit.major_in("arcsec").unwrap() > 0.0);
    let (flux, _) = convert_flux_quantity(
        &attached.lock(),
        &attached.beam_view(),
        Quantity::new(2.0, "Jy/beam").unwrap(),
        [
            Quantity::new(4.0, "arcsec").unwrap(),
            Quantity::new(2.0, "arcsec").unwrap(),
        ],
        false,
        (-1, -1),
    )
    .unwrap();
    assert!((flux.value() - 2.0).abs() < 1e-6);

    let (summary, _) = summarize_image(
        &attached,
        &SummaryOptions {
            list: true,
            verbose: true,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(summary.shape, vec![8, 8, 1, 4]);
    assert!(
        summary
            .messages
            .iter()
            .any(|m| m.contains("Restoring Beam"))
    );
    let w = to_world(&attached, &[3.0, 3.0, 0.0, 0.0], "").unwrap();
    let p = to_pixel(&attached, &w).unwrap();
    assert!((p[0] - 3.0).abs() < 1e-4);
    let pv = pixel_value_casa(&attached, &[1.0, 1.0, 0.0, 0.0])
        .unwrap()
        .unwrap();
    assert!(pv.mask);
    match pv.value {
        PixelScalar::Float(v) => assert!((v - 4.0).abs() < 1e-5),
        other => panic!("unexpected scalar {other:?}"),
    }
}

#[test]
fn pure_rust_image_tool_chunks_stats_and_masks() {
    let dir = tempfile::tempdir().unwrap();
    let (img_path, data, _state, attached) = create_test_cube(dir.path(), "base.im");

    // Chunks, region read, slice, masks, boundingbox, histograms, stats
    let chunk = get_chunk(
        &attached,
        &GetChunkRequest {
            blc: vec![0, 0, 0, 0],
            trc: vec![3, 3, 0, 0],
            dropdeg: true,
            getmask: true,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(chunk.pixels.shape(), &[4, 4]);
    let slice = extract_slice(
        &attached,
        &GetSliceRequest {
            x: &[0.0, 7.0],
            y: &[0.0, 7.0],
            coord: &[0, 0, 0, 0],
            method: "linear",
            npts: 8,
            mask: None,
        },
    )
    .unwrap();
    assert_eq!(slice.pixel.len(), 8);

    let reg = get_region(
        &attached,
        &GetRegionRequest {
            region: None,
            axes: None,
            mask: None,
            list: false,
            dropdeg: false,
            getmask: true,
            stretch: false,
        },
    )
    .unwrap();
    assert_eq!(reg.pixels.shape(), &[8, 8, 1, 4]);

    let bounds = bounding_box(&attached, None).unwrap();
    assert_eq!(bounds.image_shape, vec![8, 8, 1, 4]);

    let hist = histograms(
        &attached,
        HistogramOptions {
            axes: &[-1],
            region: None,
            mask: None,
            stretch: false,
            bins: 8,
            include: &[],
            cumulative: false,
            logarithmic: false,
        },
    )
    .unwrap();
    assert!(hist.get("values").is_some());

    let stats = image_statistics(
        &attached,
        &StatisticsRequest {
            axes: None,
            region: None,
            mask: None,
            stretch: false,
            includepix: None,
            excludepix: None,
            list: true,
            robust: true,
            logfile: None,
            append: false,
            algorithm: "classic",
            fence: -1.0,
            center: "mean",
            lside: true,
            zscore: -1.0,
            maxiter: -1,
            clmethod: "auto",
            niter: 3,
            disk: false,
            verbose: false,
        },
    )
    .unwrap();
    assert!((stats.npts.iter().sum::<f64>() - 256.0).abs() < 1e-6);

    // Mask expression, put_chunk read-back, and mask handler
    let mask_expr = format!("'{}' > 2.0", img_path.display());
    assert!(calc_mask(&attached, &mask_expr, "m1", true, true).unwrap());
    let masks = mask_handler_casa(&attached, "get", &[], false).unwrap();
    assert_eq!(masks, vec!["m1".to_owned()]);

    let scaled = data.mapv(|v| v * 2.0);
    put_chunk(
        &attached,
        &PixelChunk::Float(scaled.clone()),
        &[0, 0, 0, 0],
        &[1, 1, 1, 1],
        false,
        false,
        true,
    )
    .unwrap();
    let read_back = get_chunk(
        &attached,
        &GetChunkRequest {
            getmask: true,
            ..Default::default()
        },
    )
    .unwrap();
    match read_back.pixels {
        PixelChunk::Float(arr) => {
            assert!((arr[[0, 0, 0, 0]] - scaled[[0, 0, 0, 0]]).abs() < 1e-5);
            assert!((arr[[3, 2, 0, 1]] - scaled[[3, 2, 0, 1]]).abs() < 1e-5);
        }
        other => panic!("expected Float chunk, got {other:?}"),
    }
    let read_mask = read_back.mask.expect("mask returned");
    assert!(!read_mask[[0, 0, 0, 0]]);
    assert!(read_mask[[7, 7, 0, 3]]);
}

#[test]
fn pure_rust_image_tool_subimage_spatial_and_transforms() {
    let dir = tempfile::tempdir().unwrap();
    let (_img_path, _data, _state, attached) = create_test_cube(dir.path(), "base.im");

    // Subimage, transpose, crop, pad, collapse, regrid, smooth
    let sub_path = dir.path().join("sub.im");
    let (sub_opt, _) = extract_subimage(
        &attached,
        &SubimageRequest {
            outfile: sub_path.to_str().unwrap(),
            region: None,
            mask: None,
            dropdeg: true,
            overwrite: true,
            list: true,
            stretch: false,
            wantreturn: true,
            keepaxes: &[],
        },
        true,
        &no_external_open,
    )
    .unwrap();
    let sub_att = sub_opt.unwrap();
    assert_eq!(sub_att.lock().shape(), &[8, 8, 4]);

    let trans_att = transpose_attached(
        &sub_att,
        "",
        &TransposeOrder::Digits("102".to_owned()),
        true,
        true,
        &no_external_open,
    )
    .unwrap();
    assert_eq!(trans_att.lock().shape(), &[8, 8, 4]);

    let col_h = collapse_image(
        &attached,
        &casa_imageanalysis::tool::ops_transform::CollapseRequest {
            function: "mean",
            axes: casa_imageanalysis::tool::ops_transform::CollapseAxes::Numbers(vec![3]),
            outfile: "",
            region: None,
            spatial: "",
            chans: "",
            stokes: "",
            mask: "",
            overwrite: true,
            stretch: false,
            history_command: Some("ia.collapse()".into()),
        },
        &no_external_open,
    )
    .unwrap();
    assert_eq!(col_h.lock().shape(), &[8, 8, 1, 1]);

    let pad_h = pad_image(
        &attached,
        "",
        true,
        ImagePadOptions {
            pixels: 2,
            value: 0.0,
            padmask: true,
            region: None,
            box_selection: None,
            channels: None,
            stokes: None,
            mask: None,
            stretch: false,
            destination: None,
        },
        true,
        Some("ia.pad()".into()),
        &no_external_open,
    )
    .unwrap()
    .unwrap();
    assert_eq!(pad_h.lock().shape(), &[12, 12, 1, 4]);

    let crop_h = crop_image(
        &attached,
        "",
        true,
        ImageCropOptions {
            axes: &[],
            box_selection: None,
            region: None,
            channels: None,
            stokes: None,
            mask: None,
            stretch: false,
        },
        true,
        Some("ia.crop()".into()),
        &no_external_open,
    )
    .unwrap()
    .0
    .unwrap();
    assert_eq!(crop_h.lock().shape(), &[8, 8, 1, 4]);

    let smooth_h = smooth_axis(
        &attached,
        "",
        true,
        casa_imageanalysis::analysis::SmoothingOptions {
            axis: 3,
            kernel: SmoothingKernel::Hanning,
            decimate: None,
            region: None,
            mask: None,
            stretch: false,
            destination: None,
        },
        None,
        &no_external_open,
    )
    .unwrap();
    assert_eq!(smooth_h.lock().shape().len(), 4);

    let regrid_h = regrid_attached(
        &attached,
        "",
        true,
        casa_imageanalysis::analysis::ImageRegridOptions {
            shape: &[],
            target: ImageRegridTarget::Coordinates(None),
            axes: &[],
            region: None,
            mask: None,
            method: "linear",
            decimate: 10,
            replicate: false,
            convert_reference: true,
            dropdeg: false,
            force: false,
            as_velocity: false,
            stretch: false,
            outfile: None,
        },
        Some(("regrid", "ia.regrid()".into())),
        &no_external_open,
    )
    .unwrap();
    assert_eq!(regrid_h.lock().shape(), &[8, 8, 1, 4]);
}

#[test]
fn pure_rust_image_tool_serialization_calc_concat_and_rename() {
    let dir = tempfile::tempdir().unwrap();
    let (img_path, _data, mut state, attached) = create_test_cube(dir.path(), "base.im");

    // Conformance, serialization, adddegaxes, noise, set, imagecalc, concat, rename
    let coords_copy = attached.lock().coordinates().clone();
    let conf = check_conformance(&attached.lock(), &[8, 8, 1, 4], &coords_copy, "self").unwrap();
    assert!(conf.conforms);

    let rec = image_to_record(&attached).unwrap();
    let restored = image_from_record(&rec, "", true, &no_external_open).unwrap();
    assert_eq!(restored.lock().shape(), &[8, 8, 1, 4]);

    let shape2 = vec![4, 4];
    let (att2d, _) = create_from_shape(
        &FromShapeRequest {
            outfile: "",
            shape: shape2.clone(),
            csys_record: Some(default_record_for_shape(&shape2, true).unwrap()),
            linear: true,
            overwrite: true,
            log: false,
            pixel_type: ImagePixelType::Float32,
        },
        false,
        &no_external_open,
    )
    .unwrap();
    let (deg_att, _) = add_degenerate_axes(
        &att2d,
        &AddDegAxesRequest {
            outfile: "",
            direction: false,
            spectral: true,
            stokes: "I",
            linear: false,
            tabular: false,
            overwrite: true,
            silent: true,
        },
        false,
        &no_external_open,
    )
    .unwrap();
    assert_eq!(deg_att.lock().shape(), &[4, 4, 1, 1]);

    add_noise(&att2d, "normal", &[0.0, 0.1], &[1, 2], None, false, None).unwrap();
    set_pixels_and_mask(&att2d, Some("2.0"), -1, None, false, None).unwrap();

    // imagecalc & concatenate
    let calc_expr = format!("'{}' * 2.0", img_path.display());
    let (calc_att, _) = image_calc("", &calc_expr, true, "", false, &no_external_open).unwrap();
    assert_eq!(calc_att.lock().shape(), &[8, 8, 1, 4]);

    let img_b = dir.path().join("base_b.im");
    let (_, _) = create_from_array(
        img_b.to_str().unwrap(),
        PixelArrayInput::Float(ArrayD::ones(IxDyn(&[8, 8, 1, 4]))),
        None,
        false,
        true,
        false,
        false,
        &no_external_open,
    )
    .unwrap();

    let (concat_paged, _) = concatenate_casa_images(
        "",
        "",
        &[img_path.clone(), img_b.clone()],
        "['base.im', 'base_b.im']",
        3,
        true,
        false,
        true,
        false,
        "paged",
        false,
        &no_external_open,
    )
    .unwrap();
    assert_eq!(concat_paged.lock().shape(), &[8, 8, 1, 8]);

    let concat_virt_path = dir.path().join("joined.imgconcat");
    let concat_virt_str = concat_virt_path.to_str().unwrap();
    let (concat_virt, _) = concatenate_casa_images(
        concat_virt_str,
        concat_virt_str,
        &[img_path.clone(), img_b],
        "['base.im', 'base_b.im']",
        3,
        true,
        true,
        true,
        false,
        "nomovevirtual",
        false,
        &no_external_open,
    )
    .unwrap();
    assert_eq!(concat_virt.lock().shape(), &[8, 8, 1, 8]);

    // Rename & done
    unlock_image(&attached).unwrap();
    drop(attached);
    let renamed = dir.path().join("renamed.im");
    state
        .rename(renamed.to_str().unwrap(), false, &no_external_open)
        .unwrap();
    assert!(renamed.join("table.dat").is_file());
    assert!(!img_path.exists());
    let _ = SubimageRegion::Text("");
    state.done(true, true, &no_external_open).unwrap();
    assert!(!renamed.exists());
}

fn create_parity_cube(dir: &std::path::Path) -> (std::path::PathBuf, AttachedImage) {
    use ndarray::{ArrayD, IxDyn};

    let base_path = dir.join("cube.im");
    let mut pixels = ArrayD::<f32>::from_elem(IxDyn(&[8, 8, 1, 8]), 1.0);
    pixels[[4, 4, 0, 4]] = 5.0;
    pixels[[4, 3, 0, 4]] = 2.5;
    pixels[[3, 4, 0, 4]] = 2.5;
    let (attached, _) = create_from_array(
        base_path.to_str().unwrap(),
        PixelArrayInput::Float(pixels),
        None,
        false,
        true,
        false,
        false,
        &no_external_open,
    )
    .unwrap();
    (base_path, attached)
}

#[test]
fn beam_summary_stats_copy_and_record_round_trip_without_python() {
    use casa_core::{RecordValue, quanta::Quantity};
    use casa_imageanalysis::tool::beam_ops::{
        FullSetBeamRequest, deconvolve_beam_context, deconvolve_from_beam,
        deconvolve_gaussian_source, rotate_beam_quantity, set_restoring_beam_full,
    };
    use casa_imageanalysis::tool::conformance::is_conform;
    use casa_imageanalysis::tool::convertflux::convert_flux_quantity;
    use casa_imageanalysis::tool::copy::{CopyImageRequest, CopyRegion, copy_image};
    use casa_imageanalysis::tool::serialization::image_from_record_with_history;

    let dir = tempfile::tempdir().unwrap();
    let (base_path, attached) = create_parity_cube(dir.path());
    let empty_rec = RecordValue::default();
    let msg = set_restoring_beam_full(
        &attached,
        &FullSetBeamRequest {
            quantities: Some([
                Quantity::new(4.0, "arcsec").unwrap(),
                Quantity::new(2.0, "arcsec").unwrap(),
                Quantity::new(15.0, "deg").unwrap(),
            ]),
            beam: &empty_rec,
            beam_history: "{}",
            input_histories: ["'4arcsec'", "'2arcsec'", "'15deg'"],
            remove: false,
            log: true,
            channel: -1,
            polarization: -1,
            imagename: "",
            imagename_path: None,
            has_axis_args: true,
        },
        true,
    )
    .unwrap();
    assert!(msg.is_some());
    assert!(
        !rotate_beam_quantity(
            &attached,
            &Quantity::new(10.0, "deg").unwrap(),
            true,
            "10deg"
        )
        .unwrap()
        .is_empty()
    );

    let deconv = deconvolve_from_beam(
        &[
            Quantity::new(6.0, "arcsec").unwrap(),
            Quantity::new(4.0, "arcsec").unwrap(),
            Quantity::new(35.0, "deg").unwrap(),
        ],
        &[
            Quantity::new(4.0, "arcsec").unwrap(),
            Quantity::new(2.0, "arcsec").unwrap(),
            Quantity::new(25.0, "deg").unwrap(),
        ],
    )
    .unwrap();
    assert!(deconv.get("fit").is_some());
    let beam = deconvolve_beam_context(&attached.lock(), &attached.beam_view(), -1, -1).unwrap();
    let comp_fit = deconvolve_gaussian_source(
        &[
            Quantity::new(6.0, "arcsec").unwrap(),
            Quantity::new(4.0, "arcsec").unwrap(),
            Quantity::new(25.0, "deg").unwrap(),
        ],
        beam,
    )
    .unwrap();
    assert!(comp_fit.major > 0.0);

    set_brightness_unit(&attached, "Jy/beam", true).unwrap();
    let (converted, _) = convert_flux_quantity(
        &attached.lock(),
        &attached.beam_view(),
        Quantity::new(1.0, "Jy").unwrap(),
        [
            Quantity::new(4.0, "arcsec").unwrap(),
            Quantity::new(2.0, "arcsec").unwrap(),
        ],
        true,
        (-1, -1),
    )
    .unwrap();
    assert!(converted.value().is_finite());

    let (summary, summary_logs) = summarize_image(
        &attached,
        &SummaryOptions {
            list: true,
            pixel_order: true,
            verbose: true,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(summary.restoringbeam.is_some());
    assert!(!summary_logs.is_empty());

    let stats = image_statistics(
        &attached,
        &StatisticsRequest {
            axes: None,
            region: None,
            mask: None,
            stretch: false,
            includepix: None,
            excludepix: None,
            list: true,
            robust: true,
            logfile: None,
            append: false,
            algorithm: "classic",
            fence: -1.0,
            center: "mean",
            lside: true,
            zscore: -1.0,
            maxiter: -1,
            clmethod: "auto",
            niter: 3,
            disk: false,
            verbose: true,
        },
    )
    .unwrap();
    assert!(stats.record.get("mean").is_some());
    assert!(stats.record.get("median").is_some());

    let biweight = image_statistics(
        &attached,
        &StatisticsRequest {
            axes: None,
            region: None,
            mask: None,
            stretch: false,
            includepix: None,
            excludepix: None,
            list: false,
            robust: false,
            logfile: None,
            append: false,
            algorithm: "biweight",
            fence: -1.0,
            center: "mean",
            lside: true,
            zscore: -1.0,
            maxiter: -1,
            clmethod: "auto",
            niter: 3,
            disk: false,
            verbose: false,
        },
    )
    .unwrap();
    assert!(biweight.record.get("mean").is_some());

    let copy_path = dir.path().join("copied.im");
    let copied = copy_image(
        &CopyImageRequest {
            method: "fromimage",
            infile: base_path.to_str().unwrap(),
            resolved_infile: &base_path,
            outfile: copy_path.to_str().unwrap(),
            region: CopyRegion::Record(RecordValue::default()),
            region_history: "{}".into(),
            mask: String::new(),
            mask_history: "''".into(),
            dropdeg: false,
            overwrite: true,
            history: true,
        },
        Some(&attached),
        &no_external_open,
    )
    .unwrap();
    assert!(is_conform(&attached, copy_path.to_str().unwrap(), &copy_path).unwrap());
    let rec = image_to_record(&copied).unwrap();
    let restored = image_from_record_with_history(&rec, "", true, true, &no_external_open).unwrap();
    assert_eq!(restored.lock().shape(), &[8, 8, 1, 8]);
}

#[test]
fn pixel_edit_and_spatial_ops_run_without_python() {
    use casa_core::RecordValue;
    use casa_imageanalysis::analysis::{DecimationMethod, ImageRebinOptions};
    use casa_imageanalysis::tool::calc::calc_attached_pixels;
    use casa_imageanalysis::tool::copy::{CopyImageRequest, CopyRegion, copy_image};
    use casa_imageanalysis::tool::masks::mask_handler_casa;
    use casa_imageanalysis::tool::ops_edit::{insert_image, put_region, replace_masked_pixels};
    use casa_imageanalysis::tool::ops_spatial::{
        decimate_image, make_complex_image, pv_image, rebin_image,
    };
    use casa_imageanalysis::tool::pixels::PixelChunk;
    use casa_imageanalysis::tool::point::{PixelScalar, pixel_value_casa};
    use ndarray::{ArrayD, IxDyn};

    let dir = tempfile::tempdir().unwrap();
    let (base_path, attached) = create_parity_cube(dir.path());
    let copy_path = dir.path().join("copied.im");
    let copied = copy_image(
        &CopyImageRequest {
            method: "fromimage",
            infile: base_path.to_str().unwrap(),
            resolved_infile: &base_path,
            outfile: copy_path.to_str().unwrap(),
            region: CopyRegion::Record(RecordValue::default()),
            region_history: "{}".into(),
            mask: String::new(),
            mask_history: "''".into(),
            dropdeg: false,
            overwrite: true,
            history: true,
        },
        Some(&attached),
        &no_external_open,
    )
    .unwrap();

    let mask_expr = format!("'{}' > 0", copy_path.display());
    assert!(calc_mask(&copied, &mask_expr, "m0", true, true).unwrap());
    let masks = mask_handler_casa(&copied, "get", &[], false).unwrap();
    assert_eq!(masks, vec!["m0".to_owned()]);
    assert_eq!(
        mask_handler_casa(&copied, "set", &["m0".into()], true).unwrap(),
        vec!["T".to_owned()]
    );
    let default_mask = mask_handler_casa(&copied, "default", &[], false).unwrap();
    assert_eq!(default_mask, vec!["m0".to_owned()]);
    let pv = pixel_value_casa(&copied, &[4.0, 4.0, 0.0, 4.0])
        .unwrap()
        .unwrap();
    assert!(matches!(pv.value, PixelScalar::Float(v) if (v - 5.0).abs() < 1e-4));

    let sub_chunk = PixelChunk::Float(ArrayD::from_elem(IxDyn(&[8, 8, 1, 8]), 3.0));
    let sub_mask = ArrayD::from_elem(IxDyn(&[8, 8, 1, 8]), false);
    put_region(
        &copied,
        Some(&sub_chunk),
        Some(&sub_mask),
        None,
        false,
        true,
        false,
        Some("ia.putregion()".into()),
    )
    .unwrap();
    assert!(
        replace_masked_pixels(&copied, "0.5", None, "", false, false, false, None)
            .unwrap()
            .1
    );
    assert!(
        calc_attached_pixels(&copied, "1.0", false, false)
            .unwrap()
            .0
    );
    assert!(
        insert_image(
            &copied,
            base_path.to_str().unwrap(),
            None,
            &[0.0],
            false,
            None,
        )
        .unwrap()
        .0
    );

    let dec = decimate_image(
        &attached,
        "",
        false,
        3,
        2,
        DecimationMethod::Mean,
        None,
        None,
        false,
        None,
        &no_external_open,
    )
    .unwrap();
    assert_eq!(dec.lock().shape(), &[8, 8, 1, 4]);

    let reb = rebin_image(
        &attached,
        "",
        false,
        ImageRebinOptions {
            factors: &[2, 2, 1, 2],
            region: None,
            mask: None,
            stretch: false,
            dropdeg: false,
            crop: false,
        },
        None,
        &no_external_open,
    )
    .unwrap();
    assert_eq!(reb.lock().shape(), &[4, 4, 1, 4]);

    let cpx_path = dir.path().join("complex.im");
    make_complex_image(
        &attached,
        cpx_path.to_str().unwrap(),
        &copy_path,
        None,
        true,
        None,
        &no_external_open,
    )
    .unwrap();
    assert!(cpx_path.join("table.dat").is_file());

    let (pv_out, _) = pv_image(
        &attached,
        &casa_imageanalysis::ImpvRequest {
            mode: "coords".into(),
            start: Some(casa_imageanalysis::PvPosition::Integral([2, 4])),
            end: Some(casa_imageanalysis::PvPosition::Integral([5, 4])),
            width: casa_imageanalysis::PvAmount::Whole(1),
            unit: "arcsec".into(),
            ..Default::default()
        },
        None,
        "",
        true,
        true,
        None,
        &no_external_open,
    )
    .unwrap();
    assert!(pv_out.is_some());
}

#[test]
fn transform_and_convolution_ops_run_without_python() {
    use casa_core::quanta::Quantity;
    use casa_imageanalysis::analysis::{
        DeviationOptions, GaussianConvolutionOptions, SepConvolutionOptions, TwoPointOptions,
    };
    use casa_imageanalysis::tool::ops_transform::{
        ConvolveKernelInput, continuum_sub, convolve_2d_gaussian, convolve_image, deviation_image,
        fft_image, moments_image, sepconvolve_image, two_point_correlation,
    };
    use casa_images::resample::RegridMethod;
    use ndarray::{ArrayD, IxDyn};

    let dir = tempfile::tempdir().unwrap();
    let (_, attached) = create_parity_cube(dir.path());

    let conv_arr = ConvolveKernelInput::Array(ArrayD::from_elem(IxDyn(&[3, 3]), 1.0));
    let conv = convolve_image(
        &attached,
        "",
        &conv_arr,
        -1.0,
        None,
        "",
        false,
        false,
        None,
        &no_external_open,
    )
    .unwrap();
    assert_eq!(conv.lock().shape(), &[8, 8, 1, 8]);

    let c2d = convolve_2d_gaussian(
        &attached,
        "",
        None,
        "",
        GaussianConvolutionOptions {
            axes: vec![0, 1],
            widths: [
                Quantity::new(2.0, "pix").unwrap(),
                Quantity::new(2.0, "pix").unwrap(),
                Quantity::new(0.0, "deg").unwrap(),
            ],
            scale: -1.0,
            target_resolution: false,
        },
        -1.0,
        false,
        false,
        None,
        &no_external_open,
    )
    .unwrap();
    assert_eq!(c2d.lock().shape(), &[8, 8, 1, 8]);

    let cont = continuum_sub(
        &attached,
        "",
        "",
        None,
        &[-1],
        0,
        true,
        None,
        &no_external_open,
    )
    .unwrap();
    assert_eq!(cont.lock().shape(), &[8, 8, 1, 8]);

    let dev = deviation_image(
        &attached,
        "",
        false,
        DeviationOptions {
            region: None,
            mask: Some(""),
            stretch: false,
            destination: None,
            grid: [1, 1],
            anchor: None,
            x_length: Quantity::new(2.0, "pix").unwrap(),
            y_length: None,
            interpolation: RegridMethod::Nearest,
            statistic: "sigma",
            algorithm: "classic",
            zscore: -1.0,
            maxiter: -1,
        },
        None,
        &no_external_open,
    )
    .unwrap();
    assert_eq!(dev.lock().shape(), &[8, 8, 1, 8]);

    let fft_amp = dir.path().join("fft_amp.im");
    assert!(
        fft_image(
            &attached,
            ["", "", fft_amp.to_str().unwrap(), "", ""],
            &[0, 1],
            None,
            "",
            false,
            None,
            &no_external_open,
        )
        .unwrap()
    );
    assert!(fft_amp.join("table.dat").is_file());

    let mom = moments_image(
        &attached,
        &[0],
        3,
        None,
        None,
        false,
        true,
        &[],
        (&[-1], &[], &[0.0]),
        &[-1.0],
        &[-1.0],
        3.0,
        0.0,
        casa_core::measures::doppler::DopplerRef::RADIO,
        "",
        "",
        true,
        None,
        &no_external_open,
    )
    .unwrap();
    assert_eq!(mom.lock().shape(), &[8, 8, 1]);

    let sep = sepconvolve_image(
        &attached,
        "",
        false,
        SepConvolutionOptions {
            axes: &[0],
            types: &["boxcar".into()],
            widths: &[Quantity::new(2.0, "pix").unwrap()],
            scale: -1.0,
            region: None,
            mask: None,
            stretch: false,
            destination: None,
        },
        None,
        &no_external_open,
    )
    .unwrap();
    assert_eq!(sep.lock().shape(), &[8, 8, 1, 8]);

    let tp = two_point_correlation(
        &attached,
        "",
        false,
        TwoPointOptions {
            axes: &[0, 1],
            method: "structurefunction",
            region: None,
            mask: None,
            stretch: false,
            destination: None,
        },
        None,
        &no_external_open,
    )
    .unwrap();
    assert_eq!(tp.lock().shape(), &[15, 15, 1, 8]);
}

#[test]
fn fitting_and_profile_ops_run_without_python() {
    use casa_imageanalysis::analysis::{
        CollapseFunction, ProfileDivisor, ProfileEstimates, ProfileOutputNames, ProfilePeaks,
        ReadProfileOptions,
    };
    use casa_imageanalysis::tool::ops_fit::{
        FitComponentsRequest, FitProfileRequest, ProfileData, ProfileSigmaInput, fit_components,
        fit_profile, get_profile,
    };

    let dir = tempfile::tempdir().unwrap();
    let (_, attached) = create_parity_cube(dir.path());

    let prof = get_profile(
        &attached,
        ReadProfileOptions {
            axis: 3,
            function: CollapseFunction::Mean,
            region: None,
            mask: None,
            stretch: false,
            unit: "",
            spectral_type: "default",
            rest_frequency: None,
            spectral_frame: None,
            logfile: None,
        },
    )
    .unwrap();
    assert!(matches!(prof, ProfileData::Float(p) if p.values.len() == 8));

    let comp_rec = fit_components(
        &attached,
        FitComponentsRequest {
            box_selection: None,
            region: None,
            chans: Some("4"),
            stokes: None,
            mask: None,
            stretch: false,
            includepix: &[-1.0],
            excludepix: &[-1.0],
            residual: "",
            model: "",
            estimates: None,
            logfile: "",
            append: true,
            newestimates: "",
            complist: "",
            summary: "",
            overwrite: true,
            zero_level: None,
            rms: None,
            noise_fwhm: None,
            history_call: "ia.fitcomponents()".into(),
            record_history: false,
        },
        &no_external_open,
    )
    .unwrap();
    assert!(comp_rec.get("converged").is_some());

    let (prof_rec, _) = fit_profile(
        &attached,
        FitProfileRequest {
            selection: ["", "", ""],
            region: None,
            mask: "",
            axis: 3,
            ngauss: 0,
            poly: 0,
            min_points: 1,
            estimates_file: "",
            multifit: false,
            stretch: false,
            estimates: ProfileEstimates {
                singlets: ProfilePeaks {
                    amplitudes: &[],
                    centers: &[],
                    widths: &[],
                    fixed: &[],
                },
                functions: &[],
                multiplets: ProfilePeaks {
                    amplitudes: &[],
                    centers: &[],
                    widths: &[],
                    fixed: &[],
                },
                multiplet_sizes: &[],
                multiplet_ties: [&[], &[], &[]],
                spectral_kind: "",
                spectral_values: &[],
                spectral_fixed: &[],
            },
            good_ranges: [None, None, None],
            planes: &[],
            divisor: ProfileDivisor::Automatic,
            sigma: ProfileSigmaInput::None,
            outputs: ProfileOutputNames {
                spectra: ["", ""],
                lines: [""; 8],
                coefficients: ["", ""],
                sigma: "",
            },
            logfile: "",
            append: true,
            log_details: false,
            history_call: "ia.fitprofile()".into(),
            record_history: false,
        },
        &no_external_open,
    )
    .unwrap();
    assert!(prof_rec.get("valid").is_some());
}

#[test]
fn published_on_disk_images_stay_registered_and_support_rename_and_remove_refusal() {
    use casa_components::ComponentList;
    use casa_coordinates::CoordinateSystem;
    use casa_imageanalysis::tool::create::create_from_component_list;
    use std::collections::BTreeMap;

    let dir = tempfile::tempdir().unwrap();
    let shape_path = dir.path().join("shape_disk.im");
    let (shape_att, _) = create_from_shape(
        &FromShapeRequest {
            outfile: shape_path.to_str().unwrap(),
            shape: vec![4, 4, 1, 1],
            csys_record: None,
            linear: true,
            overwrite: true,
            log: false,
            pixel_type: ImagePixelType::Float32,
        },
        false,
        &no_external_open,
    )
    .unwrap();
    // Newly created on-disk handle must already be registered so removing it is refused.
    assert!(remove_file(&shape_path, &no_external_open).is_err());

    // In-place rename while open (Blocking #2) preserves attachment and moves table cleanly.
    assert!(lock_image(&shape_att, "write", 1).unwrap());
    let renamed_path = dir.path().join("shape_renamed.im");
    let mut state = ImageToolState::attached(shape_att);
    assert!(
        state
            .rename_casa(
                renamed_path.to_str().unwrap(),
                Ok::<_, casa_imageanalysis::tool::ImageToolError>(
                    renamed_path.to_str().unwrap().to_owned()
                ),
                false,
                &no_external_open,
            )
            .unwrap()
    );
    assert!(!shape_path.exists());
    assert!(renamed_path.join("table.dat").is_file());
    assert_eq!(state.casa_name(true).unwrap(), "shape_renamed.im");
    let att_after = state.detach().unwrap();
    put_chunk(
        &att_after,
        &PixelChunk::Float(ArrayD::from_elem(IxDyn(&[4, 4, 1, 1]), 3.5)),
        &[0, 0, 0, 0],
        &[1, 1, 1, 1],
        false,
        false,
        false,
    )
    .unwrap();
    drop(att_after);
    assert!(remove_file(&renamed_path, &no_external_open).unwrap());

    // Component-list image published on disk with cache=false is registered and readable.
    let cl_path = dir.path().join("comp_disk.im");
    let rec = default_record_for_shape(&[4, 4, 1, 2], false).unwrap();
    let coords = CoordinateSystem::from_record(&rec).unwrap();
    let (cl_att, _) = create_from_component_list(
        cl_path.to_str().unwrap(),
        vec![4, 4, 1, 2],
        coords,
        rec,
        &BTreeMap::new(),
        ComponentList::default(),
        false,
        true,
        false,
        &no_external_open,
    )
    .unwrap();
    assert!(remove_file(&cl_path, &no_external_open).is_err());
    drop(cl_att);
    assert!(remove_file(&cl_path, &no_external_open).unwrap());
}

#[test]
fn owned_virtual_concat_copy_move_and_rollback_without_python() {
    let dir = tempfile::tempdir().unwrap();
    let p1 = dir.path().join("in1.im");
    let p2 = dir.path().join("in2.im");
    for path in [&p1, &p2] {
        let (att, _) = create_from_array(
            path.to_str().unwrap(),
            PixelArrayInput::Float(ArrayD::from_elem(IxDyn(&[4, 4, 1, 2]), 2.0)),
            None,
            false,
            true,
            false,
            false,
            &no_external_open,
        )
        .unwrap();
        drop(att);
    }

    let copy_virt = dir.path().join("copy_v.imgconcat");
    let (copy_att, _) = concatenate_casa_images(
        copy_virt.to_str().unwrap(),
        copy_virt.to_str().unwrap(),
        &[p1.clone(), p2.clone()],
        "['in1.im', 'in2.im']",
        3,
        true,
        true,
        true,
        false,
        "copyvirtual",
        false,
        &no_external_open,
    )
    .unwrap();
    assert_eq!(copy_att.lock().shape(), &[4, 4, 1, 4]);
    assert!(p1.exists() && p2.exists());
    drop(copy_att);

    // Hold p2 open to verify movevirtual rolls back and leaves p1/p2 intact.
    let blocker = AttachedImage::open(&p2).unwrap();
    let move_fail = dir.path().join("move_fail.imgconcat");
    assert!(
        concatenate_casa_images(
            move_fail.to_str().unwrap(),
            move_fail.to_str().unwrap(),
            &[p1.clone(), p2.clone()],
            "['in1.im', 'in2.im']",
            3,
            true,
            true,
            true,
            false,
            "movevirtual",
            false,
            &no_external_open,
        )
        .is_err()
    );
    assert!(p1.exists() && p2.exists());
    assert!(!move_fail.exists());
    drop(blocker);

    let move_virt = dir.path().join("move_v.imgconcat");
    let (move_att, _) = concatenate_casa_images(
        move_virt.to_str().unwrap(),
        move_virt.to_str().unwrap(),
        &[p1.clone(), p2.clone()],
        "['in1.im', 'in2.im']",
        3,
        true,
        true,
        true,
        false,
        "movevirtual",
        false,
        &no_external_open,
    )
    .unwrap();
    assert_eq!(move_att.lock().shape(), &[4, 4, 1, 4]);
    assert!(!p1.exists() && !p2.exists());
}

#[test]
fn read_only_paged_image_close_done_unlock_and_open_refusal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ro.im");
    let (created, _) = create_from_array(
        path.to_str().unwrap(),
        PixelArrayInput::Float(ArrayD::from_elem(IxDyn(&[4, 4]), 7.0)),
        None,
        false,
        true,
        false,
        false,
        &no_external_open,
    )
    .unwrap();
    drop(created);

    // Read-only directory without table.lock opens a !is_writable() PagedImage attachment.
    let _ = std::fs::remove_file(path.join("table.lock"));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o555);
        std::fs::set_permissions(&path, perms).unwrap();
    }
    let ro = AttachedImage::open_resolved(&path, &path, &path).unwrap();
    assert!(matches!(
        &*ro.lock(),
        casa_imageanalysis::tool::handle::ImageHandle::Float(
            casa_imageanalysis::tool::handle::Storage::Paged(paged)
        ) if !paged.is_writable()
    ));
    assert!(unlock_image(&ro).unwrap());
    let mut state = ImageToolState::attached(ro);
    assert!(state.close().unwrap());

    let ro2 = AttachedImage::open_resolved(&path, &path, &path).unwrap();
    assert!(remove_file(&path, &no_external_open).is_err());
    let mut state2 = ImageToolState::attached(ro2);
    assert!(state2.done(false, false, &no_external_open).unwrap().0);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&path, perms).unwrap();
    }
    assert!(remove_file(&path, &no_external_open).unwrap());
}
