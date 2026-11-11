// SPDX-License-Identifier: LGPL-3.0-or-later

//! Ordinary tclean writes its PB; casa-rs's explicit opt-out remains available.

use casa_images::PagedImage;
use casa_ms::MeasurementSet;
use casa_provider_contracts::TaskReply;
use casa_types::{ScalarValue, Value};
use serde_json::json;

use super::{
    casa_return::json_run,
    fixture::{Component, Observation, Setup},
};

#[test]
fn a_default_dirty_run_writes_a_readable_pb_and_an_explicit_opt_out_does_not() {
    let point = Component {
        offset_px: [0, 0],
        flux_jy: 1.0,
        fwhm_px: None,
        spectral_index: 0.0,
    };
    let observation = Observation::synthesise(&[("point", point)]);
    let directory = observation.scratch("pb-default");
    std::fs::create_dir_all(&directory).unwrap();
    let geometry = observation.geometry();
    let base = json!({"vis": observation.measurement_set(), "niter": 0, "backend": "cpu",
        "imsize": geometry.image_size,
        "cell": geometry.cell_arcsec.map(|v| format!("{v}arcsec"))});
    for (name, opt_out) in [("default", false), ("opt-out", true)] {
        let mut values = base.clone();
        values["imagename"] = json!(directory.join(name));
        if opt_out {
            values["write_pb"] = json!(false);
        }
        assert!(matches!(
            json_run(&directory, values),
            TaskReply::Succeeded { .. }
        ));
        let path = directory.join(format!("{name}.pb"));
        if opt_out {
            assert!(!path.exists());
        } else {
            let pb = PagedImage::<f32>::open(path).expect("persisted PB reopens");
            assert_eq!(&pb.shape()[..2], &geometry.image_size);
            assert!(pb.get().unwrap().iter().all(|value| value.is_finite()));
        }
    }
}

#[test]
fn alma_labelled_seven_metre_dishes_write_the_aca_primary_beam_by_default() {
    let point = Component {
        offset_px: [0, 0],
        flux_jy: 1.0,
        fwhm_px: None,
        spectral_index: 0.0,
    };
    let mut beams = Vec::new();
    for telescope in ["ALMA", "ACA"] {
        let mut setup = Setup::vla_q_band(2);
        setup.telescope = telescope.to_string();
        for antenna in &mut setup.antennas {
            antenna.dish_diameter_m = 7.0;
        }
        let observation = Observation::synthesise_setup(setup, &[("point", point)]);
        let directory = observation.scratch("seven-metre");
        std::fs::create_dir_all(&directory).unwrap();
        let geometry = observation.geometry();
        let values = json!({
            "vis": observation.measurement_set(), "imagename": directory.join("image"),
            "niter": 0, "backend": "cpu", "imsize": geometry.image_size,
            "cell": geometry.cell_arcsec.map(|v| format!("{v}arcsec")),
        });
        let reply = json_run(&directory, values);
        assert!(
            matches!(reply, TaskReply::Succeeded { .. }),
            "{telescope}: {reply:?}"
        );
        beams.push(
            PagedImage::<f32>::open(directory.join("image.pb"))
                .unwrap()
                .get()
                .unwrap(),
        );
    }
    assert_eq!(beams[0].shape(), beams[1].shape());
    assert!(
        beams
            .iter()
            .all(|beam| beam.iter().all(|value| value.is_finite()))
    );
    let worst = beams[0]
        .iter()
        .zip(&beams[1])
        .fold(0.0_f32, |m, (a, b)| m.max((a - b).abs()));
    assert!(worst <= 1e-6, "ALMA/ACA 7 m PB difference {worst:e}");
}

/// CASA selects the VLA beam using the first cube plane in LSRK, then
/// evaluates that beam at each output plane's BARY frequency. The LSRK
/// frequency is 999.99 MHz while BARY is 1000.030 MHz, on opposite sides
/// of the L-band boundary (CASA 6.7.6.14, issue #1138).
#[test]
fn bary_cube_uses_the_lsrk_band_and_bary_plane_frequencies() {
    let mut setup = Setup::vla_q_band(5);
    setup.spectral_windows[0].start_frequency_hz = 999_990_000.0;
    setup.spectral_windows[0].channel_width_hz = 50.0e6;
    setup.geometry = super::fixture::Geometry::square(100, 8.0);
    setup.noise_jy = 0.0;
    let point = Component {
        offset_px: [50, 50],
        flux_jy: 1.0,
        fwhm_px: None,
        spectral_index: 0.0,
    };
    let observation = Observation::synthesise_setup(setup, &[("point", point)]);
    let (_, products) = observation.image(
        "bary-vla-band",
        json!({
            "specmode": "cube", "spw": "0", "start": 0, "width": 1, "nchan": 5,
            "niter": 1, "outframe": "BARY", "weighting": "natural", "write_pb": true,
            "usemask": "pb", "pbmask": 0.99,
        }),
    );
    let pb = products.get(".pb");
    // The minor-cycle PB mask uses the same selected aperture as publication.
    let mask = products.get(".mask").plane(0);
    assert_eq!(mask[(50, 50)], 1.0);
    assert_eq!(mask[(75, 50)], 0.0);
    let reference = [
        (0.9846422076_f64, 0.9419874549_f64),
        (0.9830494523, 0.9362540245),
        (0.9814318419, 0.9301775098),
        (0.9797421098, 0.9238516092),
        (0.9779247642, 0.9173852205),
    ];
    assert!(
        pb.channel_frequency_hz(0) > 1.0e9,
        "BARY lies above the band edge"
    );
    for (channel, (near, far)) in reference.into_iter().enumerate() {
        let plane = pb.plane(channel);
        assert!(
            (f64::from(plane[(75, 50)]) - near).abs() < 1e-5,
            "channel {channel}: near {} vs {near}",
            plane[(75, 50)]
        );
        assert!(
            (f64::from(plane[(99, 50)]) - far).abs() < 1e-5,
            "channel {channel}: far {} vs {far}",
            plane[(99, 50)]
        );
    }
}

/// The VLA channel is just below 1 GHz in LSRK but above it in TOPO.
fn topo_boundary_observation(first_hz: f64) -> Observation {
    let mut setup = Setup::vla_q_band(5);
    setup.spectral_windows[0].start_frequency_hz = first_hz;
    setup.spectral_windows[0].channel_width_hz = 50.0e6;
    setup.geometry = super::fixture::Geometry::square(100, 8.0);
    setup.noise_jy = 0.0;
    let point = Component {
        offset_px: [50, 50],
        flux_jy: 1.0,
        fwhm_px: None,
        spectral_index: 0.0,
    };
    Observation::synthesise_setup(setup, &[("point", point)])
}

/// CASA 6.7.6.14 selects NVSS below 1 GHz in the LSRK conversion layer,
/// although the output TOPO plane is above it (#1198).
#[test]
fn topo_cube_selects_its_vla_band_in_lsrk() {
    let observation = topo_boundary_observation(999_990_000.0);
    let (_, products) = observation.image(
        "topo-vla-band",
        json!({
            "specmode": "cube", "spw": "0", "start": 0, "width": 1, "nchan": 5,
            "niter": 1, "outframe": "TOPO", "weighting": "natural", "write_pb": true,
            "usemask": "pb", "pbmask": 0.99,
        }),
    );
    let pb = products.get(".pb");
    assert!((pb.channel_frequency_hz(0) - 1_000_047_964.291_358_5).abs() < 0.5);
    let plane = pb.plane(0);
    assert!((f64::from(plane[(75, 50)]) - 0.984_642_207_6).abs() < 1e-5);
    assert!((f64::from(plane[(99, 50)]) - 0.941_987_454_9).abs() < 1e-5);
    assert_eq!(products.get(".mask").plane(0)[(75, 50)], 0.0);
}

/// Swapping complete MAIN rows keeps all observations, including their
/// payloads, while placing the last epoch before the earliest in table order.
fn swap_first_and_last_rows(observation: &Observation) {
    let mut ms = MeasurementSet::open(observation.measurement_set()).unwrap();
    let last = ms.row_count() - 1;
    let columns = ms
        .main_table()
        .schema()
        .unwrap()
        .columns()
        .iter()
        .map(|column| column.name().to_string())
        .collect::<Vec<_>>();
    for column in columns {
        let first = ms
            .main_table()
            .cell_accessor(0, &column)
            .unwrap()
            .value()
            .unwrap()
            .cloned();
        let final_value = ms
            .main_table()
            .cell_accessor(last, &column)
            .unwrap()
            .value()
            .unwrap()
            .cloned();
        match (first, final_value) {
            (Some(first), Some(final_value)) => {
                ms.main_table_mut()
                    .row_accessor_mut()
                    .set_cell(0, &column, final_value)
                    .unwrap();
                ms.main_table_mut()
                    .row_accessor_mut()
                    .set_cell(last, &column, first)
                    .unwrap();
            }
            (None, None) => {}
            _ => panic!("row definedness differs for {column}"),
        }
    }
    ms.save().unwrap();
}

/// CASA's image epoch is the first selected physical MAIN row, even when
/// that row is later than other selected times (#1198).
#[test]
fn topo_cube_image_epoch_follows_the_first_selected_physical_row() {
    let observation = topo_boundary_observation(1_000_000_118.0);
    swap_first_and_last_rows(&observation);
    let (_, products) = observation.image(
        "topo-physical-first-row",
        json!({
            "specmode": "cube", "spw": "0", "start": 0, "width": 1, "nchan": 5,
            "niter": 0, "outframe": "TOPO", "weighting": "natural", "write_pb": true,
        }),
    );
    let pb = products.get(".pb");
    let epoch = pb
        .image
        .coordinates()
        .obs_info()
        .date
        .as_ref()
        .unwrap()
        .value()
        .as_mjd()
        * 86_400.0;
    assert!(
        (epoch - 4_895_233_133.733_246).abs() < 0.001,
        "image epoch {epoch}"
    );
    assert!(
        (pb.channel_frequency_hz(0) - 1_000_057_847.341_391_3).abs() < 0.5,
        "first TOPO frequency {}",
        pb.channel_frequency_hz(0)
    );
    assert!((f64::from(pb.plane(0)[(75, 50)]) - 0.995_832_860_5).abs() < 1e-5);
    // CASA retains both edge planes after converting the cube to TOPO;
    // rounding in the conversion must not drop either plane (#1198).
    let sumwt = products.get(".sumwt");
    for (channel, expected) in [(0, 20_791.853_515_625), (4, 16_697.605_468_75)] {
        let actual = f64::from(sumwt.plane(channel)[(0, 0)]);
        assert!(
            (actual - expected).abs() / expected < 5e-5,
            "channel {channel}: sumwt {actual}, CASA {expected}"
        );
    }
}

/// With no explicit start, the selected band sets the first output plane.
/// An image that extends past the selection has no data in its last plane.
#[test]
fn topo_cube_partial_spw_starts_at_the_selected_band() {
    let observation = topo_boundary_observation(1_000_000_118.0);
    swap_first_and_last_rows(&observation);
    let (_, products) = observation.image(
        "topo-partial-spw",
        json!({
            "specmode": "cube", "spw": "0:1~4", "width": 1, "nchan": 5,
            "niter": 0, "outframe": "TOPO", "weighting": "natural", "write_pb": true,
        }),
    );
    let pb = products.get(".pb");
    let first = pb.channel_frequency_hz(0);
    assert!(
        (first - 1_050_060_733.808_120_3).abs() < 0.5,
        "first TOPO frequency {first}"
    );
    let sumwt = products.get(".sumwt");
    assert!(sumwt.plane(1)[(0, 0)] > 0.0);
    assert_eq!(sumwt.plane(4)[(0, 0)], 0.0);
}

/// The paired irregular-channel TOPO fixture, including its physical row order.
fn irregular_topo_observation() -> Observation {
    use casa_types::ArrayValue;
    let observation = topo_boundary_observation(1_000_000_118.0);
    swap_first_and_last_rows(&observation);
    let mut ms = MeasurementSet::open(observation.measurement_set()).unwrap();
    let frequencies = vec![
        1_000_000_118.0,
        1_050_000_241.0,
        1_100_000_063.0,
        1_150_000_201.0,
        1_200_000_118.0,
    ];
    ms.spectral_window_mut()
        .unwrap()
        .set_array(
            0,
            "CHAN_FREQ",
            ArrayValue::Float64(ndarray::ArrayD::from_shape_vec(vec![5], frequencies).unwrap()),
        )
        .unwrap();
    ms.save().unwrap();
    drop(ms);
    observation
}

/// CASA keeps both endpoints when an LSRK source grid has irregular interior
/// channels and the image WCS is in TOPO (#1198).
#[test]
fn topo_cube_preserves_irregular_source_grid_endpoints() {
    let observation = irregular_topo_observation();
    let (_, products) = observation.image(
        "topo-irregular-source-grid",
        json!({
            "specmode": "cube", "spw": "0", "start": 0, "width": 1, "nchan": 5,
            "niter": 0, "outframe": "TOPO", "weighting": "natural", "write_pb": true,
        }),
    );
    let sumwt = products.get(".sumwt");
    for (channel, expected) in [(0, 20_791.853_515_625), (4, 16_697.605_468_75)] {
        let actual = f64::from(sumwt.plane(channel)[(0, 0)]);
        assert!(
            (actual - expected).abs() / expected < 5e-5,
            "channel {channel}: sumwt {actual}, CASA {expected}"
        );
    }
}

/// A cleaned irregular TOPO cube writes the same predicted visibilities as
/// CASA at its first physical row (#1198). The paired external comparison
/// also checks every row and channel.
#[test]
fn topo_irregular_cube_predicts_casa_model_visibilities() {
    use casa_types::Complex32;
    let observation = irregular_topo_observation();
    let _ = observation.image(
        "topo-irregular-model",
        json!({
            "specmode": "cube", "spw": "0", "start": 0, "width": 1, "nchan": 5,
            "niter": 5, "deconvolver": "hogbom", "outframe": "TOPO",
            "weighting": "natural", "savemodel": "modelcolumn", "write_pb": true,
        }),
    );
    let rows = super::vis_list::model_rows(observation.measurement_set());
    let expected = [
        Complex32::new(-0.063_822_08, -0.014_624_763),
        Complex32::new(-0.022_417_58, -0.082_254_78),
        Complex32::new(0.118_532_16, 0.043_736_83),
        Complex32::new(-0.013_400_649, 0.061_058_775),
        Complex32::new(-0.003_976_36, 0.074_195_11),
    ];
    let mut error = 0.0_f64;
    let mut power = 0.0_f64;
    for (channel, casa) in expected.iter().enumerate() {
        error += f64::from((rows[0][[0, channel]] - casa).norm_sqr());
        power += f64::from(casa.norm_sqr());
    }
    let nrms = (error / power).sqrt();
    assert!(nrms < 1e-3, "predicted MODEL_DATA NRMS {nrms}");
}

/// CASA excludes an unselected physical row before choosing the image epoch,
/// for both scan and timerange selection (#1198).
#[test]
fn topo_cube_image_epoch_uses_the_first_row_that_selection_keeps() {
    let observation = topo_boundary_observation(1_000_000_118.0);
    swap_first_and_last_rows(&observation);
    let mut ms = MeasurementSet::open(observation.measurement_set()).unwrap();
    let last_time = match ms
        .main_table()
        .cell_accessor(0, "TIME")
        .unwrap()
        .scalar()
        .unwrap()
    {
        ScalarValue::Float64(time) => *time,
        value => panic!("TIME is {value:?}"),
    };
    for row in 0..ms.row_count() {
        let time = match ms
            .main_table()
            .cell_accessor(row, "TIME")
            .unwrap()
            .scalar()
            .unwrap()
        {
            ScalarValue::Float64(time) => *time,
            value => panic!("TIME is {value:?}"),
        };
        if time == last_time {
            ms.main_table_mut()
                .row_accessor_mut()
                .set_cell(row, "SCAN_NUMBER", Value::Scalar(ScalarValue::Int32(2)))
                .unwrap();
        }
    }
    ms.save().unwrap();
    drop(ms);
    for (name, selection) in [
        ("scan", json!({"scan": "1"})),
        ("time", json!({"timerange": "<2013/12/31/18:30:00"})),
    ] {
        let mut values = json!({
            "specmode": "cube", "spw": "0", "start": 0, "width": 1, "nchan": 5,
            "niter": 0, "outframe": "TOPO", "weighting": "natural", "write_pb": true,
        });
        values
            .as_object_mut()
            .unwrap()
            .extend(selection.as_object().unwrap().clone());
        let (_, products) = observation.image(name, values);
        let pb = products.get(".pb");
        assert!(
            (pb.channel_frequency_hz(0) - 1_000_058_082.877_847).abs() < 0.5,
            "{name}: first TOPO frequency {}",
            pb.channel_frequency_hz(0)
        );
        let epoch = pb
            .image
            .coordinates()
            .obs_info()
            .date
            .as_ref()
            .unwrap()
            .value()
            .as_mjd()
            * 86_400.0;
        assert!(
            (epoch - 4_895_229_557.733_246).abs() < 0.001,
            "{name}: image epoch {epoch}"
        );
    }
}

/// CASA takes the image epoch from the reference MS that contains an
/// explicitly requested start frequency, even when it is the second MS.
#[test]
fn topo_cube_image_epoch_follows_the_reference_measurement_set() {
    let first = topo_boundary_observation(999_990_000.0);
    let second = topo_boundary_observation(1_000_000_118.0);
    swap_first_and_last_rows(&second);
    let (_, products) = first.image_stores(
        &[first.measurement_set(), second.measurement_set()],
        "topo-reference-ms",
        json!({
            "specmode": "cube", "spw": "0", "start": "1000010000Hz", "width": "50.1MHz",
            "nchan": 5, "niter": 0, "outframe": "TOPO", "weighting": "natural", "write_pb": true,
        }),
    );
    let pb = products.get(".pb");
    let epoch = pb
        .image
        .coordinates()
        .obs_info()
        .date
        .as_ref()
        .unwrap()
        .value()
        .as_mjd()
        * 86_400.0;
    assert!(
        (epoch - 4_895_233_133.733_246).abs() < 0.001,
        "image epoch {epoch}"
    );
    assert!(
        (pb.channel_frequency_hz(0) - 1_000_106_404.108_027).abs() < 0.5,
        "first TOPO frequency {}",
        pb.channel_frequency_hz(0)
    );
}
