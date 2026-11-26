// SPDX-License-Identifier: LGPL-3.0-or-later
//! Effective diameter and dish filtering on the CASA feather task surface.

use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};

use casa_coordinates::{
    CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
    StokesCoordinate, StokesType,
};
use casa_core::measures::{direction::DirectionRef, frequency::FrequencyRef};
use casa_images::{GaussianBeam, ImageBeamSet, ImageInfo, PagedImage};
use ndarray::{ArrayD, Axis, IxDyn};
use serde_json::{Value, json};

fn copy_images(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).unwrap();
    for entry in source.read_dir().unwrap() {
        let entry = entry.unwrap();
        if entry.file_name() == "expected.json" || entry.file_name() == "table.lock" {
            continue;
        }
        let target = destination.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_images(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn run(workspace: &Path, values: &Value) -> Output {
    let mut process = Command::new(env!("CARGO_BIN_EXE_feather"))
        .args(["--json-run", "-"])
        .current_dir(workspace)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    process
        .stdin
        .take()
        .unwrap()
        .write_all(
            json!({"surface":"feather", "values":values})
                .to_string()
                .as_bytes(),
        )
        .unwrap();
    process.wait_with_output().unwrap()
}

fn assert_casa_pixels(pixels: &ArrayD<f32>, expected: &Value, case: &str) {
    let (mut squared_error, mut squared_reference) = (0.0, 0.0);
    for (i, &value) in pixels.view().reversed_axes().iter().enumerate() {
        let reference = expected[i].as_f64().unwrap();
        squared_error += (f64::from(value) - reference).powi(2);
        squared_reference += reference.powi(2);
    }
    let nrms = (squared_error / squared_reference).sqrt();
    assert!(nrms < 2e-6, "{case} normalized RMS error {nrms}");
}

#[test]
fn mixed_beams_use_the_plane_area_ratios_and_allow_explicit_overwrite_refusal() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path();
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [0.2, 0.4],
        [-1e-5, 1e-5],
        [4., 4.],
    ));
    coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        1.4e9,
        1e6,
        0.,
        1.42e9,
    ));
    let high_beams = ImageBeamSet::from_grid(vec![
        vec![GaussianBeam::new(2e-5, 2e-5, 0.)],
        vec![GaussianBeam::new(3e-5, 3e-5, 0.)],
    ]);
    let low_beams = ImageBeamSet::new(GaussianBeam::new(6e-5, 6e-5, 0.));
    for (name, beams, value) in [
        ("high.im", high_beams.clone(), 12.),
        ("low.im", low_beams, 36.),
    ] {
        let mut image =
            PagedImage::<f32>::create(vec![8, 8, 1, 2], coordinates.clone(), root.join(name))
                .unwrap();
        image
            .set_image_info(&ImageInfo {
                beam_set: beams,
                ..ImageInfo::default()
            })
            .unwrap();
        image.set_units("Jy/beam").unwrap();
        image
            .put_slice(&ArrayD::from_elem(IxDyn(&[8, 8, 1, 2]), value), &[0; 4])
            .unwrap();
        image.save().unwrap();
    }
    let mut values = json!({"imagename":"combined.im", "highres":"high.im", "lowres":"low.im"});
    let check_product = |expected: [f32; 2]| {
        let output = PagedImage::<f32>::open(root.join("combined.im")).unwrap();
        assert_eq!(output.units(), "Jy/beam");
        let beams = output.image_info().unwrap().beam_set;
        assert_eq!((beams.n_channels(), beams.n_stokes()), (2, 1));
        for channel in 0..2 {
            let actual = beams.beam(channel, 0);
            let expected = high_beams.beam(channel, 0);
            assert!((actual.major - expected.major).abs() < 1e-15);
            assert!((actual.minor - expected.minor).abs() < 1e-15);
            assert_eq!(actual.position_angle, expected.position_angle);
        }
        let pixels = output.get().unwrap();
        for (index, actual) in pixels.indexed_iter() {
            assert!(
                (*actual - expected[index[3]]).abs() < 2e-5,
                "{index:?}: {actual}"
            );
        }
    };
    let reply = run(root, &values);
    assert!(
        reply.status.success(),
        "{}",
        String::from_utf8_lossy(&reply.stdout)
    );
    check_product([4., 9.]);

    values["sdfactor"] = json!(2.0);
    values["overwrite"] = json!(false);
    let reply = run(root, &values);
    assert!(!reply.status.success());
    check_product([4., 9.]);

    values.as_object_mut().unwrap().remove("overwrite");
    #[cfg(unix)]
    {
        let history = root.join("combined.im/logtable");
        let open = casa_tables::TableUseLock::acquire(&history).unwrap();
        let reply = run(root, &values);
        assert!(!reply.status.success());
        assert!(open.matches_directory(&history));
        check_product([4., 9.]);
    }
    let reply = run(root, &values);
    assert!(
        reply.status.success(),
        "{}",
        String::from_utf8_lossy(&reply.stdout)
    );
    check_product([8., 18.]);

    let low_beams = ImageBeamSet::from_grid(
        [6e-5, 7e-5, 8e-5]
            .map(|width| vec![GaussianBeam::new(width, width, 0.)])
            .to_vec(),
    );
    for (name, width) in [("longer", 8), ("wider", 12)] {
        let mut low_coords = coordinates.clone();
        let casa_coordinates::CoordinateModel::Direction(direction) = low_coords.coordinate(0)
        else {
            panic!("direction coordinate");
        };
        low_coords.replace_coordinate(
            0,
            direction
                .clone()
                .with_reference_pixel([width as f64 / 2., 4.]),
        );
        let shape = [width, 8, 1, 3];
        let pixels = ArrayD::from_shape_fn(IxDyn(&shape), |index| {
            if width == 8 || (8..10).contains(&index[0]) {
                (index[3] + 1) as f32
            } else {
                0.
            }
        });
        let low = format!("{name}.im");
        let mut image =
            PagedImage::<f32>::create(shape.to_vec(), low_coords, root.join(&low)).unwrap();
        image.set_units("Jy/beam").unwrap();
        image
            .set_image_info(&ImageInfo {
                beam_set: low_beams.clone(),
                ..ImageInfo::default()
            })
            .unwrap();
        image.put_slice(&pixels, &[0; 4]).unwrap();
        if width == 8 {
            let mask = ArrayD::from_shape_fn(IxDyn(&shape), |index| index[0] != 0 || index[1] != 0);
            image.put_mask("mask0", &mask).unwrap();
            image.set_default_mask("mask0").unwrap();
        }
        image.save().unwrap();
        drop(image);
        let product = format!("{name}.out");
        let reply = run(
            root,
            &json!({"imagename": product, "highres":"high.im", "lowres":low}),
        );
        assert!(
            reply.status.success(),
            "{}",
            String::from_utf8_lossy(&reply.stdout)
        );
        let output = PagedImage::<f32>::open(root.join(product)).unwrap();
        assert_eq!(output.shape(), [8, 8, 1, 2]);
        let selected_fraction = if width == 8 {
            assert!(!output.get_mask().unwrap().unwrap()[[0, 0, 0, 0]]);
            63. / 64.
        } else {
            // Low columns 8–9 fall on two of the eight high-grid columns.
            2. / 8.
        };
        let actual = output.get().unwrap();
        for (channel, scaled) in [1. / 9., 18. / 49.].into_iter().enumerate() {
            let mean = actual.index_axis(Axis(3), channel).mean().unwrap();
            assert!(
                (mean - scaled * selected_fraction).abs() < 2e-6,
                "{name} channel {channel}: {mean}"
            );
        }
    }
}

#[test]
fn dish_filters_match_casa() {
    let fixtures =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../crates/casa-images/tests/data/feather");
    let record: Value = serde_json::from_str(include_str!(
        "../../../crates/casa-images/tests/data/feather/expected.json"
    ))
    .unwrap();
    assert_eq!(record["casa_version"], "6.7.6.14");
    let workspace = tempfile::tempdir().unwrap();
    copy_images(&fixtures, workspace.path());
    for (name, case) in record["cases"].as_object().unwrap() {
        let product = format!("{name}.out");
        let path = workspace.path().join(&product);
        let mut values = case["params"].clone();
        values["imagename"] = json!(product);
        let reply = run(workspace.path(), &values);
        let message = String::from_utf8_lossy(&reply.stdout);
        let Some(expected) = case.get("product") else {
            assert!(!reply.status.success(), "{name}: {message}");
            assert!(message.contains("smaller than the original single-dish beam"));
            assert!(!path.exists());
            continue;
        };
        assert!(reply.status.success(), "{name}: {message}");
        let actual = PagedImage::<f32>::open(path).unwrap();
        assert_eq!(json!(actual.shape()), expected["shape"], "{name}");
        assert_eq!(json!(actual.units()), expected["units"], "{name}");
        assert_eq!(
            actual.default_mask_name().unwrap_or_default(),
            expected["default_mask"][0].as_str().unwrap(),
            "{name}"
        );
        let beam = actual.image_info().unwrap().beam_set.single_beam().unwrap();
        for (key, value) in [
            ("major", beam.major_in("arcsec").unwrap()),
            ("minor", beam.minor_in("arcsec").unwrap()),
            ("positionangle", beam.position_angle_in("deg").unwrap()),
        ] {
            let reference = expected["beam"][key][0].as_f64().unwrap();
            assert!(
                (value - reference).abs() < 1e-10,
                "{name} {key}: {value} vs {reference}"
            );
        }
        let data = actual.get().unwrap();
        let mask = actual.get_mask().unwrap().map_or_else(
            || vec![true; data.len()],
            |mask| mask.view().reversed_axes().iter().copied().collect(),
        );
        assert_eq!(json!(mask), expected["mask"], "{name}");
        assert_casa_pixels(&data, &expected["pixels"], name);
    }
    let output = workspace.path().join("default.out/table.dat");
    let before = fs::read(&output).unwrap();
    let reply = run(
        workspace.path(),
        &json!({
            "imagename":"default.out", "highres":"high.im", "lowres":"low.im",
            "effdishdiam":4200.0, "overwrite":true,
        }),
    );
    assert!(!reply.status.success());
    assert_eq!(fs::read(output).unwrap(), before);
}

#[test]
fn direction_units_do_not_change_the_fourier_combination() {
    let fixtures =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../crates/casa-images/tests/data/feather");
    let record: Value = serde_json::from_str(include_str!(
        "../../../crates/casa-images/tests/data/feather/expected.json"
    ))
    .unwrap();
    assert_eq!(record["casa_version"], "6.7.6.14");
    let workspace = tempfile::tempdir().unwrap();
    for name in ["high.im", "low.im"] {
        let path = workspace.path().join(name);
        copy_images(&fixtures.join(name), &path);
        let mut image = PagedImage::<f32>::open(path).unwrap();
        image.set_axis_unit(0, "'").unwrap();
        image.set_axis_unit(1, "'").unwrap();
        image.save().unwrap();
    }
    let output = run(
        workspace.path(),
        &json!({"imagename": "combined.im", "highres": "high.im", "lowres": "low.im"}),
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    let combined = PagedImage::<f32>::open(workspace.path().join("combined.im")).unwrap();
    assert_casa_pixels(
        &combined.get().unwrap(),
        &record["cases"]["default"]["product"]["pixels"],
        "arcminute grid against CASA's physical radian grid",
    );
}
