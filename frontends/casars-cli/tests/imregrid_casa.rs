// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA 6.7.6.14 regrid shapes, coordinate frames, masks and samples.

use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, DirectionCoordinate, Projection,
    ProjectionType, SpectralCoordinate, StokesCoordinate, StokesType,
};
use casa_core::measures::{direction::DirectionRef, frequency::FrequencyRef};
use casa_images::{AnyPagedImage, ImagePixel, PagedImage};
use ndarray::{ArrayD, IxDyn};
use serde_json::{Value, json};

fn copy_inputs(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).unwrap();
    for entry in source.read_dir().unwrap() {
        let entry = entry.unwrap();
        if entry.file_name() == "expected.json" || entry.file_name() == "table.lock" {
            continue;
        }
        let target = destination.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_inputs(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn execute(workspace: &Path, values: &Value) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_imregrid"))
        .args(["--json-run", "-"])
        .current_dir(workspace)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(
            json!({"surface": "imregrid", "values": values})
                .to_string()
                .as_bytes(),
        )
        .unwrap();
    child.wait_with_output().unwrap()
}

fn make_ramp(
    path: &Path,
    channels: usize,
    x_reference: f64,
    display: DirectionRef,
    increment: f64,
    rest: Vec<f64>,
) {
    let mut coords = CoordinateSystem::new();
    coords.add_coordinate(
        DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [1.0, 0.4],
            [-0.004, 0.004],
            [x_reference, 15.],
        )
        .with_conversion_direction_ref(display),
    );
    coords.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
    coords.add_coordinate(
        SpectralCoordinate::new(FrequencyRef::LSRK, 1.415e9, increment, 0., rest[0])
            .with_rest_frequencies(rest),
    );
    let shape = [30, 30, 1, channels];
    let mut image = PagedImage::<f32>::create(shape.to_vec(), coords, path).unwrap();
    let pixels = ArrayD::from_shape_fn(IxDyn(&shape), |index| (index[0] + 2 * index[1]) as f32);
    image.put_slice(&pixels, &[0; 4]).unwrap();
    image.save().unwrap();
}

#[test]
fn regridding_a_shifted_grid_uses_display_conversion_layers() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path();
    make_ramp(
        &root.join("input.im"),
        4,
        15.,
        DirectionRef::J2000,
        1000.,
        vec![1.42e9],
    );
    make_ramp(
        &root.join("template.im"),
        4,
        16.,
        DirectionRef::B1950,
        1000.,
        vec![1.42e9],
    );
    let reply = execute(
        root,
        &json!({"imagename":"input.im", "template":"template.im", "output":"output.im", "axes":[0,1], "decimate":0}),
    );
    assert!(
        reply.status.success(),
        "{}",
        String::from_utf8_lossy(&reply.stdout)
    );
    let image = PagedImage::<f32>::open(root.join("output.im")).unwrap();
    let value = image.get_at(&[15, 15, 0, 0]).unwrap();
    assert!((value - 45.661_076).abs() < 1e-3, "{value}");
}

#[test]
fn degenerate_spectral_selection_defaults_to_the_shifted_sky_grid() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path();
    make_ramp(
        &root.join("input.im"),
        4,
        15.,
        DirectionRef::J2000,
        1000.,
        vec![1.42e9],
    );
    make_ramp(
        &root.join("template.im"),
        1,
        16.,
        DirectionRef::J2000,
        1000.,
        vec![1.42e9],
    );
    let reply = execute(
        root,
        &json!({"imagename":"input.im", "template":"template.im", "output":"output.im", "axes":[3], "decimate":0}),
    );
    assert!(
        reply.status.success(),
        "{}",
        String::from_utf8_lossy(&reply.stdout)
    );
    let image = PagedImage::<f32>::open(root.join("output.im")).unwrap();
    assert_eq!(image.coordinates().reference_pixel()[0], 16.);
    assert_eq!(image.get_at(&[15, 15, 0, 0]).unwrap(), 44.);
    assert_eq!(image.shape(), [30, 30, 1, 4]);
}

#[test]
fn radio_velocity_grid_keeps_exact_scale_and_the_input_rest_frequency_list() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path();
    make_ramp(
        &root.join("input.im"),
        4,
        15.,
        DirectionRef::J2000,
        1000.,
        vec![1.42e9, 1.52e9],
    );
    make_ramp(
        &root.join("template.im"),
        4,
        15.,
        DirectionRef::J2000,
        500.,
        vec![1.42e9],
    );
    let reply = execute(
        root,
        &json!({"imagename":"input.im", "template":"template.im", "output":"output.im", "axes":[3], "decimate":0}),
    );
    assert!(
        reply.status.success(),
        "{}",
        String::from_utf8_lossy(&reply.stdout)
    );
    let image = PagedImage::<f32>::open(root.join("output.im")).unwrap();
    let coordinates = image.coordinates();
    let index = coordinates
        .find_coordinate(CoordinateType::Spectral)
        .unwrap();
    let CoordinateModel::Spectral(axis) = coordinates.coordinate(index) else {
        panic!("spectral axis")
    };
    assert_eq!(axis.increment(), [500.]);
    assert_eq!(axis.rest_frequencies(), [1.42e9, 1.52e9]);
}

#[cfg(unix)]
#[test]
fn an_open_logtable_blocks_image_replacement() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path();
    for name in ["input.im", "template.im", "output.im"] {
        make_ramp(
            &root.join(name),
            4,
            15.,
            DirectionRef::J2000,
            1000.,
            vec![1.42e9],
        );
    }
    let history = root.join("output.im/logtable");
    let before = fs::read(history.join("table.dat")).unwrap();
    let open = casa_tables::TableUseLock::acquire(&history).unwrap();
    let reply = execute(
        root,
        &json!({"imagename":"input.im", "template":"template.im", "output":"output.im", "overwrite":true, "decimate":0}),
    );
    let message = String::from_utf8_lossy(&reply.stdout);
    assert!(
        !reply.status.success() && message.contains("logtable"),
        "{message}"
    );
    assert!(open.matches_directory(&history));
    assert_eq!(fs::read(history.join("table.dat")).unwrap(), before);
}

#[test]
fn regrid_products_match_casa() {
    let directory = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../libs/casacore/casa-images/tests/data/imregrid");
    let evidence: Value = serde_json::from_str(include_str!(
        "../../../libs/casacore/casa-images/tests/data/imregrid/expected.json"
    ))
    .unwrap();
    assert_eq!(evidence["casa_version"], "6.7.6.14");
    let workspace = tempfile::tempdir().unwrap();
    copy_inputs(&directory, workspace.path());
    for (name, case) in evidence["cases"].as_object().unwrap() {
        let output = format!("{name}.out");
        let path = workspace.path().join(&output);
        let existing = case["existing"].as_str().map(|source| {
            copy_inputs(&workspace.path().join(source), &path);
            fs::read(path.join("table.dat")).unwrap()
        });
        let mut values = case["params"].clone();
        values["output"] = json!(output);
        let reply = execute(workspace.path(), &values);
        let message = String::from_utf8_lossy(&reply.stdout);
        let Some(expected) = case.get("product") else {
            assert!(!reply.status.success(), "{name}: {message}");
            if let Some(before) = existing {
                assert!(
                    message.contains("output already exists"),
                    "{name}: {message}"
                );
                assert_eq!(fs::read(path.join("table.dat")).unwrap(), before, "{name}");
                assert!(matches!(
                    AnyPagedImage::open(&path).unwrap(),
                    AnyPagedImage::Float32(_)
                ));
            } else {
                assert!(!path.exists(), "{name} published a refused output");
            }
            if name.ends_with("_frame_refused") {
                assert!(
                    message.contains("This method only supports Float valued images"),
                    "{name}: {message}"
                );
            }
            if name == "removed_linear_unselected_length" {
                assert!(
                    message.contains("an axis not being regridded must keep its input length"),
                    "{name}: {message}"
                );
            }
            if name == "complex_sfl_frame" {
                assert!(
                    message.contains("SFL projection cannot be rotated"),
                    "{name}: {message}"
                );
            }
            continue;
        };
        assert!(reply.status.success(), "{name}: {message}");
        let expected = case.get("corrected_product").unwrap_or(expected);
        let metadata = case.get("preserved_input_metadata").unwrap_or(expected);
        match AnyPagedImage::open(path).unwrap() {
            AnyPagedImage::Float32(image) => {
                assert!(expected["pixel_type"].is_null(), "{name}: real result");
                compare(name, &image, expected, metadata, |value| {
                    [f64::from(value), 0.0]
                });
            }
            AnyPagedImage::Float64(image) => {
                assert!(expected["pixel_type"].is_null(), "{name}: real result");
                compare(name, &image, expected, metadata, |value| [value, 0.0]);
            }
            AnyPagedImage::Complex32(image) => {
                assert_eq!(expected["pixel_type"], "complex", "{name}");
                compare(name, &image, expected, metadata, |value| {
                    [f64::from(value.re), f64::from(value.im)]
                });
            }
            AnyPagedImage::Complex64(image) => {
                assert_eq!(expected["pixel_type"], "dcomplex", "{name}");
                compare(name, &image, expected, metadata, |value| {
                    [value.re, value.im]
                });
            }
        }
    }
}

fn compare<T: ImagePixel + Copy>(
    name: &str,
    image: &PagedImage<T>,
    expected: &Value,
    metadata: &Value,
    components: impl Fn(T) -> [f64; 2],
) {
    assert_eq!(json!(image.shape()), expected["shape"], "{name}");
    assert_eq!(json!(image.axis_names()), expected["names"], "{name}");
    assert_eq!(json!(image.units()), metadata["brightness"], "{name}");
    let origin = vec![0; image.ndim()];
    let world = image
        .coordinates()
        .to_world(&vec![0.0; image.ndim()])
        .unwrap();
    for (index, actual) in world.into_iter().enumerate() {
        let reference = expected["pixel0_world"][index].as_f64().unwrap();
        assert!(
            (actual - reference).abs() < 1e-8 * reference.abs().max(1.0),
            "{name} axis {index}: CASA {reference}, native {actual}"
        );
    }
    let data = image.get_slice(&origin, image.shape()).unwrap();
    let mask = image.get_mask().unwrap().map_or_else(
        || vec![true; data.len()],
        |mask| mask.view().reversed_axes().iter().copied().collect(),
    );
    assert_eq!(json!(mask), expected["mask"], "{name} mask");
    let (mut residual, mut power) = (0.0, 0.0);
    for (pixel, &actual) in data.view().reversed_axes().iter().enumerate() {
        if mask[pixel] {
            let value = &expected["pixels"][pixel];
            let reference = if value.is_array() {
                [value[0].as_f64().unwrap(), value[1].as_f64().unwrap()]
            } else {
                [value.as_f64().unwrap(), 0.0]
            };
            for (actual, reference) in components(actual).into_iter().zip(reference) {
                residual += (actual - reference).powi(2);
                power += reference.powi(2);
            }
        }
    }
    let normalized_rms = (residual / power).sqrt();
    assert!(
        normalized_rms < 1e-6,
        "{name} normalized RMS error {normalized_rms}"
    );
    let info = image.image_info().unwrap();
    if let Some(alt) = metadata["beam"].get("positionangle") {
        let measured = info
            .beam_set
            .get_beam()
            .unwrap()
            .position_angle_in("deg")
            .unwrap();
        let reference = alt[0].as_f64().unwrap();
        assert!(
            (measured - reference).abs() < 1e-5,
            "{name} position angle {measured} vs {reference}"
        );
    } else {
        assert!(info.beam_set.is_empty(), "{name} beam");
    }
}
