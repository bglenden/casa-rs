// SPDX-License-Identifier: LGPL-3.0-or-later
//! casa-rs reads CASA images whose axes were transposed as CASA reads them
//! (#1001).
//!
//! casacore keeps a coordinate system's coordinates in the order they were
//! added and records the axes each owns in `pixelmap<i>`/`worldmap<i>`; a
//! transpose permutes the maps. casa-rs read the coordinates in record order
//! over consecutive axes, so it named the wrong axes without an error.
//!
//! The images under `tests/data/casa-6.7.6/axis_maps` were written by CASA
//! 6.7.6 through `tools/images/axis_map_oracle.py fixture`, which also
//! recorded what CASA reads in `axis_maps.json`: each image's shape, axis
//! types, names and units, its axis maps, and at a few pixels the world
//! coordinates and the pixel value. casa-rs reads the two whose coordinates
//! each own adjacent axes in order, and `impv`'s position-velocity image,
//! whose linear offset coordinate has an independent removed axis. It
//! refuses the three it cannot represent (#1002).

use std::path::{Path, PathBuf};

use casa_coordinates::{CoordinateSystem, CoordinateType};
use casa_images::{ImageError, ImpvRequest, ImsubimageRequest, PagedImage, impv, imsubimage};
use casa_tables::{Table, TableOptions};
use casa_types::{ArrayValue, Value};
use serde_json::Value as Json;

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/casa-6.7.6/axis_maps")
}

/// What CASA read from fixture image `name`.
fn casa(name: &str) -> Json {
    let path = fixture_dir().join("axis_maps.json");
    let text = std::fs::read_to_string(&path).expect("axis_maps.json");
    let document: Json = serde_json::from_str(&text).expect("valid JSON");
    assert_eq!(document["casa_version"].as_str(), Some("6.7.6-14"));
    document["images"][name].clone()
}

/// A copy of fixture image `name` in `scratch`.
fn copy_image(name: &str, scratch: &Path) -> PathBuf {
    fn copy(source: &Path, target: &Path) {
        std::fs::create_dir_all(target).expect("create copy");
        for entry in std::fs::read_dir(source).expect("read image directory") {
            let entry = entry.expect("image entry");
            let to = target.join(entry.file_name());
            if entry.file_type().expect("entry type").is_dir() {
                copy(&entry.path(), &to);
            } else {
                std::fs::copy(entry.path(), to).expect("copy image file");
            }
        }
    }
    let target = scratch.join(name);
    copy(&fixture_dir().join(name), &target);
    target
}

fn strings(value: &Json) -> Vec<String> {
    value
        .as_array()
        .expect("array")
        .iter()
        .map(|item| item.as_str().expect("string").to_string())
        .collect()
}

fn numbers(value: &Json) -> Vec<f64> {
    value
        .as_array()
        .expect("array")
        .iter()
        .map(|item| item.as_f64().expect("number"))
        .collect()
}

fn coordinate_type(name: &str) -> CoordinateType {
    match name {
        "Direction" => CoordinateType::Direction,
        "Spectral" => CoordinateType::Spectral,
        "Stokes" => CoordinateType::Stokes,
        "Linear" => CoordinateType::Linear,
        "Tabular" => CoordinateType::Tabular,
        other => panic!("CASA axis type {other}"),
    }
}

/// `image` has CASA's shape, axis types, names and units, and CASA's world
/// coordinates and pixel values at the pixels CASA recorded.
fn assert_reads_as_casa(image: &PagedImage<f32>, casa: &Json, label: &str) {
    let shape: Vec<usize> = numbers(&casa["shape"])
        .iter()
        .map(|&n| n as usize)
        .collect();
    assert_eq!(image.shape(), shape.as_slice(), "{label}: shape");
    let types: Vec<CoordinateType> = strings(&casa["axis_types"])
        .iter()
        .map(|name| coordinate_type(name))
        .collect();
    assert_eq!(image.axis_types(), types, "{label}: axis types");
    assert_eq!(
        image.axis_names(),
        strings(&casa["names"]),
        "{label}: names"
    );
    let coordinates = image.coordinates();
    let units: Vec<String> = (0..coordinates.n_coordinates())
        .flat_map(|index| coordinates.coordinate(index).axis_units())
        .collect();
    assert_eq!(units, strings(&casa["units"]), "{label}: units");

    for sample in casa["samples"].as_array().expect("samples") {
        let pixel: Vec<usize> = numbers(&sample["pixel"])
            .iter()
            .map(|&p| p as usize)
            .collect();
        let at: Vec<f64> = pixel.iter().map(|&p| p as f64).collect();
        let world = coordinates.to_world(&at).expect("to_world");
        for (axis, (ours, theirs)) in world.iter().zip(numbers(&sample["world"])).enumerate() {
            assert!(
                (ours - theirs).abs() <= 1e-12 * theirs.abs().max(1.0),
                "{label}: pixel {pixel:?} world axis {axis}: {ours} against CASA's {theirs}"
            );
        }
        let value = image.get_at(&pixel).expect("pixel");
        assert_eq!(
            f64::from(value),
            sample["value"].as_f64().expect("value"),
            "{label}: pixel {pixel:?} value"
        );
    }
}

#[test]
fn transposed_casa_images_read_as_casa_reads_them() {
    let scratch = tempfile::tempdir().expect("scratch");
    for name in ["transposed.image", "spectral_first.image", "pv.image"] {
        let casa = casa(name);
        let image = PagedImage::<f32>::open(copy_image(name, scratch.path())).expect(name);
        assert_reads_as_casa(&image, &casa, name);
    }
    let pv = casa("pv.image");
    assert_eq!(
        pv["maps"]["linear0"]["pixelmap"],
        serde_json::json!([0, -2])
    );

    // The case of #1001: `PagedImage::find_axis` names the axes CASA does.
    let image = PagedImage::<f32>::open(copy_image("transposed.image", scratch.path())).unwrap();
    assert_eq!(image.find_axis(CoordinateType::Stokes), Some(0));
    assert_eq!(image.find_axis(CoordinateType::Direction), Some(1));
    assert_eq!(image.find_axis(CoordinateType::Spectral), Some(3));
    let casa = casa("transposed.image");
    assert_eq!(casa["maps"]["stokes1"]["pixelmap"], serde_json::json!([0]));
    assert_eq!(
        casa["maps"]["direction0"]["pixelmap"],
        serde_json::json!([1, 2])
    );
}

#[test]
fn casa_images_whose_axes_casa_rs_cannot_represent_are_refused() {
    let scratch = tempfile::tempdir().expect("scratch");
    for (name, coordinate, refusal) in [
        (
            "swapped.image",
            "direction0",
            "Declination, Right Ascension",
        ),
        ("interleaved.image", "direction0", "which are not adjacent"),
        ("dropdeg_ra.image", "direction0", "has removed axes"),
    ] {
        let maps = &casa(name)["maps"][coordinate];
        let error = PagedImage::<f32>::open(copy_image(name, scratch.path()))
            .err()
            .unwrap_or_else(|| panic!("{name}: casa-rs read it"));
        let ImageError::InvalidMetadata(message) = &error else {
            panic!("{name}: {error:?}");
        };
        assert!(
            message.starts_with("unsupported axis layout: ")
                && message.contains(coordinate)
                && message.contains(refusal)
                && message.contains("#1002"),
            "{name} (maps {maps}): {message}"
        );
    }
}

/// The `coords` keyword record of the image at `path`.
fn coords_record(path: &Path) -> casa_types::RecordValue {
    let table = Table::open(TableOptions::new(path)).expect("open image table");
    match table.keywords().get("coords") {
        Some(Value::Record(record)) => record.clone(),
        other => panic!("coords keyword: {other:?}"),
    }
}

fn int_map(record: &casa_types::RecordValue, field: &str) -> Vec<i32> {
    match record.get(field) {
        Some(Value::Array(ArrayValue::Int32(values))) => values.iter().copied().collect(),
        other => panic!("{field}: {other:?}"),
    }
}

#[test]
fn a_transposed_image_casa_rs_rewrites_numbers_its_coordinates_in_axis_order() {
    let scratch = tempfile::tempdir().expect("scratch");
    let path = copy_image("transposed.image", scratch.path());
    let mut image = PagedImage::<f32>::open(&path).unwrap();
    let coordinates = image.coordinates().clone();
    image.set_coordinates(coordinates).unwrap();
    image.save().unwrap();
    drop(image);

    // casacore numbers coordinates in creation order; casa-rs writes them in
    // the order of their axes, each over consecutive axes. The system is the
    // same (`axis_map_oracle.py readback` checks that CASA reads it so).
    let record = coords_record(&path);
    assert_eq!(
        CoordinateSystem::record_coordinate_fields(&record).unwrap(),
        ["stokes0", "direction1", "spectral2"]
    );
    assert_eq!(int_map(&record, "pixelmap0"), [0]);
    assert_eq!(int_map(&record, "pixelmap1"), [1, 2]);
    assert_eq!(int_map(&record, "worldmap1"), [1, 2]);
    let image = PagedImage::<f32>::open(&path).unwrap();
    assert_reads_as_casa(
        &image,
        &casa("transposed.image"),
        "rewritten transposed.image",
    );
}

#[test]
fn tasks_that_take_the_direction_from_the_first_two_axes_refuse_other_layouts() {
    // Read in record order, these images claimed the direction on axes 0
    // and 1; read as CASA reads them, it is not there, and impv (whose
    // channel selection assumed the spectral axis after the path's two) and
    // a box would misread or panic.
    let scratch = tempfile::tempdir().expect("scratch");
    for name in ["transposed.image", "spectral_first.image"] {
        let input = copy_image(name, scratch.path());
        let error = imsubimage(&ImsubimageRequest {
            imagename: input.clone(),
            outfile: scratch.path().join("box.image"),
            box_pixels: Some("0,0,1,1".into()),
            chans: None,
            overwrite: false,
        })
        .unwrap_err();
        assert!(
            error.to_string().contains("box or polygon region"),
            "{name}: {error}"
        );
        let error = casa_images::imstat(
            &input,
            None,
            Some("box[[0pix,0pix],[1pix,1pix]]"),
            None,
            None,
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("a region takes the direction plane"),
            "{name}: {error}"
        );
        let error = impv(&ImpvRequest {
            imagename: input,
            outfile: scratch.path().join("pv.image"),
            mode: "coords".into(),
            start: "0,0".into(),
            end: "2,2".into(),
            width: 1,
            chans: Some("1~2".into()),
            overwrite: false,
        })
        .unwrap_err();
        assert!(
            error.to_string().contains("impv takes the direction plane"),
            "{name}: {error}"
        );
    }
}

#[test]
fn a_subimage_of_a_transposed_image_moves_each_coordinates_reference_pixel() {
    // `spectral_first.image` keeps `spectral2` over pixel axis 0, which
    // casa-rs reads as its coordinate 0. imsubimage copies CASA's record and
    // moves each coordinate's reference pixel by the selection's start; it
    // once looked the coordinates up as `spectral0`, `direction1` and
    // `stokes2`, found none, and left the channels where they were.
    let scratch = tempfile::tempdir().expect("scratch");
    let input = copy_image("spectral_first.image", scratch.path());
    let outfile = scratch.path().join("channels.image");
    imsubimage(&ImsubimageRequest {
        imagename: input.clone(),
        outfile: outfile.clone(),
        box_pixels: None,
        chans: Some("1~3".into()),
        overwrite: false,
    })
    .unwrap();

    let record = coords_record(&outfile);
    assert_eq!(
        CoordinateSystem::record_coordinate_fields(&record).unwrap(),
        ["spectral2", "direction0", "stokes1"]
    );
    let input = PagedImage::<f32>::open(&input).unwrap();
    let output = PagedImage::<f32>::open(&outfile).unwrap();
    assert_eq!(output.shape(), [3, 4, 3, 2]);
    for pixel in [[0usize, 0, 0, 0], [2, 3, 2, 1], [1, 2, 1, 0]] {
        let at = pixel.map(|p| p as f64);
        let mut from = at;
        from[0] += 1.0;
        assert_eq!(
            output.coordinates().to_world(&at).unwrap(),
            input.coordinates().to_world(&from).unwrap(),
            "pixel {pixel:?}"
        );
        let mut source = pixel;
        source[0] += 1;
        assert_eq!(
            output.get_at(&pixel).unwrap(),
            input.get_at(&source).unwrap()
        );
    }
}
