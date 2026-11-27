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
//! coordinates and the pixel value. The fixtures cover contiguous,
//! swapped, interleaved and removed axes (#1002), along with pixel and
//! coordinate samples for cutouts from `imsubimage`.

use std::path::{Path, PathBuf};

use casa_coordinates::{
    CoordinateModel, CoordinateSystem, CoordinateType, DirectionCoordinate, LinearCoordinate,
    Projection, ProjectionType, SpectralCoordinate, StokesCoordinate, StokesType,
};
use casa_core::measures::direction::DirectionRef;
use casa_core::measures::frequency::FrequencyRef;
use casa_core::{ArrayValue, Value};
use casa_imageanalysis::{
    FeatherRequest, ImpvRequest, ImsubimageRequest, PvPosition, feather, impv, imsubimage,
};
use casa_images::{GaussianBeam, ImageBeamSet, ImageInfo, ImageType, PagedImage};
use casa_tables::{Table, TableOptions};
use ndarray::Array2;
use serde_json::Value as Json;

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../libs/casacore/casa-images/tests/data/casa-6.7.6/axis_maps")
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
    let units = coordinates.world_axis_units();
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
    for name in [
        "transposed.image",
        "spectral_first.image",
        "swapped.image",
        "interleaved.image",
        "pv.image",
        "dropdeg_linear.image",
        "dropdeg_linear_between.image",
        "dropdeg_ra.image",
        "tabular_spectral_first.image",
    ] {
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

/// The `coords` keyword record of the image at `path`.
fn coords_record(path: &Path) -> casa_core::RecordValue {
    let table = Table::open(TableOptions::new(path)).expect("open image table");
    match table.keywords().get("coords") {
        Some(Value::Record(record)) => record.clone(),
        other => panic!("coords keyword: {other:?}"),
    }
}

/// The fields of `record`'s coordinates, in the order casa-rs reads them.
fn fields(record: &casa_core::RecordValue) -> Vec<String> {
    CoordinateSystem::record_coordinates(record)
        .unwrap()
        .into_iter()
        .map(|placed| placed.field)
        .collect()
}

fn int_map(record: &casa_core::RecordValue, field: &str) -> Vec<i32> {
    match record.get(field) {
        Some(Value::Array(ArrayValue::Int32(values))) => values.iter().copied().collect(),
        other => panic!("{field}: {other:?}"),
    }
}

fn mapped_rewrite_directory(scratch: &Path) -> PathBuf {
    let output = std::env::var_os("CASA_RS_AXIS_MAP_REWRITE_OUT")
        .map_or_else(|| scratch.join("rewrites"), PathBuf::from);
    std::fs::create_dir_all(&output).unwrap();
    output
}

#[test]
fn complete_axis_maps_match_casa_and_round_trip_through_native_images() {
    let scratch = tempfile::tempdir().expect("scratch");
    let output = mapped_rewrite_directory(scratch.path());
    for name in [
        "transposed.image",
        "spectral_first.image",
        "swapped.image",
        "interleaved.image",
        "dropdeg_ra.image",
        "pv.image",
        "dropdeg_linear.image",
        "dropdeg_linear_between.image",
        "tabular_spectral_first.image",
    ] {
        let source = copy_image(name, scratch.path());
        let table = Table::open(TableOptions::new(&source)).unwrap();
        let Some(Value::Record(record)) = table.keywords().get("coords") else {
            panic!("{name}: missing coordinate record");
        };
        let ArrayValue::Float32(data) = table.row_accessor().array_cell(0, "map").unwrap() else {
            panic!("{name}: missing Float32 image pixels");
        };
        let mapped = CoordinateSystem::from_mapped_record(record).unwrap();
        let oracle = casa(name);
        let shape = numbers(&oracle["shape"])
            .into_iter()
            .map(|extent| extent as usize)
            .collect::<Vec<_>>();
        assert_eq!(data.shape(), &shape);
        assert_eq!(mapped.n_pixel_axes(), shape.len());
        let world_names = (0..mapped.n_world_axes())
            .map(|axis| {
                let (coordinate, local) = mapped.world_axis_owner(axis).unwrap();
                mapped.coordinate(coordinate).axis_names()[local].clone()
            })
            .collect::<Vec<_>>();
        assert_eq!(world_names, strings(&oracle["names"]), "{name}");

        let samples = oracle["samples"].as_array().unwrap();
        let mut pixel_columns = Array2::zeros((mapped.n_pixel_axes(), samples.len()));
        let mut world_columns = Array2::zeros((mapped.n_world_axes(), samples.len()));
        for (column, sample) in samples.iter().enumerate() {
            for (axis, value) in numbers(&sample["pixel"]).into_iter().enumerate() {
                pixel_columns[(axis, column)] = value;
            }
            for (axis, value) in numbers(&sample["world"]).into_iter().enumerate() {
                world_columns[(axis, column)] = value;
            }
        }
        let world_batch = mapped.to_world_columns(pixel_columns.view()).unwrap();
        let pixel_batch = mapped.to_pixel_columns(world_columns.view()).unwrap();
        for (column, sample) in samples.iter().enumerate() {
            let input_pixel = numbers(&sample["pixel"]);
            let input_world = numbers(&sample["world"]);
            let inverse_pixel = numbers(&sample["inverse_pixel"]);
            let got_world = mapped.to_world(&input_pixel).unwrap();
            let got_pixel = mapped.to_pixel(&input_world).unwrap();
            for (axis, (&scalar, expected)) in got_world.iter().zip(input_world).enumerate() {
                let tolerance = 1e-12 * expected.abs().max(1.0);
                assert!(
                    (scalar - expected).abs() <= tolerance,
                    "{name}: world axis {axis}: {scalar} vs {expected}"
                );
                assert!((world_batch[(axis, column)] - expected).abs() <= tolerance);
            }
            for (axis, (&scalar, expected)) in got_pixel.iter().zip(inverse_pixel).enumerate() {
                let tolerance = 1e-7_f64.max(1e-10 * expected.abs());
                assert!(
                    (scalar - expected).abs() <= tolerance,
                    "{name}: inverse axis {axis}: {scalar} vs {expected}"
                );
                assert!((pixel_batch[(axis, column)] - expected).abs() <= tolerance);
            }
        }

        let path = output.join(name);
        let mut image = PagedImage::<f32>::create(shape.clone(), mapped.clone(), &path).unwrap();
        image.set_coordinates(mapped).unwrap();
        image.put_slice(data, &vec![0; shape.len()]).unwrap();
        image.save().unwrap();
        drop(image);

        let reopened = PagedImage::<f32>::open(&path).unwrap();
        assert_reads_as_casa(&reopened, &oracle, name);
        assert_eq!(reopened.get().unwrap(), *data, "{name}: pixels");
        drop(reopened);
        let saved = coords_record(&path);
        for field in record.fields() {
            if ["worldmap", "pixelmap", "worldreplace", "pixelreplace"]
                .iter()
                .any(|prefix| field.name.starts_with(prefix))
            {
                assert_eq!(
                    saved.get(&field.name),
                    Some(&field.value),
                    "{name}: {}",
                    field.name
                );
            }
        }
        let restored = CoordinateSystem::from_mapped_record(&saved).unwrap();
        assert_eq!(
            restored.n_coordinates(),
            oracle["maps"].as_object().unwrap().len(),
            "{name}"
        );
        let new_world = restored.to_world_columns(pixel_columns.view()).unwrap();
        assert_eq!(new_world, world_batch, "{name}");
    }
}

#[test]
fn a_legacy_direction_slice_can_be_added_or_replaced_in_a_native_output() {
    let scratch = tempfile::tempdir().expect("scratch");
    let output = mapped_rewrite_directory(scratch.path());
    let source = copy_image("dropdeg_ra.image", scratch.path());
    let legacy = CoordinateSystem::from_record(&coords_record(&source)).unwrap();
    let slice = legacy.coordinate(legacy.find_coordinate(CoordinateType::Direction).unwrap());
    for (name, append) in [
        ("added-direction.image", true),
        ("replaced-direction.image", false),
    ] {
        let mut coords = CoordinateSystem::new();
        coords.add_coordinate(LinearCoordinate::new(1, Vec::new(), Vec::new()));
        coords.transpose_axes(&[0], &[0]).unwrap();
        let (shape, input) = if append {
            coords.add_coordinate(slice.clone());
            (vec![3, 3], vec![0.0, 1.0])
        } else {
            coords.replace_coordinate(0, slice.clone());
            (vec![3], vec![1.0])
        };
        let world = coords.to_world(&input).unwrap();
        let path = output.join(name);
        let mut image = PagedImage::<f32>::create(shape, coords, &path).unwrap();
        image.save().unwrap();
        drop(image);
        let record = coords_record(&path);
        let slot = usize::from(append);
        for kind in ["pixelmap", "worldmap"] {
            assert_eq!(
                int_map(&record, &format!("{kind}{slot}")),
                [-(slot as i32) - 1, slot as i32],
            );
        }
        let restored = CoordinateSystem::from_mapped_record(&record).unwrap();
        let got = restored.to_world(&input).unwrap();
        assert!((world[slot] - got[slot]).abs() < 1e-12);
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
    assert_eq!(fields(&record), ["stokes0", "direction1", "spectral2"]);
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

/// `transposed.image`'s pixel [s, x, y, c] (Stokes, RA, Dec, channel).
fn transposed_value(s: usize, x: usize, y: usize, c: usize) -> f64 {
    (s + 10 * x + 100 * y + 1000 * c) as f64 + 0.5
}

/// imstat of RA 1 to 2 by Dec 0 to 1, every other pixel, on the image at
/// `path` whose direction is on pixel axes `plane`: a box, a pixel CRTF box,
/// a world CRTF box, and pixel and world CRTF polygons, each against the
/// count and sum of the image's own pixels there.
fn assert_regions_take_the_direction_plane(path: &Path, plane: [usize; 2]) {
    let image = PagedImage::<f32>::open(path).unwrap();
    let data = image.get().unwrap();
    let (npts, sum) = data
        .indexed_iter()
        .filter(|(index, _)| (1..=2).contains(&index[plane[0]]) && index[plane[1]] <= 1)
        .fold((0.0, 0.0), |(n, s), (_, value)| {
            (n + 1.0, s + f64::from(*value))
        });
    let mut blc = vec![0; data.ndim()];
    let mut trc = data.shape().iter().map(|n| n - 1).collect::<Vec<_>>();
    (blc[plane[0]], trc[plane[0]], blc[plane[1]], trc[plane[1]]) = (1, 2, 0, 1);
    // Direction world axes are numbered as its pixel axes are.
    let sky = |x: f64, y: f64| {
        let mut pixel = vec![0.0; data.ndim()];
        (pixel[plane[0]], pixel[plane[1]]) = (x, y);
        let world = image.coordinates().to_world(&pixel).unwrap();
        format!("{:.15}rad,{:.15}rad", world[plane[0]], world[plane[1]])
    };
    let label = path.display();
    for (box_pixels, region) in [
        (Some("1,0,2,1".to_string()), None),
        (None, Some("box[[1pix,0pix],[2pix,1pix]]".to_string())),
        (
            None,
            Some(format!("box[[{}],[{}]]", sky(1.0, 0.0), sky(2.0, 1.0))),
        ),
    ] {
        let stats = match casa_imageanalysis::imstat(&casa_imageanalysis::ImstatRequest {
            imagename: path.to_path_buf(),
            box_pixels: box_pixels.clone(),
            region: region.clone(),
            ..casa_imageanalysis::ImstatRequest::default()
        })
        .unwrap()
        {
            casa_imageanalysis::ImstatOutcome::Global(stats) => stats,
            other => panic!("expected global statistics: {other:?}"),
        };
        assert_eq!((stats.npts, stats.sum), (npts, sum), "{label} {region:?}");
        assert_eq!((&stats.blc, &stats.trc), (&blc, &trc), "{label} {region:?}");
    }
    for polygon in [
        "poly [[1pix,0pix],[2pix,0pix],[2pix,1pix],[1pix,1pix]]".to_string(),
        format!(
            "poly [[{}],[{}],[{}],[{}]]",
            sky(1.0, 0.0),
            sky(2.0, 0.0),
            sky(2.0, 1.0),
            sky(1.0, 1.0)
        ),
    ] {
        let stats = match casa_imageanalysis::imstat(&casa_imageanalysis::ImstatRequest {
            imagename: path.to_path_buf(),
            region: Some(polygon.clone()),
            ..casa_imageanalysis::ImstatRequest::default()
        })
        .unwrap()
        {
            casa_imageanalysis::ImstatOutcome::Global(stats) => stats,
            other => panic!("expected global statistics: {other:?}"),
        };
        assert_eq!((stats.npts, stats.sum), (npts, sum), "{label} {polygon}");
        assert_eq!((&stats.blc, &stats.trc), (&blc, &trc), "{label} {polygon}");
    }
}

/// An image over [Stokes, Freq, RA, Dec] of shape [2, 3, 5, 4]: two axes
/// before a rectangular direction plane. Pixel [s, c, x, y] holds
/// `s + 10c + 100x + 1000y + 0.5`.
fn write_stokes_frequency_first(path: &Path) {
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I, StokesType::Q]));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        1.0e11,
        1.0e8,
        1.0,
        1.0e11,
    ));
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [1.2, -0.4],
        [-1.0e-5, 1.0e-5],
        [2.0, 1.5],
    ));
    let shape = [2, 3, 5, 4];
    let mut image = PagedImage::<f32>::create(shape.to_vec(), coordinates, path).unwrap();
    let pixels = ndarray::ArrayD::from_shape_fn(ndarray::IxDyn(&shape), |index| {
        (index[0] + 10 * index[1] + 100 * index[2] + 1000 * index[3]) as f32 + 0.5
    });
    image.put_slice(&pixels, &[0, 0, 0, 0]).unwrap();
    image.save().unwrap();
}

const FRACTIONAL_TRIANGLE: &str = "poly [[0.75pix,0.75pix],[2.75pix,0.75pix],[0.75pix,2.75pix]]";

#[test]
fn crtf_polygons_match_casa_pixel_centre_selection() {
    let scratch = tempfile::tempdir().expect("scratch");
    let transposed = copy_image("transposed.image", scratch.path());
    // CASA 6.7.6-14 imstat on this fixture recorded the count, sum, and
    // bounds for both representations. World vertices come from CASA.
    let cases = [
        (
            // The six centres x + y <= 2 include the three on the diagonal.
            "poly [[0pix,0pix],[2pix,0pix],[0pix,2pix]]",
            concat!(
                "poly [[1.200016285635282rad,-0.400009999952436rad],",
                "[1.199994571454906rad,-0.400009999994715rad],",
                "[1.200016285497573rad,-0.399989999952436rad]]"
            ),
            (60.0, 124_460.0),
            [0, 0, 0, 0],
            [1, 2, 2, 4],
        ),
        (
            // Fractional boundaries surround just (1, 1) and (2, 1).
            "poly [[0.75pix,0.75pix],[2.75pix,0.75pix],[2.75pix,1.75pix],[0.75pix,1.75pix]]",
            concat!(
                "poly [[1.200008142791820rad,-0.400002499988109rad],",
                "[1.199986428680300rad,-0.400002499966969rad],",
                "[1.199986428737678rad,-0.399992499966969rad],",
                "[1.200008142757393rad,-0.399992499988109rad]]"
            ),
            (20.0, 42_320.0),
            [0, 1, 1, 0],
            [1, 2, 1, 4],
        ),
        (
            // Same bounds as a square, but (2, 2) is outside the triangle.
            FRACTIONAL_TRIANGLE,
            concat!(
                "poly [[1.200008142791820rad,-0.400002499988109rad],",
                "[1.199986428680300rad,-0.400002499966969rad],",
                "[1.200008142722967rad,-0.399982499988108rad]]"
            ),
            (30.0, 64_430.0),
            [0, 1, 1, 0],
            [1, 2, 2, 4],
        ),
    ];
    for (pixel, world, expected, blc, trc) in cases {
        for polygon in [pixel, world] {
            let stats = match casa_imageanalysis::imstat(&casa_imageanalysis::ImstatRequest {
                imagename: transposed.clone(),
                region: Some(polygon.to_string()),
                ..casa_imageanalysis::ImstatRequest::default()
            })
            .unwrap()
            {
                casa_imageanalysis::ImstatOutcome::Global(stats) => stats,
                other => panic!("expected global statistics: {other:?}"),
            };
            assert_eq!((stats.npts, stats.sum), expected, "{polygon}");
            assert_eq!(stats.blc, blc, "{polygon}");
            assert_eq!(stats.trc, trc, "{polygon}");
        }
    }
}

#[test]
fn fractional_crtf_triangle_cuts_the_casa_mask() {
    let scratch = tempfile::tempdir().expect("scratch");
    let transposed = copy_image("transposed.image", scratch.path());
    let math = scratch.path().join("math.image");
    let subimage = scratch.path().join("subimage.image");
    casa_imageanalysis::immath(&casa_imageanalysis::ImmathRequest {
        imagename: vec![transposed.clone()],
        outfile: math.clone(),
        expr: "IM0".to_string(),
        region: Some(FRACTIONAL_TRIANGLE.to_string()),
        ..Default::default()
    })
    .unwrap();
    imsubimage(&ImsubimageRequest {
        imagename: transposed,
        outfile: subimage.clone(),
        region: Some(FRACTIONAL_TRIANGLE.to_string()),
        ..Default::default()
    })
    .unwrap();

    // CASA 6.7.6-14 immath and imsubimage both retained these three
    // direction pixels in every Stokes and frequency plane.
    for path in [math, subimage] {
        let output = PagedImage::<f32>::open(&path).unwrap();
        let mask = output.get_mask().unwrap().unwrap();
        assert_eq!(mask.shape(), &[2, 2, 2, 5], "{}", path.display());
        for (index, &selected) in mask.indexed_iter() {
            assert_eq!(selected, index[1] + index[2] <= 1, "{}", path.display());
        }
    }
}

#[test]
fn regions_take_the_direction_wherever_its_axes_are() {
    // A box's or polygon's x and y run along the direction's axes wherever
    // they are, as CASA takes them (#1002); they were refused, and read in
    // record order they ran along the first two axes.
    let scratch = tempfile::tempdir().expect("scratch");
    let transposed = copy_image("transposed.image", scratch.path());
    // CASA 6.7.6's imstat of this box on [Stokes, RA, Dec, Freq].
    let stats = match casa_imageanalysis::imstat(&casa_imageanalysis::ImstatRequest {
        imagename: transposed.clone(),
        box_pixels: Some("1,0,2,1".to_string()),
        ..casa_imageanalysis::ImstatRequest::default()
    })
    .unwrap()
    {
        casa_imageanalysis::ImstatOutcome::Global(stats) => stats,
        other => panic!("expected global statistics: {other:?}"),
    };
    assert_eq!((stats.npts, stats.sum), (40.0, 82_640.0));
    assert_eq!((stats.blc, stats.trc), (vec![0, 1, 0, 0], vec![1, 2, 1, 4]));

    let rectangular = scratch.path().join("stokes_frequency_first.image");
    write_stokes_frequency_first(&rectangular);
    for (path, plane) in [
        (transposed, [1, 2]),
        (copy_image("spectral_first.image", scratch.path()), [1, 2]),
        (rectangular, [2, 3]),
    ] {
        assert_regions_take_the_direction_plane(&path, plane);
    }
}

#[test]
fn impv_checks_edge_distances_on_the_actual_direction_axes() {
    let scratch = tempfile::tempdir().expect("scratch");
    for name in ["transposed.image", "spectral_first.image"] {
        let error = impv(&ImpvRequest {
            imagename: copy_image(name, scratch.path()),
            outfile: scratch.path().join("pv.image"),
            start: Some(PvPosition::Pixel([0.0, 1.0])),
            end: Some(PvPosition::Pixel([3.0, 1.0])),
            ..ImpvRequest::default()
        })
        .unwrap_err();
        assert!(error.to_string().contains("two pixels"), "{name}: {error}");
    }
}

#[test]
fn a_box_subimage_of_a_transposed_image_cuts_the_direction_plane() {
    let scratch = tempfile::tempdir().expect("scratch");
    let input = copy_image("transposed.image", scratch.path());
    let outfile = scratch.path().join("box.image");
    imsubimage(&ImsubimageRequest {
        imagename: input.clone(),
        outfile: outfile.clone(),
        box_pixels: Some("1,0,2,1".into()),
        ..ImsubimageRequest::default()
    })
    .unwrap();
    let cut = PagedImage::<f32>::open(&outfile).unwrap();
    let image = PagedImage::<f32>::open(&input).unwrap();
    assert_eq!(cut.shape(), [2, 2, 2, 5]);
    for [s, x, y, c] in [[0usize, 0, 0, 0], [1, 1, 1, 4], [0, 1, 0, 2]] {
        assert_eq!(
            f64::from(cut.get_at(&[s, x, y, c]).unwrap()),
            transposed_value(s, x + 1, y, c)
        );
        let at = [s, x, y, c].map(|p| p as f64);
        assert_eq!(
            cut.coordinates().to_world(&at).unwrap(),
            image
                .coordinates()
                .to_world(&[at[0], at[1] + 1.0, at[2], at[3]])
                .unwrap()
        );
    }
}

/// Which of a feather pair's coordinates comes first, which places its RA,
/// Dec, Stokes and frequency axes.
#[derive(Clone, Copy, Debug)]
enum FirstAxis {
    Direction,
    Stokes,
    Frequency,
}

impl FirstAxis {
    fn axes(self) -> [usize; 4] {
        match self {
            Self::Direction => [0, 1, 2, 3],
            Self::Stokes => [1, 2, 0, 3],
            Self::Frequency => [1, 2, 3, 0],
        }
    }

    /// The index of pixel (`x`, `y`) of channel `c`.
    fn index(self, x: usize, y: usize, c: usize) -> [usize; 4] {
        let mut index = [0; 4];
        for (axis, value) in self.axes().into_iter().zip([x, y, 0, c]) {
            index[axis] = value;
        }
        index
    }
}

/// A feather pair, a high-resolution image (2″ beam) and a low-resolution
/// one (6″), of 16 by 16 pixels, one Stokes and two channels, laid out as
/// `layout` places the axes, the same pixels any way.
fn feather_pair(dir: &Path, layout: FirstAxis) -> (PathBuf, PathBuf) {
    let arcsec = std::f64::consts::PI / 648_000.0;
    let [x_axis, _, stokes_axis, channel_axis] = layout.axes();
    let coordinates = || {
        let mut placed: Vec<(usize, CoordinateModel)> = vec![
            (
                x_axis,
                DirectionCoordinate::new(
                    DirectionRef::J2000,
                    Projection::new(ProjectionType::SIN),
                    [1.2, -0.4],
                    [-arcsec, arcsec],
                    [8.0, 8.0],
                )
                .into(),
            ),
            (
                stokes_axis,
                StokesCoordinate::new(vec![StokesType::I]).into(),
            ),
            (
                channel_axis,
                SpectralCoordinate::new(FrequencyRef::LSRK, 1.0e11, 1.0e8, 0.0, 1.0e11).into(),
            ),
        ];
        placed.sort_by_key(|(axis, _)| *axis);
        let mut coordinates = CoordinateSystem::new();
        for (_, coordinate) in placed {
            coordinates.add_coordinate(coordinate);
        }
        coordinates
    };
    let mut shape = vec![0; 4];
    for (axis, length) in layout.axes().into_iter().zip([16, 16, 1, 2]) {
        shape[axis] = length;
    }
    let make = |name: &str, beam: f64, value: fn(usize, usize, usize) -> f32| {
        let path = dir.join(format!("{layout:?}-{name}.image"));
        let mut image = PagedImage::<f32>::create(shape.clone(), coordinates(), &path).unwrap();
        let [x_at, y_at, _, c_at] = layout.axes();
        let pixels = ndarray::ArrayD::from_shape_fn(ndarray::IxDyn(&shape), |index| {
            value(index[x_at], index[y_at], index[c_at])
        });
        image.put_slice(&pixels, &[0, 0, 0, 0]).unwrap();
        image.set_units("Jy/beam").unwrap();
        image
            .set_image_info(&ImageInfo {
                beam_set: ImageBeamSet::new(GaussianBeam::new(beam * arcsec, beam * arcsec, 0.0)),
                image_type: ImageType::Intensity,
                object_name: String::new(),
            })
            .unwrap();
        image.save().unwrap();
        path
    };
    let high = make("high", 2.0, |x, y, c| {
        (0.7 * x as f32 + 0.3 * y as f32 + c as f32).sin()
    });
    let low = make("low", 6.0, |x, y, c| 0.01 * (x + 2 * y + 5 * c) as f32);
    (high, low)
}

#[test]
fn feather_takes_the_direction_wherever_its_axes_are() {
    // A transpose-invariance law: a pair with Stokes or frequency first
    // feathers to the transpose of the same pair with the direction first.
    // Its FFT ran over the first two axes (#1002). CASA 6.7.6's feather fails
    // on such a pair (`docs/CASA (C++) bugs.md`); the direction-first result
    // is checked against CASA's in `image_analysis_casa`.
    let scratch = tempfile::tempdir().expect("scratch");
    let feathered = |layout: FirstAxis| {
        let (highres, lowres) = feather_pair(scratch.path(), layout);
        let imagename = scratch.path().join(format!("{layout:?}-feathered.image"));
        feather(&FeatherRequest {
            imagename: imagename.clone(),
            highres,
            lowres,
            sdfactor: 1.0,
            effdishdiam: -1.0,
            lowpassfiltersd: false,
            overwrite: false,
        })
        .unwrap();
        PagedImage::<f32>::open(&imagename).unwrap().get().unwrap()
    };
    let first = feathered(FirstAxis::Direction);
    let largest = first
        .iter()
        .fold(0.0_f32, |peak, value| peak.max(value.abs()));
    for layout in [FirstAxis::Stokes, FirstAxis::Frequency] {
        let other = feathered(layout);
        for (x, y, c) in
            (0..16).flat_map(|x| (0..16).flat_map(move |y| (0..2).map(move |c| (x, y, c))))
        {
            let value = first[ndarray::IxDyn(&FirstAxis::Direction.index(x, y, c))];
            let transposed = other[ndarray::IxDyn(&layout.index(x, y, c))];
            assert!(
                (value - transposed).abs() <= 1e-6 * largest,
                "{layout:?} [{x}, {y}, {c}]: {value} against {transposed}"
            );
        }
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
        chans: Some("1~3".into()),
        ..ImsubimageRequest::default()
    })
    .unwrap();

    let record = coords_record(&outfile);
    assert_eq!(fields(&record), ["spectral2", "direction0", "stokes1"]);
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

#[test]
fn subimages_read_as_casa_reads_the_pixels_they_cut() {
    // CASA's world coordinates and values at the input pixels each box cuts
    // (`axis_maps.json` `crops`). A linear coordinate's removed axis stays in
    // the record it copies, so each kept axis moves by its own map: the
    // reference pixels once moved by the first axes of the record, leaving
    // `dropdeg_linear.image`'s origin at [186, 240] instead of [184, 249].
    // A tabular frequency axis moves its table: it once stayed at the input's
    // channels, labelling the first output channel 1.0 GHz, not 1.1 GHz.
    let path = fixture_dir().join("axis_maps.json");
    let document: Json = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let crops = document["crops"].as_array().expect("crops");
    assert_eq!(crops.len(), 5);
    let scratch = tempfile::tempdir().expect("scratch");
    let output_dir = mapped_rewrite_directory(scratch.path());
    for crop in crops {
        let name = crop["image"].as_str().expect("image name");
        let blc = numbers(&crop["blc"]);
        let trc = numbers(&crop["trc"]);
        let input = copy_image(name, scratch.path());
        let (box_pixels, chans) = if name == "tabular_spectral_first.image" {
            // The whole image but channels 1 to 2 of axis 0.
            assert_eq!((blc[0], trc[0]), (1.0, 2.0));
            (None, Some("1~2".to_string()))
        } else {
            let image = PagedImage::<f32>::open(&input).unwrap();
            let coordinates = image.coordinates();
            let mut plane = [0, 1];
            if let Some(index) = coordinates.find_coordinate(CoordinateType::Direction)
                && let Some(axes) = coordinates.coordinate_pixel_axes(index)
                && let [Some(x), Some(y)] = axes.as_slice()
            {
                plane = [*x, *y];
            }
            let [x, y] = plane;
            let box_pixels = format!("{},{},{},{}", blc[x], blc[y], trc[x], trc[y]);
            (Some(box_pixels), None)
        };
        let outfile = output_dir.join(format!("crop-{name}"));
        imsubimage(&ImsubimageRequest {
            imagename: input,
            outfile: outfile.clone(),
            box_pixels,
            chans,
            ..ImsubimageRequest::default()
        })
        .unwrap_or_else(|error| panic!("{name}: {error}"));
        let output = PagedImage::<f32>::open(&outfile).unwrap();
        assert_reads_as_casa(&output, &crop["casa"], &format!("{name} cut"));
    }
}
