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
//! coordinates and the pixel value. casa-rs reads the images whose
//! coordinates each own adjacent axes in order, and those whose linear
//! coordinate has independent removed axes (`impv`'s position-velocity
//! image, and a linear image dropped on its first or second axis). It
//! refuses the three it cannot represent (#1002). The record also holds
//! CASA's world coordinates and values for boxes cut from three of them,
//! which casa-rs's `imsubimage` cuts alike.

use std::path::{Path, PathBuf};

use casa_coordinates::{
    CoordinateSystem, CoordinateType, DirectionCoordinate, Projection, ProjectionType,
    SpectralCoordinate, StokesCoordinate, StokesType,
};
use casa_images::{
    FeatherRequest, GaussianBeam, ImageBeamSet, ImageError, ImageInfo, ImageType, ImpvRequest,
    ImsubimageRequest, PagedImage, feather, impv, imsubimage,
};
use casa_tables::{Table, TableOptions};
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::frequency::FrequencyRef;
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
    for name in [
        "transposed.image",
        "spectral_first.image",
        "pv.image",
        "dropdeg_linear.image",
        "dropdeg_linear_between.image",
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

/// The fields of `record`'s coordinates, in the order casa-rs reads them.
fn fields(record: &casa_types::RecordValue) -> Vec<String> {
    CoordinateSystem::record_coordinates(record)
        .unwrap()
        .into_iter()
        .map(|placed| placed.field)
        .collect()
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

#[test]
fn regions_and_impv_take_the_direction_wherever_its_axes_are() {
    // `transposed.image` holds [Stokes, RA, Dec, Freq] of shape [2, 4, 3, 5].
    // A box's and an impv path's x and y are RA and Dec, as CASA takes them
    // in the direction plane (#1002); they were refused, and read in record
    // order they ran along Stokes and RA.
    let scratch = tempfile::tempdir().expect("scratch");
    let input = copy_image("transposed.image", scratch.path());

    // RA 1 to 2 by Dec 0 to 1, every Stokes and channel, as a box and a pixel
    // CRTF box: CASA 6.7.6's imstat of either gives npts 40, sum 82640, blc
    // [0, 1, 0, 0] and trc [1, 2, 1, 4].
    let expected = (0..2)
        .flat_map(|s| (1..=2).flat_map(move |x| (0..=1).map(move |y| (s, x, y))))
        .flat_map(|(s, x, y)| (0..5).map(move |c| transposed_value(s, x, y, c)))
        .sum::<f64>();
    assert_eq!(expected, 82_640.0);
    for (box_pixels, region) in [
        (Some("1,0,2,1"), None),
        (None, Some("box[[1pix,0pix],[2pix,1pix]]")),
    ] {
        let stats = casa_images::imstat(&input, box_pixels, region, None, None).unwrap();
        let label = format!("{box_pixels:?} {region:?}");
        assert_eq!((stats.npts, stats.sum), (40.0, expected), "{label}");
        assert_eq!(
            (stats.blc.as_slice(), stats.trc.as_slice()),
            (&[0, 1, 0, 0][..], &[1, 2, 1, 4][..]),
            "{label}"
        );
    }
    // A polygon's mask runs along RA and Dec too. casa-rs places a pix
    // vertex at a pixel's corner where CASA places it at its centre (#1021),
    // so these vertices hold the same four pixels.
    let polygon = "poly [[1pix,0pix],[3pix,0pix],[3pix,2pix],[1pix,2pix]]";
    let stats = casa_images::imstat(&input, None, Some(polygon), None, None).unwrap();
    assert_eq!((stats.npts, stats.sum), (40.0, expected));

    // The same box and polygon in world coordinates: RA and Dec are world
    // axes 1 and 2, as the pixel axes are.
    let image = PagedImage::<f32>::open(&input).unwrap();
    let sky = |x: f64, y: f64| {
        let world = image.coordinates().to_world(&[0.0, x, y, 0.0]).unwrap();
        format!("{:.15}rad,{:.15}rad", world[1], world[2])
    };
    let world_box = format!("box[[{}],[{}]]", sky(1.0, 0.0), sky(2.0, 1.0));
    let stats = casa_images::imstat(&input, None, Some(&world_box), None, None).unwrap();
    assert_eq!((stats.npts, stats.sum), (40.0, expected), "{world_box}");
    assert_eq!(
        (stats.blc.as_slice(), stats.trc.as_slice()),
        (&[0, 1, 0, 0][..], &[1, 2, 1, 4][..])
    );
    let world_polygon = format!(
        "poly [[{}],[{}],[{}],[{}]]",
        sky(1.0, 0.0),
        sky(3.0, 0.0),
        sky(3.0, 2.0),
        sky(1.0, 2.0)
    );
    let stats = casa_images::imstat(&input, None, Some(&world_polygon), None, None).unwrap();
    assert_eq!((stats.npts, stats.sum), (40.0, expected), "{world_polygon}");

    let outfile = scratch.path().join("box.image");
    imsubimage(&ImsubimageRequest {
        imagename: input.clone(),
        outfile: outfile.clone(),
        box_pixels: Some("1,0,2,1".into()),
        chans: None,
        overwrite: false,
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

    // A path along RA at Dec 1, of channels 1 to 2.
    let pv_path = scratch.path().join("pv.image");
    let summary = impv(&ImpvRequest {
        imagename: input,
        outfile: pv_path.clone(),
        mode: "coords".into(),
        start: "0,1".into(),
        end: "3,1".into(),
        width: 1,
        chans: Some("1~2".into()),
        overwrite: false,
    })
    .unwrap();
    assert_eq!(summary.shape, [2, 4, 1, 2]);
    let pv = PagedImage::<f32>::open(&pv_path).unwrap();
    for [s, x, c] in [[0usize, 0, 0], [1, 3, 1], [0, 2, 1]] {
        assert_eq!(
            f64::from(pv.get_at(&[s, x, 0, c]).unwrap()),
            transposed_value(s, x, 1, c + 1)
        );
    }
}

/// A feather pair, a high-resolution image (2″ beam) and a low-resolution
/// one (6″), over [RA, Dec, Stokes, Freq] or [Stokes, RA, Dec, Freq] of 16 by
/// 16 pixels, one Stokes and two channels, the same pixels either way.
fn feather_pair(dir: &Path, direction_first: bool) -> (PathBuf, PathBuf) {
    let arcsec = std::f64::consts::PI / 648_000.0;
    let coordinates = || {
        let mut coordinates = CoordinateSystem::new();
        let direction = DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [1.2, -0.4],
            [-arcsec, arcsec],
            [8.0, 8.0],
        );
        let stokes = StokesCoordinate::new(vec![StokesType::I]);
        if direction_first {
            coordinates.add_coordinate(direction);
            coordinates.add_coordinate(stokes);
        } else {
            coordinates.add_coordinate(stokes);
            coordinates.add_coordinate(direction);
        }
        coordinates.add_coordinate(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            1.0e11,
            1.0e8,
            0.0,
            1.0e11,
        ));
        coordinates
    };
    let shape = if direction_first {
        vec![16, 16, 1, 2]
    } else {
        vec![1, 16, 16, 2]
    };
    let make = |name: &str, beam: f64, value: fn(usize, usize, usize) -> f32| {
        let path = dir.join(format!("{direction_first}-{name}.image"));
        let mut image = PagedImage::<f32>::create(shape.clone(), coordinates(), &path).unwrap();
        let pixels = ndarray::ArrayD::from_shape_fn(ndarray::IxDyn(&shape), |index| {
            let (x, y) = if direction_first {
                (index[0], index[1])
            } else {
                (index[1], index[2])
            };
            value(x, y, index[3])
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
    // A pair with Stokes first feathers to the transpose of the same pair
    // with the direction first; its FFT ran over Stokes and RA (#1002).
    let scratch = tempfile::tempdir().expect("scratch");
    let mut outputs = Vec::new();
    for direction_first in [true, false] {
        let (highres, lowres) = feather_pair(scratch.path(), direction_first);
        let imagename = scratch
            .path()
            .join(format!("{direction_first}-feathered.image"));
        feather(&FeatherRequest {
            imagename: imagename.clone(),
            highres,
            lowres,
            sdfactor: 1.0,
            overwrite: false,
        })
        .unwrap();
        outputs.push(PagedImage::<f32>::open(&imagename).unwrap().get().unwrap());
    }
    let (first, transposed) = (&outputs[0], &outputs[1]);
    assert_eq!(transposed.shape(), [1, 16, 16, 2]);
    let largest = first
        .iter()
        .fold(0.0_f32, |peak, value| peak.max(value.abs()));
    for ((x, y, c), &value) in first
        .indexed_iter()
        .map(|(index, value)| ((index[0], index[1], index[3]), value))
    {
        let other = transposed[ndarray::IxDyn(&[0, x, y, c])];
        assert!(
            (value - other).abs() <= 1e-6 * largest,
            "[{x}, {y}, {c}]: {value} against {other}"
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
    assert_eq!(crops.len(), 3);
    let scratch = tempfile::tempdir().expect("scratch");
    for crop in crops {
        let name = crop["image"].as_str().expect("image name");
        let blc = numbers(&crop["blc"]);
        let trc = numbers(&crop["trc"]);
        let (box_pixels, chans) = if name == "tabular_spectral_first.image" {
            // The whole image but channels 1 to 2 of axis 0.
            assert_eq!((blc[0], trc[0]), (1.0, 2.0));
            (None, Some("1~2".to_string()))
        } else {
            let box_pixels = format!("{},{},{},{}", blc[0], blc[1], trc[0], trc[1]);
            (Some(box_pixels), None)
        };
        let outfile = scratch.path().join(format!("crop-{name}"));
        imsubimage(&ImsubimageRequest {
            imagename: copy_image(name, scratch.path()),
            outfile: outfile.clone(),
            box_pixels,
            chans,
            overwrite: false,
        })
        .unwrap_or_else(|error| panic!("{name}: {error}"));
        let output = PagedImage::<f32>::open(&outfile).unwrap();
        assert_reads_as_casa(&output, &crop["casa"], &format!("{name} cut"));
    }
}
