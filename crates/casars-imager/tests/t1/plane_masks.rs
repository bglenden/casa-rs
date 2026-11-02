// SPDX-License-Identifier: LGPL-3.0-or-later

//! Clean masks per plane, as CASA makes them: `auto-multithresh` masks each
//! channel of a cube and each Stokes plane of its own
//! (`SDMaskHandler::autoMaskByMultiThreshold`), and a mask image is
//! expanded to the image's channels and Stokes planes
//! (`SDMaskHandler::expandMask`): a continuum Stokes I mask to every plane,
//! a continuum mask of the image's Stokes planes to every channel, a cube
//! Stokes I mask to every Stokes plane; a continuum mask of another number
//! of Stokes planes, or a cube mask of other channels, is refused.

use std::path::Path;

use casa_coordinates::{CoordinateModel, CoordinateSystem, StokesCoordinate, StokesType};
use casa_images::PagedImage;
use ndarray::{Array2, ArrayD, Axis, IxDyn};
use serde_json::{Value, json};

use super::fixture::{Component, IMAGE_SIZE, Observation, Products, component_pixel};

const POINT: Component = Component {
    offset_px: [24, 16],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: 0.0,
};
const GAUSSIAN: Component = Component {
    offset_px: [-30, -26],
    flux_jy: 0.5,
    fwhm_px: Some(8.0),
    spectral_index: 0.0,
};
/// The four native channels the cubes image.
const CHANNELS: usize = 4;

/// Plane `channel`, `stokes` of a product's `[x, y, stokes, channel]` values.
fn plane(values: &ArrayD<f32>, stokes: usize, channel: usize) -> Array2<f32> {
    values
        .index_axis(Axis(3), channel)
        .index_axis(Axis(2), stokes)
        .to_owned()
        .into_dimensionality()
        .expect("a direction plane")
}

/// Every plane of `products`' `.model` lies inside the same plane of its
/// `.mask`.
fn assert_model_inside_each_plane(products: &Products) {
    let model = products.get(".model").values();
    let mask = products.get(".mask").values();
    assert_eq!(model.shape(), mask.shape());
    let outside = model
        .iter()
        .zip(mask.iter())
        .filter(|(model, mask)| **mask == 0.0 && **model != 0.0)
        .count();
    assert_eq!(outside, 0, "model pixels outside their plane's mask");
}

/// An automatic mask plane over both sources and only about them: within
/// 32 pixels of a source and under a 25th of the plane.
fn assert_holds_the_sources(mask: &Array2<f32>, label: &str) {
    let sources = [component_pixel(POINT), component_pixel(GAUSSIAN)];
    for [x, y] in sources {
        assert_eq!(mask[[x, y]], 1.0, "{label}: the source at [{x}, {y}]");
    }
    let farthest = mask
        .indexed_iter()
        .filter(|(_, value)| **value != 0.0)
        .map(|((x, y), _)| {
            sources
                .iter()
                .map(|[sx, sy]| x.abs_diff(*sx).pow(2) + y.abs_diff(*sy).pow(2))
                .min()
                .expect("two sources")
        })
        .max()
        .unwrap_or(0);
    assert!(
        farthest <= 32 * 32,
        "{label}: a masked pixel {farthest} px²"
    );
    let masked = mask.iter().filter(|value| **value != 0.0).count();
    assert!(
        masked < IMAGE_SIZE * IMAGE_SIZE / 25,
        "{label}: {masked} pixels"
    );
}

/// A cube's channels are masked each of its own: a leading channel with no
/// data (the image starts a channel below the band) keeps an empty mask,
/// where the one-plane mask took the first channel's PSF beam and failed on
/// its empty PSF, and every channel with data masks both sources.
#[test]
fn a_cube_is_automasked_channel_by_channel() {
    let observation =
        Observation::synthesise_band(CHANNELS, &[("point", POINT), ("gaussian", GAUSSIAN)]);
    let (summary, products) = observation.image(
        "cube-automask",
        json!({
            "specmode": "cube",
            "nchan": CHANNELS + 1,
            "start": "43.872GHz",
            "width": "128MHz",
            "outframe": "TOPO",
            "weighting": "natural",
            "deconvolver": "hogbom",
            "niter": 2000,
            "threshold": "0.01Jy",
            "usemask": "auto-multithresh",
        }),
    );
    let sumwt = products.get(".sumwt").channel_values();
    assert_eq!(sumwt.len(), CHANNELS + 1);
    assert_eq!(sumwt[0], 0.0, "the leading channel holds no data");
    let mask = products.get(".mask").values();
    assert!(
        plane(&mask, 0, 0).iter().all(|value| *value == 0.0),
        "the channel without data is not masked"
    );
    for (channel, sumwt) in sumwt.iter().enumerate().skip(1) {
        assert!(*sumwt > 0.0, "channel {channel} holds data");
        assert_holds_the_sources(&plane(&mask, 0, channel), &format!("channel {channel}"));
    }
    assert_model_inside_each_plane(&products);
    eprintln!(
        "T1 cube auto-multithresh: {} minor iterations, {} major cycles",
        summary.actual_minor_iterations, summary.major_cycles
    );
}

/// Each Stokes plane is masked of its own: Stokes I about both sources;
/// Q and U, which parallel-hand data do not feed, and V, the noise of the
/// difference of the hands, stay empty, and nothing is cleaned in them.
#[test]
fn each_stokes_plane_is_automasked_of_its_own() {
    let observation = Observation::synthesise(&[("point", POINT), ("gaussian", GAUSSIAN)]);
    let (_, products) = observation.image(
        "iquv-automask",
        json!({
            "stokes": "IQUV",
            "weighting": "natural",
            "deconvolver": "hogbom",
            "niter": 500,
            "threshold": "0.005Jy",
            "usemask": "auto-multithresh",
        }),
    );
    let mask = products.get(".mask").values();
    assert_holds_the_sources(&plane(&mask, 0, 0), "Stokes I");
    for stokes in 1..4 {
        assert!(
            plane(&mask, stokes, 0).iter().all(|value| *value == 0.0),
            "Stokes plane {stokes} is not masked"
        );
    }
    assert_model_inside_each_plane(&products);
}

/// A box of half-width `half` about `centre`, on the image grid.
fn box_about(centre: [usize; 2], half: usize) -> Array2<f32> {
    Array2::from_shape_fn((IMAGE_SIZE, IMAGE_SIZE), |(x, y)| {
        f32::from(x.abs_diff(centre[0]) <= half && y.abs_diff(centre[1]) <= half)
    })
}

/// The expected pixels of each Stokes plane and channel.
type Planes<'a> = dyn Fn(usize, usize) -> Array2<f32> + 'a;

/// A mask image at `path` on `coordinates` with `stokes` Stokes planes and
/// `channels` channels, `planes(stokes, channel)` its pixels.
fn write_mask(
    path: &Path,
    coordinates: &CoordinateSystem,
    (stokes, channels): (&[StokesType], usize),
    planes: impl Fn(usize, usize) -> Array2<f32>,
) {
    let mut coordinates = coordinates.clone();
    let index = (0..coordinates.n_coordinates())
        .find(|index| matches!(coordinates.coordinate(*index), CoordinateModel::Stokes(_)))
        .expect("a Stokes coordinate");
    coordinates.replace_coordinate(index, StokesCoordinate::new(stokes.to_vec()));
    let shape = vec![IMAGE_SIZE, IMAGE_SIZE, stokes.len(), channels];
    let values = ArrayD::from_shape_fn(IxDyn(&shape), |index| {
        planes(index[2], index[3])[[index[0], index[1]]]
    });
    let mut image = PagedImage::<f32>::create(shape, coordinates, path).expect("create mask");
    image.put_slice(&values, &[0; 4]).expect("write mask");
    image.save().expect("save mask");
}

/// tclean's expansion of mask images to an image's channels and Stokes
/// planes, and its refusals.
#[test]
fn a_mask_image_expands_to_the_planes_as_casa_expands_it() {
    let observation =
        Observation::synthesise_band(CHANNELS, &[("point", POINT), ("gaussian", GAUSSIAN)]);
    let cube = json!({"specmode": "cube", "stokes": "IQUV", "niter": 0});
    let (_, grid) = observation.image("expand-grid", cube);
    let coordinates = grid.get(".psf").image.coordinates().clone();
    let point = box_about(component_pixel(POINT), 6);
    let gaussian = box_about(component_pixel(GAUSSIAN), 10);
    let both = &point + &gaussian;
    let empty = Array2::<f32>::zeros((IMAGE_SIZE, IMAGE_SIZE));
    let i = [StokesType::I];
    let iquv = [StokesType::I, StokesType::Q, StokesType::U, StokesType::V];
    let path = |name: &str| observation.scratch(name);
    write_mask(&path("cont-i.mask"), &coordinates, (&i, 1), |_, _| {
        point.clone()
    });
    let by_stokes = |stokes: usize| match stokes {
        0 => point.clone(),
        1 => gaussian.clone(),
        2 => both.clone(),
        _ => empty.clone(),
    };
    write_mask(
        &path("cont-iquv.mask"),
        &coordinates,
        (&iquv, 1),
        |stokes, _| by_stokes(stokes),
    );
    let by_channel = |channel: usize| {
        if channel < 2 {
            point.clone()
        } else {
            gaussian.clone()
        }
    };
    write_mask(
        &path("cube-i.mask"),
        &coordinates,
        (&i, CHANNELS),
        |_, channel| by_channel(channel),
    );
    let run = |name: &str, mask: &str, controls: Value| {
        let mut controls = controls;
        let fields = controls.as_object_mut().expect("controls");
        fields.insert("mask".to_string(), json!(path(mask)));
        fields.insert("niter".to_string(), json!(10));
        fields.insert("deconvolver".to_string(), json!("hogbom"));
        observation.try_image(name, controls)
    };
    let cases: [(&str, &str, Value, &Planes); 5] = [
        // A continuum Stokes I mask to every channel, every Stokes plane,
        // and both.
        (
            "i-to-cube",
            "cont-i.mask",
            json!({"specmode": "cube"}),
            &|_, _| point.clone(),
        ),
        (
            "i-to-iquv",
            "cont-i.mask",
            json!({"stokes": "IQUV"}),
            &|_, _| point.clone(),
        ),
        (
            "i-to-cube-iquv",
            "cont-i.mask",
            json!({"specmode": "cube", "stokes": "IQUV"}),
            &|_, _| point.clone(),
        ),
        // A continuum IQUV mask to every channel, Stokes plane by plane.
        (
            "iquv-to-cube-iquv",
            "cont-iquv.mask",
            json!({"specmode": "cube", "stokes": "IQUV"}),
            &|stokes, _| by_stokes(stokes),
        ),
        // A cube Stokes I mask to every Stokes plane, channel by channel.
        (
            "cube-i-to-cube-iquv",
            "cube-i.mask",
            json!({"specmode": "cube", "stokes": "IQUV"}),
            &|_, channel| by_channel(channel),
        ),
    ];
    for (name, mask, controls, expected) in cases {
        let (_, products) =
            run(name, mask, controls).unwrap_or_else(|error| panic!("{name}: {error}"));
        let values = products.get(".mask").values();
        let shape = values.shape().to_vec();
        for stokes in 0..shape[2] {
            for channel in 0..shape[3] {
                assert_eq!(
                    plane(&values, stokes, channel),
                    expected(stokes, channel),
                    "{name}: Stokes {stokes}, channel {channel}"
                );
            }
        }
        assert_model_inside_each_plane(&products);
    }

    // A continuum mask of two Stokes planes cannot extend to four, and a
    // cube mask of other channels cannot extend to the cube's
    // (`expandMask`, `compareSpectralCoordinate`).
    write_mask(
        &path("cont-iv.mask"),
        &coordinates,
        (&[StokesType::I, StokesType::V], 1),
        |_, _| point.clone(),
    );
    write_mask(
        &path("cube-3-i.mask"),
        &coordinates,
        (&i, CHANNELS - 1),
        |_, _| point.clone(),
    );
    for (name, mask, reason) in [
        (
            "iv-to-cube-iquv",
            "cont-iv.mask",
            "Cannot extend the input mask of 2 Stokes planes",
        ),
        ("cube-3-to-cube-iquv", "cube-3-i.mask", "channels"),
    ] {
        let refusal = run(name, mask, json!({"specmode": "cube", "stokes": "IQUV"}))
            .err()
            .unwrap_or_else(|| panic!("{name} is refused"));
        assert!(refusal.contains(reason), "{name}: {refusal}");
    }
}
