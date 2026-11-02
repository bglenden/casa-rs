// SPDX-License-Identifier: LGPL-3.0-or-later
//! casa-images' image analysis against CASA 6.7.6's (#841 Phase 6a).
//!
//! `tests/data/image_analysis_cases.json` describes small images and the
//! operations `simanalyze` and `simalma` make on them. CASA built the
//! images, ran the operations, and recorded the results in
//! `tests/data/image_analysis_casa.json`
//! (`tools/images/image_analysis_oracle.py`). Each test rebuilds the images
//! from the pixels and masks CASA recorded and from the case file's
//! coordinates, checks their world coordinates against CASA's, and compares
//! casa-rs's results.
//!
//! Pixels are compared on the pixels both sides keep, to a fraction of the
//! largest of CASA's: float32 rounding for regrids, products and quotients,
//! and CASA's single-precision FFTs for `convolve2d` and `feather`. Every
//! value on either side must be finite. At least nine tenths of CASA's valid
//! pixels must be valid in casa-rs too, so an output masked entirely, or
//! nearly, cannot pass.
//!
//! Masks are compared exactly, except where positions fall on whole pixels.
//! Where the grids' increments divide evenly, as in `simanalyze`'s recipe
//! and `convimage`'s Jy/pixel regrid, casa-rs computes those positions
//! exactly on a shared tangent plane, and casacore to about 1e-12 pixel on
//! either side. The two truncate to neighbouring pixels, so their stencils
//! differ by one, and where a masked pixel or the image's edge lies at the
//! stencil's edge, the masks differ. That is rounding at a tie, which
//! bitwise reproducibility is not a goal to break. Such a case regrids again
//! onto the template moved by ±1e-6 pixel on each axis, which puts every
//! tied position on one side or the other. At each output where the masks
//! differ, both CASA's mask and casa-rs's must be one of those regrids'.
//! Where CASA kept the pixel, one of those regrids must keep it with CASA's
//! value. Every other output must agree exactly. Cases whose positions avoid
//! whole pixels have no such allowance.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, DirectionCoordinate, Projection,
    ProjectionType, SpectralCoordinate, StokesCoordinate, StokesType,
};
use casa_images::{
    FeatherRequest, GaussianBeam, ImageBeamSet, ImageInfo, ImageInterface, ImmathRequest,
    ImpbcorRequest, PagedImage, RegridMethod, RegridTemplate, TempImage, convolve2d, regrid,
};
use casa_lattices::{LatticeStatistics, Statistic, TempStoragePolicy};
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::frequency::FrequencyRef;
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use serde_json::Value;

const ARCSEC: f64 = std::f64::consts::PI / (180.0 * 3600.0);
const DEG: f64 = std::f64::consts::PI / 180.0;

fn data(name: &str) -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data")
        .join(name);
    serde_json::from_str(&std::fs::read_to_string(path).expect("read the fixture"))
        .expect("parse the fixture")
}

fn numbers(value: &Value) -> Vec<f64> {
    value
        .as_array()
        .expect("a list")
        .iter()
        .map(|v| v.as_f64().expect("a number"))
        .collect()
}

fn usizes(value: &Value) -> Vec<usize> {
    numbers(value).into_iter().map(|v| v as usize).collect()
}

fn bools(value: &Value) -> Option<Vec<bool>> {
    value.as_array().map(|values| {
        values
            .iter()
            .map(|v| v.as_bool().expect("a bool"))
            .collect()
    })
}

fn beam_of(value: &Value) -> GaussianBeam {
    GaussianBeam::new(
        value["major_arcsec"].as_f64().unwrap() * ARCSEC,
        value["minor_arcsec"].as_f64().unwrap() * ARCSEC,
        value["pa_deg"].as_f64().unwrap() * DEG,
    )
}

/// The coordinate system the oracle gives an image of `spec`.
fn coordinates_of(spec: &Value) -> CoordinateSystem {
    let refval = numbers(&spec["refval_rad"]);
    let refpix = numbers(&spec["refpix"]);
    let inc = numbers(&spec["inc_arcsec"]);
    let frame: DirectionRef = spec["frame"].as_str().unwrap().parse().expect("a frame");
    let mut coordinates = CoordinateSystem::new();
    let mut direction = DirectionCoordinate::new(
        frame,
        Projection::new(ProjectionType::SIN),
        [refval[0], refval[1]],
        [inc[0] * ARCSEC, inc[1] * ARCSEC],
        [refpix[0], refpix[1]],
    );
    if let Some(pc) = spec["pc"].as_array() {
        // `setlineartransform` takes casacore's matrix, the transpose of the
        // FITS PC matrix casa-rs holds: element [i][j] is casa-rs's
        // pc[[j, i]], as the world-coordinate check below confirms.
        let pc: Vec<Vec<f64>> = pc.iter().map(numbers).collect();
        direction =
            direction.with_pc_matrix(ndarray::array![[pc[0][0], pc[1][0]], [pc[0][1], pc[1][1]]]);
    }
    coordinates.add_coordinate(direction);
    coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
    if spec["axes"] == "cube" {
        coordinates.add_coordinate(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            330.076e9,
            50e6,
            0.0,
            330.076e9,
        ));
    }
    coordinates
}

/// The images of the case file, built from CASA's pixels and saved in `dir`.
struct Images {
    paths: BTreeMap<String, PathBuf>,
    _dir: tempfile::TempDir,
}

impl Images {
    fn build(cases: &Value, casa: &Value) -> Self {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let mut paths = BTreeMap::new();
        for (name, spec) in cases["images"].as_object().unwrap() {
            let recorded = &casa["images"][name];
            let shape = usizes(&spec["shape"]);
            let coordinates = coordinates_of(spec);
            for check in recorded["world"].as_array().unwrap() {
                let ours = coordinates.to_world(&numbers(&check["pixel"])).unwrap();
                for (a, b) in ours.iter().zip(numbers(&check["world"])) {
                    assert!(
                        (a - b).abs() <= 1e-12 * b.abs().max(1.0),
                        "{name}: world {ours:?} against CASA's {:?}",
                        check["world"]
                    );
                }
            }
            let mut image = TempImage::<f32>::new(
                shape.clone(),
                coordinates,
                TempStoragePolicy::Auto {
                    memory_budget_bytes: 64 << 20,
                    scratch: casa_lattices::ScratchSpace::SystemTemp,
                },
            )
            .unwrap();
            let pixels: Vec<f32> = numbers(&recorded["pixels"])
                .into_iter()
                .map(|v| v as f32)
                .collect();
            let pixels = ArrayD::from_shape_vec(IxDyn(&shape).f(), pixels).unwrap();
            image.put_slice(&pixels, &vec![0; shape.len()]).unwrap();
            image.set_units(spec["unit"].as_str().unwrap()).unwrap();
            if !spec["beam"].is_null() {
                image
                    .set_image_info(&ImageInfo {
                        beam_set: ImageBeamSet::new(beam_of(&spec["beam"])),
                        ..ImageInfo::default()
                    })
                    .unwrap();
            }
            if let Some(mask) = bools(&recorded["mask"]) {
                let mask = ArrayD::from_shape_vec(IxDyn(&shape).f(), mask).unwrap();
                image.put_mask("mask0", &mask).unwrap();
                image.set_default_mask("mask0").unwrap();
            }
            let path = dir.path().join(format!("{name}.image"));
            image.save_as(&path).unwrap();
            paths.insert(name.clone(), path);
        }
        Self { paths, _dir: dir }
    }

    fn open(&self, name: &str) -> PagedImage<f32> {
        PagedImage::open(&self.paths[name]).expect("open a built image")
    }
}

fn fixtures() -> (Value, Value, Images) {
    let cases = data("image_analysis_cases.json");
    let casa = data("image_analysis_casa.json");
    assert_eq!(casa["casa_version"], "6.7.6-14");
    let images = Images::build(&cases, &casa);
    (cases, casa, images)
}

fn case<'a>(cases: &'a Value, name: &str) -> &'a Value {
    cases["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == name)
        .unwrap_or_else(|| panic!("case {name}"))
}

/// CASA's pixels, mask (all kept when it recorded none) and largest
/// magnitude.
fn casa_pixels(casa: &Value) -> (Vec<f64>, Vec<bool>, f64) {
    let values = numbers(&casa["pixels"]);
    let mask = bools(&casa["mask"]).unwrap_or_else(|| vec![true; values.len()]);
    let peak = values.iter().fold(0.0f64, |m, v| m.max(v.abs()));
    (values, mask, peak)
}

/// The largest difference on the pixels both keep, over CASA's largest
/// magnitude, and the pixels whose masks differ. Every value must be
/// finite, and at least nine tenths of the pixels CASA keeps kept by
/// casa-rs too.
fn mismatches(name: &str, ours: &[f64], our_mask: &[bool], casa: &Value) -> (f64, Vec<usize>) {
    let (theirs, their_mask, peak) = casa_pixels(casa);
    assert_eq!(ours.len(), theirs.len(), "{name}: sizes");
    assert!(
        ours.iter().chain(&theirs).all(|v| v.is_finite()),
        "{name}: a value is not finite"
    );
    let mut worst = 0.0f64;
    let mut masks = Vec::new();
    let mut compared = 0usize;
    for k in 0..theirs.len() {
        if our_mask[k] != their_mask[k] {
            masks.push(k);
        } else if our_mask[k] {
            compared += 1;
            worst = worst.max((ours[k] - theirs[k]).abs() / peak);
        }
    }
    let kept = their_mask.iter().filter(|v| **v).count();
    assert!(
        kept > 0 && peak > 0.0,
        "{name}: CASA kept nothing to compare"
    );
    assert!(
        10 * compared >= 9 * kept,
        "{name}: {compared} pixels compared of the {kept} CASA kept"
    );
    println!(
        "{name}: max |difference| / peak {worst:.3e} over {compared} pixels, {} mask \
         differences",
        masks.len()
    );
    (worst, masks)
}

fn compare(name: &str, ours: &[f64], our_mask: &[bool], casa: &Value) -> (f64, usize) {
    let (worst, masks) = mismatches(name, ours, our_mask, casa);
    (worst, masks.len())
}

/// `template` with its direction reference pixel moved by `shift` pixels.
fn nudged(template: &RegridTemplate, shift: [f64; 2]) -> RegridTemplate {
    let mut moved = template.clone();
    let index = moved
        .coordinates
        .find_coordinate(CoordinateType::Direction)
        .unwrap();
    let CoordinateModel::Direction(direction) = moved.coordinates.coordinate(index) else {
        unreachable!()
    };
    let refpix = direction.reference_pixel();
    let changed = direction
        .clone()
        .with_reference_pixel([refpix[0] + shift[0], refpix[1] + shift[1]]);
    moved.coordinates.replace_coordinate(index, changed);
    moved
}

/// Each mask difference explained as a tie: CASA's mask and casa-rs's are
/// both those of the regrid onto `template` moved ±1e-6 pixel on each axis,
/// and where CASA keeps the pixel, one of those regrids keeps it with CASA's
/// value.
fn ties_explain(
    name: &str,
    (input, template, method): (&PagedImage<f32>, &RegridTemplate, RegridMethod),
    (masks, our_mask): (&[usize], &[bool]),
    casa: &Value,
    tolerance: f64,
) {
    const NUDGE: f64 = 1e-6;
    let (theirs, their_mask, peak) = casa_pixels(casa);
    let neighbours: Vec<(Vec<f64>, Vec<bool>)> = [
        [NUDGE, NUDGE],
        [NUDGE, -NUDGE],
        [-NUDGE, NUDGE],
        [-NUDGE, -NUDGE],
    ]
    .iter()
    .map(|shift| {
        let moved = regrid(input, &nudged(template, *shift), method).unwrap();
        (flat(&moved.data), flat_mask(&moved.mask))
    })
    .collect();
    for &k in masks {
        let options: Vec<bool> = neighbours.iter().map(|(_, mask)| mask[k]).collect();
        assert!(
            options.contains(&their_mask[k]) && options.contains(&our_mask[k]),
            "{name}: output {k}'s masks are not those of neighbouring stencils"
        );
        if their_mask[k] {
            assert!(
                neighbours
                    .iter()
                    .any(|(values, mask)| mask[k]
                        && (values[k] - theirs[k]).abs() <= tolerance * peak),
                "{name}: output {k}'s value is no neighbouring stencil's"
            );
        }
    }
}

/// The output keeps the axes CASA keeps: its Stokes labels and its channel.
fn check_other_axes(name: &str, coordinates: &CoordinateSystem, casa: &Value) {
    for k in 0..coordinates.n_coordinates() {
        match coordinates.coordinate(k) {
            CoordinateModel::Stokes(stokes) => {
                let ours: Vec<String> = stokes.stokes().iter().map(|s| format!("{s:?}")).collect();
                let theirs: Vec<String> = casa["stokes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|s| s.as_str().unwrap().to_string())
                    .collect();
                assert_eq!(ours, theirs, "{name}: Stokes");
            }
            CoordinateModel::Spectral(spectral) => {
                let theirs = casa["spectral_refval_hz"].as_f64().unwrap();
                let ours = spectral.reference_value()[0];
                assert!(
                    (ours - theirs).abs() <= 1e-12 * theirs,
                    "{name}: channel {ours} Hz against CASA's {theirs}"
                );
            }
            _ => {}
        }
    }
}

fn flat(array: &ArrayD<f32>) -> Vec<f64> {
    array.t().iter().map(|v| f64::from(*v)).collect()
}

fn flat_mask(array: &ArrayD<bool>) -> Vec<bool> {
    array.t().iter().copied().collect()
}

/// The template a case names: another image's grid, changed as `simanalyze`
/// changes it, and with its one channel moved or its one Stokes plane
/// relabelled to probe the axes casacore does not regrid.
fn template_of(spec: &Value, images: &Images) -> RegridTemplate {
    let mut template = RegridTemplate::of(&images.open(spec["from"].as_str().unwrap()));
    if !spec["refpix"].is_null() {
        let index = template
            .coordinates
            .find_coordinate(CoordinateType::Direction)
            .unwrap();
        let CoordinateModel::Direction(direction) = template.coordinates.coordinate(index) else {
            unreachable!()
        };
        let scale = spec["inc_scale"].as_f64().unwrap();
        let increment = direction.increment();
        let refpix = numbers(&spec["refpix"]);
        let changed = direction
            .clone()
            .with_reference_pixel([refpix[0], refpix[1]])
            .with_increment([increment[0] * scale, increment[1] * scale]);
        template.coordinates.replace_coordinate(index, changed);
        template.shape = usizes(&spec["shape"]);
    }
    if let Some(shift) = spec["spectral_shift_hz"].as_f64() {
        let index = template
            .coordinates
            .find_coordinate(CoordinateType::Spectral)
            .unwrap();
        let moved =
            SpectralCoordinate::new(FrequencyRef::LSRK, 330.076e9 + shift, 50e6, 0.0, 330.076e9);
        template.coordinates.replace_coordinate(index, moved);
    }
    if let Some(stokes) = spec["stokes"].as_array() {
        let index = template
            .coordinates
            .find_coordinate(CoordinateType::Stokes)
            .unwrap();
        let labels = stokes
            .iter()
            .map(|s| StokesType::from_name(s.as_str().unwrap()).unwrap())
            .collect();
        template
            .coordinates
            .replace_coordinate(index, StokesCoordinate::new(labels));
    }
    template
}

/// Compare a regrid with CASA's: the values to `tolerance` of the peak, the
/// axes it keeps, and the masks exactly except, for a case whose positions
/// fall on whole pixels (`ties`), where neighbouring stencils explain them.
fn check_regrid(name: &str, tolerance: f64, ties: bool) {
    let (cases, casa, images) = fixtures();
    let case = case(&cases, name);
    let method = case
        .get("interpolation")
        .or_else(|| case.get("method"))
        .and_then(Value::as_str)
        .unwrap();
    let method = RegridMethod::from_casa(method).unwrap();
    let input = images.open(case["input"].as_str().unwrap());
    let template = template_of(&case["template"], &images);
    let regridded = regrid(&input, &template, method).unwrap();
    let theirs = &casa["cases"][name];
    assert_eq!(template.shape, usizes(&theirs["shape"]), "{name}: shape");
    check_other_axes(name, &regridded.coordinates, theirs);
    let our_mask = flat_mask(&regridded.mask);
    let (worst, masks) = mismatches(name, &flat(&regridded.data), &our_mask, theirs);
    assert!(worst <= tolerance, "{name}: {worst:e}");
    if !masks.is_empty() {
        assert!(ties, "{name}: {} mask differences", masks.len());
        ties_explain(
            name,
            (&input, &template, method),
            (&masks, &our_mask),
            theirs,
            tolerance,
        );
    }
}

#[test]
fn imregrid_of_simanalyzes_template_record_matches_casa() {
    // Its positions fall on whole pixels (see the module docs). Nearest is
    // tested on the edge grid: on half pixels it would be a tie everywhere.
    for method in ["cubic", "linear"] {
        check_regrid(&format!("imregrid_simanalyze_recipe_{method}"), 2e-7, true);
    }
}

#[test]
fn imregrid_near_masked_pixels_and_past_the_edges_matches_casa() {
    for method in ["cubic", "linear", "nearest"] {
        check_regrid(&format!("imregrid_edges_{method}"), 2e-7, false);
    }
}

#[test]
fn casas_decimated_positions_differ_only_at_ties_from_exact_ones() {
    // CASA interpolates the positions between a coarser grid of converted
    // ones; on a shared tangent plane they are affine, so the interpolation
    // is exact but for rounding, and the masks differ only at ties.
    check_regrid("imregrid_simanalyze_recipe_cubic_decimated", 2e-7, true);
}

#[test]
fn regrid_of_a_jy_per_pixel_model_scales_by_the_pixel_area_as_casa_does() {
    // `convimage`'s grids put positions on whole pixels at the image's edge;
    // the shifted grid puts none there, and its masks agree exactly.
    check_regrid("regrid_convimage", 2e-7, true);
    check_regrid("regrid_jypixel_cubic", 2e-7, true);
    check_regrid("regrid_jypixel_shifted_cubic", 2e-7, false);
    check_regrid("regrid_jypixel_shifted_linear", 2e-7, false);
}

#[test]
fn every_channel_of_a_cube_is_regridded_as_casa_regrids_it() {
    check_regrid("imregrid_two_channels_cubic", 2e-7, false);
}

#[test]
fn axes_of_one_pixel_keep_the_inputs_coordinate_as_casa_does() {
    // CASA keeps the input's channel for a template one channel away, and
    // its Stokes I for a template labelled Q.
    check_regrid("imregrid_one_channel_moved", 2e-7, false);
    check_regrid("imregrid_one_stokes_plane_relabelled", 2e-7, false);
}

#[test]
fn convolve2d_matches_casa() {
    let (cases, casa, images) = fixtures();
    for name in [
        "convolve2d_points",
        "convolve2d_negative_pa",
        "convolve2d_oblong_cube",
        "convolve2d_convimage",
        "convolve2d_rotated",
    ] {
        let case = case(&cases, name);
        let input = images.open(case["input"].as_str().unwrap());
        let convolved = convolve2d(&input, &beam_of(&case["beam"])).unwrap();
        let theirs = &casa["cases"][name];
        let mask = convolved
            .mask
            .as_ref()
            .map_or_else(|| vec![true; convolved.data.len()], flat_mask);
        let (worst, masks) = compare(name, &flat(&convolved.data), &mask, theirs);
        assert!(worst <= 2e-6, "{name}: {worst:e}");
        assert_eq!(masks, 0, "{name}");
        assert_eq!(convolved.units, theirs["unit"].as_str().unwrap(), "{name}");
        let beam = &theirs["beam"];
        let close = |ours: f64, casa: &Value| (ours - casa.as_f64().unwrap()).abs() < 1e-6;
        assert!(
            close(convolved.beam.major / ARCSEC, &beam["major_arcsec"]),
            "{name}"
        );
        assert!(
            close(convolved.beam.minor / ARCSEC, &beam["minor_arcsec"]),
            "{name}"
        );
        assert!(
            close(convolved.beam.position_angle / DEG, &beam["pa_deg"]),
            "{name}"
        );
    }
}

#[test]
fn convolve2d_refuses_what_casa_refuses_or_casa_rs_does_not_convolve() {
    let (_, _, images) = fixtures();
    // Jy/beam: CASA convolves the beams numerically; casa-rs does not.
    let error = convolve2d(
        &images.open("int_cube"),
        &GaussianBeam::new(1e-5, 1e-5, 0.0),
    );
    assert!(error.unwrap_err().to_string().contains("Jy/pixel"));
}

#[test]
fn robust_statistics_match_casa() {
    let (_, casa, images) = fixtures();
    for (name, input) in [
        ("statistics_robust", "stats_even"),
        ("statistics_robust_unmasked", "int_cube"),
    ] {
        let image = images.open(input);
        let mut stats = LatticeStatistics::new(&image);
        if let Some(mask) = image.default_mask().unwrap() {
            stats.set_pixel_mask(mask);
        }
        let theirs = &casa["cases"][name];
        for (key, statistic) in [
            ("npts", Statistic::Npts),
            ("sum", Statistic::Sum),
            ("mean", Statistic::Mean),
            ("median", Statistic::Median),
            ("medabsdevmed", Statistic::MedAbsDevMed),
            ("quartile", Statistic::Quartile),
            ("q1", Statistic::Q1),
            ("q3", Statistic::Q3),
            ("min", Statistic::Min),
            ("max", Statistic::Max),
            ("rms", Statistic::Rms),
            ("sigma", Statistic::Sigma),
        ] {
            let ours = *stats
                .get_statistic(statistic)
                .unwrap()
                .iter()
                .next()
                .unwrap();
            let casa = theirs[key].as_f64().unwrap();
            assert!(
                (ours - casa).abs() <= 1e-6 * casa.abs().max(1e-3),
                "{name} {key}: {ours} against CASA's {casa}"
            );
        }
    }
}

/// The pixels and mask of a saved output image.
fn read(path: &Path) -> (Vec<f64>, Vec<bool>) {
    let image = PagedImage::<f32>::open(path).unwrap();
    let data = image.get().unwrap();
    let mask = image
        .default_mask()
        .unwrap()
        .map_or_else(|| vec![true; data.len()], |mask| flat_mask(&mask));
    (flat(&data), mask)
}

#[test]
fn impbcor_immath_and_feather_match_casa() {
    let (cases, casa, images) = fixtures();
    let out = tempfile::tempdir().unwrap();
    for name in ["impbcor_divide", "impbcor_multiply"] {
        let case = case(&cases, name);
        let outfile = out.path().join(name);
        casa_images::impbcor(&ImpbcorRequest {
            imagename: images.paths[case["input"].as_str().unwrap()].clone(),
            pbimage: images.paths[case["pb"].as_str().unwrap()].clone(),
            outfile: outfile.clone(),
            mode: case["mode"].as_str().unwrap().into(),
            cutoff: case["cutoff"].as_f64().unwrap(),
            box_selection: None,
            region: None,
            chans: None,
            stokes: None,
            mask: None,
            stretch: false,
            overwrite: false,
        })
        .unwrap();
        let (data, mask) = read(&outfile);
        let (worst, masks) = compare(name, &data, &mask, &casa["cases"][name]);
        assert!(worst <= 2e-7 && masks == 0, "{name}: {worst:e}, {masks}");
    }
    let case = case(&cases, "immath_scale");
    let outfile = out.path().join("immath_scale");
    casa_images::immath(&ImmathRequest {
        imagename: vec![images.paths[case["input"].as_str().unwrap()].clone()],
        outfile: outfile.clone(),
        expr: case["expr"].as_str().unwrap().into(),
        overwrite: false,
    })
    .unwrap();
    let (data, mask) = read(&outfile);
    let (worst, masks) = compare("immath_scale", &data, &mask, &casa["cases"]["immath_scale"]);
    assert!(worst <= 2e-7 && masks == 0, "immath: {worst:e}, {masks}");
    let feathered = out.path().join("feather");
    casa_images::feather(&FeatherRequest {
        imagename: feathered.clone(),
        highres: images.paths["int_cube"].clone(),
        lowres: images.paths["sd_low"].clone(),
        sdfactor: 1.0,
        overwrite: false,
    })
    .unwrap();
    let (data, mask) = read(&feathered);
    let (worst, masks) = compare("feather", &data, &mask, &casa["cases"]["feather_defaults"]);
    assert!(worst <= 1e-5 && masks == 0, "feather: {worst:e}, {masks}");
}
