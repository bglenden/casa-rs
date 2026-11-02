// SPDX-License-Identifier: LGPL-3.0-or-later
//! `casa_simulation::simutil::analysis` against CASA 6.7.6.
//!
//! `tests/data/simanalyze_analysis_inputs.tar.gz` holds the images CASA
//! made, and each case's convolved model as CASA made it;
//! `tests/data/simanalyze_analysis_casa.json.gz` holds what CASA made of
//! them (`tools/simulation/simanalyze_analysis_oracle.py`). casa-rs reads the
//! same inputs and is compared with CASA:
//! - `convimage`'s regrid and convolution, pixel by pixel to 1e-6 of the
//!   image's peak, with masks, units and beams the same;
//! - the difference and fidelity of CASA's own convolved model, which the
//!   same single-precision arithmetic makes the same, except where the
//!   fidelity's floor binds: there CASA's is the floor it wrote with `%f`,
//!   and casa-rs's the floor itself;
//! - the difference and fidelity of casa-rs's convolved model, by normalized
//!   rms;
//! - `statim`, `cellsize` and `flatimage`.
//!
//! With `CASA_RS_SIMANALYZE_FIXTURE` set, the tests compare an evidence
//! record instead (`simanalyze_analysis_oracle.py --evidence`: a
//! `simanalyze` project's flats).

use std::path::{Path, PathBuf};

use casa_coordinates::CoordinateModel;
use casa_images::{ImageInterface, PagedImage};
use casa_simulation::simutil::analysis::{AnalysisError, cell_size, convimage, fidelity, statim};
use casa_simulation::simutil::skymodel::flat_image;
use serde_json::Value;

const ARCSEC: f64 = std::f64::consts::PI / (180.0 * 3600.0);

/// CASA's record, and its inputs.
struct Fixture {
    record: Value,
    inputs: PathBuf,
    _unpacked: Option<tempfile::TempDir>,
    outputs: tempfile::TempDir,
}

impl Fixture {
    /// The checked-in fixture, or with `CASA_RS_SIMANALYZE_FIXTURE` an
    /// evidence record with its inputs as a directory.
    fn load() -> Self {
        let data = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data");
        let directory = std::env::var_os("CASA_RS_SIMANALYZE_FIXTURE").map_or(data, PathBuf::from);
        let file = std::fs::File::open(directory.join("simanalyze_analysis_casa.json.gz"))
            .expect("the CASA fixture is checked in");
        let record: Value = serde_json::from_reader(flate2::read::GzDecoder::new(file))
            .expect("the CASA fixture is JSON");
        assert_eq!(record["casa_version"], "6.7.6-14");
        let archive = directory.join("simanalyze_analysis_inputs.tar.gz");
        let (inputs, unpacked) = if archive.exists() {
            let unpacked = tempfile::tempdir().expect("a scratch directory");
            let archive = std::fs::File::open(archive).expect("the CASA inputs are readable");
            tar::Archive::new(flate2::read::GzDecoder::new(archive))
                .unpack(unpacked.path())
                .expect("the CASA inputs unpack");
            (unpacked.path().join("inputs"), Some(unpacked))
        } else {
            (directory.join("inputs"), None)
        };
        Self {
            record,
            inputs,
            _unpacked: unpacked,
            outputs: tempfile::tempdir().expect("a scratch directory"),
        }
    }

    fn input(&self, name: &str) -> PathBuf {
        self.inputs.join(name)
    }

    /// A copy of the input `name` in a new output directory `case`.
    fn copy(&self, case: &str, name: &str) -> PathBuf {
        let directory = self.outputs.path().join(case);
        std::fs::create_dir_all(&directory).unwrap();
        let copy = directory.join(name);
        copy_tree(&self.input(name), &copy);
        copy
    }
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn numbers(value: &Value) -> Vec<f64> {
    value
        .as_array()
        .expect("an array")
        .iter()
        .map(|v| v.as_f64().expect("a number"))
        .collect()
}

/// An image's pixels and mask in Fortran order, with what CASA records of it.
struct Image {
    shape: Vec<usize>,
    pixels: Vec<f64>,
    mask: Vec<bool>,
    unit: String,
    beam: Option<[f64; 3]>,
    stokes: Vec<String>,
}

impl Image {
    fn read(path: &Path) -> Self {
        let image = PagedImage::<f32>::open(path).expect("open an output");
        let data = image.get().unwrap();
        let fortran = |array: &ndarray::ArrayD<f32>| -> Vec<f64> {
            array.t().iter().map(|v| f64::from(*v)).collect::<Vec<_>>()
        };
        let pixels = fortran(&data);
        let mask = image.default_mask().unwrap().map_or_else(
            || vec![true; pixels.len()],
            |mask| mask.t().iter().copied().collect(),
        );
        let coordinates = image.coordinates();
        let stokes = (0..coordinates.n_coordinates())
            .find_map(|index| match coordinates.coordinate(index) {
                CoordinateModel::Stokes(stokes) => {
                    Some(stokes.stokes().iter().map(|s| format!("{s:?}")).collect())
                }
                _ => None,
            })
            .unwrap_or_default();
        let beam = image.image_info().unwrap().beam_set.single_beam().map(|b| {
            [
                b.major / ARCSEC,
                b.minor / ARCSEC,
                b.position_angle.to_degrees(),
            ]
        });
        Self {
            shape: image.shape().to_vec(),
            pixels,
            mask,
            unit: image.units().to_string(),
            beam,
            stokes,
        }
    }

    /// CASA's record of an image (`image_analysis_oracle.describe`).
    fn casa(record: &Value) -> Self {
        let pixels = numbers(&record["pixels"]);
        let mask = record["mask"].as_array().map_or_else(
            || vec![true; pixels.len()],
            |mask| mask.iter().map(|v| v.as_bool().unwrap()).collect(),
        );
        Self {
            shape: record["shape"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_u64().unwrap() as usize)
                .collect(),
            pixels,
            mask,
            unit: record["unit"].as_str().unwrap().to_string(),
            beam: (!record["beam"].is_null()).then(|| {
                let beam = &record["beam"];
                ["major_arcsec", "minor_arcsec", "pa_deg"].map(|key| beam[key].as_f64().unwrap())
            }),
            stokes: record["stokes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap().to_string())
                .collect(),
        }
    }

    fn peak(&self) -> f64 {
        self.pixels.iter().fold(0.0, |peak, v| peak.max(v.abs()))
    }
}

/// Assert that `ours` has CASA's shape, mask, unit (unless `unit` says what
/// casa-rs writes instead), beam and Stokes axis, and that its pixels are
/// within `tolerance` of CASA's peak; return the largest difference over the
/// peak.
fn assert_matches(
    name: &str,
    ours: &Image,
    casa: &Image,
    unit: Option<&str>,
    tolerance: f64,
) -> f64 {
    assert_eq!(ours.shape, casa.shape, "{name}: shape");
    assert_eq!(ours.unit, unit.unwrap_or(&casa.unit), "{name}: unit");
    assert_eq!(ours.stokes, casa.stokes, "{name}: Stokes");
    match (ours.beam, casa.beam) {
        (Some(a), Some(b)) => {
            for (a, b) in a.iter().zip(b) {
                assert!(
                    (a - b).abs() <= 1e-6 * b.abs().max(1.0),
                    "{name}: beam {a} against CASA's {b}"
                );
            }
        }
        (a, b) => assert_eq!(a.is_some(), b.is_some(), "{name}: a beam"),
    }
    let differences = ours
        .mask
        .iter()
        .zip(&casa.mask)
        .filter(|(a, b)| a != b)
        .count();
    assert_eq!(
        differences, 0,
        "{name}: {differences} pixels masked differently"
    );
    assert!(
        ours.pixels.iter().all(|v| v.is_finite()),
        "{name}: values not finite"
    );
    let peak = casa.peak();
    let worst = ours
        .pixels
        .iter()
        .zip(&casa.pixels)
        .map(|(a, b)| (a - b).abs())
        .fold(0.0, f64::max)
        / peak;
    assert!(
        worst <= tolerance,
        "{name}: {worst:e} of the peak {peak} from CASA"
    );
    worst
}

/// The rms of the difference over CASA's unmasked pixels that `chosen`
/// keeps, over CASA's rms there.
fn normalized_rms(ours: &Image, casa: &Image, chosen: &[bool]) -> f64 {
    let (mut difference, mut power) = (0.0, 0.0);
    for (k, (a, b)) in ours.pixels.iter().zip(&casa.pixels).enumerate() {
        if casa.mask[k] && chosen[k] {
            difference += (a - b) * (a - b);
            power += b * b;
        }
    }
    (difference / power).sqrt()
}

fn convimage_cases(record: &Value) -> impl Iterator<Item = (&String, &Value)> {
    record["convimage"].as_object().unwrap().iter()
}

#[test]
fn convimage_regrids_and_convolves_as_casa_does() {
    let fixture = Fixture::load();
    for (case, casa) in convimage_cases(&fixture.record) {
        let model = fixture.copy(case, casa["model"].as_str().unwrap());
        let image = fixture.input(casa["image"].as_str().unwrap());
        let result = convimage(&model, &image);
        if let Some(error) = casa["error"].as_str() {
            let Err(AnalysisError::Analysis(ours)) = result else {
                panic!("{case}: casa-rs accepted what CASA refused ({error}): {result:?}");
            };
            if error.starts_with("Exception: ") {
                assert_eq!(format!("Exception: {ours}"), error, "{case}");
            } else {
                assert!(ours.contains("one restoring beam"), "{case}: {ours}");
            }
            continue;
        }
        let result = result.unwrap_or_else(|error| panic!("{case}: {error}"));
        let after = Image::read(&model);
        let casa_after = &casa["model_after"];
        assert_eq!(after.unit, casa_after["unit"], "{case}: the model's unit");
        assert_eq!(
            after.beam.map(|b| b.map(|v| (v * 1e6).round())),
            Some(
                ["major_arcsec", "minor_arcsec", "pa_deg"].map(|key| (casa_after["beam"][key]
                    .as_f64()
                    .unwrap()
                    * 1e6)
                    .round())
            ),
            "{case}: the model is given the image's beam"
        );
        let regrid = assert_matches(
            &format!("{case} regrid"),
            &Image::read(&result.regrid),
            &Image::casa(&casa["regrid"]),
            None,
            1e-6,
        );
        let convolved = assert_matches(
            &format!("{case} convolved"),
            &Image::read(&result.convolved),
            &Image::casa(&casa["convolved"]),
            None,
            1e-6,
        );
        eprintln!("{case}: regrid {regrid:e}, convolved {convolved:e} of the peak");
    }
}

/// CASA's floor as `simanalyze` wrote it into the expression.
fn written_floor(casa: &Value) -> f64 {
    casa["floor_written"].as_str().unwrap().parse().unwrap()
}

#[test]
fn difference_and_fidelity_of_casas_convolved_model_match_casa() {
    let fixture = Fixture::load();
    for (case, casa) in convimage_cases(&fixture.record) {
        if casa["error"].is_string() {
            continue;
        }
        let convolved = fixture.input(&format!("{case}.casa.conv"));
        let image = fixture.input(casa["image"].as_str().unwrap());
        let root = fixture.outputs.path().join(format!("{case}.casa"));
        let result = fidelity(&convolved, &image, &root).unwrap();
        let theirs = casa["medabsdevmed"].as_f64().unwrap();
        assert!(
            (result.medabsdevmed - theirs).abs() <= 1e-9 * theirs,
            "{case}: medabsdevmed {} against CASA's {theirs}",
            result.medabsdevmed
        );
        let scalar = casa["scalar"].as_f64().unwrap();
        assert!(
            (result.scalar - scalar).abs() <= 1e-9 * scalar,
            "{case}: scalar fidelity {} against CASA's {scalar}",
            result.scalar
        );
        let difference = Image::read(&result.difference);
        assert_matches(
            &format!("{case} difference"),
            &difference,
            &Image::casa(&casa["diff"]),
            None,
            1e-7,
        );

        // Where the floor binds, CASA divides by the floor it wrote and
        // casa-rs by the floor itself; elsewhere both divide by |diff|.
        let ours = Image::read(&result.fidelity);
        let theirs = Image::casa(&casa["fidelity"]);
        let model = Image::read(&convolved);
        let floor = (result.medabsdevmed / 2f64.sqrt()) as f32;
        let written = written_floor(casa) as f32;
        let mut floored = 0;
        for k in 0..ours.pixels.len() {
            let numerator = model.pixels[k].abs() as f32;
            let magnitude = difference.pixels[k].abs() as f32;
            let casa_expected = f64::from(numerator / magnitude.max(written));
            let ours_expected = f64::from(numerator / magnitude.max(floor));
            assert_eq!(
                theirs.pixels[k] as f32, casa_expected as f32,
                "{case}: CASA's fidelity at {k} is not |conv| / max(|diff|, {written})"
            );
            assert_eq!(ours.pixels[k], ours_expected, "{case}: fidelity at {k}");
            floored += usize::from(magnitude < floor.max(written));
        }
        assert_eq!(ours.mask, theirs.mask, "{case}: the fidelity's mask");
        assert_eq!(ours.unit, "", "{case}: the fidelity is a ratio");
        assert_eq!(
            theirs.unit, "Jy/beam",
            "{case}: CASA labels the fidelity Jy/beam"
        );
        eprintln!("{case}: the floor binds at {floored} pixels");
    }
}

#[test]
fn difference_and_fidelity_of_casa_rss_convolved_model_match_casa() {
    let fixture = Fixture::load();
    for (case, casa) in convimage_cases(&fixture.record) {
        if casa["error"].is_string() {
            continue;
        }
        let model = fixture.copy(case, casa["model"].as_str().unwrap());
        let image = fixture.input(casa["image"].as_str().unwrap());
        let convolved = convimage(&model, &image).unwrap();
        let root = fixture.outputs.path().join(case).join("sim");
        let result = fidelity(&convolved.convolved, &image, &root).unwrap();
        let theirs = casa["medabsdevmed"].as_f64().unwrap();
        let relative = (result.medabsdevmed - theirs).abs() / theirs;
        assert!(
            relative <= 1e-5,
            "{case}: medabsdevmed {} against CASA's {theirs}",
            result.medabsdevmed
        );
        let casa_difference = Image::casa(&casa["diff"]);
        let everywhere = vec![true; casa_difference.pixels.len()];
        let difference = normalized_rms(
            &Image::read(&result.difference),
            &casa_difference,
            &everywhere,
        );
        assert!(
            difference <= 1e-6,
            "{case}: the difference's NRMS {difference:e}"
        );
        // The fidelity where neither floor binds; the other test covers the
        // floors.
        let floor = (result.medabsdevmed / 2f64.sqrt()).max(written_floor(casa));
        let unfloored: Vec<bool> = casa_difference
            .pixels
            .iter()
            .map(|v| v.abs() >= floor)
            .collect();
        let fidelity = normalized_rms(
            &Image::read(&result.fidelity),
            &Image::casa(&casa["fidelity"]),
            &unfloored,
        );
        assert!(fidelity <= 1e-5, "{case}: the fidelity's NRMS {fidelity:e}");
        eprintln!(
            "{case}: medabsdevmed {relative:e}, difference {difference:e}, fidelity {fidelity:e} NRMS"
        );
    }
}

#[test]
fn statim_matches_casa() {
    let fixture = Fixture::load();
    for (name, casa) in fixture.record["statim"].as_object().unwrap() {
        let ours = statim(&fixture.input(name));
        if let Some(error) = casa["error"].as_str() {
            let Err(AnalysisError::Analysis(ours)) = ours else {
                panic!("{name}: casa-rs accepted what CASA refused: {ours:?}");
            };
            assert_eq!(format!("Exception: {ours}"), error, "{name}");
            continue;
        }
        let ours = ours.unwrap();
        let scale = casa_statim_scale(&fixture.input(name));
        for (key, value) in [("min", ours.min), ("max", ours.max), ("rms", ours.rms)] {
            let value = value * scale;
            let theirs = casa[key].as_f64().unwrap();
            assert!(
                (value - theirs).abs() <= 1e-6 * theirs.abs().max(1e-30),
                "{name} {key}: {value} against CASA's {theirs}"
            );
        }
        assert_eq!(ours.unit, casa["unit"], "{name}: unit");
    }
}

/// The factor CASA's `statim` scales an image's statistics by and casa-rs
/// does not: the pixel area in square arcseconds for a Jy/beam image without
/// a restoring beam, and 1 otherwise (`docs/CASA (C++) bugs.md`).
fn casa_statim_scale(path: &Path) -> f64 {
    let image = PagedImage::<f32>::open(path).unwrap();
    if image.units() != "Jy/beam" || !image.image_info().unwrap().beam_set.is_empty() {
        return 1.0;
    }
    let [x, y] = cell_size(image.coordinates()).unwrap();
    x * y / (ARCSEC * ARCSEC)
}

#[test]
fn cellsize_matches_casa() {
    let fixture = Fixture::load();
    for (name, casa) in fixture.record["cellsize"].as_object().unwrap() {
        let image = PagedImage::<f32>::open(fixture.input(name)).unwrap();
        let ours = cell_size(image.coordinates());
        if let Some(error) = casa["error"].as_str() {
            let Err(AnalysisError::Analysis(ours)) = ours else {
                panic!("{name}: casa-rs accepted what CASA refused: {ours:?}");
            };
            assert_eq!(format!("Exception: {ours}"), error, "{name}");
            continue;
        }
        let ours = ours.unwrap();
        assert_eq!(casa["units"], serde_json::json!(["rad", "rad"]), "{name}");
        for (a, b) in ours.iter().zip(numbers(&casa["cell"])) {
            assert!(
                (a - b).abs() <= 1e-15 * b,
                "{name}: cell {a} against CASA's {b}"
            );
        }
    }
}

#[test]
fn flatimage_of_a_clean_image_matches_casa() {
    let fixture = Fixture::load();
    for (name, casa) in fixture.record["flatimage"].as_object().unwrap() {
        let copy = fixture.copy("flat", name);
        let flat = flat_image(&copy, None).unwrap();
        assert_matches(
            name,
            &Image::read(&flat),
            &Image::casa(&casa["flat"]),
            None,
            1e-6,
        );
    }
}
