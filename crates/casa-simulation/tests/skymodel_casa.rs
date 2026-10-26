// SPDX-License-Identifier: LGPL-3.0-or-later
//! `casa_simulation::simutil::skymodel` against CASA 6.7.6.
//!
//! `tests/data/skymodel_inputs.tar.gz` holds the images (CASA images and
//! FITS files) and component lists CASA made, and
//! `tests/data/skymodel_casa.json.gz` what CASA made of them
//! (`tools/simulation/skymodel_oracle.py`): `modifymodel`'s returns and
//! images, `flatimage`'s images, images after `ia.modify`, and
//! `simobserve`'s component-only model. casa-rs reads the same inputs and
//! is compared with CASA: strings exactly, numbers to 1e-12, the
//! coordinate-system record's values to 1e-12 and its strings exactly, and
//! pixels to 1e-6 of the image's peak.
//!
//! The documented departures (module docs of
//! `casa_simulation::simutil::skymodel`) are asserted where they apply:
//! `incell` as two cells, a spectral axis first, unlabelled Stokes planes,
//! an unset `compwidth`, and a component spectrum in another frame.
//!
//! With `CASA_RS_SKYMODEL_OUT` set, the images casa-rs writes are kept
//! there (`<kind>/<case>`), for `skymodel_oracle.py --check` to read in
//! CASA. With `CASA_RS_SKYMODEL_FIXTURE` set, the tests compare an evidence
//! record instead (`skymodel_oracle.py --evidence`: casatestdata's models).

use std::path::{Path, PathBuf};

use casa_components::ComponentList;
use casa_images::PagedImage;
use casa_simulation::simutil::python_float;
use casa_simulation::simutil::skymodel::{
    ComponentModel, ModelOverrides, ModelSummary, ModelValue, SkyModelError, add_components,
    flat_image, modify_model,
};
use casa_types::quanta::Quantity;
use casa_types::{ArrayValue, RecordValue, ScalarValue};
use serde_json::{Map, Value, json};

fn data_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data")
}

/// CASA's record, and its inputs.
struct Fixture {
    record: Value,
    /// The directory holding `inputs/`.
    inputs: PathBuf,
    /// The checked-in inputs, unpacked.
    _unpacked: Option<tempfile::TempDir>,
    outputs: tempfile::TempDir,
}

impl Fixture {
    /// The checked-in fixture, or with `CASA_RS_SKYMODEL_FIXTURE` an
    /// evidence record (`skymodel_oracle.py --evidence`) with its inputs as
    /// a directory.
    fn load() -> Self {
        let directory =
            std::env::var_os("CASA_RS_SKYMODEL_FIXTURE").map_or_else(data_dir, PathBuf::from);
        let file = std::fs::File::open(directory.join("skymodel_casa.json.gz"))
            .expect("the CASA fixture is checked in");
        let record = serde_json::from_reader(flate2::read::GzDecoder::new(file))
            .expect("the CASA fixture is JSON");
        let archive = directory.join("skymodel_inputs.tar.gz");
        let (inputs, unpacked) = if archive.exists() {
            let unpacked = tempfile::tempdir().expect("a scratch directory");
            let archive = std::fs::File::open(archive).expect("the CASA inputs are readable");
            tar::Archive::new(flate2::read::GzDecoder::new(archive))
                .unpack(unpacked.path())
                .expect("the CASA inputs unpack");
            (unpacked.path().to_path_buf(), Some(unpacked))
        } else {
            (directory, None)
        };
        Self {
            record,
            inputs,
            _unpacked: unpacked,
            outputs: tempfile::tempdir().expect("a scratch directory"),
        }
    }

    fn input(&self, name: &str) -> PathBuf {
        self.inputs.join("inputs").join(name)
    }

    fn cases(&self, kind: &str) -> &[Value] {
        self.record[kind].as_array().expect(kind)
    }

    /// Where casa-rs writes a case's image: `CASA_RS_SKYMODEL_OUT` when it
    /// is set, else a scratch directory.
    fn output(&self, kind: &str, name: &str) -> PathBuf {
        let root = std::env::var_os("CASA_RS_SKYMODEL_OUT")
            .map_or_else(|| self.outputs.path().to_path_buf(), PathBuf::from);
        let directory = root.join(kind);
        std::fs::create_dir_all(&directory).expect("an output directory");
        directory.join(name)
    }
}

fn text(value: &Value) -> &str {
    value
        .as_str()
        .unwrap_or_else(|| panic!("a string, not {value}"))
}

/// A parameter CASA takes as a string or a list, as a list (empty for
/// `""`).
fn one_or_many(value: &Value) -> Vec<String> {
    match value {
        Value::String(text) if text.is_empty() => Vec::new(),
        Value::String(text) => vec![text.clone()],
        Value::Array(items) => items.iter().map(|item| text(item).to_string()).collect(),
        other => panic!("a string or a list, not {other}"),
    }
}

fn close(ours: f64, casa: f64) -> bool {
    ours == casa || (ours - casa).abs() <= 1e-12 * casa.abs().max(ours.abs())
}

/// A quantity as CASA's record of it.
fn quantity_json(value: &Quantity) -> Value {
    json!({"unit": value.unit().name(), "value": value.value()})
}

fn model_value_json(value: &ModelValue) -> Value {
    match value {
        ModelValue::Given(text) => json!(text),
        ModelValue::Model(value) => quantity_json(value),
    }
}

fn summary_json(summary: &ModelSummary) -> Value {
    json!([
        summary.refdir,
        summary.cell.iter().map(quantity_json).collect::<Vec<_>>(),
        summary.size.iter().map(quantity_json).collect::<Vec<_>>(),
        summary.nchan,
        model_value_json(&summary.specrefval),
        summary.specrefpix,
        model_value_json(&summary.width),
        summary.stokes,
    ])
}

/// Where `ours` differs from `casa`: strings, booleans and lengths exactly,
/// numbers to 1e-12.
fn differences(ours: &Value, casa: &Value, at: &str, found: &mut Vec<String>) {
    match (ours, casa) {
        (Value::Number(a), Value::Number(b)) => {
            let (a, b) = (a.as_f64().expect("a number"), b.as_f64().expect("a number"));
            if !close(a, b) {
                found.push(format!("{at}: {a:?}, CASA {b:?}"));
            }
        }
        (Value::Array(a), Value::Array(b)) if a.len() == b.len() => {
            for (index, (a, b)) in a.iter().zip(b).enumerate() {
                differences(a, b, &format!("{at}[{index}]"), found);
            }
        }
        (Value::Object(a), Value::Object(b)) => {
            for (key, b) in b {
                match a.get(key) {
                    Some(a) => differences(a, b, &format!("{at}.{key}"), found),
                    None => found.push(format!("{at}.{key}: missing")),
                }
            }
        }
        (a, b) if a == b => {}
        (a, b) => found.push(format!("{at}: {a}, CASA {b}")),
    }
}

fn scalar_json(value: &ScalarValue) -> Value {
    match value {
        ScalarValue::Bool(v) => json!(v),
        ScalarValue::UInt8(v) => json!(v),
        ScalarValue::UInt16(v) => json!(v),
        ScalarValue::UInt32(v) => json!(v),
        ScalarValue::Int16(v) => json!(v),
        ScalarValue::Int32(v) => json!(v),
        ScalarValue::Int64(v) => json!(v),
        ScalarValue::Float32(v) => json!(v),
        ScalarValue::Float64(v) => json!(v),
        ScalarValue::String(v) => json!(v),
        ScalarValue::Complex32(_) | ScalarValue::Complex64(_) => json!(null),
    }
}

/// An array as numpy's `tolist()` writes it: nested by the first index.
fn nested<T: Clone>(array: &ndarray::ArrayD<T>, leaf: &dyn Fn(&T) -> Value) -> Value {
    if array.ndim() <= 1 {
        return Value::Array(array.iter().map(leaf).collect());
    }
    Value::Array(
        array
            .outer_iter()
            .map(|row| nested(&row.to_owned(), leaf))
            .collect(),
    )
}

fn array_json(value: &ArrayValue) -> Value {
    match value {
        ArrayValue::Bool(a) => nested(a, &|v| json!(v)),
        ArrayValue::Int32(a) => nested(a, &|v| json!(v)),
        ArrayValue::Int64(a) => nested(a, &|v| json!(v)),
        ArrayValue::Float32(a) => nested(a, &|v| json!(v)),
        ArrayValue::Float64(a) => nested(a, &|v| json!(v)),
        ArrayValue::String(a) => nested(a, &|v| json!(v)),
        _ => json!(null),
    }
}

fn record_json(record: &RecordValue) -> Value {
    let mut object = Map::new();
    for field in record.fields() {
        let value = match &field.value {
            casa_types::Value::Scalar(value) => scalar_json(value),
            casa_types::Value::Array(value) => array_json(value),
            casa_types::Value::Record(value) => record_json(value),
            casa_types::Value::TableRef(path) => json!(path),
        };
        object.insert(field.name.clone(), value);
    }
    Value::Object(object)
}

/// The parts of CASA's coordinate record compared: each coordinate's
/// values, units, frames, projection and Stokes, the rest frequency, and the
/// observation information.
fn compared_record(casa: &Value) -> Value {
    let mut kept = Map::new();
    for (key, value) in casa.as_object().expect("a record") {
        let fields: &[&str] = if key.starts_with("direction") {
            &[
                "system",
                "projection",
                "projection_parameters",
                "crval",
                "crpix",
                "cdelt",
                "pc",
                "units",
            ]
        } else if key.starts_with("stokes") {
            &["stokes", "crpix", "cdelt"]
        } else if key.starts_with("spectral") {
            &["system", "restfreq", "unit", "wcs"]
        } else if key == "telescope" || key == "observer" {
            kept.insert(key.clone(), value.clone());
            continue;
        } else if key == "obsdate" {
            kept.insert(key.clone(), json!({"m0": {"value": value["m0"]["value"]}}));
            continue;
        } else {
            continue;
        };
        let mut coordinate = Map::new();
        for field in fields {
            let mut value = value[*field].clone();
            if *field == "wcs" {
                value = json!({"crval": value["crval"], "cdelt": value["cdelt"], "crpix": value["crpix"]});
            }
            coordinate.insert((*field).to_string(), value);
        }
        kept.insert(key.clone(), Value::Object(coordinate));
    }
    Value::Object(kept)
}

/// Where the image casa-rs wrote at `path` differs from CASA's `casa`, with
/// pixels to `tolerance` of the peak.
fn image_differences(path: &Path, casa: &Value, tolerance: f64) -> Vec<String> {
    let image = PagedImage::<f32>::open(path).expect("casa-rs wrote an image");
    let mut found = Vec::new();
    differences(&json!(image.shape()), &casa["shape"], "shape", &mut found);
    differences(&json!(image.units()), &casa["unit"], "unit", &mut found);
    let ours = record_json(&image.coordinates().to_record());
    differences(&ours, &compared_record(&casa["csys"]), "csys", &mut found);
    let pixels = image.get().expect("pixels");
    let theirs: Vec<f64> = casa["pixels"]
        .as_array()
        .expect("pixels")
        .iter()
        .map(|value| value.as_f64().unwrap_or(f64::NAN))
        .collect();
    let peak = theirs
        .iter()
        .fold(0.0_f64, |peak, value| peak.max(value.abs()));
    let worst = pixels
        .t()
        .iter()
        .zip(&theirs)
        .map(|(ours, theirs)| (f64::from(*ours) - theirs).abs())
        .fold(0.0_f64, f64::max);
    if pixels.len() != theirs.len() || worst > tolerance * peak.max(f64::MIN_POSITIVE) {
        found.push(format!("pixels differ by {worst:.3e} (peak {peak:.3e})"));
    }
    found
}

fn assert_no_differences(what: &str, found: &[String]) {
    assert!(found.is_empty(), "{what}:\n  {}", found.join("\n  "));
}

fn overrides(params: &Value) -> ModelOverrides {
    ModelOverrides {
        inbright: text(&params["inbright"]).to_string(),
        indirection: one_or_many(&params["indirection"]),
        incell: one_or_many(&params["incell"]),
        incenter: text(&params["incenter"]).to_string(),
        inwidth: text(&params["inwidth"]).to_string(),
    }
}

#[test]
fn modifymodel_writes_and_returns_what_casa_does() {
    let fixture = Fixture::load();
    for case in fixture.cases("modify") {
        let name = text(&case["name"]);
        let output = fixture.output("modify", name);
        let result = modify_model(
            &fixture.input(text(&case["input"])),
            &output,
            &overrides(&case["params"]),
        );
        match name {
            // Departures: two cells (CASA raises TypeError), and a spectral
            // axis first (CASA raises "internal error").
            "dsf_incell_list" => {
                let summary = result.expect("casa-rs takes two cells");
                assert_eq!(
                    [summary.cell[0].value(), summary.cell[1].value()],
                    [0.1, 0.2],
                    "{name}"
                );
                continue;
            }
            "freq_first" => {
                check_spectral_axis_first(&fixture, &output, result);
                continue;
            }
            _ => {}
        }
        if case["result"] == json!(false) {
            assert!(
                matches!(result, Err(SkyModelError::Indirections(_))),
                "{name}: {result:?}"
            );
            continue;
        }
        if let Some(error) = case.get("error") {
            let message = result.expect_err(name).to_string();
            assert_eq!(message, text(&error["message"]), "{name}");
            continue;
        }
        let summary = result.unwrap_or_else(|error| panic!("{name}: {error}"));
        let mut found = Vec::new();
        differences(
            &summary_json(&summary),
            &case["result"],
            "result",
            &mut found,
        );
        let mut casa_image = case["image"].clone();
        // Departure: CASA labels the planes by their count, as
        // `ia.fromshape` does; casa-rs labels three unlabelled planes IQV, as
        // `modifymodel` reports, and keeps a model's own labels.
        let relabelled = match name {
            "dir_freq_linear" => Some((json!(["I", "Q", "U"]), json!(["I", "Q", "V"]))),
            "iv" => Some((json!(["I", "Q"]), json!(["I", "V"]))),
            _ => None,
        };
        if let Some((casas, ours)) = relabelled {
            assert_eq!(casa_image["csys"]["stokes1"]["stokes"], casas, "{name}");
            casa_image["csys"]["stokes1"]["stokes"] = ours;
        }
        found.extend(image_differences(&output, &casa_image, 1e-6));
        assert_no_differences(name, &found);
    }
}

/// `freq_first.im` (axes spectral, direction, direction, Stokes) comes out
/// as the others do, its pixels moved to `[x, y, Stokes, channel]`.
fn check_spectral_axis_first(
    fixture: &Fixture,
    output: &Path,
    result: Result<ModelSummary, SkyModelError>,
) {
    let summary = result.expect("casa-rs permutes the axes");
    assert_eq!(summary.nchan, 3);
    let input = PagedImage::<f32>::open(fixture.input("freq_first.im"))
        .expect("the input")
        .get()
        .expect("pixels");
    let written = PagedImage::<f32>::open(output)
        .expect("the output")
        .get()
        .expect("pixels");
    assert_eq!(written.shape(), [16, 12, 1, 3]);
    for ((x, y, s, c), value) in written
        .view()
        .into_dimensionality::<ndarray::Ix4>()
        .expect("four axes")
        .indexed_iter()
    {
        let expected = input[[c, x, y, s]];
        let expected = if expected.is_nan() { 0.0 } else { expected };
        assert_eq!(*value, expected, "pixel {x} {y} {s} {c}");
    }
}

#[test]
fn flatimage_collapses_as_casa_does() {
    let fixture = Fixture::load();
    for case in fixture.cases("flat") {
        let name = text(&case["name"]);
        // The flat image is made from casa-rs's own modifymodel output,
        // written under the name the oracle's check reads it by.
        let model = fixture
            .cases("modify")
            .iter()
            .find(|model| model["name"] == case["name"])
            .unwrap_or_else(|| panic!("{name}: no modifymodel case"));
        let complist = case["complist"].as_str();
        let output = match complist {
            Some(list) => format!("{name}+{list}"),
            None => name.to_string(),
        };
        let output = fixture.output("flat", &output);
        modify_model(
            &fixture.input(text(&model["input"])),
            &output,
            &overrides(&model["params"]),
        )
        .unwrap_or_else(|error| panic!("{name}: {error}"));
        let complist = complist.map(|list| fixture.input(list));
        let flat = flat_image(&output, complist.as_deref())
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        let what = format!("{name} with {:?}", case["complist"]);
        assert_no_differences(&what, &image_differences(&flat, &case["image"], 1e-6));
    }
}

#[test]
fn components_are_added_to_images_as_casa_adds_them() {
    let fixture = Fixture::load();
    for case in fixture.cases("project") {
        let name = text(&case["name"]);
        let target = fixture.output("project", name);
        copy_dir(&fixture.input(text(&case["input"])), &target);
        let list = ComponentList::read_table(fixture.input(text(&case["complist"])))
            .expect("the component list");
        add_components(&target, &list).unwrap_or_else(|error| panic!("{name}: {error}"));
        // Departure: where the image's spectral frame is not the
        // components' (a TOPO spectrum on an LSRK image, LSRK spectra on a
        // TOPO cube), CASA converts the reference frequency through the
        // image's default measures frame; casa-rs takes it as it is. The
        // spectral indices turn the shift into flux differences of about
        // 1e-4.
        let tolerance = if ["dsf_topo", "dfs_cube_spectral"].contains(&name) {
            2e-4
        } else {
            1e-6
        };
        assert_no_differences(name, &image_differences(&target, &case["image"], tolerance));
    }
}

fn copy_dir(from: &Path, to: &Path) {
    if to.exists() {
        std::fs::remove_dir_all(to).expect("an old copy removed");
    }
    std::fs::create_dir_all(to).expect("a copy");
    for entry in std::fs::read_dir(from).expect("an image directory") {
        let entry = entry.expect("an entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("a type").is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).expect("a file copied");
        }
    }
}

#[test]
fn the_component_only_model_is_casas() {
    let fixture = Fixture::load();
    for case in fixture.cases("component_model") {
        let name = text(&case["name"]);
        let list = ComponentList::read_table(fixture.input(text(&case["complist"])))
            .expect("the component list");
        let nchan = case["comp_nchan"].as_u64().expect("comp_nchan") as usize;
        let model = ComponentModel::from_list(&list, text(&case["compwidth"]), nchan)
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        let parameters = &case["parameters"];
        let cell_text = format!(
            "{}arcsec",
            python_float(case["psfsize"].as_f64().expect("psfsize") / 20.0)
        );
        let cell = Quantity::read(&cell_text).expect("a cell");
        let ours = json!({
            "model_refdir": model.refdir,
            "model_specrefval": quantity_json(&model.specrefval),
            "model_width": quantity_json(&model.width),
            "model_nchan": model.nchan,
            "model_specrefpix": model.specrefpix,
            "model_stokes": model.stokes,
            "model_size": model.size,
            "model_cell": [quantity_json(&cell), quantity_json(&cell)],
            "modimsize": model.image_size(&cell).expect("a size"),
        });
        let mut expected = parameters.clone();
        expected
            .as_object_mut()
            .expect("a record")
            .remove("compdirs");
        let mut found = Vec::new();
        differences(&ours, &expected, "parameters", &mut found);
        let path = fixture.output("component_model", name);
        let flat = model
            .write(&list, &[cell.clone(), cell], &path)
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        let mut casa_image = case["image"].clone();
        if name == "width_unset" {
            // Departure: CASA's increment for an unset compwidth is the
            // model's RA in radians, read as hertz; casa-rs uses 2 GHz.
            let casa_increment = casa_image["csys"]["spectral2"]["wcs"]["cdelt"]
                .as_f64()
                .expect("an increment");
            assert!((casa_increment - 4.976).abs() < 1e-3, "{casa_increment}");
            casa_image["csys"]["spectral2"]["wcs"]["cdelt"] = json!(2.0e9);
        }
        found.extend(image_differences(&path, &casa_image, 1e-6));
        found.extend(
            image_differences(&flat, &case["flat"], 1e-6)
                .into_iter()
                .map(|difference| format!("flat {difference}")),
        );
        assert_no_differences(name, &found);
    }
}
