// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's signature and the public native task envelope, including refusals.
use serde_json::{Value, json};
use std::path::{Path, PathBuf};

fn fixture(root: &Path) -> PathBuf {
    let archive = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../libs/imaging/casa-imaging/tests/data/tsdimaging_task_inputs.tar.gz");
    let output = std::process::Command::new("tar")
        .args(["-xzf"])
        .arg(archive)
        .arg("-C")
        .arg(root)
        .status()
        .unwrap();
    assert!(output.success());
    root.join("map.ms")
}
fn invoke(root: &Path, values: Value) -> (bool, Value) {
    let request = root.join("request.json");
    std::fs::write(
        &request,
        serde_json::to_vec(&json!({"surface":"tsdimaging", "values":values})).unwrap(),
    )
    .unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_tsdimaging"))
        .arg("--json-run")
        .arg(request)
        .output()
        .unwrap();
    let reply = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|_| panic!("{}", String::from_utf8_lossy(&output.stderr)));
    (output.status.success(), reply)
}
fn parameters(input: &Path, output: &Path) -> Value {
    json!({ "infiles":input, "outfile":output, "imsize":[40,40], "cell":"3arcsec",
        "phasecenter":"J2000 270deg -23deg", "gridfunction":"SF", "convsupport":6 })
}

#[test]
fn surface_preserves_all_33_casa_67614_parameters_in_order_and_type() {
    let oracle: Value =
        serde_json::from_str(include_str!("data/tsdimaging_defaults_casa.json")).unwrap();
    assert_eq!(oracle["casa_version"], "6.7.6.14");
    let bundle = casa_contracts::builtin_surface_bundle("tsdimaging").unwrap();
    let mut bindings: Vec<_> = bundle.surface.bindings().iter().collect();
    bindings.sort_by_key(|binding| binding.order);
    assert_eq!(bindings.len(), 33);
    for (binding, expected) in bindings
        .into_iter()
        .zip(oracle["parameters"].as_array().unwrap())
    {
        assert_eq!(binding.name, expected["name"].as_str().unwrap());
        let casa_contracts::DefaultSpec::Literal { value } = &binding.default else {
            panic!("CASA literal default");
        };
        assert_eq!(
            value.to_plain_json(),
            expected["default"],
            "{} default",
            binding.name
        );
    }
    casa_tasks::imaging::single_dish::tsdimaging_task_schema_bundle()
        .validate()
        .unwrap();
}

#[test]
fn public_task_publishes_both_images_and_selects_the_requested_antenna() {
    let root = tempfile::tempdir().unwrap();
    let input = fixture(root.path());
    let mut full = parameters(&input, &root.path().join("all"));
    let (success, reply) = invoke(root.path(), full.clone());
    assert!(success, "{reply}");
    assert_eq!(reply["status"], "succeeded");
    assert_eq!(reply["value"], json!({"kind":"none"}));
    assert_eq!(reply["result"]["artifacts"].as_array().unwrap().len(), 2);
    let original = casa_images::PagedImage::<f32>::open(root.path().join("all.image")).unwrap();
    full["outfile"] = json!(root.path().join("selected"));
    full["antenna"] = json!("T701&&&");
    full["field"] = json!("none");
    let (success, reply) = invoke(root.path(), full);
    assert!(success, "{reply}");
    let selected =
        casa_images::PagedImage::<f32>::open(root.path().join("selected.image")).unwrap();
    assert_eq!(original.get().unwrap(), selected.get().unwrap());
    assert!(casa_images::PagedImage::<f32>::open(root.path().join("selected.weight")).is_ok());
}

#[test]
fn later_modes_refuse_before_publishing_any_products() {
    let root = tempfile::tempdir().unwrap();
    let input = fixture(root.path());
    for (name, value) in [
        ("pointingcolumn", json!("target")),
        ("clipminmax", json!(true)),
        ("outframe", json!("BARY")),
        ("mode", json!("frequency")),
        ("width", json!(2)),
        ("imsize", json!([])),
        ("infiles", json!([&input, &input])),
    ] {
        let out = root.path().join(name);
        let mut values = parameters(&input, &out);
        values[name] = value;
        let (success, reply) = invoke(root.path(), values);
        assert!(!success, "{name}: {reply}");
        assert!(
            reply["message"].as_str().unwrap().contains("938"),
            "{name}: {reply}"
        );
        assert!(!out.with_extension("image").exists());
        assert!(!out.with_extension("weight").exists());
    }
}

#[test]
fn a_valid_moving_field_refuses_explicitly_before_any_output() {
    let root = tempfile::tempdir().unwrap();
    let archive = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../libs/casacore/casa-ms/tests/fixtures/ms_trial/ms_trial_ephemeris.ms.tgz");
    assert!(
        std::process::Command::new("tar")
            .arg("-xzf")
            .arg(archive)
            .arg("-C")
            .arg(root.path())
            .status()
            .unwrap()
            .success()
    );
    let out = root.path().join("moving");
    let mut values = parameters(&root.path().join("ms_trial_ephemeris.ms"), &out);
    values["intent"] = json!("");
    let (success, reply) = invoke(root.path(), values);
    assert!(!success, "{reply}");
    let message = reply["message"].as_str().unwrap();
    assert!(
        message.contains("moving-source") && message.contains("938"),
        "{reply}"
    );
    assert!(!out.with_extension("image").exists());
    assert!(!out.with_extension("weight").exists());
}

fn compare_pixels(image: &casa_images::PagedImage<f32>, reference: &Value) {
    use base64::{Engine, engine::general_purpose::STANDARD};
    let bytes = STANDARD
        .decode(reference["pixels"].as_str().unwrap())
        .unwrap();
    let expected: Vec<_> = bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|v| f32::from_le_bytes(*v))
        .collect();
    let actual = image.get().unwrap();
    assert_eq!(actual.len(), expected.len());
    let (mut error, mut power) = (0., 0.);
    for (&a, &b) in actual
        .as_slice_memory_order()
        .unwrap()
        .iter()
        .zip(&expected)
    {
        error += (f64::from(a) - f64::from(b)).powi(2);
        power += f64::from(b).powi(2);
    }
    assert!((error / power).sqrt() <= 1e-3);
    let expected_mask = STANDARD
        .decode(reference["mask"].as_str().unwrap())
        .unwrap();
    let mask = image.get_mask().unwrap().map_or_else(
        || vec![1; actual.len()],
        |m| {
            m.as_slice_memory_order()
                .unwrap()
                .iter()
                .map(|v| u8::from(*v))
                .collect()
        },
    );
    assert_eq!(mask, expected_mask);
    assert_eq!(image.units(), reference["unit"].as_str().unwrap());
}

fn compare_coordinates(image: &casa_images::PagedImage<f32>, reference: &Value) {
    use casa_coordinates::CoordinateModel;
    let coordinates = image.coordinates();
    let flatten = |read: fn(&CoordinateModel) -> Vec<f64>| {
        (0..coordinates.n_coordinates())
            .flat_map(|i| read(coordinates.coordinate(i)))
            .collect::<Vec<_>>()
    };
    for (key, values) in [
        ("referencevalue", flatten(|c| c.reference_value())),
        ("increment", flatten(|c| c.increment())),
        ("referencepixel", flatten(|c| c.reference_pixel())),
    ] {
        let expected = reference[key].as_array().unwrap();
        assert_eq!(values.len(), expected.len());
        for (actual, expected) in values.into_iter().zip(expected) {
            let expected = expected.as_f64().unwrap();
            assert!(
                (actual - expected).abs() <= 1e-15 + 1e-10 * expected.abs(),
                "{key}: {actual} vs {expected}"
            );
        }
    }
    let CoordinateModel::Direction(direction) = coordinates.coordinate(0) else {
        panic!("direction")
    };
    let CoordinateModel::Spectral(spectral) = coordinates.coordinate(2) else {
        panic!("spectral")
    };
    assert_eq!(direction.direction_ref().to_string(), reference["frame"][0]);
    assert_eq!(spectral.frequency_ref().to_string(), reference["frame"][2]);
    let rest = reference["rest_frequency_hz"].as_f64().unwrap();
    assert!(
        (spectral.rest_frequency() - rest).abs() <= rest * 1e-10,
        "rest frequency: {} vs {rest}",
        spectral.rest_frequency()
    );
}

#[test]
fn selected_channels_and_declared_unit_precedence_match_actual_casa() {
    let root = tempfile::tempdir().unwrap();
    let data =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../libs/imaging/casa-imaging/tests/data");
    assert!(
        std::process::Command::new("tar")
            .arg("-xzf")
            .arg(data.join("tsdimaging_selection_inputs.tar.gz"))
            .arg("-C")
            .arg(root.path())
            .status()
            .unwrap()
            .success()
    );
    let oracle: Value = serde_json::from_reader(flate2::read::GzDecoder::new(
        std::fs::File::open(data.join("tsdimaging_selection_casa.json.gz")).unwrap(),
    ))
    .unwrap();
    assert_eq!(oracle["casa_version"], "6.7.6.14");
    for case in oracle["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let mut values = case["parameters"].clone();
        values["infiles"] = json!(root.path().join(format!("{name}.ms")));
        let output = root.path().join(name);
        values["outfile"] = json!(output);
        let (success, reply) = invoke(root.path(), values);
        if case.get("error").is_some() {
            assert!(!success, "{name}: {reply}");
            assert_eq!(reply["kind"], "validation");
            continue;
        }
        assert!(success, "{name}: {reply}");
        let image = casa_images::PagedImage::<f32>::open(output.with_extension("image")).unwrap();
        let weight = casa_images::PagedImage::<f32>::open(output.with_extension("weight")).unwrap();
        assert_eq!(
            image.shape().to_vec(),
            case["image"]["shape"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_u64().unwrap() as usize)
                .collect::<Vec<_>>()
        );
        compare_pixels(&image, &case["image"]);
        compare_pixels(&weight, &case["weight"]);
        compare_coordinates(&image, &case["wcs"]);
    }
}
