// SPDX-License-Identifier: LGPL-3.0-or-later
//! The native task against CASA 6.7.6-14, including stored uncertainties,
//! raw row flags, pointing metadata, input ordering and atomic append.
use casa_ms::{
    MeasurementSet, SubtableId,
    concat::{ConcatRequest, concat},
};
use casa_tables::Table;
use casa_types::{ArrayValue, ScalarValue, Value};
use serde_json::{Value as Json, json};
use std::collections::BTreeMap;
use std::path::Path;

fn fixtures() -> (tempfile::TempDir, Json) {
    let root = tempfile::tempdir().unwrap();
    tar::Archive::new(flate2::read::GzDecoder::new(
        &include_bytes!("fixtures/concat/concat_inputs.tar.gz")[..],
    ))
    .unpack(root.path())
    .unwrap();
    let references = serde_json::from_reader(flate2::read::GzDecoder::new(
        &include_bytes!("fixtures/concat/concat_casa.json.gz")[..],
    ))
    .unwrap();
    (root, references)
}

#[test]
fn parameter_order_and_defaults_match_casa_67614() {
    let reference: Json = serde_json::from_str(include_str!(
        "../../../resources/casa-compat/casa-6.7.6.14/tasks/concat.json"
    ))
    .unwrap();
    let bundle = casa_contracts::builtin_surface_bundle("concat").unwrap();
    let bindings = bundle.surface.bindings();
    let parameters = reference["parameters"].as_array().unwrap();
    assert_eq!(bindings.len(), parameters.len());
    for (binding, parameter) in bindings.iter().zip(parameters) {
        assert_eq!(binding.name, parameter["name"].as_str().unwrap());
        let value = match parameter["default"].as_str().unwrap() {
            "''" => json!({"kind":"string","value":""}),
            "[]" => json!({"kind":"array","value":[]}),
            "False" => json!({"kind":"bool","value":false}),
            "True" => json!({"kind":"bool","value":true}),
            other => panic!("unclassified CASA default {other}"),
        };
        assert_eq!(
            serde_json::to_value(&binding.default).unwrap(),
            json!({"kind":"literal","value":value})
        );
    }
}

fn request(root: &Path, parameters: &Json, output: &str) -> ConcatRequest {
    let mut values = json!({"vis":[], "concatvis":root.join(output), "freqtol":"", "dirtol":"",
        "respectname":false, "timesort":false, "copypointing":true, "visweightscale":[], "forcesingleephemfield":""});
    for (name, value) in parameters.as_object().unwrap() {
        values[name] = value.clone();
    }
    values["vis"] = json!(
        parameters["vis"]
            .as_array()
            .unwrap()
            .iter()
            .map(|name| root.join(name.as_str().unwrap()))
            .collect::<Vec<_>>()
    );
    serde_json::from_value(values).unwrap()
}

fn scalar(value: &ScalarValue) -> Json {
    match value {
        ScalarValue::Bool(v) => json!(v),
        ScalarValue::Int32(v) => json!(v),
        ScalarValue::Float32(v) => number(*v as f64),
        ScalarValue::Float64(v) => number(*v),
        ScalarValue::String(v) => json!(v),
        other => panic!("unexpected fixture scalar {other:?}"),
    }
}

fn number(value: f64) -> Json {
    if value.is_finite() {
        json!(value)
    } else {
        json!(value.to_string().to_lowercase())
    }
}

fn encode(value: &Value) -> Json {
    macro_rules! array {
        ($values:expr, $encode:expr) => { json!({"shape":$values.shape(), "values":$values.iter().map($encode).collect::<Vec<_>>()}) };
    }
    match value {
        Value::Scalar(v) => scalar(v),
        Value::Record(v) => {
            json!({"record":v.fields().iter().map(|f| (f.name.clone(),encode(&f.value))).collect::<BTreeMap<_,_>>()})
        }
        Value::Array(v) => match v {
            ArrayValue::Bool(v) => array!(v, |v| json!(v)),
            ArrayValue::Int32(v) => array!(v, |v| json!(v)),
            ArrayValue::Float32(v) => array!(v, |v| number(*v as f64)),
            ArrayValue::Float64(v) => array!(v, |v| number(*v)),
            ArrayValue::Complex32(v) => array!(v, |v| json!({"complex":[v.re,v.im]})),
            ArrayValue::Complex64(v) => array!(v, |v| json!({"complex":[v.re,v.im]})),
            ArrayValue::String(v) => array!(v, |v| json!(v)),
            other => panic!("unexpected fixture array {other:?}"),
        },
        other => panic!("unexpected fixture value {other:?}"),
    }
}

fn snapshot(table: &Table) -> Json {
    let mut columns = BTreeMap::new();
    for column in table.schema().unwrap().columns() {
        let name = column.name();
        let values: Vec<Json> = (0..table.row_count())
            .map(|row| {
                if table.is_cell_defined(row, name).unwrap() {
                    encode(table.row_accessor().row(row).unwrap().get(name).unwrap())
                } else {
                    Json::Null
                }
            })
            .collect();
        if values.iter().any(|v| !v.is_null()) {
            columns.insert(name.to_string(), values);
        }
    }
    json!({"rows":table.row_count(), "columns":columns})
}

fn normalize_empty_strings(value: &mut Json) {
    match value {
        Json::Object(map) => {
            if map.contains_key("shape")
                && map
                    .get("values")
                    .and_then(Json::as_array)
                    .is_some_and(|values| values.iter().all(|v| v.as_str() == Some("")))
            {
                map.insert("shape".into(), json!([0]));
                map.insert("values".into(), json!([]));
            } else {
                for value in map.values_mut() {
                    normalize_empty_strings(value);
                }
            }
        }
        Json::Array(values) => {
            for value in values {
                normalize_empty_strings(value);
            }
        }
        _ => {}
    }
}

fn order_feeds(table: &mut Json) {
    // FEED rows are addressed by antenna/feed/window, never by row number.
    let count = table["rows"].as_u64().unwrap() as usize;
    let columns = &table["columns"];
    let mut order: Vec<usize> = (0..count).collect();
    order.sort_by(|a, b| {
        for name in [
            "ANTENNA_ID",
            "FEED_ID",
            "SPECTRAL_WINDOW_ID",
            "TIME",
            "INTERVAL",
        ] {
            let compared = columns[name][*a]
                .as_f64()
                .unwrap()
                .total_cmp(&columns[name][*b].as_f64().unwrap());
            if !compared.is_eq() {
                return compared;
            }
        }
        std::cmp::Ordering::Equal
    });
    for column in table["columns"].as_object_mut().unwrap().values_mut() {
        let original = column.as_array().unwrap();
        *column = json!(
            order
                .iter()
                .map(|row| original[*row].clone())
                .collect::<Vec<_>>()
        );
    }
}

fn compare(actual: &Json, expected: &Json, path: &str) {
    match (actual, expected) {
        (Json::Number(a), Json::Number(b)) => {
            let (a, b) = (a.as_f64().unwrap(), b.as_f64().unwrap());
            let difference = if path.ends_with("/values/0")
                && (path.starts_with("FIELD/columns/") && path.contains("_DIR/")
                    || path.starts_with("SOURCE/columns/DIRECTION/"))
            {
                // Longitude is circular; casacore and MDirection can choose
                // opposite sides of the same full-turn representation.
                (a - b + std::f64::consts::PI).rem_euclid(std::f64::consts::TAU)
                    - std::f64::consts::PI
            } else {
                a - b
            };
            let relative =
                if path.contains("SIGMA") || path.contains("WEIGHT") || path.contains("DATA/") {
                    1.0e-6
                } else {
                    1.0e-12
                };
            assert!(
                difference.abs() <= 1.0e-10 + relative * b.abs(),
                "{path}: {a} != {b}"
            );
        }
        (Json::Array(a), Json::Array(b)) => {
            assert_eq!(a.len(), b.len(), "{path}");
            for (index, (a, b)) in a.iter().zip(b).enumerate() {
                compare(a, b, &format!("{path}/{index}"));
            }
        }
        (Json::Object(a), Json::Object(b)) => {
            assert_eq!(
                a.keys().collect::<std::collections::BTreeSet<_>>(),
                b.keys().collect::<std::collections::BTreeSet<_>>(),
                "{path}"
            );
            for (name, b) in b {
                compare(&a[name], b, &format!("{path}/{name}"));
            }
        }
        _ => assert_eq!(actual, expected, "{path}"),
    }
}

fn compare_ms(path: &Path, expected: &Json) {
    let ms = MeasurementSet::open_retained_read(path).unwrap();
    for (name, reference) in expected.as_object().unwrap() {
        let table = match name.as_str() {
            "MAIN" => ms.main_table(),
            other => {
                let id = [
                    SubtableId::Antenna,
                    SubtableId::DataDescription,
                    SubtableId::Feed,
                    SubtableId::Field,
                    SubtableId::FlagCmd,
                    SubtableId::Observation,
                    SubtableId::Pointing,
                    SubtableId::Polarization,
                    SubtableId::Processor,
                    SubtableId::Source,
                    SubtableId::SpectralWindow,
                    SubtableId::State,
                ]
                .into_iter()
                .find(|id| id.name() == other)
                .unwrap();
                ms.subtable(id).unwrap()
            }
        };
        let mut actual = snapshot(table);
        let mut reference = reference.clone();
        // CASA's simulated inputs have PROCESSOR_ID=0 beside an empty
        // PROCESSOR table. Compare the absence of a processor: native output
        // uses the valid MS sentinel -1 (docs/CASA (C++) bugs.md).
        if name == "MAIN" && expected["PROCESSOR"]["rows"] == 0 {
            assert!(
                actual["columns"]["PROCESSOR_ID"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|value| value == &json!(-1))
            );
            for value in reference["columns"]["PROCESSOR_ID"].as_array_mut().unwrap() {
                assert!(value == &json!(0) || value == &json!(-1));
                *value = json!(-1);
            }
        }
        if name == "FEED" {
            order_feeds(&mut actual);
            order_feeds(&mut reference);
        }
        if name == "OBSERVATION" {
            // Undefined and empty provenance string vectors carry no entries.
            for table in [&mut actual, &mut reference] {
                for column in ["LOG", "SCHEDULE"] {
                    if let Some(values) = table["columns"][column].as_array_mut() {
                        for value in values {
                            if value.is_null() {
                                *value = json!({"shape":[0],"values":[]});
                            }
                        }
                    }
                }
            }
        }
        if name == "SOURCE"
            && actual["columns"].get("POSITION").is_none()
            && reference["columns"]
                .get("POSITION")
                .and_then(Json::as_array)
                .is_some_and(|values| {
                    values.iter().all(|value| {
                        value.is_null()
                            || value
                                .get("values")
                                .and_then(Json::as_array)
                                .is_some_and(Vec::is_empty)
                    })
                })
        {
            // An undefined or zero-length optional position states no finite
            // source position. Real position vectors are always compared.
            reference["columns"]
                .as_object_mut()
                .unwrap()
                .remove("POSITION");
        }
        normalize_empty_strings(&mut actual);
        normalize_empty_strings(&mut reference);
        compare(&actual, &reference, name);
    }
    let history = ms.subtable(SubtableId::History).unwrap();
    assert!(history.row_count() > 0);
    assert_eq!(
        history.row_accessor().row(0).unwrap().get("ORIGIN"),
        Some(&Value::Scalar(ScalarValue::String("concat".into())))
    );
}

fn expected_tables(case: &Json) -> Json {
    let mut tables = case["tables"].clone();
    if let Some(correction) = case.get("first_sigma_spectrum_correction") {
        // Independent variance law: sigma scales by 1/sqrt(weight factor).
        // The fixture retains CASA's actual, unscaled first-input spectrum;
        // task_oracle.py verifies that specific CASA bug before recording it.
        let scale = correction["scale"].as_f64().unwrap().sqrt();
        let rows = correction["rows"].as_u64().unwrap() as usize;
        for row in tables["MAIN"]["columns"]["SIGMA_SPECTRUM"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .take(rows)
        {
            for value in row["values"].as_array_mut().unwrap() {
                *value = json!(value.as_f64().unwrap() / scale);
            }
        }
    }
    tables
}

fn case_matches_casa(name: &str) {
    let (root, references) = fixtures();
    assert_eq!(references["casa_version"], "6.7.6.14");
    let case = &references["cases"][name];
    let request = request(root.path(), &case["parameters"], &format!("{name}.out.ms"));
    if name == "pair" {
        let run = root.path().join("run.json");
        std::fs::write(
            &run,
            serde_json::to_vec(&json!({"surface":"concat","values":request})).unwrap(),
        )
        .unwrap();
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_concat"))
            .args(["--json-run", run.to_str().unwrap()])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let reply: Json = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(reply["value"]["kind"], "none");
    } else {
        concat(&request).unwrap_or_else(|error| panic!("{name}: {error}"));
    }
    compare_ms(&request.concatvis, &expected_tables(case));
}

#[test]
fn simulation_pair_matches_casa_via_public_task() {
    case_matches_casa("pair");
}
#[test]
fn input_order_keeps_the_weight_with_its_measurement_set() {
    case_matches_casa("reversed");
}
#[test]
fn copypointing_false_matches_casa() {
    case_matches_casa("without_pointing");
}
#[test]
fn stored_uncertainties_and_raw_flags_match_casa() {
    case_matches_casa("weighted");
}

#[test]
fn append_preserves_existing_output_and_input_history() {
    let (root, references) = fixtures();
    let history = |path: &Path| {
        MeasurementSet::open_retained_read(path)
            .unwrap()
            .subtable(SubtableId::History)
            .unwrap()
            .row_count()
    };
    let twelve = root.path().join("twelve.ms");
    let seven = root.path().join("seven.ms");
    let before = [history(&twelve), history(&seven)];
    let first = request(
        root.path(),
        &json!({"vis":["twelve.ms"],"visweightscale":[4.]}),
        "append.ms",
    );
    concat(&first).unwrap();
    let mut append = request(
        root.path(),
        &json!({"vis":["seven.ms"],"visweightscale":[0.34]}),
        "append.ms",
    );
    append.freqtol = json!("wrong-unit");
    assert!(concat(&append).is_err());
    assert_eq!(
        MeasurementSet::open_retained_read(&first.concatvis)
            .unwrap()
            .main_table()
            .row_count(),
        MeasurementSet::open_retained_read(&twelve)
            .unwrap()
            .main_table()
            .row_count()
    );
    append.freqtol = json!("");
    concat(&append).unwrap();
    compare_ms(
        &first.concatvis,
        &expected_tables(&references["cases"]["pair"]),
    );
    assert_eq!([history(&twelve), history(&seven)], before);
}

#[test]
fn unsupported_and_invalid_arguments_refuse_before_publication() {
    let (root, references) = fixtures();
    let base = request(
        root.path(),
        &references["cases"]["pair"]["parameters"],
        "refused.ms",
    );
    for name in [
        "respectname",
        "timesort",
        "forcesingleephemfield",
        "visweightscale",
        "freqtol",
        "dirtol",
    ] {
        let mut values = serde_json::to_value(&base).unwrap();
        values[name] = match name {
            "respectname" | "timesort" => json!(true),
            "forcesingleephemfield" => json!(0),
            "visweightscale" => json!([0., 1.]),
            "freqtol" => json!("1s"),
            _ => json!("1Hz"),
        };
        let request = serde_json::from_value(values).unwrap();
        assert!(concat(&request).is_err(), "{name}");
        assert!(!base.concatvis.exists(), "{name}");
    }
}
