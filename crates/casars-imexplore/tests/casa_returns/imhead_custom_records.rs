// SPDX-License-Identifier: LGPL-3.0-or-later
//! Custom-record writes and reads compared with CASA.
use super::*;

#[test]
fn large_integer_writes_match_casa() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let oracle: Value = serde_json::from_slice(
        &std::fs::read(fixture.join("large_integer_expected.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(oracle["casa_version"], "6.7.6-14");
    let workspace = tempfile::tempdir().unwrap();
    for (index, case) in oracle["cases"].as_array().unwrap().iter().enumerate() {
        let path = workspace.path().join(format!("large-int-{index}.im"));
        copy_tree(&fixture.join("normal.im"), &path);
        let (_, reply) = json_run(
            workspace.path(),
            "imhead",
            json!({"imagename":path, "mode":case["mode"], "hdkey":case["key"], "hdvalue":case["value"]}),
        );
        assert!(
            matches!(
                reply,
                Some(TaskReply::Succeeded {
                    value: CasaValue::None,
                    ..
                })
            ),
            "{index}: {reply:?}"
        );
        let got = returned(
            workspace.path(),
            "imhead",
            json!({"imagename":path, "mode":"get", "hdkey":case["key"]}),
        );
        if case["key"] == "cdelt1" {
            let got = got.to_plain_json();
            assert_eq!(got["unit"], case["get"]["unit"]);
            let actual = got["value"].as_f64().unwrap();
            let expected = case["get"]["value"].as_f64().unwrap();
            assert!(
                (actual - expected).abs() <= expected.abs() * 1e-12,
                "{index}: {actual} != {expected}"
            );
        } else {
            assert_eq!(got.to_plain_json(), case["get"], "{index}");
        }
    }
}

#[test]
fn custom_record_sequences_match_casa() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let oracle: Value = serde_json::from_slice(
        &std::fs::read(fixture.join("custom_record_expected.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(oracle["casa_version"], "6.7.6-14");
    let workspace = tempfile::tempdir().unwrap();
    for (index, case) in oracle["cases"].as_array().unwrap().iter().enumerate() {
        let path = workspace.path().join(format!("record-{index}.im"));
        copy_tree(&fixture.join("normal.im"), &path);
        for op in case["operations"].as_array().unwrap() {
            let before = casa_images::PagedImage::<f32>::open(&path)
                .unwrap()
                .history()
                .unwrap()
                .len();
            let (output, reply) = json_run(
                workspace.path(),
                "imhead",
                json!({"imagename": path, "mode": op["mode"], "hdkey": "custom", "hdvalue": op["value"]}),
            );
            if op.get("error").is_some() {
                assert!(
                    matches!(reply, Some(TaskReply::Failed { .. })),
                    "{index}: {reply:?}"
                );
            } else {
                assert!(
                    matches!(
                        reply,
                        Some(TaskReply::Succeeded {
                            value: CasaValue::None,
                            ..
                        })
                    ),
                    "{index}: {reply:?}: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
            let history = image.history().unwrap();
            let expected = op["history"].as_array().unwrap();
            assert_eq!(
                history.len() - before,
                expected.len(),
                "{index}: history count"
            );
            if !expected.is_empty() {
                assert_eq!(history[before], expected[0], "{index}: metadata history");
            }
            if expected.len() == 3 {
                let task = expected[2]
                    .as_str()
                    .unwrap()
                    .replace("<IMAGE>", path.to_str().unwrap());
                assert_eq!(history[before + 2], task, "{index}: task history");
            }
        }
        if let Some(expected) = case.get("get") {
            let value = returned(
                workspace.path(),
                "imhead",
                json!({"imagename": path, "mode":"get", "hdkey":"custom"}),
            );
            assert_eq!(value.to_plain_json(), *expected, "{index}: get");
        } else {
            let (_, reply) = json_run(
                workspace.path(),
                "imhead",
                json!({"imagename":path, "mode":"get", "hdkey":"custom"}),
            );
            assert!(
                matches!(reply, Some(TaskReply::Failed { .. })),
                "{index}: missing get"
            );
        }
    }
}

#[test]
fn casa_created_record_returns_typed_values() {
    let root =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix/custom_record.im");
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("record.im");
    copy_tree(&root, &path);
    let value = returned(
        workspace.path(),
        "imhead",
        json!({"imagename": path, "mode":"get", "hdkey":"custom"}),
    );
    assert_eq!(entry(&value, "a"), &CasaValue::Bool { value: true });
    assert_eq!(
        entry(&value, "b"),
        &CasaValue::IntArray {
            dtype: IntDtype::Int64,
            shape: vec![2],
            data: vec![1, 2]
        }
    );
    assert_eq!(
        entry(&value, "matrix"),
        &CasaValue::IntArray {
            dtype: IntDtype::Int64,
            shape: vec![2, 3],
            data: vec![1, 4, 2, 5, 3, 6]
        }
    );
    use casa_provider_contracts::{CasaFloat, ComplexDtype};
    assert_eq!(
        entry(&value, "complex"),
        &CasaValue::ComplexArray {
            dtype: ComplexDtype::Complex128,
            shape: vec![2, 2],
            data: vec![
                [CasaFloat(1.0), CasaFloat(2.0)],
                [CasaFloat(5.0), CasaFloat(6.0)],
                [CasaFloat(3.0), CasaFloat(4.0)],
                [CasaFloat(7.0), CasaFloat(8.0)]
            ],
        }
    );
    assert_eq!(
        entry(entry(&value, "nested"), "x"),
        &CasaValue::string("abc")
    );
}

#[test]
fn casa_created_record_returns_complex_scalars() {
    use casa_provider_contracts::CasaFloat;
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let oracle: Value = serde_json::from_slice(
        &std::fs::read(fixture.join("custom_complex_expected.json")).unwrap(),
    )
    .unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("complex.im");
    copy_tree(&fixture.join("custom_complex.im"), &path);
    let value = returned(
        workspace.path(),
        "imhead",
        json!({"imagename": path, "mode":"get", "hdkey":"custom"}),
    );
    assert_eq!(value.to_plain_json(), oracle["record"]);
    assert_eq!(
        entry(&value, "scalar"),
        &CasaValue::Complex {
            real: CasaFloat(1.25),
            imag: CasaFloat(-2.5)
        }
    );
    assert_eq!(
        entry(entry(&value, "nested"), "imag"),
        &CasaValue::Complex {
            real: CasaFloat(0.0),
            imag: CasaFloat(-3.0)
        }
    );
}

#[test]
fn unsigned_record_array_matches_casa() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let oracle: Value = serde_json::from_slice(
        &std::fs::read(fixture.join("custom_uint32_expected.json")).unwrap(),
    )
    .unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("unsigned.im");
    copy_tree(&fixture.join("custom_uint32.im"), &path);
    let value = returned(
        workspace.path(),
        "imhead",
        json!({"imagename": path, "mode":"get", "hdkey":"custom"}),
    );
    let array = entry(&value, "value");
    assert_eq!(
        array,
        &CasaValue::UInt64Array {
            shape: vec![2],
            data: vec![1, u64::from(u32::MAX)]
        }
    );
    assert_eq!(array.to_plain_json(), oracle["value"]);
    assert_eq!(oracle["dtype"], "uint64");
}

#[test]
fn invalid_typed_requests_do_not_modify_the_image() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix/normal.im");
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("image.im");
    copy_tree(&fixture, &path);
    let table = std::fs::read(path.join("table.dat")).unwrap();
    for extra in [
        json!({"hdvalue": {"kind": "complex", "value": [1.0, 2.0]}}),
        json!({"verbose": {"kind": "bool", "value": true}}),
    ] {
        let mut child = Command::new(env!("CARGO_BIN_EXE_imexplore"))
            .args(["imhead", "--json-run", "-"])
            .current_dir(workspace.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        write!(child.stdin.take().unwrap(), "{}", json!({
            "surface": "imhead", "values": {"imagename": path, "mode": "add", "hdkey": "custom", "hdvalue": "plain"}, "typed_values": extra
        })).unwrap();
        let output = child.wait_with_output().unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(matches!(
            serde_json::from_slice::<TaskReply>(&output.stdout).unwrap(),
            TaskReply::Failed { .. }
        ));
        assert_eq!(std::fs::read(path.join("table.dat")).unwrap(), table);
    }
}
