// SPDX-License-Identifier: LGPL-3.0-or-later
//! Polarization reference-value writes compared with CASA.
use super::*;

#[test]
fn polarization_edits_match_casa() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let oracle: Value =
        serde_json::from_slice(&std::fs::read(fixture.join("stokes_edit_expected.json")).unwrap())
            .unwrap();
    assert_eq!(oracle["casa_version"], "6.7.6-14");
    let workspace = tempfile::tempdir().unwrap();
    for (index, case) in oracle["cases"].as_array().unwrap().iter().enumerate() {
        let path = workspace.path().join(format!("stokes-{index}.im"));
        copy_tree(
            &fixture.join(format!("{}.im", case["image"].as_str().unwrap())),
            &path,
        );
        let before = casa_images::PagedImage::<f32>::open(&path)
            .unwrap()
            .history()
            .unwrap()
            .len();
        let (output, reply) = json_run(
            workspace.path(),
            "imhead",
            json!({"imagename":path, "mode":"put", "hdkey":case["key"], "hdvalue":case["value"]}),
        );
        if case.get("error").is_some() {
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
        let expected = case["history"].as_array().unwrap();
        assert_eq!(
            history.len() - before,
            expected.len(),
            "{index}: history count"
        );
        if !expected.is_empty() {
            assert_eq!(history[before], expected[0], "{index}: history");
        }
        drop(image);
        let value = returned(
            workspace.path(),
            "imhead",
            json!({"imagename":path, "mode":"get", "hdkey":case["key"]}),
        );
        assert_eq!(value.to_plain_json(), case["get"], "{index}: get");
    }
}

#[test]
fn unsupported_stokes_names_do_not_modify_the_image() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix/normal.im");
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("stokes.im");
    copy_tree(&fixture, &path);
    let before = casa_images::PagedImage::<f32>::open(&path)
        .unwrap()
        .history()
        .unwrap();
    let (_, reply) = json_run(
        workspace.path(),
        "imhead",
        json!({"imagename":path, "mode":"put", "hdkey":"crval3", "hdvalue":"bad"}),
    );
    assert!(matches!(reply, Some(TaskReply::Failed { message, .. }) if message.contains("#977")));
    let image = casa_images::PagedImage::<f32>::open(&path).unwrap();
    assert_eq!(image.history().unwrap(), before);
    drop(image);
    let value = returned(
        workspace.path(),
        "imhead",
        json!({"imagename":path, "mode":"get", "hdkey":"crval3"}),
    );
    assert_eq!(value.to_plain_json(), json!(["I"]));
}
