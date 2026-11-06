// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's read-only imhead behavior on a FITS image.

use super::imhead_mutations::assert_header_value_near;
use super::*;

#[test]
fn imhead_reads_fits_without_modifying_it() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_fits");
    let oracle: Value =
        serde_json::from_slice(&std::fs::read(fixture.join("expected.json")).unwrap()).unwrap();
    assert_eq!(oracle["casa_version"], "6.7.6-14");
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("image.fits");
    std::fs::copy(fixture.join("image.fits"), &path).unwrap();
    let before = std::fs::read(&path).unwrap();
    for (index, (name, case)) in oracle["cases"].as_object().unwrap().iter().enumerate() {
        let (mode, key) = name.split_once(':').unwrap_or((name, "bunit"));
        let (_, reply) = json_run(
            workspace.path(),
            "imhead",
            json!({
                "imagename": path, "mode": mode, "hdkey": key, "hdvalue": "Jy/beam"
            }),
        );
        if case.get("error").is_some() {
            assert!(
                matches!(reply, Some(TaskReply::Failed { ref message, .. }) if message.contains("read-only")),
                "{name}: {reply:?}"
            );
        } else {
            let Some(TaskReply::Succeeded { value, .. }) = reply else {
                panic!("{name}: {reply:?}");
            };
            assert_eq!(describe(&value), case["structure"], "{name}");
            assert_header_value_near(&value.to_plain_json(), &case["value"], index);
        }
        assert_eq!(
            std::fs::read(&path).unwrap(),
            before,
            "{name} modified FITS"
        );
    }
    let history = casa_images::image::image_history_entries(&path).unwrap();
    let messages = history
        .iter()
        .map(|entry| entry.message.as_str())
        .collect::<Vec<_>>();
    let expected = oracle["history"]
        .as_array()
        .unwrap()
        .iter()
        .map(|message| message.as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(messages, expected);
    assert!(history[0].time_mjd_seconds > 0.0);
    assert_eq!(history.last().unwrap().time_mjd_seconds, -1.0);
}

#[test]
fn imhead_reads_scaled_integer_fits_with_blank_pixels() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_fits");
    let oracle: Value =
        serde_json::from_slice(&std::fs::read(fixture.join("expected.json")).unwrap()).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("integer.fits");
    std::fs::copy(fixture.join("integer.fits"), &path).unwrap();
    for (index, (name, case)) in oracle["integer_cases"]
        .as_object()
        .unwrap()
        .iter()
        .enumerate()
    {
        let (mode, key) = name.split_once(':').unwrap();
        let value = returned(
            workspace.path(),
            "imhead",
            json!({
                "imagename": path, "mode": mode, "hdkey": key
            }),
        );
        assert_eq!(describe(&value), case["structure"], "{name}");
        assert_header_value_near(&value.to_plain_json(), &case["value"], index);
    }
}

#[test]
fn imhead_reads_fits_per_plane_beams() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_fits");
    let oracle: Value =
        serde_json::from_slice(&std::fs::read(fixture.join("expected.json")).unwrap()).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("beams.fits");
    std::fs::copy(fixture.join("beams.fits"), &path).unwrap();
    for (index, (name, case)) in oracle["beam_cases"].as_object().unwrap().iter().enumerate() {
        let (mode, key) = name.split_once(':').unwrap();
        let value = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": mode, "hdkey": key}),
        );
        assert_eq!(describe(&value), case["structure"], "{name}");
        assert_header_value_near(&value.to_plain_json(), &case["value"], index);
    }
    let imported = workspace.path().join("beams.im");
    casa_images::import_fits(&path, &imported, false).unwrap();
    let summary = returned(
        workspace.path(),
        "imhead",
        json!({"imagename": imported, "mode": "summary"}),
    )
    .to_plain_json();
    assert_header_value_near(
        &summary["perplanebeams"],
        &oracle["beam_cases"]["summary:"]["value"]["perplanebeams"],
        0,
    );
}

#[test]
fn imhead_reads_fits_single_beam() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_fits");
    let oracle: Value =
        serde_json::from_slice(&std::fs::read(fixture.join("expected.json")).unwrap()).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("single.fits");
    std::fs::copy(fixture.join("single.fits"), &path).unwrap();
    for (index, (name, case)) in oracle["single_cases"]
        .as_object()
        .unwrap()
        .iter()
        .enumerate()
    {
        let (mode, key) = name.split_once(':').unwrap();
        let value = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": mode, "hdkey": key}),
        );
        assert_eq!(describe(&value), case["structure"], "{name}");
        assert_header_value_near(&value.to_plain_json(), &case["value"], index);
    }
}

#[test]
fn imhead_normalizes_fits_spectral_axis_and_keeps_rest_frequency_in_hz() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_fits");
    let oracle: Value =
        serde_json::from_slice(&std::fs::read(fixture.join("expected.json")).unwrap()).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("scaled.fits");
    std::fs::copy(fixture.join("scaled.fits"), &path).unwrap();
    for (index, (name, case)) in oracle["scaled_cases"]
        .as_object()
        .unwrap()
        .iter()
        .enumerate()
    {
        let (mode, key) = name.split_once(':').unwrap();
        let value = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": path, "mode": mode, "hdkey": key}),
        );
        assert_eq!(describe(&value), case["structure"], "{name}");
        assert_header_value_near(&value.to_plain_json(), &case["value"], index);
    }
    let imported = workspace.path().join("scaled.im");
    casa_images::import_fits(&path, &imported, false).unwrap();
    let rest = returned(
        workspace.path(),
        "imhead",
        json!({"imagename": imported, "mode": "get", "hdkey": "restfreq"}),
    )
    .to_plain_json();
    assert_header_value_near(&rest, &oracle["scaled_cases"]["get:restfreq"]["value"], 0);
    let exported = workspace.path().join("exported.fits");
    casa_images::export_fits(&imported, &exported, false, false).unwrap();
    let bytes = std::fs::read(&exported).unwrap();
    let cards = bytes
        .as_chunks::<80>()
        .0
        .iter()
        .take_while(|card| &card[..8] != b"END     ")
        .map(|card| std::str::from_utf8(card).unwrap())
        .collect::<Vec<_>>();
    let header = casa_coordinates::fits::FitsHeader::from_cards(&cards);
    assert_eq!(header.get_string("CUNIT3"), Some("Hz"));
    let expected = oracle["scaled_cases"]["get:restfreq"]["value"]["value"]
        .as_f64()
        .unwrap();
    assert!((header.get_float("RESTFRQ").unwrap() - expected).abs() < 1e-3);
}

#[test]
fn imhead_cli_prints_every_returned_field() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_fits");
    let workspace = tempfile::tempdir().unwrap();
    for (file, mode, key) in [
        ("image.fits", "get", "datamin"),
        ("image.fits", "get", "mykey"),
        ("beams.fits", "list", ""),
    ] {
        let image = fixture.join(file);
        let output = Command::new(env!("CARGO_BIN_EXE_imexplore"))
            .args([
                "imhead",
                image.to_str().unwrap(),
                "--mode",
                mode,
                "--hdkey",
                key,
                "--no-save-last",
            ])
            .current_dir(workspace.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let printed: Value = serde_json::from_slice(&output.stdout).unwrap();
        let returned = returned(
            workspace.path(),
            "imhead",
            json!({"imagename": image, "mode": mode, "hdkey": key}),
        )
        .to_plain_json();
        assert_eq!(printed, returned, "{file} {mode} {key}");
    }
}

#[test]
fn imhead_reads_fits_radio_velocity_as_casa_frequencies() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_fits");
    let oracle: Value =
        serde_json::from_slice(&std::fs::read(fixture.join("expected.json")).unwrap()).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    for (file, cases) in [
        ("velocity.fits", "velocity_cases"),
        ("velocity-no-rest.fits", "no_rest_cases"),
    ] {
        let path = workspace.path().join(file);
        std::fs::copy(fixture.join(file), &path).unwrap();
        for (index, (name, case)) in oracle[cases].as_object().unwrap().iter().enumerate() {
            let (mode, key) = name.split_once(':').unwrap();
            let value = returned(
                workspace.path(),
                "imhead",
                json!({"imagename": path, "mode": mode, "hdkey": key}),
            );
            assert_eq!(describe(&value), case["structure"], "{file} {name}");
            assert_header_value_near(&value.to_plain_json(), &case["value"], index);
        }
    }
}
