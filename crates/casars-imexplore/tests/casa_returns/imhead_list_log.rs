// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[test]
fn list_messages_match_casa_and_follow_logging_configuration() {
    let data = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data");
    let oracle: Value = serde_json::from_slice(
        &std::fs::read(data.join("imhead_crpix/list_log_expected.json")).unwrap(),
    )
    .unwrap();
    let workspace = tempfile::tempdir().unwrap();
    for (name, messages) in oracle["cases"].as_object().unwrap() {
        let image = workspace
            .path()
            .join(format!("{}.im", name.replace('/', "_")));
        copy_tree(&data.join(format!("{name}.im")), &image);
        let expected: Vec<&str> = messages
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        let mut summary = casa_images::imhead(&image).unwrap();
        summary.extrema = casa_images::imhead_extrema(&image).unwrap().map(Box::new);
        assert_eq!(
            casa_images::analysis::imhead_list_log(&summary),
            expected,
            "{name}"
        );

        let output = Command::new(env!("CARGO_BIN_EXE_imexplore"))
            .args([
                "imhead",
                image.to_str().unwrap(),
                "--mode",
                "list",
                "--no-save-last",
            ])
            .current_dir(workspace.path())
            .env_remove("CASA_RS_LOG_TABLE")
            .env_remove("CASA_RS_LOG_STDERR_PRIORITY")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stderr = String::from_utf8(output.stderr).unwrap();
        let actual: Vec<&str> = stderr
            .lines()
            .filter_map(|line| {
                line.split_once("\tImageMetaDataBase::_toLog\t")
                    .map(|(_, message)| message)
            })
            .collect();
        assert_eq!(actual, expected, "{name}");
    }
    let image = workspace.path().join("imhead_crpix_normal.im");
    let output = Command::new(env!("CARGO_BIN_EXE_imexplore"))
        .args([
            "imhead",
            image.to_str().unwrap(),
            "--mode",
            "list",
            "--log-stderr-priority",
            "off",
            "--no-save-last",
        ])
        .current_dir(workspace.path())
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(!String::from_utf8_lossy(&output.stderr).contains("ImageMetaDataBase::_toLog"));

    let log_root = workspace.path().join("captured");
    std::fs::create_dir(&log_root).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_imexplore"))
        .args([
            "imhead",
            image.to_str().unwrap(),
            "--mode",
            "list",
            "--no-save-last",
        ])
        .current_dir(workspace.path())
        .env("CASA_RS_LOG_TABLE", log_root.join("logtable"))
        .env("CASA_RS_LOG_STDERR_PRIORITY", "off")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let entries = casa_images::image::image_history_entries(&log_root).unwrap();
    let messages: Vec<&str> = entries
        .iter()
        .filter(|entry| entry.location == "ImageMetaDataBase::_toLog")
        .map(|entry| entry.message.as_str())
        .collect();
    let expected = oracle["cases"]["imhead_crpix/normal"].as_array().unwrap();
    assert_eq!(
        messages,
        expected
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect::<Vec<_>>()
    );
}

#[test]
fn summary_reports_tabular_velocity_from_the_local_coordinate_mapping() {
    let data = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/imhead_crpix");
    let workspace = tempfile::tempdir().unwrap();
    let image = workspace.path().join("tabular.im");
    copy_tree(&data.join("verbose_tabular.im"), &image);
    let oracle: Value =
        serde_json::from_slice(&std::fs::read(data.join("verbose_expected.json")).unwrap())
            .unwrap();
    let summary = casa_images::imhead(&image).unwrap();
    let log = casa_images::analysis::imhead_summary_log(&image, &summary, true).unwrap();
    assert!(log.iter().any(|line| line.contains("Frequency (tabular)")));
    let velocity = log
        .iter()
        .find_map(|line| {
            line.split_once("increment ")
                .filter(|(before, _)| before.contains("Radio velocity"))
                .map(|(_, after)| {
                    after
                        .split_whitespace()
                        .next()
                        .unwrap()
                        .parse::<f64>()
                        .unwrap()
                })
        })
        .unwrap();
    let expected = oracle["tabular_velocity_increment_km_s"].as_f64().unwrap();
    assert!(
        (velocity - expected).abs() < 0.001,
        "{velocity} versus CASA {expected}"
    );
}
