// SPDX-License-Identifier: LGPL-3.0-or-later

//! The MeasurementSet tasks' guards (#931): CASA's parameters are the consent.
//! A product a second run would replace is refused without `--overwrite`, as
//! CASA refuses it, before anything is written or flagged; read-only modes
//! and refused arguments leave the MeasurementSet byte for byte as it was.

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use common::{TIME_BASE_SECONDS, create_msexplore_fixture_ms};

/// Every file under `path`, by relative path, with its bytes.
fn snapshot(path: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(root: &Path, path: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
        if path.is_dir() {
            for entry in std::fs::read_dir(path).unwrap() {
                walk(root, &entry.unwrap().path(), files);
            }
        } else {
            let relative = path.strip_prefix(root).unwrap().to_path_buf();
            files.insert(relative, std::fs::read(path).unwrap());
        }
    }
    let mut files = BTreeMap::new();
    walk(path, path, &mut files);
    files
}

fn run(binary: &str, args: &[&str]) -> Output {
    Command::new(binary)
        .args(args)
        .output()
        .expect("run task binary")
}

fn flag_versions(ms: &Path) -> PathBuf {
    let mut name = ms.as_os_str().to_owned();
    name.push(".flagversions");
    PathBuf::from(name)
}

#[test]
fn mstransform_and_split_refuse_an_existing_output_ms() {
    let temp = tempfile::tempdir().unwrap();
    let ms = create_msexplore_fixture_ms(temp.path());
    let ms = ms.to_str().unwrap();
    let out = temp.path().join("out.ms");
    let args = [
        "--vis",
        ms,
        "--outputvis",
        out.to_str().unwrap(),
        "--spw",
        "0",
        "--datacolumn",
        "data",
    ];

    let first = run(env!("CARGO_BIN_EXE_mstransform"), &args);
    assert!(first.status.success(), "{first:?}");
    let before = snapshot(&out);

    let second = run(env!("CARGO_BIN_EXE_mstransform"), &args);
    assert!(!second.status.success());
    let stderr = String::from_utf8_lossy(&second.stderr);
    assert!(
        stderr.contains("already exists - will not overwrite it"),
        "{stderr}"
    );
    assert_eq!(snapshot(&out), before);
}

#[test]
fn msexplore_refuses_an_existing_output_before_it_writes_or_flags_anything() {
    let temp = tempfile::tempdir().unwrap();
    let ms = create_msexplore_fixture_ms(temp.path());
    // The plot is written last, after any flag edit, so it is the output
    // whose guard must run first.
    let plot = temp.path().join("plot.txt");
    let outputs = [
        "--preset",
        "amplitude_vs_time",
        "--plot-output",
        plot.to_str().unwrap(),
        "--plot-format",
        "txt",
    ];
    let xmin = format!("{}", TIME_BASE_SECONDS - 0.1);
    let xmax = format!("{}", TIME_BASE_SECONDS + 0.1);
    let flag_edit = [
        "--flag-action",
        "flag",
        "--flag-xmin",
        &xmin,
        "--flag-xmax",
        &xmax,
        "--flag-ymin",
        "-0.1",
        "--flag-ymax",
        "0.1",
        "--flag-apply",
    ];
    let ms_arg = ms.to_str().unwrap();

    let mut first = outputs.to_vec();
    first.push(ms_arg);
    let output = run(env!("CARGO_BIN_EXE_msexplore"), &first);
    assert!(output.status.success(), "{output:?}");
    let before = (snapshot(&ms), snapshot(&plot));

    // Asking again, now also to apply a flag edit, without --overwrite.
    let mut second = outputs.to_vec();
    second.extend(flag_edit);
    second.push(ms_arg);
    let refused = run(env!("CARGO_BIN_EXE_msexplore"), &second);
    assert!(!refused.status.success());
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(stderr.contains("refusing to overwrite"), "{stderr}");
    assert_eq!((snapshot(&ms), snapshot(&plot)), before);
    assert!(!flag_versions(&ms).exists());

    second.insert(0, "--overwrite");
    let replaced = run(env!("CARGO_BIN_EXE_msexplore"), &second);
    assert!(replaced.status.success(), "{replaced:?}");
    assert_ne!(snapshot(&ms), before.0, "the flag edit was applied");
}

/// The flag arguments of a plot-region edit around the fixture's first
/// samples.
fn region_flag_edit(xmin: &str, xmax: &str) -> Vec<String> {
    [
        "--preset",
        "amplitude_vs_time",
        "--flag-action",
        "flag",
        "--flag-xmin",
        xmin,
        "--flag-xmax",
        xmax,
        "--flag-ymin",
        "-0.1",
        "--flag-ymax",
        "0.1",
        "--flag-apply",
    ]
    .map(String::from)
    .to_vec()
}

#[test]
fn msexplore_refuses_conflicting_outputs_and_flag_arguments_before_anything() {
    // Astra's #947 F2 probes: one file named for the summary and the flag
    // preview flagged 256 samples before the refusal, and a region edit
    // without its bounds wrote the summary first.
    let temp = tempfile::tempdir().unwrap();
    let ms = create_msexplore_fixture_ms(temp.path());
    let ms_arg = ms.to_str().unwrap().to_string();
    let before = snapshot(&ms);
    let same = temp.path().join("same.txt");
    let same_again = temp.path().join(".").join("same.txt");
    let summary = temp.path().join("summary.txt");
    let path = |path: &Path| path.to_str().unwrap().to_string();
    let xmin = format!("{}", TIME_BASE_SECONDS - 0.1);
    let xmax = format!("{}", TIME_BASE_SECONDS + 0.1);

    let selected = |summary: &Path, preview: &Path| {
        vec![
            ms_arg.clone(),
            "-o".into(),
            path(summary),
            "--flag-action".into(),
            "flag".into(),
            "--flag-selected".into(),
            "--flag-apply".into(),
            "--flag-output".into(),
            path(preview),
        ]
    };
    let mut wrong_plot = region_flag_edit(&xmin, &xmax);
    wrong_plot.extend(["--flag-plotindex", "3", "-o"].map(String::from));
    wrong_plot.extend([path(&summary), ms_arg.clone()]);
    let cases: [(Vec<String>, &str); 4] = [
        (selected(&same, &same), "both name"),
        (selected(&same, &same_again), "both name"),
        (
            vec![
                ms_arg.clone(),
                "--preset".into(),
                "amplitude_vs_time".into(),
                "--flag-action".into(),
                "flag".into(),
                "-o".into(),
                path(&summary),
            ],
            "requires --flag-xmin",
        ),
        // Refused only once the plot is built from the MeasurementSet.
        (wrong_plot, "plot_index"),
    ];
    for (args, expected) in cases {
        let args = args.iter().map(String::as_str).collect::<Vec<_>>();
        let output = run(env!("CARGO_BIN_EXE_msexplore"), &args);
        assert!(!output.status.success(), "{args:?} succeeded");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(expected), "{args:?}: {stderr}");
        assert_eq!(snapshot(&ms), before, "{args:?} changed the MeasurementSet");
        assert!(!same.exists() && !summary.exists(), "{args:?} wrote output");
    }
    assert!(!flag_versions(&ms).exists());

    // The same refusals on the provider path.
    let spec = casa_ms::MsExploreSpec {
        ms_path: ms.clone(),
        summary_format: casa_ms::MeasurementSetSummaryOutputFormat::Text,
        selection: casa_ms::MsSelection::default(),
        header_items: Vec::new(),
        page_title: None,
        exprange: casa_ms::MsPageExportRange::Current,
        max_plot_points: casa_ms::DEFAULT_MAX_PLOT_POINTS,
        plots: vec![casa_ms::MsPlotSpec::from_preset(
            casa_ms::MsPlotPreset::AmplitudeVsTime,
        )],
    };
    let edit = |plot_index| casa_ms::MsFlagEditSpec {
        action: casa_ms::MsFlagAction::Flag,
        region: casa_ms::MsFlagRegion {
            x_min: TIME_BASE_SECONDS - 0.1,
            x_max: TIME_BASE_SECONDS + 0.1,
            y_min: -0.1,
            y_max: 0.1,
        },
        plot_index,
        panel_key: None,
        extcorr: false,
        extchannel: false,
    };
    let request =
        |summary: &Path, preview: Option<&Path>, plot_index| casa_ms::MsExploreRunTaskRequest {
            spec: spec.clone(),
            summary_output_path: Some(summary.to_path_buf()),
            overwrite_outputs: false,
            flag_edit: Some(casa_ms::MsExploreFlagEditRequest {
                edit: edit(plot_index),
                apply: true,
                output_path: preview.map(Path::to_path_buf),
            }),
            plot_export: None,
        };
    for (request, expected) in [
        (request(&same, Some(&same_again), None), "both name"),
        (request(&summary, None, Some(3)), "plot_index"),
    ] {
        let error = request.execute().expect_err("refused");
        assert!(error.contains(expected), "{error}");
        assert_eq!(snapshot(&ms), before);
        assert!(!same.exists() && !summary.exists());
    }

    // An edit that is allowed still flags, and writes both outputs.
    let applied = request(&summary, Some(&same), None).execute().unwrap();
    assert!(applied.flag_edit_preview.unwrap().affected_samples > 0);
    assert_ne!(snapshot(&ms), before);
    assert!(same.exists() && summary.exists());
}

#[test]
fn read_only_modes_leave_the_measurement_set_unchanged() {
    let temp = tempfile::tempdir().unwrap();
    let ms = create_msexplore_fixture_ms(temp.path());
    let ms_arg = ms.to_str().unwrap();
    let before = snapshot(&ms);

    for (binary, args) in [
        (
            env!("CARGO_BIN_EXE_flagdata"),
            vec!["--vis", ms_arg, "--mode", "summary"],
        ),
        (
            env!("CARGO_BIN_EXE_flagmanager"),
            vec!["--vis", ms_arg, "--mode", "list"],
        ),
        (env!("CARGO_BIN_EXE_msexplore"), vec![ms_arg]),
    ] {
        let output = run(binary, &args);
        assert!(output.status.success(), "{binary} {args:?}: {output:?}");
        assert_eq!(snapshot(&ms), before, "{binary} {args:?}");
    }
    assert!(!flag_versions(&ms).exists());
}

#[test]
fn transform_refusals_are_validation_at_the_parameter_at_fault() {
    // Astra's #962 F4: the same input and output, and an spw selector the
    // transform could not parse, failed as execution failures, unnamed.
    let temp = tempfile::tempdir().unwrap();
    let ms = create_msexplore_fixture_ms(temp.path());
    let ms_arg = ms.to_str().unwrap();
    let before = snapshot(&ms);
    let out = temp.path().join("out.ms");
    let out_arg = out.to_str().unwrap();
    let request = temp.path().join("request.json");
    let binary = env!("CARGO_BIN_EXE_mstransform");

    for surface in ["mstransform", "split"] {
        for (values, parameter) in [
            (
                serde_json::json!({"vis": ms_arg, "outputvis": ms_arg}),
                "outputvis",
            ),
            (
                serde_json::json!({"vis": ms_arg, "outputvis": out_arg, "spw": "0:bogus"}),
                "spw",
            ),
        ] {
            let mut line = vec![surface.to_string(), "--no-save-last".to_string()];
            for (name, value) in values.as_object().unwrap() {
                line.extend([format!("--{name}"), value.as_str().unwrap().to_string()]);
            }
            let refused = Command::new(binary).args(&line).output().unwrap();
            assert_eq!(refused.status.code(), Some(2), "{line:?}");
            let stderr = String::from_utf8_lossy(&refused.stderr);
            assert!(stderr.contains(parameter), "{line:?}: {stderr}");

            let run = serde_json::json!({"surface": surface, "values": values});
            std::fs::write(&request, run.to_string()).unwrap();
            let replied = run_json(binary, &request);
            assert_eq!(replied.status.code(), Some(2), "{run}");
            let reply: serde_json::Value = serde_json::from_slice(&replied.stdout).unwrap();
            assert_eq!(reply["kind"], "validation", "{reply}");
            assert_eq!(reply["parameter"], parameter, "{reply}");

            assert_eq!(snapshot(&ms), before, "{surface} {parameter}");
            assert!(!out.exists(), "{surface} {parameter}");
        }
    }
}

fn run_json(binary: &str, request: &Path) -> Output {
    run(binary, &["--json-run", request.to_str().unwrap()])
}

#[test]
fn refused_arguments_change_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let ms = create_msexplore_fixture_ms(temp.path());
    let ms_arg = ms.to_str().unwrap();
    let before = snapshot(&ms);
    let out = temp.path().join("out.ms");

    for (binary, args) in [
        (
            env!("CARGO_BIN_EXE_flagdata"),
            vec!["--vis", ms_arg, "--mode", "manual", "--spw", "0:99~100"],
        ),
        (
            env!("CARGO_BIN_EXE_mstransform"),
            vec![
                "--vis",
                ms_arg,
                "--outputvis",
                out.to_str().unwrap(),
                "--spw",
                "9",
            ],
        ),
    ] {
        let output = run(binary, &args);
        assert!(!output.status.success(), "{binary} {args:?} succeeded");
        assert_eq!(snapshot(&ms), before, "{binary} {args:?}");
    }
    assert!(!flag_versions(&ms).exists());
    assert!(!out.exists());
}
