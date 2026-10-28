// SPDX-License-Identifier: LGPL-3.0-or-later

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use casa_task_runtime::{ParameterRuntime, TaskCommandLine};
use tempfile::{NamedTempFile, tempdir};

use super::*;
use crate::task_contract::{ImportVlaScanTaskRequest, ImportVlaTaskRequest};
use crate::{AntennaNameScheme, BandName};

fn args(values: &[&str]) -> Vec<OsString> {
    values.iter().map(OsString::from).collect()
}

/// A command line as the front end resolves it, read into the options.
fn options(line: &[&str]) -> Result<ImportVlaOptions, String> {
    let bundle = casa_provider_contracts::builtin_surface_bundle("importvla").unwrap();
    let line = TaskCommandLine::parse(&bundle, &args(line))?;
    let session = ParameterRuntime::default()
        .open_session(line.open_session_request(bundle))
        .map_err(|error| error.to_string())?;
    options_from(&TaskInvocation::new(&session, Path::new("."))).map_err(|error| error.message)
}

fn sample_report() -> ImportReport {
    ImportReport {
        vis: PathBuf::from("/tmp/out.ms"),
        logical_records_seen: 10,
        logical_records_imported: 8,
        logical_records_skipped: 2,
        main_rows_written: 16,
    }
}

fn physical_block(current: u16, total: u16, payload: &[u8]) -> [u8; crate::PHYSICAL_RECORD_SIZE] {
    let mut block = [0_u8; crate::PHYSICAL_RECORD_SIZE];
    block[0..2].copy_from_slice(&current.to_be_bytes());
    block[2..4].copy_from_slice(&total.to_be_bytes());
    block[4..4 + payload.len()].copy_from_slice(payload);
    block
}

fn logical_record_bytes(length_bytes: usize, revision: u16, obs_day: u32) -> Vec<u8> {
    let mut bytes = vec![0_u8; length_bytes];
    bytes[0..4].copy_from_slice(&((length_bytes / 2) as i32).to_be_bytes());
    bytes[2 * 3..2 * 3 + 2].copy_from_slice(&revision.to_be_bytes());
    bytes[2 * 4..2 * 4 + 4].copy_from_slice(&obs_day.to_be_bytes());
    bytes[2 * 17..2 * 17 + 2].copy_from_slice(&27_u16.to_be_bytes());
    bytes
}

fn synthetic_archive_file() -> NamedTempFile {
    let logical = logical_record_bytes(64, 26, 49_999);
    let block = physical_block(1, 1, &logical);
    let file = NamedTempFile::new().expect("temp archive");
    fs::write(file.path(), block).expect("write temp archive");
    file
}

struct CurrentDirGuard(PathBuf);

impl Drop for CurrentDirGuard {
    fn drop(&mut self) {
        let _ = std::env::set_current_dir(&self.0);
    }
}

fn cwd_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn with_temp_cwd<T>(f: impl FnOnce(&Path) -> T) -> T {
    let _lock = cwd_lock().lock().expect("cwd lock");
    let restore = CurrentDirGuard(std::env::current_dir().expect("original cwd"));
    let temp = tempdir().expect("temp cwd");
    std::env::set_current_dir(temp.path()).expect("set temp cwd");
    let result = f(temp.path());
    drop(restore);
    result
}

#[test]
fn command_schema_describes_public_importvla_surface() {
    let schema = command_schema("importvla");
    assert_eq!(schema.command_id, "importvla");
    assert_eq!(schema.display_name, "ImportVLA");
    assert!(schema.render_help().contains("--archivefiles"));
    assert!(schema.render_help().contains("--vis"));
}

#[test]
fn the_front_end_answers_the_machine_actions_and_help() {
    let front_end = task_front_end();
    let schema = front_end.dispatch(&args(&["--json-schema"]));
    assert_eq!(schema.code, 0, "{schema:?}");
    let schema: serde_json::Value = serde_json::from_str(&schema.stdout).unwrap();
    assert_eq!(
        schema["parameter_surfaces"][0]["surface"]["id"],
        "importvla"
    );
    assert!(schema["semantic"]["front_end"]["reply_schema"].is_object());
    let help = front_end.dispatch(&args(&["--help"]));
    assert!(
        help.stdout.contains("--json-run <SOURCE>"),
        "{}",
        help.stdout
    );
    assert!(help.stdout.contains("--archivefiles"), "{}", help.stdout);
}

#[test]
fn options_follow_the_command_line() {
    let implicit = options(&["--archivefiles", "a.exp,b.xp1"]).expect("implicit vis");
    assert_eq!(implicit.archivefiles.len(), 2);
    assert!(implicit.vis.is_none());

    let every = options(&[
        "--archivefiles",
        "one.xp1,two.xp5, ,three.exp",
        "--vis",
        "out.ms",
        "--bandname",
        "Ka",
        "--frequencytol",
        "1.25MHz",
        "--project",
        "proj",
        "--starttime",
        "2020/01/01",
        "--stoptime",
        "2020/01/02",
        "--autocorr",
        "--no-applytsys",
        "--keepblanks",
        "--evlabands",
        "--antnamescheme",
        "old",
    ])
    .expect("every option");
    assert_eq!(
        every.archivefiles,
        vec![
            PathBuf::from("one.xp1"),
            PathBuf::from("two.xp5"),
            PathBuf::from("three.exp")
        ]
    );
    assert_eq!(every.vis, Some(PathBuf::from("out.ms")));
    assert_eq!(every.bandname, Some(BandName::Ka));
    assert_eq!(every.frequencytol_hz, 1_250_000.0);
    assert_eq!(every.project.as_deref(), Some("proj"));
    assert_eq!(every.starttime.as_deref(), Some("2020/01/01"));
    assert_eq!(every.stoptime.as_deref(), Some("2020/01/02"));
    assert!(!every.applytsys);
    assert!(every.autocorr);
    assert_eq!(every.antnamescheme, AntennaNameScheme::Old);
    assert!(every.keepblanks);
    assert!(every.evlabands);

    // The alias spelling names the same parameter.
    let alias = options(&["--archivefile", "one.xp1"]).expect("alias");
    assert_eq!(alias.archivefiles, vec![PathBuf::from("one.xp1")]);
}

#[test]
fn repeated_archive_flags_name_every_archive() {
    // Astra's #962 F1: a repeated --archivefile kept only its last archive.
    let line = [
        "--archivefile",
        "a.exp",
        "--archivefiles",
        "b.exp,c.exp",
        "--archivefile",
        "d.exp",
        "--vis",
        "out.ms",
    ];
    let every = ["a.exp", "b.exp", "c.exp", "d.exp"];
    let resolved = options(&line).expect("repeated archives");
    assert_eq!(
        resolved.archivefiles,
        every.map(PathBuf::from).to_vec(),
        "the callback's options"
    );

    // casars run sends the binary the same list.
    let bundle = casa_provider_contracts::builtin_surface_bundle("importvla").unwrap();
    let command_line = TaskCommandLine::parse(&bundle, &args(&line)).unwrap();
    let session = ParameterRuntime::default()
        .open_session(command_line.open_session_request(bundle))
        .unwrap();
    let invocation = casa_task_runtime::project_provider_invocation(&session).unwrap();
    let request: serde_json::Value =
        serde_json::from_str(invocation.stdin.as_deref().unwrap()).unwrap();
    assert_eq!(request["values"]["archivefiles"], serde_json::json!(every));
}

#[test]
fn a_comma_separated_archive_string_names_the_same_archives_on_every_route() {
    // Astra's #962 F2: a string given through --json-run or a profile was
    // normalized as one path before it was split, so `a.exp,sub/..`
    // collapsed and a.exp was lost.
    let bundle = casa_provider_contracts::builtin_surface_bundle("importvla").unwrap();
    let both = vec![PathBuf::from("a.exp"), PathBuf::from("b.exp")];
    let from_command_line = options(&["--archivefiles", "a.exp,sub/../b.exp", "--vis", "out.ms"])
        .expect("command line")
        .archivefiles;
    assert_eq!(from_command_line, both);
    for value in [
        casa_provider_contracts::ParameterValue::String("a.exp, sub/../b.exp".into()),
        casa_provider_contracts::ParameterValue::Array(vec![
            casa_provider_contracts::ParameterValue::String("a.exp".into()),
            casa_provider_contracts::ParameterValue::String("sub/../b.exp".into()),
        ]),
    ] {
        let mut session =
            casa_task_runtime::ParameterSession::defaults(bundle.clone()).expect("session");
        session
            .set("archivefiles", value.clone())
            .expect("set archivefiles");
        let resolved = options_from(&TaskInvocation::new(&session, Path::new(".")))
            .expect("options")
            .archivefiles;
        assert_eq!(resolved, both, "{value:?}");
    }
}

#[test]
fn refused_options_name_the_parameter() {
    assert!(
        options(&["--archivefiles", "a.xp1", "--bogus"])
            .unwrap_err()
            .contains("--bogus")
    );
    let front_end = task_front_end();
    let refused = front_end.dispatch(&args(&[
        "--archivefiles",
        "a.xp1",
        "--frequencytol",
        "fast",
        "--no-save-last",
    ]));
    assert_eq!(refused.code, 2, "{refused:?}");
    assert!(
        refused
            .stderr
            .iter()
            .any(|line| line.contains("frequencytol")),
        "{refused:?}"
    );
}

#[test]
fn shared_request_reader_reads_files_and_reports_missing_paths() {
    let file = NamedTempFile::new().expect("temp json");
    fs::write(file.path(), "{\"kind\":\"scan\",\"request\":{}}").expect("write temp json");
    let payload = casa_task_runtime::read_task_request(file.path().to_str().expect("utf8 path"))
        .expect("read file");
    assert!(payload.contains("\"kind\":\"scan\""));

    let missing =
        casa_task_runtime::read_task_request("/definitely/missing/importvla.json").unwrap_err();
    assert!(
        missing
            .to_string()
            .contains("failed to read JSON request from")
    );
}

#[test]
fn import_text_reports_the_import() {
    let text = import_text(&sample_report());
    assert!(text.starts_with("importvla disk import\nvis: /tmp/out.ms\n"));
    assert!(text.contains("logical records skipped: 2\n"));
    assert!(text.ends_with("main rows written: 16\n"));
}

#[test]
fn a_scan_request_executes_through_the_provider_request() {
    let archive = synthetic_archive_file();
    let request = ImportVlaTaskRequest::Scan(ImportVlaScanTaskRequest {
        options: ImportVlaOptions {
            archivefiles: vec![archive.path().to_path_buf()],
            ..ImportVlaOptions::default()
        },
    });
    let payload = serde_json::to_string(&request).expect("serialize scan request");
    let decoded: ImportVlaTaskRequest = serde_json::from_str(&payload).expect("deserialize");
    assert!(matches!(
        decoded.execute().expect("scan"),
        ImportVlaTaskResult::Scan(_)
    ));
}

#[test]
fn run_import_covers_the_import_execution_branch() {
    let path = synthetic_archive_file().path().to_path_buf();
    let temp = tempdir().expect("temp import dir");
    let options = ImportVlaOptions {
        archivefiles: vec![path],
        vis: Some(temp.path().join("import.ms")),
        ..ImportVlaOptions::default()
    };
    assert!(run(&options).is_err());
}

#[test]
fn run_without_vis_cleans_up_failed_default_output_path() {
    let path = synthetic_archive_file().path().to_path_buf();
    let result = with_temp_cwd(|temp| {
        let expected_vis = temp.join(
            path.file_stem()
                .expect("archive stem")
                .to_string_lossy()
                .to_string()
                + ".ms",
        );
        let result = run(&ImportVlaOptions {
            archivefiles: vec![path.clone()],
            ..ImportVlaOptions::default()
        });
        assert!(
            !expected_vis.exists(),
            "failed implicit imports should clean up the derived temp cwd output"
        );
        result
    });

    let error = result.expect_err("synthetic archive import should fail later in import");
    assert!(!error.to_string().contains("scan"));
    assert!(!error.to_string().contains("required for import"));
}

#[test]
fn run_without_vis_keeps_an_existing_default_output() {
    let path = synthetic_archive_file().path().to_path_buf();
    let error = with_temp_cwd(|temp| {
        let existing = temp.join(
            path.file_stem()
                .expect("archive stem")
                .to_string_lossy()
                .to_string()
                + ".ms",
        );
        fs::create_dir(&existing).expect("existing default output");
        fs::write(existing.join("table.dat"), b"first product").expect("product file");
        let result = run(&ImportVlaOptions {
            archivefiles: vec![path.clone()],
            ..ImportVlaOptions::default()
        });
        assert_eq!(
            fs::read(existing.join("table.dat")).expect("product kept"),
            b"first product"
        );
        result
    })
    .expect_err("an existing output is refused");
    assert!(error.to_string().contains("already exists"), "{error}");
}
