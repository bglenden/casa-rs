// SPDX-License-Identifier: LGPL-3.0-or-later
//! Schema-backed CLI support for disk-based `importvla`.

use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::json;

use crate::task_contract::{ImportVlaTaskRequest, importvla_task_schema_bundle};
use crate::{
    AntennaNameScheme, BandName, ImportVlaOptions, VlaError,
    import_archive_files_to_measurement_set_from_options,
};
pub use casa_ms::presentation::UiCommandSchema;

#[derive(Debug)]
enum CliAction {
    Help,
    Run {
        options: ImportVlaOptions,
        json: bool,
    },
}

/// Parse environment arguments, run `importvla`, and return a process exit code.
pub fn run_env(program_name: &str) -> i32 {
    let (logging_guard, args) =
        match casa_logging::init_global_from_env_and_args(std::env::args_os().skip(1)) {
            Ok((guard, args)) => (guard, args),
            Err(error) => {
                eprintln!("Error: failed to initialize logging: {error}");
                return 1;
            }
        };
    tracing::info!("importvla started");
    let code = run_with_cli_args(program_name, args);
    if code == 0 {
        tracing::info!("importvla completed");
    } else {
        tracing::error!(
            casa.priority = "SEVERE",
            exit_code = code,
            "importvla failed"
        );
    }
    if let Err(error) = logging_guard.flush() {
        eprintln!("Error: failed to flush logging: {error}");
        return 1;
    }
    code
}

/// Run `importvla` with already-filtered CLI arguments.
pub fn run_with_cli_args(program_name: &str, args: impl IntoIterator<Item = OsString>) -> i32 {
    let args = args.into_iter().collect::<Vec<_>>();
    let host = casa_task_runtime::TaskCliHost::new(
        importvla_task_schema_bundle(),
        |request: ImportVlaTaskRequest| request.execute(),
    );
    match host.dispatch(&args) {
        Ok(Some(output)) => {
            println!("{output}");
            return 0;
        }
        Ok(None) => {}
        Err(error) => {
            eprintln!("Error: {error}");
            return error.exit_code();
        }
    }
    let schema = command_schema(program_name);
    match parse_args(args) {
        Ok(CliAction::Help) => {
            print!("{}", render_help(&schema));
            0
        }
        Ok(CliAction::Run { options, json }) => match run(options, json) {
            Ok(()) => 0,
            Err(error) => {
                eprintln!("Error: {error}");
                1
            }
        },
        Err(error) => {
            eprintln!("Error: {error}\n");
            eprintln!("{}", render_help(&schema));
            1
        }
    }
}

/// Build the machine-readable command schema for `importvla`.
pub fn command_schema(program_name: &str) -> UiCommandSchema {
    let bundle = casa_provider_contracts::builtin_surface_bundle("importvla")
        .expect("built-in importvla parameter surface must remain valid");
    let mut schema: UiCommandSchema =
        serde_json::from_value(casa_provider_contracts::project_ui_form(&bundle))
            .expect("canonical importvla UI projection must match UiCommandSchema");
    schema.invocation_name = program_name.to_string();
    schema.usage = format!("{program_name} [parameters]");
    schema
}
fn parse_args(args: impl IntoIterator<Item = OsString>) -> Result<CliAction, VlaError> {
    let args = args.into_iter().collect::<Vec<_>>();
    let string_args = args
        .iter()
        .map(|value| {
            value.to_str().ok_or_else(|| VlaError::InvalidArgument {
                argument: "argv",
                message: "non-utf8 argument".to_string(),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    if string_args
        .iter()
        .any(|arg| *arg == "-h" || *arg == "--help")
    {
        return Ok(CliAction::Help);
    }
    parse_run_args(args)
}

fn parse_run_args(args: Vec<OsString>) -> Result<CliAction, VlaError> {
    let mut args = args.into_iter();
    let mut options = ImportVlaOptions::default();
    let mut json = false;

    while let Some(arg) = args.next() {
        let Some(arg) = arg.to_str() else {
            return Err(VlaError::InvalidArgument {
                argument: "argv",
                message: "non-utf8 argument".to_string(),
            });
        };
        match arg {
            "--json" => json = true,
            "--archivefile" => {
                let value = next_value(&mut args, "archivefile")?;
                options.archivefiles.push(PathBuf::from(value));
            }
            "--archivefiles" => {
                let value = next_value(&mut args, "archivefiles")?;
                append_archivefiles(&mut options, &value);
            }
            "--vis" => {
                options.vis = Some(PathBuf::from(next_value(&mut args, "vis")?));
            }
            "--bandname" => {
                options.bandname = Some(next_value(&mut args, "bandname")?.parse()?);
            }
            "--frequencytol" => {
                let value = next_value(&mut args, "frequencytol")?;
                options.frequencytol_hz = ImportVlaOptions::parse_frequencytol(&value)?;
            }
            "--project" => {
                options.project = Some(next_value(&mut args, "project")?.to_string());
            }
            "--starttime" => {
                options.starttime = Some(next_value(&mut args, "starttime")?.to_string());
            }
            "--stoptime" => {
                options.stoptime = Some(next_value(&mut args, "stoptime")?.to_string());
            }
            "--autocorr" => options.autocorr = true,
            "--no-applytsys" => options.applytsys = false,
            "--applytsys" => options.applytsys = true,
            "--keepblanks" => options.keepblanks = true,
            "--evlabands" => options.evlabands = true,
            "--antnamescheme" => {
                options.antnamescheme = next_value(&mut args, "antnamescheme")?.parse()?;
            }
            other => {
                return Err(VlaError::InvalidArgument {
                    argument: "argv",
                    message: format!("unrecognized argument `{other}`"),
                });
            }
        }
    }

    options.require_archivefiles()?;
    Ok(CliAction::Run { options, json })
}

fn next_value(
    args: &mut impl Iterator<Item = OsString>,
    argument: &'static str,
) -> Result<String, VlaError> {
    let value = args.next().ok_or_else(|| VlaError::InvalidArgument {
        argument,
        message: "missing value".to_string(),
    })?;
    value.into_string().map_err(|_| VlaError::InvalidArgument {
        argument,
        message: "non-utf8 value".to_string(),
    })
}

fn append_archivefiles(options: &mut ImportVlaOptions, value: &str) {
    for item in value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
    {
        options.archivefiles.push(PathBuf::from(item));
    }
}

fn run(options: ImportVlaOptions, json_output: bool) -> Result<(), VlaError> {
    tracing::info!(
        archive_count = options.archivefiles.len(),
        vis = ?options.vis,
        json_output,
        "importvla run started"
    );
    let cleanup_vis = if options.vis.is_none() {
        Some(options.effective_vis_for_import()?)
    } else {
        None
    };
    let report = match import_archive_files_to_measurement_set_from_options(&options) {
        Ok(report) => report,
        Err(error) => {
            tracing::error!(
                casa.priority = "SEVERE",
                error = %error,
                cleanup_vis = ?cleanup_vis,
                "importvla import failed"
            );
            if let Some(path) = cleanup_vis.as_deref().filter(|path| path.exists()) {
                cleanup_failed_import_output(path).map_err(|cleanup_error| {
                    VlaError::import(format!(
                        "{error}; cleanup failed for {}: {cleanup_error}",
                        path.display()
                    ))
                })?;
            }
            return Err(error);
        }
    };
    if json_output {
        let payload = json!({
            "mode": "disk-import",
            "options": render_options_json(&options),
            "report": report,
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&payload).map_err(|error| VlaError::InvalidArgument {
                argument: "json",
                message: error.to_string(),
            })?
        );
    } else {
        render_import_text(&report);
    }
    tracing::info!(
        archive_count = options.archivefiles.len(),
        vis = ?report.vis,
        rows = report.main_rows_written,
        "importvla run completed"
    );
    Ok(())
}

fn cleanup_failed_import_output(path: &Path) -> Result<(), std::io::Error> {
    if path.is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    }
}

fn render_options_json(options: &ImportVlaOptions) -> serde_json::Value {
    json!({
        "archivefiles": options.archivefiles,
        "vis": options.vis,
        "bandname": options.bandname.map(BandName::as_task_token),
        "frequencytol_hz": options.frequencytol_hz,
        "project": options.project,
        "starttime": options.starttime,
        "stoptime": options.stoptime,
        "applytsys": options.applytsys,
        "autocorr": options.autocorr,
        "antnamescheme": match options.antnamescheme {
            AntennaNameScheme::New => "new",
            AntennaNameScheme::Old => "old",
        },
        "keepblanks": options.keepblanks,
        "evlabands": options.evlabands,
    })
}

#[cfg_attr(not(test), allow(dead_code))]
fn render_text(options: &ImportVlaOptions, summary: &crate::ArchiveSummary) {
    println!("importvla disk scan");
    if let Some(vis) = &options.vis {
        println!("planned vis: {}", vis.display());
    }
    println!("archive files: {}", options.archivefiles.len());
    println!("logical records: {}", summary.logical_records);
    println!("logical bytes: {}", summary.logical_bytes);
    println!();
    for file in &summary.files {
        println!("file: {}", file.path.display());
        println!("  logical records: {}", file.logical_records);
        println!("  logical bytes: {}", file.logical_bytes);
        if let Some((min, max)) = file.revision_range {
            println!("  revision range: {min}..{max}");
        }
        if let Some((min, max)) = file.obs_day_range {
            println!("  obs day range: {min}..{max}");
        }
        println!("  max antennas: {}", file.max_antennas);
        let cda_line = file
            .used_cda_histogram
            .iter()
            .map(|(used, count)| format!("{used}=>{count}"))
            .collect::<Vec<_>>()
            .join(", ");
        println!("  used CDA histogram: {cda_line}");
        println!();
    }
    println!("note: this first wave scans disk archive files and reassembles logical records.");
}

fn render_import_text(report: &crate::ImportReport) {
    println!("importvla disk import");
    println!("vis: {}", report.vis.display());
    println!("logical records seen: {}", report.logical_records_seen);
    println!(
        "logical records imported: {}",
        report.logical_records_imported
    );
    println!(
        "logical records skipped: {}",
        report.logical_records_skipped
    );
    println!("main rows written: {}", report.main_rows_written);
}

fn render_help(schema: &UiCommandSchema) -> String {
    format!(
        "{}\n\n{}\n\nCompatibility:\n  --archivefile PATH       Add one VLA export file on disk (repeatable CLI alias)\n  --json                   Emit legacy text-command output as JSON\n",
        schema.render_help(),
        casa_task_runtime::task_cli_machine_help("ImportVlaTaskRequest")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_contract::ImportVlaScanTaskRequest;
    use crate::{ArchiveFileSummary, ArchiveSummary, ImportReport};
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::sync::{Mutex, OnceLock};
    use tempfile::{NamedTempFile, tempdir};

    fn sample_options() -> ImportVlaOptions {
        ImportVlaOptions {
            archivefiles: vec![PathBuf::from("/tmp/a.exp"), PathBuf::from("/tmp/b.xp1")],
            vis: Some(PathBuf::from("/tmp/out.ms")),
            bandname: Some(BandName::Ka),
            frequencytol_hz: 12.5,
            project: Some("proj".to_string()),
            starttime: Some("start".to_string()),
            stoptime: Some("stop".to_string()),
            applytsys: false,
            autocorr: true,
            antnamescheme: AntennaNameScheme::Old,
            keepblanks: true,
            evlabands: true,
        }
    }

    fn sample_summary() -> ArchiveSummary {
        let mut histogram = BTreeMap::new();
        histogram.insert(1, 2);
        histogram.insert(3, 4);
        ArchiveSummary {
            vis: Some(PathBuf::from("/tmp/out.ms")),
            files: vec![ArchiveFileSummary {
                path: PathBuf::from("/tmp/a.exp"),
                logical_records: 7,
                logical_bytes: 1024,
                revision_range: Some((3, 5)),
                obs_day_range: Some((10, 20)),
                max_antennas: 27,
                used_cda_histogram: histogram,
            }],
            logical_records: 7,
            logical_bytes: 1024,
        }
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

    fn physical_block(
        current: u16,
        total: u16,
        payload: &[u8],
    ) -> [u8; crate::PHYSICAL_RECORD_SIZE] {
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
    fn parse_args_recognizes_machine_actions() {
        assert_eq!(
            casa_task_runtime::parse_task_cli_action(&[OsString::from("--json-schema")]).unwrap(),
            Some(casa_task_runtime::TaskCliAction::JsonSchema)
        );
        assert_eq!(
            casa_task_runtime::parse_task_cli_action(&[
                OsString::from("--json-run"),
                OsString::from("-"),
            ])
            .unwrap(),
            Some(casa_task_runtime::TaskCliAction::JsonRun("-".into()))
        );
    }

    #[test]
    fn parse_args_supports_scan_and_import_modes() {
        match parse_args([
            OsString::from("--archivefiles"),
            OsString::from("a.exp,b.xp1"),
        ])
        .expect("implicit vis args")
        {
            CliAction::Run { options, json } => {
                assert_eq!(options.archivefiles.len(), 2);
                assert!(options.vis.is_none());
                assert!(!json);
            }
            other => panic!("expected run action, got {other:?}"),
        }

        match parse_args([
            OsString::from("--archivefiles"),
            OsString::from("a.exp"),
            OsString::from("--vis"),
            OsString::from("out.ms"),
            OsString::from("--json"),
        ])
        .expect("import args")
        {
            CliAction::Run { options, json } => {
                assert_eq!(options.vis, Some(PathBuf::from("out.ms")));
                assert!(json);
            }
            other => panic!("expected run action, got {other:?}"),
        }
    }

    #[test]
    fn parse_run_args_covers_supported_option_branches() {
        match parse_run_args(vec![
            OsString::from("--json"),
            OsString::from("--archivefile"),
            OsString::from("one.xp1"),
            OsString::from("--archivefiles"),
            OsString::from("two.xp5, ,three.exp"),
            OsString::from("--vis"),
            OsString::from("out.ms"),
            OsString::from("--bandname"),
            OsString::from("Ka"),
            OsString::from("--frequencytol"),
            OsString::from("1.25MHz"),
            OsString::from("--project"),
            OsString::from("proj"),
            OsString::from("--starttime"),
            OsString::from("2020/01/01"),
            OsString::from("--stoptime"),
            OsString::from("2020/01/02"),
            OsString::from("--autocorr"),
            OsString::from("--no-applytsys"),
            OsString::from("--keepblanks"),
            OsString::from("--evlabands"),
            OsString::from("--antnamescheme"),
            OsString::from("old"),
        ])
        .expect("parse run args")
        {
            CliAction::Run { options, json } => {
                assert!(json);
                assert_eq!(
                    options.archivefiles,
                    vec![
                        PathBuf::from("one.xp1"),
                        PathBuf::from("two.xp5"),
                        PathBuf::from("three.exp")
                    ]
                );
                assert_eq!(options.vis, Some(PathBuf::from("out.ms")));
                assert_eq!(options.bandname, Some(BandName::Ka));
                assert_eq!(options.frequencytol_hz, 1_250_000.0);
                assert_eq!(options.project.as_deref(), Some("proj"));
                assert_eq!(options.starttime.as_deref(), Some("2020/01/01"));
                assert_eq!(options.stoptime.as_deref(), Some("2020/01/02"));
                assert!(!options.applytsys);
                assert!(options.autocorr);
                assert_eq!(options.antnamescheme, AntennaNameScheme::Old);
                assert!(options.keepblanks);
                assert!(options.evlabands);
            }
            other => panic!("expected run action, got {other:?}"),
        }
    }

    #[test]
    fn parse_args_and_helpers_report_expected_errors() {
        assert!(matches!(
            parse_run_args(vec![
                OsString::from("--archivefiles"),
                OsString::from("a.xp1"),
                OsString::from("--bogus")
            ])
            .unwrap_err(),
            VlaError::InvalidArgument {
                argument: "argv",
                ..
            }
        ));
        assert!(matches!(
            parse_run_args(vec![OsString::from("--archivefiles")]).unwrap_err(),
            VlaError::InvalidArgument {
                argument: "archivefiles",
                ..
            }
        ));
        assert!(matches!(
            next_value(&mut std::iter::empty(), "archivefile").unwrap_err(),
            VlaError::InvalidArgument {
                argument: "archivefile",
                ..
            }
        ));
    }

    #[test]
    fn append_archivefiles_and_render_options_json_cover_legacy_shapes() {
        let mut options = ImportVlaOptions::default();
        append_archivefiles(&mut options, "one.xp1, , two.xp5 ,,three.exp");
        assert_eq!(
            options.archivefiles,
            vec![
                PathBuf::from("one.xp1"),
                PathBuf::from("two.xp5"),
                PathBuf::from("three.exp")
            ]
        );

        let payload = render_options_json(&sample_options());
        assert_eq!(
            payload["archivefiles"]
                .as_array()
                .expect("archivefiles")
                .len(),
            2
        );
        assert_eq!(payload["bandname"], "Ka");
        assert_eq!(payload["antnamescheme"], "old");
        assert_eq!(payload["applytsys"], false);
        assert_eq!(payload["evlabands"], true);
    }

    #[test]
    fn shared_request_reader_reads_files_and_reports_missing_paths() {
        let file = NamedTempFile::new().expect("temp json");
        fs::write(file.path(), "{\"kind\":\"scan\",\"request\":{}}").expect("write temp json");
        let payload =
            casa_task_runtime::read_task_request(file.path().to_str().expect("utf8 path"))
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
    fn render_text_paths_cover_scan_and_import_output_shapes() {
        let options = sample_options();
        let summary = sample_summary();
        let report = sample_report();

        render_text(&options, &summary);
        render_import_text(&report);

        let help = render_help(&command_schema("importvla-test"));
        assert!(help.contains("Machine-readable:"));
        assert!(help.contains("Compatibility:"));
    }

    #[test]
    fn run_json_request_covers_explicit_scan_operation_when_archive_is_available() {
        let archive = synthetic_archive_file();
        let path = archive.path().to_path_buf();
        let request_file = NamedTempFile::new().expect("request file");
        let request = ImportVlaTaskRequest::Scan(ImportVlaScanTaskRequest {
            options: ImportVlaOptions {
                archivefiles: vec![path],
                ..ImportVlaOptions::default()
            },
        });
        fs::write(
            request_file.path(),
            serde_json::to_string(&request).expect("serialize scan request"),
        )
        .expect("write request file");
        let host = casa_task_runtime::TaskCliHost::new(
            importvla_task_schema_bundle(),
            |request: ImportVlaTaskRequest| request.execute(),
        );
        host.dispatch(&[
            OsString::from("--json-run"),
            request_file.path().as_os_str().to_owned(),
        ])
        .expect("run json request")
        .expect("machine output");
    }

    #[test]
    fn run_import_json_covers_import_execution_branch_when_archive_is_available() {
        let path = synthetic_archive_file().path().to_path_buf();
        let temp = tempdir().expect("temp import dir");
        let options = ImportVlaOptions {
            archivefiles: vec![path],
            vis: Some(temp.path().join("import.ms")),
            ..ImportVlaOptions::default()
        };
        assert!(run(options, true).is_err());
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
            let result = run(
                ImportVlaOptions {
                    archivefiles: vec![path.clone()],
                    ..ImportVlaOptions::default()
                },
                true,
            );
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
    fn render_help_mentions_json_contract() {
        let help = render_help(&command_schema("importvla-test"));
        assert!(help.contains("--json-schema"));
        assert!(help.contains("--protocol-info"));
        assert!(help.contains("--json-run <SOURCE>"));
    }

    #[test]
    fn json_request_executes_scan_operation() {
        let request = ImportVlaTaskRequest::Scan(ImportVlaScanTaskRequest {
            options: ImportVlaOptions {
                archivefiles: vec![PathBuf::from("/tmp/a.exp")],
                ..ImportVlaOptions::default()
            },
        });
        let payload = serde_json::to_string(&request).expect("serialize request");
        let decoded: ImportVlaTaskRequest =
            serde_json::from_str(&payload).expect("deserialize request");
        match decoded {
            ImportVlaTaskRequest::Scan(scan) => {
                assert_eq!(scan.options.archivefiles, vec![PathBuf::from("/tmp/a.exp")]);
            }
            other => panic!("expected scan request, got {other:?}"),
        }
    }
}
