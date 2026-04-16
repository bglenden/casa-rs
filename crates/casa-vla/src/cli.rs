// SPDX-License-Identifier: LGPL-3.0-or-later
//! Schema-backed CLI support for disk-based `importvla`.

use std::ffi::OsString;
use std::fs;
use std::path::PathBuf;

use serde_json::json;

use crate::task_contract::{
    ImportVlaProtocolInfo, ImportVlaTaskRequest, ImportVlaTaskSchemaBundle,
};
use crate::{
    AntennaNameScheme, BandName, ImportVlaOptions, VlaError,
    import_archive_files_to_measurement_set_from_options, scan_disk_archive_files_from_options,
};
pub use casa_ms::ui_schema::{
    UiActionKind, UiArgumentParser, UiArgumentSchema, UiCommandSchema, UiValueKind,
};

const UI_SCHEMA_VERSION: u32 = 1;
const COMMAND_ID: &str = "importvla";
const DISPLAY_NAME: &str = "ImportVLA";
const CATEGORY: &str = "Import";
const SUMMARY: &str = "scan or import old VLA export archives from disk";

#[derive(Debug)]
enum CliAction {
    Help,
    UiSchema,
    JsonSchema,
    ProtocolInfo,
    JsonRun(String),
    Run {
        options: ImportVlaOptions,
        json: bool,
    },
}

/// Parse environment arguments, run `importvla`, and return a process exit code.
pub fn run_env(program_name: &str) -> i32 {
    let schema = command_schema(program_name);
    match parse_args(std::env::args_os().skip(1)) {
        Ok(CliAction::Help) => {
            print!("{}", render_help(&schema));
            0
        }
        Ok(CliAction::UiSchema) => match schema.render_json_pretty() {
            Ok(payload) => {
                print!("{payload}");
                0
            }
            Err(error) => {
                eprintln!("Error: failed to serialize --ui-schema output: {error}");
                1
            }
        },
        Ok(CliAction::JsonSchema) => {
            match serde_json::to_string_pretty(&ImportVlaTaskSchemaBundle::current()) {
                Ok(payload) => {
                    print!("{payload}");
                    0
                }
                Err(error) => {
                    eprintln!("Error: failed to serialize --json-schema output: {error}");
                    1
                }
            }
        }
        Ok(CliAction::ProtocolInfo) => {
            match serde_json::to_string_pretty(&ImportVlaProtocolInfo::current()) {
                Ok(payload) => {
                    print!("{payload}");
                    0
                }
                Err(error) => {
                    eprintln!("Error: failed to serialize --protocol-info output: {error}");
                    1
                }
            }
        }
        Ok(CliAction::JsonRun(source)) => match run_json_request(&source) {
            Ok(()) => 0,
            Err(error) => {
                eprintln!("Error: {error}");
                1
            }
        },
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
    UiCommandSchema {
        schema_version: UI_SCHEMA_VERSION,
        command_id: COMMAND_ID.to_string(),
        invocation_name: program_name.to_string(),
        display_name: DISPLAY_NAME.to_string(),
        category: CATEGORY.to_string(),
        summary: SUMMARY.to_string(),
        usage: format!("{program_name} --archivefiles PATH[,PATH...] [options]"),
        arguments: vec![
            option_argument(OptionArgumentConfig {
                id: "archivefiles",
                label: "Archive Files",
                order: 0,
                flags: &["--archivefiles", "--archivefile"],
                metavar: "PATH[,PATH...]",
                value_kind: UiValueKind::Path,
                default: None,
                choices: &[],
                help: "VLA export files on disk; repeat `--archivefile` or pass a comma-separated list",
                group: "Input",
                required: true,
                advanced: false,
                hidden_in_tui: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "vis",
                label: "Output MeasurementSet",
                order: 1,
                flags: &["--vis"],
                metavar: "PATH",
                value_kind: UiValueKind::Path,
                default: None,
                choices: &[],
                help: "Output MeasurementSet path; leave blank to run a scan only",
                group: "Output",
                required: false,
                advanced: false,
                hidden_in_tui: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "bandname",
                label: "Band",
                order: 2,
                flags: &["--bandname"],
                metavar: "NAME",
                value_kind: UiValueKind::Choice,
                default: None,
                choices: &["4", "P", "L", "S", "C", "X", "U", "K", "Ka", "Q"],
                help: "Optional CASA `importvla` band selector",
                group: "Selection",
                required: false,
                advanced: false,
                hidden_in_tui: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "frequencytol",
                label: "Frequency Tolerance",
                order: 3,
                flags: &["--frequencytol"],
                metavar: "QUANTITY",
                value_kind: UiValueKind::String,
                default: Some("150000.0Hz"),
                choices: &[],
                help: "Spectral-window matching tolerance in CASA quantity syntax",
                group: "Selection",
                required: false,
                advanced: false,
                hidden_in_tui: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "project",
                label: "Project",
                order: 4,
                flags: &["--project"],
                metavar: "NAME",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Optional project-code selector",
                group: "Selection",
                required: false,
                advanced: true,
                hidden_in_tui: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "starttime",
                label: "Start Time",
                order: 5,
                flags: &["--starttime"],
                metavar: "VALUE",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Optional inclusive CASA-style start time",
                group: "Selection",
                required: false,
                advanced: true,
                hidden_in_tui: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "stoptime",
                label: "Stop Time",
                order: 6,
                flags: &["--stoptime"],
                metavar: "VALUE",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
                help: "Optional inclusive CASA-style stop time",
                group: "Selection",
                required: false,
                advanced: true,
                hidden_in_tui: false,
            }),
            toggle_argument(ToggleArgumentConfig {
                id: "applytsys",
                label: "Apply Tsys",
                order: 7,
                help: "Apply nominal sensitivity scaling",
                true_flags: &["--applytsys"],
                false_flags: &["--no-applytsys"],
                default: true,
                group: "Selection",
                advanced: false,
                hidden_in_tui: false,
            }),
            toggle_argument(ToggleArgumentConfig {
                id: "autocorr",
                label: "Keep Autocorrelations",
                order: 8,
                help: "Keep auto-correlation rows",
                true_flags: &["--autocorr"],
                false_flags: &[],
                default: false,
                group: "Selection",
                advanced: false,
                hidden_in_tui: false,
            }),
            option_argument(OptionArgumentConfig {
                id: "antnamescheme",
                label: "Antenna Naming",
                order: 9,
                flags: &["--antnamescheme"],
                metavar: "MODE",
                value_kind: UiValueKind::Choice,
                default: Some("new"),
                choices: &["new", "old"],
                help: "Use CASA `new` or `old` antenna names",
                group: "Selection",
                required: false,
                advanced: true,
                hidden_in_tui: false,
            }),
            toggle_argument(ToggleArgumentConfig {
                id: "keepblanks",
                label: "Keep Blank Sources",
                order: 10,
                help: "Preserve blank source names",
                true_flags: &["--keepblanks"],
                false_flags: &[],
                default: false,
                group: "Selection",
                advanced: true,
                hidden_in_tui: false,
            }),
            toggle_argument(ToggleArgumentConfig {
                id: "evlabands",
                label: "Use EVLA Bands",
                order: 11,
                help: "Use EVLA band centers and bandwidths for band selection",
                true_flags: &["--evlabands"],
                false_flags: &[],
                default: false,
                group: "Selection",
                advanced: true,
                hidden_in_tui: false,
            }),
            toggle_argument(ToggleArgumentConfig {
                id: "json",
                label: "JSON Output",
                order: 12,
                help: "Emit JSON instead of plain text",
                true_flags: &["--json"],
                false_flags: &[],
                default: false,
                group: "Output",
                advanced: true,
                hidden_in_tui: true,
            }),
            action_argument(
                "help",
                "Help",
                13,
                &["-h", "--help"],
                UiActionKind::Help,
                "Show command help",
                "Actions",
            ),
            action_argument(
                "ui_schema",
                "UI Schema",
                14,
                &["--ui-schema"],
                UiActionKind::UiSchema,
                "Emit the machine-readable launcher schema",
                "Actions",
            ),
        ],
        managed_output: None,
    }
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
    if string_args.contains(&"--ui-schema") {
        return Ok(CliAction::UiSchema);
    }
    if string_args.contains(&"--json-schema") {
        return Ok(CliAction::JsonSchema);
    }
    if string_args.contains(&"--protocol-info") {
        return Ok(CliAction::ProtocolInfo);
    }
    if let Some(index) = string_args.iter().position(|arg| *arg == "--json-run") {
        let source = string_args
            .get(index + 1)
            .ok_or_else(|| VlaError::InvalidArgument {
                argument: "json-run",
                message: "missing value".to_string(),
            })?;
        return Ok(CliAction::JsonRun((*source).to_string()));
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

fn run_json_request(source: &str) -> Result<(), String> {
    let payload = read_json_source(source)?;
    let request = serde_json::from_str::<ImportVlaTaskRequest>(&payload)
        .map_err(|error| format!("parse importvla task request: {error}"))?;
    let result = request.execute()?;
    serde_json::to_string_pretty(&result)
        .map(|json| {
            println!("{json}");
        })
        .map_err(|error| format!("serialize importvla task result: {error}"))
}

fn read_json_source(source: &str) -> Result<String, String> {
    if source == "-" {
        let mut stdin = std::io::stdin();
        let mut payload = String::new();
        use std::io::Read as _;
        stdin
            .read_to_string(&mut payload)
            .map_err(|error| format!("read importvla task request from stdin: {error}"))?;
        Ok(payload)
    } else {
        fs::read_to_string(source)
            .map_err(|error| format!("read importvla task request from {source}: {error}"))
    }
}

fn run(options: ImportVlaOptions, json_output: bool) -> Result<(), VlaError> {
    if options.vis.is_some() {
        let report = import_archive_files_to_measurement_set_from_options(&options)?;
        if json_output {
            let payload = json!({
                "mode": "disk-import",
                "options": render_options_json(&options),
                "report": report,
            });
            println!(
                "{}",
                serde_json::to_string_pretty(&payload).map_err(|error| {
                    VlaError::InvalidArgument {
                        argument: "json",
                        message: error.to_string(),
                    }
                })?
            );
        } else {
            render_import_text(&options, &report);
        }
    } else {
        let summary = scan_disk_archive_files_from_options(&options)?;
        if json_output {
            let payload = json!({
                "mode": "disk-scan",
                "options": render_options_json(&options),
                "summary": summary,
            });
            println!(
                "{}",
                serde_json::to_string_pretty(&payload).map_err(|error| {
                    VlaError::InvalidArgument {
                        argument: "json",
                        message: error.to_string(),
                    }
                })?
            );
        } else {
            render_text(&options, &summary);
        }
    }
    Ok(())
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

fn render_import_text(options: &ImportVlaOptions, report: &crate::ImportReport) {
    println!("importvla disk import");
    if let Some(vis) = &options.vis {
        println!("vis: {}", vis.display());
    }
    println!("archive files: {}", options.archivefiles.len());
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
        "{}\n\nMachine-readable:\n  --ui-schema              Emit the launcher/TUI schema\n  --json-schema            Emit the canonical importvla task JSON schema\n  --protocol-info          Emit the importvla task protocol descriptor\n  --json-run <SOURCE>      Execute one JSON ImportVlaTaskRequest from SOURCE or - for stdin\n\nCompatibility:\n  --archivefile PATH       Add one VLA export file on disk (repeatable CLI alias)\n  --json                   Emit legacy text-command output as JSON\n",
        schema.render_help()
    )
}

struct OptionArgumentConfig<'a> {
    id: &'a str,
    label: &'a str,
    order: usize,
    flags: &'a [&'a str],
    metavar: &'a str,
    value_kind: UiValueKind,
    default: Option<&'a str>,
    choices: &'a [&'a str],
    help: &'a str,
    group: &'a str,
    required: bool,
    advanced: bool,
    hidden_in_tui: bool,
}

fn option_argument(config: OptionArgumentConfig<'_>) -> UiArgumentSchema {
    UiArgumentSchema {
        id: config.id.to_string(),
        label: config.label.to_string(),
        order: config.order,
        parser: UiArgumentParser::Option {
            flags: config.flags.iter().map(ToString::to_string).collect(),
            metavar: config.metavar.to_string(),
            choices: config.choices.iter().map(ToString::to_string).collect(),
        },
        value_kind: config.value_kind,
        required: config.required,
        default: config.default.map(ToString::to_string),
        help: config.help.to_string(),
        group: config.group.to_string(),
        advanced: config.advanced,
        hidden_in_tui: config.hidden_in_tui,
    }
}

struct ToggleArgumentConfig<'a> {
    id: &'a str,
    label: &'a str,
    order: usize,
    help: &'a str,
    true_flags: &'a [&'a str],
    false_flags: &'a [&'a str],
    default: bool,
    group: &'a str,
    advanced: bool,
    hidden_in_tui: bool,
}

fn toggle_argument(config: ToggleArgumentConfig<'_>) -> UiArgumentSchema {
    UiArgumentSchema {
        id: config.id.to_string(),
        label: config.label.to_string(),
        order: config.order,
        parser: UiArgumentParser::Toggle {
            true_flags: config.true_flags.iter().map(ToString::to_string).collect(),
            false_flags: config.false_flags.iter().map(ToString::to_string).collect(),
        },
        value_kind: UiValueKind::Bool,
        required: false,
        default: Some(config.default.to_string()),
        help: config.help.to_string(),
        group: config.group.to_string(),
        advanced: config.advanced,
        hidden_in_tui: config.hidden_in_tui,
    }
}

fn action_argument(
    id: &str,
    label: &str,
    order: usize,
    flags: &[&str],
    action: UiActionKind,
    help: &str,
    group: &str,
) -> UiArgumentSchema {
    UiArgumentSchema {
        id: id.to_string(),
        label: label.to_string(),
        order,
        parser: UiArgumentParser::Action {
            flags: flags.iter().map(ToString::to_string).collect(),
            action,
        },
        value_kind: UiValueKind::None,
        required: false,
        default: None,
        help: help.to_string(),
        group: group.to_string(),
        advanced: true,
        hidden_in_tui: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_contract::ImportVlaScanTaskRequest;
    use crate::{ArchiveFileSummary, ArchiveSummary, ImportReport};
    use std::collections::BTreeMap;
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
        assert!(matches!(
            parse_args([OsString::from("--ui-schema")]).expect("ui schema action"),
            CliAction::UiSchema
        ));
        assert!(matches!(
            parse_args([OsString::from("--json-schema")]).expect("json schema action"),
            CliAction::JsonSchema
        ));
        assert!(matches!(
            parse_args([OsString::from("--protocol-info")]).expect("protocol info action"),
            CliAction::ProtocolInfo
        ));
        match parse_args([OsString::from("--json-run"), OsString::from("-")])
            .expect("json run action")
        {
            CliAction::JsonRun(source) => assert_eq!(source, "-"),
            other => panic!("expected json run action, got {other:?}"),
        }
    }

    #[test]
    fn parse_args_supports_scan_and_import_modes() {
        match parse_args([
            OsString::from("--archivefiles"),
            OsString::from("a.exp,b.xp1"),
        ])
        .expect("scan args")
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
            parse_args([OsString::from("--json-run")]).unwrap_err(),
            VlaError::InvalidArgument {
                argument: "json-run",
                ..
            }
        ));
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
    fn read_json_source_reads_files_and_reports_missing_paths() {
        let file = NamedTempFile::new().expect("temp json");
        fs::write(file.path(), "{\"kind\":\"scan\",\"request\":{}}").expect("write temp json");
        let payload =
            read_json_source(file.path().to_str().expect("utf8 path")).expect("read file");
        assert!(payload.contains("\"kind\":\"scan\""));

        let missing = read_json_source("/definitely/missing/importvla.json").unwrap_err();
        assert!(missing.contains("read importvla task request from"));
    }

    #[test]
    fn render_text_paths_cover_scan_and_import_output_shapes() {
        let options = sample_options();
        let summary = sample_summary();
        let report = sample_report();

        render_text(&options, &summary);
        render_import_text(&options, &report);

        let help = render_help(&command_schema("importvla-test"));
        assert!(help.contains("Machine-readable:"));
        assert!(help.contains("Compatibility:"));
    }

    #[test]
    fn run_scan_and_json_request_cover_real_archive_paths_when_available() {
        let archive = synthetic_archive_file();
        let path = archive.path().to_path_buf();
        let options = ImportVlaOptions {
            archivefiles: vec![path.clone()],
            ..ImportVlaOptions::default()
        };
        run(options.clone(), false).expect("run text scan");
        run(options, true).expect("run json scan");

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
        run_json_request(request_file.path().to_str().expect("utf8 request path"))
            .expect("run json request");
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
