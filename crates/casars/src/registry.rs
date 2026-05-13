// SPDX-License-Identifier: LGPL-3.0-or-later
use std::collections::BTreeSet;
use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;
use std::time::SystemTime;

use casa_calibration::CalibrationTaskSchemaBundle;
use casa_images::imexplore_ui_schema_json;
use casa_ms::MsExploreTaskSchemaBundle;
use casa_ms::ui_schema::UiCommandSchema;
use casa_vla::ImportVlaTaskSchemaBundle;
use casars_imagebrowser_protocol::ImageBrowserSessionSchemaBundle;
use casars_imager::ImagerTaskSchemaBundle;
use serde::Deserialize;

const TASK_CATALOG_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../resources/task-catalog.json"
));

#[derive(Debug, Clone)]
pub(crate) struct RegistryApp {
    pub id: String,
    pub category: String,
    pub display_name: String,
    shell_kind: AppShellKind,
    kind: RegistryAppKind,
}

#[derive(Debug, Clone)]
enum RegistryAppKind {
    Subprocess {
        binary_name: String,
        cargo_package: String,
        override_env: String,
        interaction: AppInteraction,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppInteraction {
    OneShot,
    BrowserSession(BrowserAppKind),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppShellKind {
    Inspect,
    Browser,
    Workflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BrowserAppKind {
    Table,
    Image,
}

#[derive(Debug, Deserialize)]
struct TaskCatalog {
    tasks: Vec<TaskCatalogEntry>,
}

#[derive(Debug, Clone, Deserialize)]
struct TaskCatalogEntry {
    id: String,
    category: String,
    display_name: String,
    binary_name: String,
    cargo_package: String,
    override_env: String,
    shell_kind: String,
    interaction: String,
    browser_kind: Option<String>,
    show_in_tui: bool,
}

fn task_catalog_entries() -> &'static [TaskCatalogEntry] {
    static CATALOG: OnceLock<Vec<TaskCatalogEntry>> = OnceLock::new();
    CATALOG.get_or_init(|| {
        serde_json::from_str::<TaskCatalog>(TASK_CATALOG_JSON)
            .expect("resources/task-catalog.json should parse")
            .tasks
    })
}

fn registry_app_from_catalog(entry: &TaskCatalogEntry) -> Option<RegistryApp> {
    if !entry.show_in_tui {
        return None;
    }
    let shell_kind = match entry.shell_kind.as_str() {
        "inspect" => AppShellKind::Inspect,
        "browser" => AppShellKind::Browser,
        "workflow" => AppShellKind::Workflow,
        _ => return None,
    };
    let interaction = match entry.interaction.as_str() {
        "one_shot" => AppInteraction::OneShot,
        "browser_session" => {
            let browser_kind = match entry.browser_kind.as_deref() {
                Some("table") => BrowserAppKind::Table,
                Some("image") => BrowserAppKind::Image,
                _ => return None,
            };
            AppInteraction::BrowserSession(browser_kind)
        }
        _ => return None,
    };
    Some(RegistryApp {
        id: entry.id.clone(),
        category: entry.category.clone(),
        display_name: entry.display_name.clone(),
        shell_kind,
        kind: RegistryAppKind::Subprocess {
            binary_name: entry.binary_name.clone(),
            cargo_package: entry.cargo_package.clone(),
            override_env: entry.override_env.clone(),
            interaction,
        },
    })
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedCommand {
    program: OsString,
    prefix_args: Vec<OsString>,
}

impl ResolvedCommand {
    pub(crate) fn command(&self) -> Command {
        let mut command = Command::new(&self.program);
        command.args(&self.prefix_args);
        command
    }

    #[cfg(test)]
    pub(crate) fn direct(program: impl Into<OsString>) -> Self {
        Self {
            program: program.into(),
            prefix_args: Vec::new(),
        }
    }
}

impl RegistryApp {
    pub(crate) fn load_schema(&self) -> Result<UiCommandSchema, String> {
        let schema = self.load_base_schema()?;
        project_registry_alias_schema(self, schema)
    }

    fn load_base_schema(&self) -> Result<UiCommandSchema, String> {
        match &self.kind {
            RegistryAppKind::Subprocess { binary_name, .. } => {
                if !self.has_explicit_binary_override() {
                    match binary_name.as_str() {
                        "msexplore" => {
                            return MsExploreTaskSchemaBundle::current().ui_schema_projection();
                        }
                        "calibrate" => {
                            return CalibrationTaskSchemaBundle::current().ui_schema_projection();
                        }
                        "casars-importvla" => {
                            return ImportVlaTaskSchemaBundle::current().ui_schema_projection();
                        }
                        "casars-imager" => {
                            return ImagerTaskSchemaBundle::current().ui_schema_projection();
                        }
                        "imexplore" => {
                            let ui_schema =
                                serde_json::from_str(&imexplore_ui_schema_json("imexplore")?)
                                    .map_err(|error| {
                                        format!("parse embedded imexplore schema: {error}")
                                    })?;
                            let projected = ImageBrowserSessionSchemaBundle::current(ui_schema)
                                .ui_schema_projection()?;
                            return serde_json::from_value(projected).map_err(|error| {
                                format!("parse embedded imexplore schema projection: {error}")
                            });
                        }
                        _ => {}
                    }
                }
                let resolved = self.resolve_command()?;
                if let Some(schema) = load_canonical_ui_schema(&resolved, binary_name, &self.id)? {
                    return Ok(schema);
                }
                let mut command = resolved.command();
                add_schema_args(&mut command, binary_name, &self.id);
                let output = command
                    .arg("--ui-schema")
                    .output()
                    .map_err(|error| format!("spawn {binary_name} --ui-schema: {error}"))?;
                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    return Err(format!(
                        "{binary_name} --ui-schema exited with {}: {}",
                        output.status,
                        stderr.trim()
                    ));
                }
                serde_json::from_slice(&output.stdout)
                    .map_err(|error| format!("parse {binary_name} --ui-schema output: {error}"))
            }
        }
    }

    fn has_explicit_binary_override(&self) -> bool {
        let RegistryAppKind::Subprocess {
            binary_name,
            override_env,
            ..
        } = &self.kind;
        env::var_os(override_env).is_some()
            || env::var_os(format!("CARGO_BIN_EXE_{binary_name}")).is_some()
    }

    pub(crate) fn resolve_command(&self) -> Result<ResolvedCommand, String> {
        let RegistryAppKind::Subprocess {
            binary_name,
            cargo_package,
            override_env,
            ..
        } = &self.kind;

        if let Some(path) = env::var_os(override_env) {
            return Ok(ResolvedCommand {
                program: path,
                prefix_args: Vec::new(),
            });
        }

        if let Some(path) = env::var_os(format!("CARGO_BIN_EXE_{binary_name}")) {
            return Ok(ResolvedCommand {
                program: path,
                prefix_args: Vec::new(),
            });
        }

        if let Some(path) = sibling_binary(binary_name) {
            if !self.prefers_cargo_workspace_fallback_for_stale_sibling()
                || !sibling_binary_is_stale_for_current_process(&path)
            {
                return Ok(ResolvedCommand {
                    program: path.into_os_string(),
                    prefix_args: Vec::new(),
                });
            }
        }

        let cargo = env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
        let manifest_path = workspace_manifest_path();
        Ok(ResolvedCommand {
            program: cargo,
            prefix_args: vec![
                OsString::from("run"),
                OsString::from("--manifest-path"),
                manifest_path.into_os_string(),
                OsString::from("-q"),
                OsString::from("-p"),
                OsString::from(cargo_package),
                OsString::from("--bin"),
                OsString::from(binary_name),
                OsString::from("--"),
            ],
        })
    }

    fn prefers_cargo_workspace_fallback_for_stale_sibling(&self) -> bool {
        matches!(self.id.as_str(), "msexplore" | "calibrate" | "importvla")
    }

    pub(crate) fn is_browser_session(&self) -> bool {
        matches!(
            self.kind,
            RegistryAppKind::Subprocess {
                interaction: AppInteraction::BrowserSession(_),
                ..
            }
        )
    }

    pub(crate) fn shell_kind(&self) -> AppShellKind {
        self.shell_kind
    }

    pub(crate) fn browser_kind(&self) -> Option<BrowserAppKind> {
        match self.kind {
            RegistryAppKind::Subprocess {
                interaction: AppInteraction::BrowserSession(kind),
                ..
            } => Some(kind),
            RegistryAppKind::Subprocess {
                interaction: AppInteraction::OneShot,
                ..
            } => None,
        }
    }

    pub(crate) fn browser_path_field_id(&self) -> Option<&'static str> {
        match self.browser_kind()? {
            BrowserAppKind::Table => Some("table_path"),
            BrowserAppKind::Image => Some("image_path"),
        }
    }

    pub(crate) fn ready_status_line(&self) -> &'static str {
        match (self.shell_kind, &self.kind) {
            (
                AppShellKind::Browser,
                RegistryAppKind::Subprocess {
                    interaction: AppInteraction::BrowserSession(_),
                    ..
                },
            ) => "Ready. Press r to open the browser session.",
            (AppShellKind::Inspect, _) => {
                "Ready. Press r to run and refresh the current inspection view."
            }
            (AppShellKind::Workflow, _) => "Ready. Press r to run the selected workflow stage.",
            (_, _) => "Ready. Press r to run the selected command.",
        }
    }
}

fn project_registry_alias_schema(
    app: &RegistryApp,
    schema: UiCommandSchema,
) -> Result<UiCommandSchema, String> {
    let mut value = serde_json::to_value(schema)
        .map_err(|error| format!("serialize {} schema for alias projection: {error}", app.id))?;
    apply_registry_alias_schema(app, &mut value);
    serde_json::from_value(value)
        .map_err(|error| format!("parse {} alias schema projection: {error}", app.id))
}

fn apply_registry_alias_schema(app: &RegistryApp, schema: &mut serde_json::Value) {
    let Some(alias) = registry_task_alias(app.id.as_str()) else {
        return;
    };
    let Some(object) = schema.as_object_mut() else {
        return;
    };
    object.insert("command_id".to_string(), app.id.clone().into());
    object.insert("display_name".to_string(), app.display_name.clone().into());
    object.insert("category".to_string(), app.category.clone().into());
    object.insert("summary".to_string(), alias.summary.into());
    object.insert("usage".to_string(), alias.usage.into());

    let visible = alias
        .visible_arguments
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let required = alias
        .required_arguments
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if let Some(arguments) = object
        .get_mut("arguments")
        .and_then(serde_json::Value::as_array_mut)
    {
        for extra in alias.extra_arguments {
            arguments.push(registry_extra_argument_schema(*extra));
        }
        arguments.retain_mut(|argument| {
            let Some(argument_object) = argument.as_object_mut() else {
                return false;
            };
            let id = argument_object
                .get("id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string();
            if argument_object
                .get("hidden_in_tui")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
                && argument_object.get("default").is_some()
            {
                return true;
            }
            if id == "mode"
                && let Some(mode) = alias.mode
            {
                argument_object.insert("default".to_string(), mode.into());
                argument_object.insert("hidden_in_tui".to_string(), true.into());
                argument_object.insert("required".to_string(), false.into());
                argument_object.insert("advanced".to_string(), true.into());
                argument_object.insert("order".to_string(), 0.into());
                return true;
            }
            if id == "help" || id == "ui_schema" {
                return true;
            }
            if visible.contains(id.as_str()) {
                if alias.subcommand.is_some() && id == "image_path" {
                    argument_object.insert("order".to_string(), 1.into());
                }
                argument_object.insert(
                    "required".to_string(),
                    required.contains(id.as_str()).into(),
                );
                return true;
            }
            false
        });
    }
}

struct RegistryTaskAlias {
    mode: Option<&'static str>,
    subcommand: Option<&'static str>,
    summary: &'static str,
    usage: &'static str,
    visible_arguments: &'static [&'static str],
    required_arguments: &'static [&'static str],
    extra_arguments: &'static [RegistryExtraAliasArgument],
}

#[derive(Clone, Copy)]
struct RegistryExtraAliasArgument {
    id: &'static str,
    label: &'static str,
    order: u64,
    parser: RegistryExtraAliasParser,
    value_kind: &'static str,
    required: bool,
    default: Option<&'static str>,
    help: &'static str,
    group: &'static str,
    advanced: bool,
    hidden: bool,
}

#[derive(Clone, Copy)]
enum RegistryExtraAliasParser {
    Option {
        flags: &'static [&'static str],
        metavar: &'static str,
        choices: &'static [&'static str],
    },
    Positional {
        metavar: &'static str,
    },
    Toggle {
        true_flags: &'static [&'static str],
        false_flags: &'static [&'static str],
    },
}

fn registry_task_alias(task_id: &str) -> Option<RegistryTaskAlias> {
    match task_id {
        "uvcontsub" => Some(RegistryTaskAlias {
            mode: Some("continuum_subtract"),
            subcommand: None,
            summary: "Subtract continuum emission from a MeasurementSet.",
            usage: "calibrate --mode continuum_subtract --ms <input.ms> --output-ms <output.ms> --fitspw <spw:channels>",
            visible_arguments: &[
                "measurement_set",
                "output_measurement_set",
                "fit_spw",
                "fit_order",
                "stats_datacolumn",
                "format",
                "output",
                "overwrite",
                "selectdata",
                "field",
                "spw",
                "antenna",
                "scan",
                "observation",
                "array",
                "timerange",
                "msselect",
            ],
            required_arguments: &["measurement_set", "output_measurement_set", "fit_spw"],
            extra_arguments: &[],
        }),
        "applycal" => Some(RegistryTaskAlias {
            mode: Some("apply"),
            subcommand: None,
            summary: "Apply one or more calibration tables to a MeasurementSet.",
            usage: "calibrate --mode apply --ms <input.ms> --gaintables <caltable[,caltable...]>",
            visible_arguments: &[
                "measurement_set",
                "gaintables",
                "callib",
                "gainfield",
                "interp",
                "spwmap",
                "apply_mode",
                "calwt",
                "parang",
                "format",
                "output",
                "overwrite",
                "selectdata",
                "field",
                "spw",
                "antenna",
                "scan",
                "observation",
                "array",
                "timerange",
                "msselect",
            ],
            required_arguments: &["measurement_set"],
            extra_arguments: &[],
        }),
        "gaincal" => Some(RegistryTaskAlias {
            mode: Some("solve_gain"),
            subcommand: None,
            summary: "Solve antenna gain calibration for a MeasurementSet.",
            usage: "calibrate --mode solve_gain --ms <input.ms> --out <gain.cal> --refant <antenna>",
            visible_arguments: &[
                "measurement_set",
                "out_table",
                "refant",
                "gaintables",
                "callib",
                "gainfield",
                "interp",
                "spwmap",
                "gain_type",
                "solve_mode",
                "solint",
                "gain_combine",
                "gain_model_source",
                "smodel",
                "min_snr",
                "solnorm",
                "format",
                "output",
                "overwrite",
                "selectdata",
                "field",
                "spw",
                "antenna",
                "scan",
                "observation",
                "array",
                "timerange",
                "msselect",
            ],
            required_arguments: &["measurement_set", "out_table", "refant"],
            extra_arguments: &[],
        }),
        "bandpass" => Some(RegistryTaskAlias {
            mode: Some("solve_bandpass"),
            subcommand: None,
            summary: "Solve bandpass calibration for a MeasurementSet.",
            usage: "calibrate --mode solve_bandpass --ms <input.ms> --out <bandpass.cal> --refant <antenna>",
            visible_arguments: &[
                "measurement_set",
                "out_table",
                "refant",
                "gaintables",
                "callib",
                "gainfield",
                "interp",
                "spwmap",
                "bandpass_combine",
                "bandtype",
                "smodel",
                "min_snr",
                "solnorm",
                "format",
                "output",
                "overwrite",
                "selectdata",
                "field",
                "spw",
                "antenna",
                "scan",
                "observation",
                "array",
                "timerange",
                "msselect",
            ],
            required_arguments: &["measurement_set", "out_table", "refant"],
            extra_arguments: &[],
        }),
        "fluxscale" => Some(RegistryTaskAlias {
            mode: Some("fluxscale"),
            subcommand: None,
            summary: "Scale gain solutions using reference calibrator fields.",
            usage: "calibrate --mode fluxscale --in <gain.cal> --out <flux.cal> --reference <field[,field...]>",
            visible_arguments: &[
                "fluxscale_input",
                "out_table",
                "reference_fields",
                "transfer_fields",
                "refspwmap",
                "gainthreshold",
                "incremental",
                "format",
                "output",
                "overwrite",
            ],
            required_arguments: &["fluxscale_input", "out_table", "reference_fields"],
            extra_arguments: &[],
        }),
        "gencal" => Some(RegistryTaskAlias {
            mode: Some("gencal"),
            subcommand: None,
            summary: "Generate a calibration table such as antpos, gceff, or opac.",
            usage: "calibrate --mode gencal --ms <input.ms> --out <caltable> --caltype antpos|gceff|opac",
            visible_arguments: &[
                "measurement_set",
                "out_table",
                "caltype",
                "antenna",
                "spw",
                "parameter",
                "gaincurve_table",
                "format",
                "output",
                "overwrite",
            ],
            required_arguments: &["measurement_set", "out_table", "caltype"],
            extra_arguments: &[
                RegistryExtraAliasArgument {
                    id: "caltype",
                    label: "Calibration Type",
                    order: 16,
                    parser: RegistryExtraAliasParser::Option {
                        flags: &["--caltype"],
                        metavar: "TYPE",
                        choices: &["antpos", "gceff", "opac"],
                    },
                    value_kind: "choice",
                    required: true,
                    default: None,
                    help: "Generated calibration family.",
                    group: "Gencal",
                    advanced: false,
                    hidden: false,
                },
                RegistryExtraAliasArgument {
                    id: "parameter",
                    label: "Parameter",
                    order: 19,
                    parser: RegistryExtraAliasParser::Option {
                        flags: &["--parameter"],
                        metavar: "VALUE[,VALUE...]",
                        choices: &[],
                    },
                    value_kind: "string",
                    required: false,
                    default: None,
                    help: "Numeric parameter list for the selected generated calibration type.",
                    group: "Gencal",
                    advanced: false,
                    hidden: false,
                },
                RegistryExtraAliasArgument {
                    id: "gaincurve_table",
                    label: "Gaincurve Table",
                    order: 20,
                    parser: RegistryExtraAliasParser::Option {
                        flags: &["--gaincurve-table"],
                        metavar: "PATH",
                        choices: &[],
                    },
                    value_kind: "path",
                    required: false,
                    default: None,
                    help: "Optional gaincurve input table for gceff.",
                    group: "Gencal",
                    advanced: true,
                    hidden: false,
                },
            ],
        }),
        "split" => Some(RegistryTaskAlias {
            mode: None,
            subcommand: None,
            summary: "Create a selected MeasurementSet subset, equivalent to CASA split.",
            usage: "mstransform --ms <input.ms> --out <output.ms> [--spw <spw[:channels]>] [--width <n>]",
            visible_arguments: &[
                "ms",
                "out",
                "spw",
                "width",
                "field",
                "scan",
                "antenna",
                "timerange",
                "msselect",
                "datacolumn",
                "keepflags",
            ],
            required_arguments: &["ms", "out"],
            extra_arguments: &[],
        }),
        "plotms" => Some(RegistryTaskAlias {
            mode: None,
            subcommand: None,
            summary: "Plot MeasurementSet visibility data with CASA plotms-style selections and axes.",
            usage: "msexplore <input.ms> --preset amp-vs-uvdist --plot-output <plot.png>",
            visible_arguments: &[
                "ms_path",
                "format",
                "output",
                "overwrite",
                "selectdata",
                "field",
                "spw",
                "timerange",
                "uvrange",
                "antenna",
                "scan",
                "correlation",
                "observation",
                "array",
                "intent",
                "feed",
                "msselect",
                "page_spec",
                "preset",
                "x_axis",
                "y_axis",
                "y_axis2",
                "data_column",
                "color_by",
                "avgchannel",
                "avgtime",
                "avgscan",
                "avgfield",
                "avgbaseline",
                "avgantenna",
                "avgspw",
                "scalar",
                "freqframe",
                "restfreq",
                "veldef",
                "iteraxis",
                "gridrows",
                "gridcols",
                "xselfscale",
                "yselfscale",
                "xsharedaxis",
                "ysharedaxis",
                "title",
                "xlabel",
                "ylabel",
                "symbol_size",
                "showlegend",
                "legendposition",
                "showmajorgrid",
                "showminorgrid",
                "headeritems",
                "max_points",
                "plot_output",
                "plot_format",
                "plot_width",
                "plot_height",
                "flag_action",
                "flag_xmin",
                "flag_xmax",
                "flag_ymin",
                "flag_ymax",
                "flag_plotindex",
                "flag_panel",
                "flag_extcorr",
                "flag_extchannel",
                "flag_selected",
                "flag_apply",
                "flag_output",
            ],
            required_arguments: &["ms_path"],
            extra_arguments: &[],
        }),
        "imhead" => Some(RegistryTaskAlias {
            mode: None,
            subcommand: Some("imhead"),
            summary: "Inspect or update CASA image header metadata.",
            usage: "imexplore imhead <image> [--json] [--mode summary|list|put] [--hdkey key --hdvalue value]",
            visible_arguments: &["image_path", "json", "mode", "hdkey", "hdvalue"],
            required_arguments: &["image_path"],
            extra_arguments: &[
                RegistryExtraAliasArgument {
                    id: "subcommand",
                    label: "Subcommand",
                    order: 0,
                    parser: RegistryExtraAliasParser::Positional { metavar: "TASK" },
                    value_kind: "string",
                    required: false,
                    default: Some("imhead"),
                    help: "Hidden imexplore subcommand used to invoke imhead.",
                    group: "Meta",
                    advanced: true,
                    hidden: true,
                },
                RegistryExtraAliasArgument {
                    id: "json",
                    label: "JSON",
                    order: 2,
                    parser: RegistryExtraAliasParser::Toggle {
                        true_flags: &["--json"],
                        false_flags: &[],
                    },
                    value_kind: "bool",
                    required: false,
                    default: Some("false"),
                    help: "Emit machine-readable JSON.",
                    group: "Output",
                    advanced: false,
                    hidden: false,
                },
                RegistryExtraAliasArgument {
                    id: "mode",
                    label: "Mode",
                    order: 3,
                    parser: RegistryExtraAliasParser::Option {
                        flags: &["--mode"],
                        metavar: "MODE",
                        choices: &["summary", "list", "put"],
                    },
                    value_kind: "choice",
                    required: false,
                    default: Some("summary"),
                    help: "Header operation mode.",
                    group: "Header",
                    advanced: false,
                    hidden: false,
                },
                RegistryExtraAliasArgument {
                    id: "hdkey",
                    label: "Header Key",
                    order: 4,
                    parser: RegistryExtraAliasParser::Option {
                        flags: &["--hdkey"],
                        metavar: "KEY",
                        choices: &[],
                    },
                    value_kind: "string",
                    required: false,
                    default: None,
                    help: "Header keyword to update when mode is put.",
                    group: "Header",
                    advanced: false,
                    hidden: false,
                },
                RegistryExtraAliasArgument {
                    id: "hdvalue",
                    label: "Header Value",
                    order: 5,
                    parser: RegistryExtraAliasParser::Option {
                        flags: &["--hdvalue"],
                        metavar: "VALUE",
                        choices: &[],
                    },
                    value_kind: "string",
                    required: false,
                    default: None,
                    help: "Header value to write when mode is put.",
                    group: "Header",
                    advanced: false,
                    hidden: false,
                },
            ],
        }),
        "imstat" => Some(RegistryTaskAlias {
            mode: None,
            subcommand: Some("imstat"),
            summary: "Compute CASA image statistics over optional pixel, region, and channel selections.",
            usage: "imexplore imstat <image> [--box x0,y0,x1,y1] [--region path|box[[x0pix,y0pix],[x1pix,y1pix]]|world CRTF box] [--chans 0~4] [--json]",
            visible_arguments: &["image_path", "box", "region", "chans", "json"],
            required_arguments: &["image_path"],
            extra_arguments: &[
                RegistryExtraAliasArgument {
                    id: "subcommand",
                    label: "Subcommand",
                    order: 0,
                    parser: RegistryExtraAliasParser::Positional { metavar: "TASK" },
                    value_kind: "string",
                    required: false,
                    default: Some("imstat"),
                    help: "Hidden imexplore subcommand used to invoke imstat.",
                    group: "Meta",
                    advanced: true,
                    hidden: true,
                },
                RegistryExtraAliasArgument {
                    id: "box",
                    label: "Box",
                    order: 2,
                    parser: RegistryExtraAliasParser::Option {
                        flags: &["--box"],
                        metavar: "x0,y0,x1,y1",
                        choices: &[],
                    },
                    value_kind: "string",
                    required: false,
                    default: None,
                    help: "Inclusive pixel box.",
                    group: "Selection",
                    advanced: false,
                    hidden: false,
                },
                RegistryExtraAliasArgument {
                    id: "chans",
                    label: "Channels",
                    order: 4,
                    parser: RegistryExtraAliasParser::Option {
                        flags: &["--chans"],
                        metavar: "range",
                        choices: &[],
                    },
                    value_kind: "string",
                    required: false,
                    default: None,
                    help: "CASA channel range, for example 4~12.",
                    group: "Selection",
                    advanced: false,
                    hidden: false,
                },
                RegistryExtraAliasArgument {
                    id: "region",
                    label: "Region",
                    order: 3,
                    parser: RegistryExtraAliasParser::Option {
                        flags: &["--region"],
                        metavar: "path|CRTF box",
                        choices: &[],
                    },
                    value_kind: "path",
                    required: false,
                    default: None,
                    help: "CASA CRTF region file, inline CRTF pixel box such as box[[100pix,100pix],[150pix,150pix]], or a world-coordinate CRTF box exported by the image explorer.",
                    group: "Selection",
                    advanced: false,
                    hidden: false,
                },
                RegistryExtraAliasArgument {
                    id: "json",
                    label: "JSON",
                    order: 5,
                    parser: RegistryExtraAliasParser::Toggle {
                        true_flags: &["--json"],
                        false_flags: &[],
                    },
                    value_kind: "bool",
                    required: false,
                    default: Some("false"),
                    help: "Emit machine-readable JSON.",
                    group: "Output",
                    advanced: false,
                    hidden: false,
                },
            ],
        }),
        _ => None,
    }
}

fn registry_extra_argument_schema(argument: RegistryExtraAliasArgument) -> serde_json::Value {
    let parser = match argument.parser {
        RegistryExtraAliasParser::Option {
            flags,
            metavar,
            choices,
        } => serde_json::json!({
            "kind": "option",
            "flags": flags,
            "metavar": metavar,
            "choices": choices,
        }),
        RegistryExtraAliasParser::Positional { metavar } => serde_json::json!({
            "kind": "positional",
            "metavar": metavar,
        }),
        RegistryExtraAliasParser::Toggle {
            true_flags,
            false_flags,
        } => serde_json::json!({
            "kind": "toggle",
            "true_flags": true_flags,
            "false_flags": false_flags,
        }),
    };
    serde_json::json!({
        "id": argument.id,
        "label": argument.label,
        "order": argument.order,
        "parser": parser,
        "value_kind": argument.value_kind,
        "required": argument.required,
        "default": argument.default,
        "help": argument.help,
        "group": argument.group,
        "advanced": argument.advanced,
        "hidden_in_tui": argument.hidden,
    })
}

fn load_canonical_ui_schema(
    resolved: &ResolvedCommand,
    binary_name: &str,
    task_id: &str,
) -> Result<Option<UiCommandSchema>, String> {
    let mut command = resolved.command();
    add_schema_args(&mut command, binary_name, task_id);
    let output = command
        .arg("--json-schema")
        .output()
        .map_err(|error| format!("spawn {binary_name} --json-schema: {error}"))?;
    if !output.status.success() {
        return Ok(None);
    }
    let bundle = serde_json::from_slice::<serde_json::Value>(&output.stdout)
        .map_err(|error| format!("parse {binary_name} --json-schema output: {error}"))?;
    let Some(ui_schema) = bundle.pointer("/projections/ui_schema").cloned() else {
        return Ok(None);
    };
    serde_json::from_value(ui_schema)
        .map(Some)
        .map_err(|error| format!("parse {binary_name} projected ui schema: {error}"))
}

fn add_schema_args(command: &mut Command, binary_name: &str, task_id: &str) {
    if binary_name == "casars-casa-task" {
        command.arg("--task").arg(task_id);
    }
}

pub(crate) fn resolve_app(id: Option<&str>) -> Result<RegistryApp, String> {
    let requested = id.unwrap_or("msexplore");
    registered_apps()
        .into_iter()
        .find(|app| app.id == requested)
        .ok_or_else(|| {
            let expected = registered_apps()
                .into_iter()
                .map(|app| app.id)
                .collect::<Vec<_>>()
                .join(", ");
            format!("unknown casars app {requested:?}; expected one of: {expected}")
        })
}

pub(crate) fn registered_apps() -> Vec<RegistryApp> {
    task_catalog_entries()
        .iter()
        .filter_map(registry_app_from_catalog)
        .collect()
}

#[cfg(test)]
pub(crate) fn calibrate_app() -> RegistryApp {
    resolve_app(Some("calibrate")).expect("calibrate should be in task catalog")
}

#[cfg(test)]
pub(crate) fn importvla_app() -> RegistryApp {
    resolve_app(Some("importvla")).expect("importvla should be in task catalog")
}

#[cfg(test)]
pub(crate) fn imager_app() -> RegistryApp {
    resolve_app(Some("imager")).expect("imager should be in task catalog")
}

#[cfg(test)]
pub(crate) fn simobserve_app() -> RegistryApp {
    resolve_app(Some("simobserve")).expect("simobserve should be in task catalog")
}

#[cfg(test)]
pub(crate) fn tablebrowser_app() -> RegistryApp {
    resolve_app(Some("tablebrowser")).expect("tablebrowser should be in task catalog")
}

#[cfg(test)]
pub(crate) fn imexplore_app() -> RegistryApp {
    resolve_app(Some("imexplore")).expect("imexplore should be in task catalog")
}

#[cfg(test)]
pub(crate) fn imhead_app() -> RegistryApp {
    resolve_app(Some("imhead")).expect("imhead should be in task catalog")
}

#[cfg(test)]
pub(crate) fn imstat_app() -> RegistryApp {
    resolve_app(Some("imstat")).expect("imstat should be in task catalog")
}

#[cfg(test)]
pub(crate) fn immoments_app() -> RegistryApp {
    resolve_app(Some("immoments")).expect("immoments should be in task catalog")
}

#[cfg(test)]
pub(crate) fn exportfits_app() -> RegistryApp {
    resolve_app(Some("exportfits")).expect("exportfits should be in task catalog")
}

#[cfg(test)]
pub(crate) fn msexplore_app() -> RegistryApp {
    resolve_app(Some("msexplore")).expect("msexplore should be in task catalog")
}

#[cfg(test)]
pub(crate) fn plotms_app() -> RegistryApp {
    resolve_app(Some("plotms")).expect("plotms should be in task catalog")
}

fn sibling_binary(binary_name: &str) -> Option<PathBuf> {
    let mut path = env::current_exe().ok()?;
    path.pop();
    path.push(binary_name);
    path.set_extension(env::consts::EXE_EXTENSION);
    if path.exists() { Some(path) } else { None }
}

fn workspace_manifest_path() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(|path| path.parent())
        .unwrap_or_else(|| {
            panic!("casars manifest dir should live under <workspace>/crates/casars")
        })
        .join("Cargo.toml")
}

fn sibling_binary_is_stale_for_current_process(sibling_path: &std::path::Path) -> bool {
    let current_exe = match env::current_exe() {
        Ok(path) => path,
        Err(_) => return false,
    };
    let current_modified = file_modified_time(&current_exe);
    let sibling_modified = file_modified_time(sibling_path);
    is_binary_stale(sibling_modified, current_modified)
}

fn file_modified_time(path: &std::path::Path) -> Option<SystemTime> {
    fs::metadata(path).ok()?.modified().ok()
}

fn is_binary_stale(
    binary_modified: Option<SystemTime>,
    reference_modified: Option<SystemTime>,
) -> bool {
    match (binary_modified, reference_modified) {
        (Some(binary_modified), Some(reference_modified)) => binary_modified < reference_modified,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_ms::MsPlotPreset;
    use casa_ms::ui_schema::UiArgumentParser;
    use std::fs;
    use std::time::Duration;

    #[test]
    fn resolve_app_defaults_and_rejects_unknown_ids() {
        assert_eq!(resolve_app(None).unwrap().id, "msexplore");
        assert_eq!(resolve_app(Some("msexplore")).unwrap().id, "msexplore");
        assert_eq!(resolve_app(Some("calibrate")).unwrap().id, "calibrate");
        assert_eq!(resolve_app(Some("importvla")).unwrap().id, "importvla");
        assert_eq!(resolve_app(Some("simobserve")).unwrap().id, "simobserve");
        assert_eq!(
            resolve_app(Some("tablebrowser")).unwrap().id,
            "tablebrowser"
        );
        assert_eq!(resolve_app(Some("imexplore")).unwrap().id, "imexplore");
        assert_eq!(resolve_app(Some("immoments")).unwrap().id, "immoments");
        assert_eq!(resolve_app(Some("exportfits")).unwrap().id, "exportfits");
        assert_eq!(resolve_app(Some("split")).unwrap().id, "split");
        assert_eq!(resolve_app(Some("applycal")).unwrap().id, "applycal");
        assert!(
            resolve_app(Some("bogus"))
                .unwrap_err()
                .contains("unknown casars app")
        );
    }

    #[test]
    fn registered_apps_are_projected_from_shared_task_catalog() {
        let apps = registered_apps();
        let ids = apps.iter().map(|app| app.id.as_str()).collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![
                "msexplore",
                "calibrate",
                "importvla",
                "imager",
                "simobserve",
                "tablebrowser",
                "imexplore",
                "imhead",
                "imstat",
                "immoments",
                "exportfits",
                "mstransform",
                "split",
                "uvcontsub",
                "applycal",
                "gaincal",
                "bandpass",
                "fluxscale",
                "gencal",
                "plotms",
                "plotcal",
                "flagdata",
                "flagmanager",
                "imcollapse",
                "imfit",
                "impbcor",
                "widebandpbcor",
                "imcontsub",
                "impv",
                "imsubimage",
                "immath",
                "imregrid",
                "feather",
                "importfits",
                "concat",
                "statwt",
                "hanningsmooth",
                "clearcal",
                "delmod",
                "ft",
                "simanalyze",
                "simalma",
            ]
        );
        assert!(
            task_catalog_entries()
                .iter()
                .any(|entry| entry.id == "mstransform"
                    && entry.show_in_tui
                    && entry.binary_name == "mstransform")
        );
        assert_eq!(exportfits_app().id, "exportfits");
    }

    #[test]
    fn app_metadata_matches_interaction_kind() {
        let msexplore = msexplore_app();
        assert!(!msexplore.is_browser_session());
        assert_eq!(msexplore.browser_kind(), None);
        assert_eq!(msexplore.browser_path_field_id(), None);
        assert_eq!(
            msexplore.ready_status_line(),
            "Ready. Press r to run and refresh the current inspection view."
        );

        let calibrate = calibrate_app();
        assert!(!calibrate.is_browser_session());
        assert_eq!(calibrate.browser_kind(), None);
        assert_eq!(calibrate.browser_path_field_id(), None);
        assert_eq!(
            calibrate.ready_status_line(),
            "Ready. Press r to run the selected workflow stage."
        );

        let importvla = importvla_app();
        assert!(!importvla.is_browser_session());
        assert_eq!(importvla.browser_kind(), None);
        assert_eq!(importvla.browser_path_field_id(), None);
        assert_eq!(
            importvla.ready_status_line(),
            "Ready. Press r to run the selected workflow stage."
        );

        let simobserve = simobserve_app();
        assert!(!simobserve.is_browser_session());
        assert_eq!(simobserve.browser_kind(), None);
        assert_eq!(
            simobserve.ready_status_line(),
            "Ready. Press r to run the selected workflow stage."
        );

        let tablebrowser = tablebrowser_app();
        assert!(tablebrowser.is_browser_session());
        assert_eq!(tablebrowser.browser_kind(), Some(BrowserAppKind::Table));
        assert_eq!(tablebrowser.browser_path_field_id(), Some("table_path"));
        assert_eq!(
            tablebrowser.ready_status_line(),
            "Ready. Press r to open the browser session."
        );

        let imexplore = imexplore_app();
        assert!(imexplore.is_browser_session());
        assert_eq!(imexplore.browser_kind(), Some(BrowserAppKind::Image));
        assert_eq!(imexplore.browser_path_field_id(), Some("image_path"));

        let immoments = immoments_app();
        assert!(!immoments.is_browser_session());
        assert_eq!(immoments.browser_kind(), None);
        assert_eq!(
            immoments.ready_status_line(),
            "Ready. Press r to run the selected workflow stage."
        );
    }

    #[test]
    fn resolve_command_prefers_override_environment() {
        let _guard = crate::test_env_lock();
        let app = msexplore_app();
        unsafe {
            env::set_var("CASARS_MSEXPLORE_BIN", "/tmp/custom-msexplore");
        }

        let resolved = app.resolve_command().expect("resolve override");
        let command = resolved.command();
        assert_eq!(command.get_program(), "/tmp/custom-msexplore");
        assert_eq!(command.get_args().count(), 0);

        unsafe {
            env::remove_var("CASARS_MSEXPLORE_BIN");
        }
    }

    #[test]
    fn resolve_command_falls_back_to_cargo_run_prefix() {
        let _guard = crate::test_env_lock();
        let app = calibrate_app();
        unsafe {
            env::remove_var("CASARS_CALIBRATE_BIN");
            env::remove_var("CARGO_BIN_EXE_calibrate");
            env::set_var("CARGO", "cargo");
        }
        let resolved = app.resolve_command().expect("resolve cargo fallback");
        let command = resolved.command();
        assert_eq!(command.get_program(), "cargo");
        let args = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            args,
            vec![
                "run",
                "--manifest-path",
                workspace_manifest_path().to_string_lossy().as_ref(),
                "-q",
                "-p",
                "casa-calibration",
                "--bin",
                "calibrate",
                "--"
            ]
        );
        unsafe {
            env::remove_var("CARGO");
        }
    }

    #[test]
    fn resolve_command_prefers_cargo_bin_environment_before_sibling_or_cargo() {
        let _guard = crate::test_env_lock();
        let app = msexplore_app();
        unsafe {
            env::remove_var("CASARS_MSEXPLORE_BIN");
            env::set_var("CARGO_BIN_EXE_msexplore", "/tmp/cargo-bin-msexplore");
        }

        let resolved = app.resolve_command().expect("resolve cargo bin env");
        let command = resolved.command();
        assert_eq!(command.get_program(), "/tmp/cargo-bin-msexplore");
        assert_eq!(command.get_args().count(), 0);

        unsafe {
            env::remove_var("CARGO_BIN_EXE_msexplore");
        }
    }

    #[test]
    fn msexplore_load_schema_includes_every_shipped_preset() {
        let schema = msexplore_app()
            .load_schema()
            .expect("load msexplore schema");
        let preset = schema
            .arguments
            .iter()
            .find(|argument| argument.id == "preset")
            .expect("preset argument");
        let UiArgumentParser::Option { choices, .. } = &preset.parser else {
            panic!("preset should be an option parser");
        };
        let expected = MsPlotPreset::ALL
            .iter()
            .map(|preset| preset.as_str().to_string())
            .collect::<Vec<_>>();
        assert_eq!(choices, &expected);
    }

    #[test]
    fn calibrate_load_schema_describes_public_workflow_surface() {
        let schema = calibrate_app()
            .load_schema()
            .expect("load calibrate schema");
        assert_eq!(schema.command_id, "calibrate");
        assert_eq!(schema.display_name, "Calibrate");
        assert_eq!(schema.category, "Calibration");
        let workflow_mode = schema
            .arguments
            .iter()
            .find(|argument| argument.id == "mode")
            .expect("workflow mode argument");
        let UiArgumentParser::Option { choices, .. } = &workflow_mode.parser else {
            panic!("mode should be an option parser");
        };
        assert_eq!(
            choices,
            &[
                "apply",
                "summary",
                "stats",
                "export_corrected_data",
                "continuum_subtract",
                "solve_gain",
                "solve_bandpass",
                "fluxscale",
                "gencal",
            ]
        );
        let gaintables = schema
            .arguments
            .iter()
            .find(|argument| argument.id == "gaintables")
            .expect("gaintables argument");
        let UiArgumentParser::Option { choices, .. } = &gaintables.parser else {
            panic!("gaintables should be an option parser");
        };
        assert!(choices.is_empty());
        let mode = schema
            .arguments
            .iter()
            .find(|argument| argument.id == "apply_mode")
            .expect("apply mode argument");
        let UiArgumentParser::Option { choices, .. } = &mode.parser else {
            panic!("apply_mode should be an option parser");
        };
        assert_eq!(choices, &["calflag", "calonly", "trial"]);
    }

    #[test]
    fn importvla_load_schema_describes_public_workflow_surface() {
        let schema = importvla_app()
            .load_schema()
            .expect("load importvla schema");
        assert_eq!(schema.command_id, "importvla");
        assert_eq!(schema.display_name, "ImportVLA");
        assert_eq!(schema.category, "Import");
        let archivefiles = schema
            .arguments
            .iter()
            .find(|argument| argument.id == "archivefiles")
            .expect("archivefiles argument");
        let UiArgumentParser::Option { choices, .. } = &archivefiles.parser else {
            panic!("archivefiles should be an option parser");
        };
        assert!(choices.is_empty());
        let antnamescheme = schema
            .arguments
            .iter()
            .find(|argument| argument.id == "antnamescheme")
            .expect("antnamescheme argument");
        let UiArgumentParser::Option { choices, .. } = &antnamescheme.parser else {
            panic!("antnamescheme should be an option parser");
        };
        assert_eq!(choices, &["new", "old"]);
    }

    #[test]
    fn imager_load_schema_describes_public_workflow_surface() {
        let schema = imager_app().load_schema().expect("load imager schema");
        assert_eq!(schema.command_id, "imager");
        assert_eq!(schema.display_name, "Imager");
        assert_eq!(schema.category, "Imaging");
        let specmode = schema
            .arguments
            .iter()
            .find(|argument| argument.id == "specmode")
            .expect("specmode argument");
        assert_eq!(specmode.group, "Stages");
        let managed_output = schema.managed_output.expect("managed output");
        assert_eq!(managed_output.renderer, "imager-run-v1");
        for argument_id in [
            "ms",
            "imagename",
            "specmode",
            "channel_count",
            "start",
            "width",
            "outframe",
            "restfreq",
            "deconvolver",
            "weighting",
            "gridder",
            "perchanweightdensity",
            "restoringbeam",
            "usemask",
            "noisethreshold",
            "sidelobethreshold",
            "lownoisethreshold",
            "minbeamfrac",
            "negativethreshold",
            "scales",
            "smallscalebias",
            "wterm",
            "wprojplanes",
            "nterms",
            "savemodel",
            "outlierfile",
            "write_pb",
            "pbcor",
            "pblimit",
        ] {
            let argument = schema
                .arguments
                .iter()
                .find(|argument| argument.id == argument_id)
                .unwrap_or_else(|| panic!("missing imager argument {argument_id}"));
            assert!(
                !argument.hidden_in_tui,
                "{argument_id} should be TUI invokable"
            );
        }

        for argument_id in [
            "ms",
            "imagename",
            "imsize",
            "cell_arcsec",
            "field",
            "phasecenter_field",
            "spw",
            "specmode",
            "dirty_only",
            "deconvolver",
            "weighting",
            "gridder",
            "niter",
            "threshold_jy",
        ] {
            let argument = schema
                .arguments
                .iter()
                .find(|argument| argument.id == argument_id)
                .unwrap_or_else(|| panic!("missing imager argument {argument_id}"));
            assert!(
                !argument.advanced,
                "{argument_id} should be in the default imager form"
            );
        }

        for argument_id in [
            "savemodel",
            "ddid",
            "polarization",
            "wterm",
            "nmajor",
            "gain",
            "usemask",
            "sidelobethreshold",
            "noisethreshold",
        ] {
            let argument = schema
                .arguments
                .iter()
                .find(|argument| argument.id == argument_id)
                .unwrap_or_else(|| panic!("missing imager argument {argument_id}"));
            assert!(
                argument.advanced,
                "{argument_id} should stay behind advanced or conditional disclosure"
            );
        }

        let dirty_order = schema
            .arguments
            .iter()
            .find(|argument| argument.id == "dirty_only")
            .expect("dirty_only")
            .order;
        for argument_id in ["niter", "threshold_jy"] {
            let argument = schema
                .arguments
                .iter()
                .find(|argument| argument.id == argument_id)
                .unwrap_or_else(|| panic!("missing imager argument {argument_id}"));
            assert_eq!(argument.group, "Stages");
            assert!(
                argument.order > dirty_order,
                "{argument_id} should appear immediately after dirty_only"
            );
            assert!(
                argument.help.contains("Dirty Only"),
                "{argument_id} should explain the Dirty Only interaction"
            );
        }
    }

    #[test]
    fn simobserve_load_schema_describes_public_workflow_surface() {
        let schema = simobserve_app()
            .load_schema()
            .expect("load simobserve schema");
        assert_eq!(schema.command_id, "simobserve");
        assert_eq!(schema.display_name, "SimObserve");
        assert_eq!(schema.category, "Simulation");
        assert!(schema.argument("model").is_some());
        assert!(schema.argument("out").is_some());
    }

    #[test]
    fn imexplore_load_schema_describes_browser_surface_without_subprocess() {
        let _guard = crate::test_env_lock();
        unsafe {
            env::remove_var("CASARS_IMEXPLORE_BIN");
            env::remove_var("CARGO_BIN_EXE_imexplore");
        }

        let schema = imexplore_app()
            .load_schema()
            .expect("load imexplore schema");
        assert_eq!(schema.command_id, "imexplore");
        assert_eq!(schema.display_name, "ImExplore");
        assert_eq!(schema.category, "Images");
        let image_path = schema
            .arguments
            .iter()
            .find(|argument| argument.id == "image_path")
            .expect("image path argument");
        assert_eq!(image_path.group, "Input");
        let stretch = schema
            .arguments
            .iter()
            .find(|argument| argument.id == "stretch")
            .expect("stretch argument");
        let UiArgumentParser::Option { choices, .. } = &stretch.parser else {
            panic!("stretch should be an option parser");
        };
        assert_eq!(
            choices,
            &["percentile99", "percentile95", "minmax", "zscale", "manual"]
        );
    }

    #[test]
    fn alias_load_schema_describes_task_specific_tui_surface() {
        let _guard = crate::test_env_lock();
        unsafe {
            env::remove_var("CASARS_MSEXPLORE_BIN");
            env::remove_var("CARGO_BIN_EXE_msexplore");
            env::remove_var("CASARS_IMEXPLORE_BIN");
            env::remove_var("CARGO_BIN_EXE_imexplore");
        }

        let plotms = plotms_app().load_schema().expect("load plotms schema");
        assert_eq!(plotms.command_id, "plotms");
        assert_eq!(plotms.display_name, "PlotMS");
        assert!(plotms.argument("ms_path").is_some());
        assert!(plotms.argument("plot_output").is_some());
        assert!(plotms.argument("flag_apply").is_some());
        assert!(plotms.argument("stretch").is_none());

        let imhead = imhead_app().load_schema().expect("load imhead schema");
        assert_eq!(imhead.command_id, "imhead");
        let subcommand = imhead.argument("subcommand").expect("subcommand");
        assert_eq!(subcommand.default.as_deref(), Some("imhead"));
        assert!(subcommand.hidden_in_tui);
        assert_eq!(imhead.argument("image_path").expect("image_path").order, 1);
        assert!(!imhead.argument("mode").expect("mode").hidden_in_tui);
        assert!(imhead.argument("stretch").is_none());

        let imstat = imstat_app().load_schema().expect("load imstat schema");
        assert_eq!(imstat.command_id, "imstat");
        assert_eq!(
            imstat
                .argument("subcommand")
                .and_then(|arg| arg.default.as_deref()),
            Some("imstat")
        );
        assert!(imstat.argument("box").is_some());
        assert!(imstat.argument("region").is_some());
        assert!(imstat.argument("chans").is_some());
        assert!(imstat.argument("clip_low").is_none());
    }

    #[test]
    fn load_schema_surfaces_subprocess_failures_for_overridden_binaries() {
        let _guard = crate::test_env_lock();
        unsafe {
            env::set_var("CASARS_IMEXPLORE_BIN", "/definitely/missing/imexplore");
        }

        let error = imexplore_app()
            .load_schema()
            .expect_err("missing override binary should fail");
        assert!(error.contains("spawn imexplore --json-schema"));

        unsafe {
            env::remove_var("CASARS_IMEXPLORE_BIN");
        }
    }

    #[test]
    fn load_schema_reports_nonzero_exit_status_and_parse_errors_from_overrides() {
        let _guard = crate::test_env_lock();

        unsafe {
            env::set_var("CASARS_IMEXPLORE_BIN", "/bin/sh");
        }
        let error = imexplore_app()
            .load_schema()
            .expect_err("shell should reject --ui-schema");
        assert!(error.contains("imexplore --ui-schema exited with"));
        assert!(error.contains("--ui-schema"));

        unsafe {
            env::set_var("CASARS_IMEXPLORE_BIN", "/bin/echo");
        }
        let error = imexplore_app()
            .load_schema()
            .expect_err("echo output should not parse as JSON");
        assert!(error.contains("parse imexplore --json-schema output"));

        unsafe {
            env::remove_var("CASARS_IMEXPLORE_BIN");
        }
    }

    #[test]
    fn stale_binary_detection_requires_binary_older_than_reference() {
        let older = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        let newer = SystemTime::UNIX_EPOCH + Duration::from_secs(2);
        assert!(is_binary_stale(Some(older), Some(newer)));
        assert!(!is_binary_stale(Some(newer), Some(older)));
        assert!(!is_binary_stale(Some(newer), Some(newer)));
        assert!(!is_binary_stale(None, Some(newer)));
        assert!(!is_binary_stale(Some(older), None));
    }

    #[test]
    fn workspace_ms_apps_prefer_cargo_fallback_for_stale_siblings() {
        assert!(msexplore_app().prefers_cargo_workspace_fallback_for_stale_sibling());
        assert!(calibrate_app().prefers_cargo_workspace_fallback_for_stale_sibling());
        assert!(!imager_app().prefers_cargo_workspace_fallback_for_stale_sibling());
        assert!(!tablebrowser_app().prefers_cargo_workspace_fallback_for_stale_sibling());
        assert!(!imexplore_app().prefers_cargo_workspace_fallback_for_stale_sibling());
    }

    #[test]
    fn resolve_command_uses_existing_sibling_binary_for_non_workspace_apps() {
        let _guard = crate::test_env_lock();
        let app = tablebrowser_app();
        let mut sibling_path = env::current_exe().expect("current exe");
        sibling_path.pop();
        sibling_path.push("tablebrowser");
        sibling_path.set_extension(env::consts::EXE_EXTENSION);
        let _ = fs::remove_file(&sibling_path);
        fs::write(&sibling_path, b"#!/bin/sh\n").expect("create sibling binary placeholder");
        unsafe {
            env::remove_var("CASARS_TABLEBROWSER_BIN");
            env::remove_var("CARGO_BIN_EXE_tablebrowser");
            env::remove_var("CARGO");
        }

        let resolved = app.resolve_command().expect("resolve sibling binary");
        let command = resolved.command();
        assert_eq!(command.get_program(), sibling_path.as_os_str());
        assert_eq!(command.get_args().count(), 0);
        assert!(!sibling_binary_is_stale_for_current_process(
            std::path::Path::new("/definitely/missing")
        ));

        fs::remove_file(&sibling_path).expect("remove sibling binary placeholder");
    }

    #[test]
    fn resolved_command_direct_and_manifest_helpers_cover_simple_paths() {
        let direct = ResolvedCommand::direct("demo-tool");
        let command = direct.command();
        assert_eq!(command.get_program(), "demo-tool");
        assert_eq!(command.get_args().count(), 0);

        let manifest_path = workspace_manifest_path();
        assert!(manifest_path.ends_with("Cargo.toml"));
        assert!(file_modified_time(std::path::Path::new("/definitely/missing")).is_none());
    }
}
