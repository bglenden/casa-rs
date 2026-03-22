// SPDX-License-Identifier: LGPL-3.0-or-later
//! Schema-backed CLI support for `listobs` and `msinfo`.

use std::collections::HashMap;
use std::ffi::OsString;
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::{ListObsOptions, ListObsOutputFormat, ListObsSummary};

const UI_SCHEMA_VERSION: u32 = 1;
const COMMAND_ID: &str = "listobs";
const DISPLAY_NAME: &str = "ListObs";
const CATEGORY: &str = "MeasurementSet";
const SUMMARY: &str = "render a CASA-style MeasurementSet summary";

struct ArgumentCommon<'a> {
    order: usize,
    id: &'a str,
    label: &'a str,
    help: &'a str,
    group: &'a str,
    advanced: bool,
    hidden_in_tui: bool,
}

struct PositionalSpec<'a> {
    common: ArgumentCommon<'a>,
    metavar: &'a str,
    value_kind: UiValueKind,
    required: bool,
}

struct OptionSpec<'a> {
    common: ArgumentCommon<'a>,
    flags: &'a [&'a str],
    metavar: &'a str,
    value_kind: UiValueKind,
    default: Option<&'a str>,
    choices: &'a [&'a str],
}

struct ToggleSpec<'a> {
    common: ArgumentCommon<'a>,
    true_flags: &'a [&'a str],
    false_flags: &'a [&'a str],
    default: bool,
}

/// Parse the environment arguments, run the summary, and return a process exit code.
pub fn run_env(program_name: &str) -> i32 {
    let schema = command_schema(program_name);
    match parse_args(&schema, std::env::args_os().skip(1)) {
        Ok(CliAction::Help) => {
            print!("{}", schema.render_help());
            0
        }
        Ok(CliAction::UiSchema) => match schema.render_json_pretty() {
            Ok(json) => {
                print!("{json}");
                0
            }
            Err(error) => {
                eprintln!("Error: failed to serialize --ui-schema output: {error}");
                1
            }
        },
        Ok(CliAction::Run(options)) => {
            match ListObsSummary::from_path_with_options(&options.path, &options.listobs)
                .map_err(|error| error.to_string())
                .and_then(|summary| {
                    summary
                        .render(options.format)
                        .map_err(|error| error.to_string())
                })
                .and_then(|rendered| write_output(&options, &rendered))
            {
                Ok(()) => 0,
                Err(error) => {
                    eprintln!("Error: {error}");
                    1
                }
            }
        }
        Err(error) => {
            eprintln!("Error: {error}\n");
            eprintln!("{}", schema.render_help());
            1
        }
    }
}

/// Build the machine-readable command schema for `listobs` or `msinfo`.
pub fn command_schema(program_name: &str) -> UiCommandSchema {
    UiCommandSchema {
        schema_version: UI_SCHEMA_VERSION,
        command_id: COMMAND_ID.to_string(),
        invocation_name: program_name.to_string(),
        display_name: DISPLAY_NAME.to_string(),
        category: CATEGORY.to_string(),
        summary: SUMMARY.to_string(),
        usage: format!("{program_name} [OPTIONS] <ms-path>"),
        arguments: vec![
            positional_argument(PositionalSpec {
                common: ArgumentCommon {
                    order: 0,
                    id: "ms_path",
                    label: "MeasurementSet Path",
                    help: "Path to the MeasurementSet root directory",
                    group: "Input",
                    advanced: false,
                    hidden_in_tui: false,
                },
                metavar: "ms-path",
                value_kind: UiValueKind::Path,
                required: true,
            }),
            option_argument(OptionSpec {
                common: ArgumentCommon {
                    order: 1,
                    id: "format",
                    label: "Output Format",
                    help: "Output format: text or json (default: text)",
                    group: "Output",
                    advanced: false,
                    hidden_in_tui: true,
                },
                flags: &["--format"],
                metavar: "FORMAT",
                value_kind: UiValueKind::Choice,
                default: Some("text"),
                choices: &["text", "json"],
            }),
            option_argument(OptionSpec {
                common: ArgumentCommon {
                    order: 2,
                    id: "output",
                    label: "Output Path",
                    help: "Write the rendered output to PATH",
                    group: "Output",
                    advanced: true,
                    hidden_in_tui: false,
                },
                flags: &["-o", "--output"],
                metavar: "PATH",
                value_kind: UiValueKind::Path,
                default: None,
                choices: &[],
            }),
            option_argument(OptionSpec {
                common: ArgumentCommon {
                    order: 3,
                    id: "listfile",
                    label: "List File",
                    help: "CASA-compatible alias for --output",
                    group: "Output",
                    advanced: true,
                    hidden_in_tui: false,
                },
                flags: &["--listfile"],
                metavar: "PATH",
                value_kind: UiValueKind::Path,
                default: None,
                choices: &[],
            }),
            toggle_argument(ToggleSpec {
                common: ArgumentCommon {
                    order: 4,
                    id: "verbose",
                    label: "Verbose Report",
                    help: "Render the verbose CASA-style report (default: enabled)",
                    group: "Presentation",
                    advanced: false,
                    hidden_in_tui: false,
                },
                true_flags: &["--verbose"],
                false_flags: &["--no-verbose"],
                default: true,
            }),
            toggle_argument(ToggleSpec {
                common: ArgumentCommon {
                    order: 5,
                    id: "selectdata",
                    label: "Apply Selection",
                    help: "Apply selection flags below (default: enabled)",
                    group: "Selection",
                    advanced: false,
                    hidden_in_tui: false,
                },
                true_flags: &["--selectdata"],
                false_flags: &["--no-selectdata"],
                default: true,
            }),
            option_argument(OptionSpec {
                common: ArgumentCommon {
                    order: 6,
                    id: "field",
                    label: "Field",
                    help: "Select field ids, ranges, names, or simple '*' globs",
                    group: "Selection",
                    advanced: false,
                    hidden_in_tui: false,
                },
                flags: &["--field"],
                metavar: "EXPR",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
            }),
            option_argument(OptionSpec {
                common: ArgumentCommon {
                    order: 7,
                    id: "spw",
                    label: "Spectral Window",
                    help: "Select spectral-window ids or ranges",
                    group: "Selection",
                    advanced: false,
                    hidden_in_tui: false,
                },
                flags: &["--spw"],
                metavar: "EXPR",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
            }),
            option_argument(OptionSpec {
                common: ArgumentCommon {
                    order: 8,
                    id: "antenna",
                    label: "Antenna",
                    help: "Select antenna ids, names, or exact baselines a&&b",
                    group: "Selection",
                    advanced: false,
                    hidden_in_tui: false,
                },
                flags: &["--antenna"],
                metavar: "EXPR",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
            }),
            option_argument(OptionSpec {
                common: ArgumentCommon {
                    order: 9,
                    id: "scan",
                    label: "Scan",
                    help: "Select scan numbers or ranges",
                    group: "Selection",
                    advanced: false,
                    hidden_in_tui: false,
                },
                flags: &["--scan"],
                metavar: "EXPR",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
            }),
            option_argument(OptionSpec {
                common: ArgumentCommon {
                    order: 10,
                    id: "observation",
                    label: "Observation",
                    help: "Select observation ids or ranges",
                    group: "Selection",
                    advanced: false,
                    hidden_in_tui: false,
                },
                flags: &["--observation"],
                metavar: "EXPR",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
            }),
            option_argument(OptionSpec {
                common: ArgumentCommon {
                    order: 11,
                    id: "array",
                    label: "Array",
                    help: "Select array ids or ranges",
                    group: "Selection",
                    advanced: false,
                    hidden_in_tui: false,
                },
                flags: &["--array"],
                metavar: "EXPR",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
            }),
            option_argument(OptionSpec {
                common: ArgumentCommon {
                    order: 12,
                    id: "timerange",
                    label: "Time Range",
                    help: "Select rows by CASA-style UTC time expressions",
                    group: "Selection",
                    advanced: false,
                    hidden_in_tui: false,
                },
                flags: &["--timerange"],
                metavar: "EXPR",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
            }),
            option_argument(OptionSpec {
                common: ArgumentCommon {
                    order: 13,
                    id: "uvrange",
                    label: "UV Range",
                    help: "Select UV-distance ranges in m/lambda units",
                    group: "Selection",
                    advanced: false,
                    hidden_in_tui: false,
                },
                flags: &["--uvrange"],
                metavar: "EXPR",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
            }),
            option_argument(OptionSpec {
                common: ArgumentCommon {
                    order: 14,
                    id: "correlation",
                    label: "Correlation",
                    help: "Select rows by correlation products such as XX,YY",
                    group: "Selection",
                    advanced: false,
                    hidden_in_tui: false,
                },
                flags: &["--correlation"],
                metavar: "EXPR",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
            }),
            option_argument(OptionSpec {
                common: ArgumentCommon {
                    order: 15,
                    id: "intent",
                    label: "Intent",
                    help: "Select scan intents by exact name or simple '*' globs",
                    group: "Selection",
                    advanced: false,
                    hidden_in_tui: false,
                },
                flags: &["--intent"],
                metavar: "EXPR",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
            }),
            option_argument(OptionSpec {
                common: ArgumentCommon {
                    order: 16,
                    id: "feed",
                    label: "Feed",
                    help: "Select feed ids or ranges (currently unsupported)",
                    group: "Selection",
                    advanced: true,
                    hidden_in_tui: false,
                },
                flags: &["--feed"],
                metavar: "EXPR",
                value_kind: UiValueKind::String,
                default: None,
                choices: &[],
            }),
            toggle_argument(ToggleSpec {
                common: ArgumentCommon {
                    order: 17,
                    id: "listunfl",
                    label: "List Unflagged Rows",
                    help: "Include fractional unflagged-row counts",
                    group: "Presentation",
                    advanced: false,
                    hidden_in_tui: false,
                },
                true_flags: &["--listunfl"],
                false_flags: &[],
                default: false,
            }),
            option_argument(OptionSpec {
                common: ArgumentCommon {
                    order: 18,
                    id: "cachesize",
                    label: "Cache Size",
                    help: "CASA-style metadata cache size in MiB (currently unsupported)",
                    group: "Selection",
                    advanced: true,
                    hidden_in_tui: false,
                },
                flags: &["--cachesize"],
                metavar: "MIB",
                value_kind: UiValueKind::Float,
                default: None,
                choices: &[],
            }),
            toggle_argument(ToggleSpec {
                common: ArgumentCommon {
                    order: 19,
                    id: "overwrite",
                    label: "Overwrite Output",
                    help: "Replace an existing --output/--listfile target",
                    group: "Output",
                    advanced: true,
                    hidden_in_tui: false,
                },
                true_flags: &["--overwrite"],
                false_flags: &[],
                default: false,
            }),
            action_argument(
                20,
                "ui_schema",
                "UI Schema",
                &["--ui-schema"],
                UiActionKind::UiSchema,
                "Print the machine-readable UI schema for this command",
            ),
            action_argument(
                21,
                "help",
                "Help",
                &["-h", "--help"],
                UiActionKind::Help,
                "Print this help message",
            ),
        ],
        managed_output: Some(UiManagedOutputSchema {
            renderer: "listobs-summary-v1".to_string(),
            stdout_format: "json".to_string(),
            inject_arguments: vec![UiInjectedArgument {
                flag: "--format".to_string(),
                value: "json".to_string(),
            }],
            raw_stdout_available: true,
            raw_stderr_available: true,
        }),
    }
}

#[derive(Debug)]
enum CliAction {
    Help,
    UiSchema,
    Run(Box<CliOptions>),
}

#[derive(Debug)]
struct CliOptions {
    path: PathBuf,
    format: ListObsOutputFormat,
    output: Option<PathBuf>,
    overwrite: bool,
    listobs: ListObsOptions,
}

#[derive(Debug, Clone)]
enum ParsedValue {
    String(String),
    Bool(bool),
}

#[derive(Debug, Default)]
struct ParsedValues {
    values: HashMap<String, ParsedValue>,
}

impl ParsedValues {
    fn insert_string(&mut self, id: &str, value: String) {
        self.values
            .insert(id.to_string(), ParsedValue::String(value));
    }

    fn insert_bool(&mut self, id: &str, value: bool) {
        self.values.insert(id.to_string(), ParsedValue::Bool(value));
    }

    fn optional_string(&self, id: &str) -> Result<Option<String>, String> {
        match self.values.get(id) {
            Some(ParsedValue::String(value)) => Ok(Some(value.clone())),
            Some(ParsedValue::Bool(_)) => Err(format!("internal type mismatch for {id}")),
            None => Ok(None),
        }
    }

    fn required_string(&self, id: &str) -> Result<String, String> {
        self.optional_string(id)?
            .ok_or_else(|| format!("missing required argument {id}"))
    }

    fn bool_or_default(&self, schema: &UiCommandSchema, id: &str) -> Result<bool, String> {
        match self.values.get(id) {
            Some(ParsedValue::Bool(value)) => Ok(*value),
            Some(ParsedValue::String(_)) => Err(format!("internal type mismatch for {id}")),
            None => schema
                .argument(id)
                .and_then(UiArgumentSchema::default_bool)
                .ok_or_else(|| format!("missing default for boolean argument {id}")),
        }
    }
}

/// Machine-readable UI schema emitted by `--ui-schema`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UiCommandSchema {
    /// Schema version for the `--ui-schema` payload itself.
    pub schema_version: u32,
    /// Stable command identifier shared across aliases.
    pub command_id: String,
    /// Invocation name used for this schema instance, such as `listobs`.
    pub invocation_name: String,
    /// Human-friendly command name for launcher UIs.
    pub display_name: String,
    /// Category label for grouping related apps.
    pub category: String,
    /// One-line command summary.
    pub summary: String,
    /// Usage line rendered in `--help`.
    pub usage: String,
    /// Ordered argument definitions for parsing, help text, and TUI forms.
    pub arguments: Vec<UiArgumentSchema>,
    /// Structured-output contract used by rich TUI renderers.
    pub managed_output: Option<UiManagedOutputSchema>,
}

impl UiCommandSchema {
    /// Render the command's human-readable help text from the schema.
    pub fn render_help(&self) -> String {
        let mut out = String::new();
        let positionals = self
            .arguments
            .iter()
            .filter(|argument| matches!(argument.parser, UiArgumentParser::Positional { .. }))
            .collect::<Vec<_>>();
        let options = self
            .arguments
            .iter()
            .filter(|argument| !matches!(argument.parser, UiArgumentParser::Positional { .. }))
            .collect::<Vec<_>>();

        let _ = writeln!(out, "{} - {}", self.invocation_name, self.summary);
        let _ = writeln!(out);
        let _ = writeln!(out, "Usage:");
        let _ = writeln!(out, "  {}", self.usage);
        if !positionals.is_empty() {
            let _ = writeln!(out);
            let _ = writeln!(out, "Arguments:");
            write_help_section(&mut out, &positionals);
        }
        if !options.is_empty() {
            let _ = writeln!(out);
            let _ = writeln!(out, "Options:");
            write_help_section(&mut out, &options);
        }
        out
    }

    /// Serialize the schema as pretty-printed JSON.
    pub fn render_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Return an argument definition by stable identifier.
    pub fn argument(&self, id: &str) -> Option<&UiArgumentSchema> {
        self.arguments.iter().find(|argument| argument.id == id)
    }
}

/// Argument definition within the `--ui-schema` payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UiArgumentSchema {
    /// Stable argument identifier used by launchers and tests.
    pub id: String,
    /// Human-friendly label for form rendering.
    pub label: String,
    /// Zero-based presentation and parsing order.
    pub order: usize,
    /// Parsing model for this argument.
    pub parser: UiArgumentParser,
    /// Value type expected by the argument.
    pub value_kind: UiValueKind,
    /// Whether the argument must be provided explicitly.
    pub required: bool,
    /// Default value encoded as a string when applicable.
    pub default: Option<String>,
    /// Help text used by `--help` and TUI affordances.
    pub help: String,
    /// Logical group name for TUI sections.
    pub group: String,
    /// Whether the argument should be treated as advanced.
    pub advanced: bool,
    /// Whether the argument should be hidden from the TUI form.
    pub hidden_in_tui: bool,
}

impl UiArgumentSchema {
    fn help_spec(&self) -> String {
        match &self.parser {
            UiArgumentParser::Positional { metavar } => format!("<{metavar}>"),
            UiArgumentParser::Option { flags, metavar, .. } => {
                format!("{} <{metavar}>", flags.join(", "))
            }
            UiArgumentParser::Toggle {
                true_flags,
                false_flags,
            } => true_flags
                .iter()
                .chain(false_flags.iter())
                .cloned()
                .collect::<Vec<_>>()
                .join(", "),
            UiArgumentParser::Action { flags, .. } => flags.join(", "),
        }
    }

    /// Return the boolean default value when this argument is a toggle.
    pub fn default_bool(&self) -> Option<bool> {
        self.default.as_deref().and_then(|value| match value {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        })
    }
}

/// Parsing mode for one command argument.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UiArgumentParser {
    /// A positional argument consumed in declaration order.
    Positional {
        /// Placeholder name shown in help output.
        metavar: String,
    },
    /// An option taking an explicit value.
    Option {
        /// Accepted short and long flags.
        flags: Vec<String>,
        /// Placeholder name shown in help output.
        metavar: String,
        /// Allowed values for choice-style options.
        choices: Vec<String>,
    },
    /// A boolean toggle with explicit enable and disable flags.
    Toggle {
        /// Flags that set the toggle to `true`.
        true_flags: Vec<String>,
        /// Flags that set the toggle to `false`.
        false_flags: Vec<String>,
    },
    /// A meta action such as `--help`.
    Action {
        /// Accepted flags that trigger the action.
        flags: Vec<String>,
        /// Action semantics for the flag.
        action: UiActionKind,
    },
}

/// High-level value type used by the TUI form renderer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UiValueKind {
    /// No associated value.
    None,
    /// Filesystem path.
    Path,
    /// Free-form string.
    String,
    /// Boolean toggle.
    Bool,
    /// Enumerated choice.
    Choice,
    /// Floating-point number.
    Float,
}

/// Meta action exposed as a flag in the command schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UiActionKind {
    /// Render human-readable help.
    Help,
    /// Emit the machine-readable UI schema.
    UiSchema,
}

/// Structured-output contract for rich UI renderers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UiManagedOutputSchema {
    /// Renderer identifier understood by the launcher.
    pub renderer: String,
    /// Expected stdout encoding for the structured renderer.
    pub stdout_format: String,
    /// Arguments the launcher should inject when it wants structured output.
    pub inject_arguments: Vec<UiInjectedArgument>,
    /// Whether raw stdout inspection should remain available.
    pub raw_stdout_available: bool,
    /// Whether raw stderr inspection should remain available.
    pub raw_stderr_available: bool,
}

/// One launcher-managed argument injection for structured output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UiInjectedArgument {
    /// Flag name to inject into the subprocess argv.
    pub flag: String,
    /// Value paired with the injected flag.
    pub value: String,
}

fn parse_args(
    schema: &UiCommandSchema,
    args: impl IntoIterator<Item = OsString>,
) -> Result<CliAction, String> {
    let args = args.into_iter().collect::<Vec<_>>();
    let positional_ids = schema
        .arguments
        .iter()
        .filter_map(|argument| match argument.parser {
            UiArgumentParser::Positional { .. } => Some(argument.id.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let mut positional_index = 0;
    let mut parsed = ParsedValues::default();
    let mut index = 0;

    while index < args.len() {
        let arg = &args[index];
        let raw = arg.to_string_lossy();
        if raw.starts_with('-') && raw != "-" {
            if let Some(action) = parse_action_flag(schema, raw.as_ref()) {
                return Ok(action);
            }
            if let Some((id, value)) = parse_option_assignment(schema, raw.as_ref()) {
                parsed.insert_string(id, value);
                index += 1;
                continue;
            }
            if let Some((id, value)) = parse_toggle_flag(schema, raw.as_ref()) {
                parsed.insert_bool(id, value);
                index += 1;
                continue;
            }
            if let Some(id) = parse_option_flag(schema, raw.as_ref()) {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| {
                        format!("missing value for {raw:?} in {}", schema.invocation_name)
                    })?
                    .to_string_lossy()
                    .to_string();
                parsed.insert_string(id, value);
                index += 1;
                continue;
            }
            return Err(format!("unknown option {raw:?}"));
        }

        let positional_id = positional_ids
            .get(positional_index)
            .ok_or_else(|| "expected exactly one MeasurementSet path".to_string())?;
        parsed.insert_string(positional_id, raw.to_string());
        positional_index += 1;
        index += 1;
    }

    for argument in &schema.arguments {
        if argument.required && !parsed.values.contains_key(&argument.id) {
            return Err(match &argument.parser {
                UiArgumentParser::Positional { metavar } => {
                    format!("missing required argument <{metavar}>")
                }
                UiArgumentParser::Option { flags, .. } | UiArgumentParser::Action { flags, .. } => {
                    format!("missing required option {}", flags.join(", "))
                }
                UiArgumentParser::Toggle {
                    true_flags,
                    false_flags,
                } => format!(
                    "missing required toggle {}",
                    true_flags
                        .iter()
                        .chain(false_flags.iter())
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            });
        }
    }

    build_run_options(schema, &parsed).map(|options| CliAction::Run(Box::new(options)))
}

fn parse_action_flag(schema: &UiCommandSchema, raw: &str) -> Option<CliAction> {
    schema
        .arguments
        .iter()
        .find_map(|argument| match &argument.parser {
            UiArgumentParser::Action { flags, action } if flags.iter().any(|flag| flag == raw) => {
                Some(match action {
                    UiActionKind::Help => CliAction::Help,
                    UiActionKind::UiSchema => CliAction::UiSchema,
                })
            }
            _ => None,
        })
}

fn parse_toggle_flag<'a>(schema: &'a UiCommandSchema, raw: &str) -> Option<(&'a str, bool)> {
    schema
        .arguments
        .iter()
        .find_map(|argument| match &argument.parser {
            UiArgumentParser::Toggle {
                true_flags,
                false_flags,
            } if true_flags.iter().any(|flag| flag == raw) => Some((argument.id.as_str(), true)),
            UiArgumentParser::Toggle {
                true_flags: _,
                false_flags,
            } if false_flags.iter().any(|flag| flag == raw) => Some((argument.id.as_str(), false)),
            _ => None,
        })
}

fn parse_option_flag<'a>(schema: &'a UiCommandSchema, raw: &str) -> Option<&'a str> {
    schema
        .arguments
        .iter()
        .find_map(|argument| match &argument.parser {
            UiArgumentParser::Option { flags, .. } if flags.iter().any(|flag| flag == raw) => {
                Some(argument.id.as_str())
            }
            _ => None,
        })
}

fn parse_option_assignment<'a>(
    schema: &'a UiCommandSchema,
    raw: &str,
) -> Option<(&'a str, String)> {
    schema
        .arguments
        .iter()
        .find_map(|argument| match &argument.parser {
            UiArgumentParser::Option { flags, .. } => flags.iter().find_map(|flag| {
                flag.strip_prefix("--").and_then(|_| {
                    raw.strip_prefix(&format!("{flag}="))
                        .map(|value| (argument.id.as_str(), value.to_string()))
                })
            }),
            _ => None,
        })
}

fn build_run_options(
    schema: &UiCommandSchema,
    parsed: &ParsedValues,
) -> Result<CliOptions, String> {
    let output = parsed.optional_string("output")?;
    let listfile = parsed.optional_string("listfile")?;
    if output.is_some() && listfile.is_some() {
        return Err("cannot combine --output and --listfile".to_string());
    }

    let format = parsed
        .optional_string("format")?
        .or_else(|| {
            schema
                .argument("format")
                .and_then(|argument| argument.default.clone())
        })
        .unwrap_or_else(|| "text".to_string());

    let cachesize_mb = parsed
        .optional_string("cachesize")?
        .map(|value| {
            value
                .parse::<f32>()
                .map_err(|_| format!("invalid float value for --cachesize: {value:?}"))
        })
        .transpose()?;

    Ok(CliOptions {
        path: PathBuf::from(parsed.required_string("ms_path")?),
        format: ListObsOutputFormat::parse(&format)?,
        output: output.or(listfile).map(PathBuf::from),
        overwrite: parsed.bool_or_default(schema, "overwrite")?,
        listobs: ListObsOptions {
            verbose: parsed.bool_or_default(schema, "verbose")?,
            selectdata: parsed.bool_or_default(schema, "selectdata")?,
            field: parsed.optional_string("field")?,
            spw: parsed.optional_string("spw")?,
            antenna: parsed.optional_string("antenna")?,
            scan: parsed.optional_string("scan")?,
            observation: parsed.optional_string("observation")?,
            array: parsed.optional_string("array")?,
            timerange: parsed.optional_string("timerange")?,
            uvrange: parsed.optional_string("uvrange")?,
            correlation: parsed.optional_string("correlation")?,
            intent: parsed.optional_string("intent")?,
            feed: parsed.optional_string("feed")?,
            listunfl: parsed.bool_or_default(schema, "listunfl")?,
            cachesize_mb,
        },
    })
}

fn write_help_section(out: &mut String, arguments: &[&UiArgumentSchema]) {
    let width = arguments
        .iter()
        .map(|argument| argument.help_spec().len())
        .max()
        .unwrap_or(0);
    for argument in arguments {
        let spec = argument.help_spec();
        let _ = writeln!(out, "  {spec:<width$}  {}", argument.help);
    }
}

fn positional_argument(spec: PositionalSpec<'_>) -> UiArgumentSchema {
    let PositionalSpec {
        common,
        metavar,
        value_kind,
        required,
    } = spec;
    UiArgumentSchema {
        id: common.id.to_string(),
        label: common.label.to_string(),
        order: common.order,
        parser: UiArgumentParser::Positional {
            metavar: metavar.to_string(),
        },
        value_kind,
        required,
        default: None,
        help: common.help.to_string(),
        group: common.group.to_string(),
        advanced: common.advanced,
        hidden_in_tui: common.hidden_in_tui,
    }
}

fn option_argument(spec: OptionSpec<'_>) -> UiArgumentSchema {
    let OptionSpec {
        common,
        flags,
        metavar,
        value_kind,
        default,
        choices,
    } = spec;
    UiArgumentSchema {
        id: common.id.to_string(),
        label: common.label.to_string(),
        order: common.order,
        parser: UiArgumentParser::Option {
            flags: flags.iter().map(|flag| (*flag).to_string()).collect(),
            metavar: metavar.to_string(),
            choices: choices.iter().map(|choice| (*choice).to_string()).collect(),
        },
        value_kind,
        required: false,
        default: default.map(str::to_string),
        help: common.help.to_string(),
        group: common.group.to_string(),
        advanced: common.advanced,
        hidden_in_tui: common.hidden_in_tui,
    }
}

fn toggle_argument(spec: ToggleSpec<'_>) -> UiArgumentSchema {
    let ToggleSpec {
        common,
        true_flags,
        false_flags,
        default,
    } = spec;
    UiArgumentSchema {
        id: common.id.to_string(),
        label: common.label.to_string(),
        order: common.order,
        parser: UiArgumentParser::Toggle {
            true_flags: true_flags.iter().map(|flag| (*flag).to_string()).collect(),
            false_flags: false_flags.iter().map(|flag| (*flag).to_string()).collect(),
        },
        value_kind: UiValueKind::Bool,
        required: false,
        default: Some(default.to_string()),
        help: common.help.to_string(),
        group: common.group.to_string(),
        advanced: common.advanced,
        hidden_in_tui: common.hidden_in_tui,
    }
}

fn action_argument(
    order: usize,
    id: &str,
    label: &str,
    flags: &[&str],
    action: UiActionKind,
    help: &str,
) -> UiArgumentSchema {
    UiArgumentSchema {
        id: id.to_string(),
        label: label.to_string(),
        order,
        parser: UiArgumentParser::Action {
            flags: flags.iter().map(|flag| (*flag).to_string()).collect(),
            action,
        },
        value_kind: UiValueKind::None,
        required: false,
        default: None,
        help: help.to_string(),
        group: "System".to_string(),
        advanced: false,
        hidden_in_tui: true,
    }
}

fn write_output(options: &CliOptions, rendered: &str) -> Result<(), String> {
    if let Some(path) = &options.output {
        if path.exists() && !options.overwrite {
            return Err(format!(
                "refusing to overwrite existing output file {}; pass --overwrite to replace it",
                path.display()
            ));
        }
        fs::write(path, format!("{rendered}\n"))
            .map_err(|error| format!("write {}: {error}", path.display()))
    } else {
        println!("{rendered}");
        Ok(())
    }
}
