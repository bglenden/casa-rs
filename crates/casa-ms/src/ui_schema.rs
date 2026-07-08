// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared machine-readable CLI schema for launcher-style frontends.

use std::fmt::Write as _;

use casa_logging::{
    CasaPriority, LOG_STDERR_PRIORITY_FLAG, LOG_TABLE_FLAG, LOG_TABLE_PRIORITY_FLAG,
};
use serde::{Deserialize, Serialize};

/// Machine-readable UI schema emitted by `--ui-schema`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UiCommandSchema {
    /// Schema version for the `--ui-schema` payload itself.
    pub schema_version: u32,
    /// Stable command identifier shared across aliases.
    pub command_id: String,
    /// Invocation name used for this schema instance, such as `msexplore`.
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
    /// Structured-output contract used by rich UI renderers.
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

/// Return the shared CASA logging options exposed by task-style CLIs.
pub fn logging_argument_schemas(order_base: usize) -> Vec<UiArgumentSchema> {
    vec![
        UiArgumentSchema {
            id: "log_table".to_string(),
            label: "Log Table".to_string(),
            order: order_base,
            parser: UiArgumentParser::Option {
                flags: vec![LOG_TABLE_FLAG.to_string()],
                metavar: "PATH".to_string(),
                choices: Vec::new(),
            },
            value_kind: UiValueKind::Path,
            required: false,
            default: None,
            help: "Write CASA-compatible log records to this table path.".to_string(),
            group: "Logging".to_string(),
            advanced: true,
            hidden_in_tui: false,
        },
        UiArgumentSchema {
            id: "log_table_priority".to_string(),
            label: "Log Table Priority".to_string(),
            order: order_base + 1,
            parser: UiArgumentParser::Option {
                flags: vec![LOG_TABLE_PRIORITY_FLAG.to_string()],
                metavar: "PRIORITY".to_string(),
                choices: casa_log_priority_choices(false),
            },
            value_kind: UiValueKind::Choice,
            required: false,
            default: Some("INFO".to_string()),
            help: "Minimum CASA priority written to the log table.".to_string(),
            group: "Logging".to_string(),
            advanced: true,
            hidden_in_tui: false,
        },
        UiArgumentSchema {
            id: "log_stderr_priority".to_string(),
            label: "Stderr Log Priority".to_string(),
            order: order_base + 2,
            parser: UiArgumentParser::Option {
                flags: vec![LOG_STDERR_PRIORITY_FLAG.to_string()],
                metavar: "PRIORITY".to_string(),
                choices: casa_log_priority_choices(true),
            },
            value_kind: UiValueKind::Choice,
            required: false,
            default: Some("WARN".to_string()),
            help: "Minimum CASA priority mirrored to stderr; use off to disable.".to_string(),
            group: "Logging".to_string(),
            advanced: true,
            hidden_in_tui: false,
        },
    ]
}

fn casa_log_priority_choices(include_off: bool) -> Vec<String> {
    let mut choices = CasaPriority::ALL
        .into_iter()
        .map(|priority| priority.as_casa_str().to_string())
        .collect::<Vec<_>>();
    if include_off {
        choices.push("off".to_string());
    }
    choices
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
