// SPDX-License-Identifier: LGPL-3.0-or-later
use std::ffi::OsString;

use casacore_ms::listobs::cli::{UiActionKind, UiArgumentParser, UiCommandSchema};

use crate::registry::{RegistryApp, registered_apps, resolve_app};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StartupPrefill {
    pub id: String,
    pub value: StartupValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StartupValue {
    Text(String),
    Toggle(bool),
}

#[derive(Debug, Clone)]
pub(crate) struct StartupLaunch {
    pub app: RegistryApp,
    pub prefill: Vec<StartupPrefill>,
    pub auto_run: bool,
}

#[derive(Debug, Clone)]
pub(crate) enum StartupSelection {
    Launcher,
    PrintText(String),
    App(StartupLaunch),
}

pub(crate) fn parse_startup_args(
    args: impl IntoIterator<Item = OsString>,
) -> Result<StartupSelection, String> {
    let mut args = args.into_iter();
    let Some(first) = args.next() else {
        return Ok(StartupSelection::Launcher);
    };
    let first = decode_arg(first, "casars argument")?;
    if matches!(first.as_str(), "-h" | "--help") {
        return Ok(StartupSelection::PrintText(render_casars_help()));
    }

    let (app_id, app_args) = if first == "--app" {
        let app_id = args
            .next()
            .ok_or_else(|| "missing app id after --app".to_string())
            .and_then(|value| decode_arg(value, "app id"))?;
        (app_id, args.collect::<Vec<_>>())
    } else if first.starts_with('-') {
        return Err(format!(
            "unknown casars option {first:?}; use --help to see supported startup syntax"
        ));
    } else {
        (first, args.collect::<Vec<_>>())
    };

    let app = resolve_app(Some(&app_id))?;
    if app_args.is_empty() {
        return Ok(StartupSelection::App(StartupLaunch {
            app,
            prefill: Vec::new(),
            auto_run: false,
        }));
    }

    let schema = app.load_schema()?;
    match parse_schema_prefill_args(&schema, app_args)? {
        SchemaPrefillParse::PrintText(text) => Ok(StartupSelection::PrintText(text)),
        SchemaPrefillParse::Prefill { values, auto_run } => {
            Ok(StartupSelection::App(StartupLaunch {
                app,
                prefill: values,
                auto_run,
            }))
        }
    }
}

#[derive(Debug)]
enum SchemaPrefillParse {
    PrintText(String),
    Prefill {
        values: Vec<StartupPrefill>,
        auto_run: bool,
    },
}

fn parse_schema_prefill_args(
    schema: &UiCommandSchema,
    args: Vec<OsString>,
) -> Result<SchemaPrefillParse, String> {
    let mut values = Vec::<StartupPrefill>::new();
    let positional_ids = schema
        .arguments
        .iter()
        .filter_map(|argument| match argument.parser {
            UiArgumentParser::Positional { .. } if !argument.hidden_in_tui => {
                Some(argument.id.as_str())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let mut positional_index = 0usize;
    let mut raw_args = args
        .into_iter()
        .map(|value| decode_arg(value, &schema.command_id))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .peekable();
    let mut end_of_options = false;
    let mut saw_prefill = false;

    while let Some(raw) = raw_args.next() {
        if !end_of_options && raw == "--" {
            end_of_options = true;
            continue;
        }

        if !end_of_options && raw.starts_with('-') {
            let Some(argument) = find_flag_argument(schema, &raw) else {
                return Err(format!("unknown {} argument {raw:?}", schema.command_id));
            };

            match &argument.parser {
                UiArgumentParser::Action { action, .. } => {
                    if saw_prefill || raw_args.peek().is_some() {
                        return Err(format!(
                            "{raw} cannot be combined with other {} startup arguments",
                            schema.command_id
                        ));
                    }
                    let text = match action {
                        UiActionKind::Help => schema.render_help(),
                        UiActionKind::UiSchema => schema.render_json_pretty().map_err(|error| {
                            format!("serialize {} ui schema: {error}", schema.command_id)
                        })?,
                    };
                    return Ok(SchemaPrefillParse::PrintText(text));
                }
                _ if argument.hidden_in_tui => {
                    return Err(format!(
                        "{} is managed internally by casars and cannot be set on startup",
                        argument.label
                    ));
                }
                UiArgumentParser::Toggle { true_flags, .. } => {
                    let enabled = true_flags.iter().any(|flag| flag == &raw);
                    upsert_value(
                        &mut values,
                        argument.id.clone(),
                        StartupValue::Toggle(enabled),
                    );
                    saw_prefill = true;
                }
                UiArgumentParser::Option { choices, .. } => {
                    let value = raw_args.next().ok_or_else(|| {
                        format!(
                            "missing value for {raw} in {} startup arguments",
                            schema.command_id
                        )
                    })?;
                    if !choices.is_empty() && !choices.iter().any(|choice| choice == &value) {
                        return Err(format!(
                            "invalid value {value:?} for {}; expected one of: {}",
                            argument.label,
                            choices.join(", ")
                        ));
                    }
                    upsert_value(&mut values, argument.id.clone(), StartupValue::Text(value));
                    saw_prefill = true;
                }
                UiArgumentParser::Positional { .. } => {
                    unreachable!("flags never map to positional arguments")
                }
            }
            continue;
        }

        let positional_id = positional_ids.get(positional_index).ok_or_else(|| {
            format!(
                "unexpected extra positional argument {raw:?} for {}",
                schema.command_id
            )
        })?;
        upsert_value(
            &mut values,
            (*positional_id).to_string(),
            StartupValue::Text(raw),
        );
        positional_index += 1;
        saw_prefill = true;
    }

    Ok(SchemaPrefillParse::Prefill {
        values,
        auto_run: saw_prefill,
    })
}

fn find_flag_argument<'a>(
    schema: &'a UiCommandSchema,
    raw: &str,
) -> Option<&'a casacore_ms::listobs::cli::UiArgumentSchema> {
    schema
        .arguments
        .iter()
        .find(|argument| match &argument.parser {
            UiArgumentParser::Option { flags, .. } | UiArgumentParser::Action { flags, .. } => {
                flags.iter().any(|flag| flag == raw)
            }
            UiArgumentParser::Toggle {
                true_flags,
                false_flags,
            } => true_flags
                .iter()
                .chain(false_flags.iter())
                .any(|flag| flag == raw),
            UiArgumentParser::Positional { .. } => false,
        })
}

fn upsert_value(values: &mut Vec<StartupPrefill>, id: String, value: StartupValue) {
    if let Some(existing) = values.iter_mut().find(|entry| entry.id == id) {
        existing.value = value;
    } else {
        values.push(StartupPrefill { id, value });
    }
}

fn decode_arg(value: OsString, context: &str) -> Result<String, String> {
    value.into_string().map_err(|raw| {
        format!(
            "{context} must be valid UTF-8; received {:?}",
            raw.to_string_lossy()
        )
    })
}

fn render_casars_help() -> String {
    let mut out = String::from(
        "casars - interactive launcher for CASA-oriented terminal apps\n\nUsage:\n  casars\n  casars <app-id> [app-args...]\n  casars --app <app-id> [app-args...]\n  casars --help\n\nApps:\n",
    );
    for app in registered_apps() {
        out.push_str(&format!(
            "  {:<12} {} / {}\n",
            app.id, app.category, app.display_name
        ));
    }
    out.push_str(
        "\nExamples:\n  casars\n  casars tablebrowser /path/to/table\n  casars --app listobs /path/to.ms --field 3C286\n",
    );
    out
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use casacore_ms::listobs::cli::command_schema;
    use serde_json::json;

    use super::{StartupSelection, StartupValue, parse_schema_prefill_args, parse_startup_args};

    #[test]
    fn empty_startup_args_open_launcher() {
        match parse_startup_args(Vec::<OsString>::new()).expect("parse startup args") {
            StartupSelection::Launcher => {}
            other => panic!("expected launcher, got {other:?}"),
        }
    }

    #[test]
    fn explicit_app_flag_selects_app_without_prefill() {
        match parse_startup_args(vec![
            OsString::from("--app"),
            OsString::from("tablebrowser"),
        ])
        .expect("parse startup args")
        {
            StartupSelection::App(selection) => {
                assert_eq!(selection.app.id, "tablebrowser");
                assert!(selection.prefill.is_empty());
                assert!(!selection.auto_run);
            }
            other => panic!("expected app startup, got {other:?}"),
        }
    }

    #[test]
    fn schema_prefill_parses_tablebrowser_positional_argument() {
        let schema = serde_json::from_value(json!({
            "schema_version": 1,
            "command_id": "tablebrowser",
            "invocation_name": "tablebrowser",
            "display_name": "Table Browser",
            "category": "Tables",
            "summary": "browse arbitrary casacore tables",
            "usage": "tablebrowser <table-path>",
            "arguments": [
                {
                    "id": "table_path",
                    "label": "Table Path",
                    "order": 0,
                    "parser": { "kind": "positional", "metavar": "table-path" },
                    "value_kind": "path",
                    "required": true,
                    "default": null,
                    "help": "Path to the casacore table root directory",
                    "group": "Input",
                    "advanced": false,
                    "hidden_in_tui": false
                }
            ],
            "managed_output": null
        }))
        .expect("tablebrowser schema");

        let result = parse_schema_prefill_args(&schema, vec![OsString::from("/tmp/example.ms")])
            .expect("parse tablebrowser startup args");
        let super::SchemaPrefillParse::Prefill { values, auto_run } = result else {
            panic!("expected prefill");
        };
        assert!(auto_run);
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].id, "table_path");
        assert_eq!(
            values[0].value,
            StartupValue::Text("/tmp/example.ms".to_string())
        );
    }

    #[test]
    fn schema_prefill_parses_listobs_options_and_toggles() {
        let schema = command_schema("listobs");
        let result = parse_schema_prefill_args(
            &schema,
            vec![
                OsString::from("/tmp/example.ms"),
                OsString::from("--field"),
                OsString::from("3C286"),
                OsString::from("--no-verbose"),
            ],
        )
        .expect("parse listobs startup args");
        let super::SchemaPrefillParse::Prefill { values, auto_run } = result else {
            panic!("expected prefill");
        };
        assert!(auto_run);
        assert!(values.iter().any(|entry| {
            entry.id == "ms_path"
                && entry.value == StartupValue::Text("/tmp/example.ms".to_string())
        }));
        assert!(values.iter().any(|entry| {
            entry.id == "field" && entry.value == StartupValue::Text("3C286".to_string())
        }));
        assert!(
            values.iter().any(|entry| {
                entry.id == "verbose" && entry.value == StartupValue::Toggle(false)
            })
        );
    }

    #[test]
    fn schema_prefill_rejects_hidden_arguments() {
        let schema = command_schema("listobs");
        let error = parse_schema_prefill_args(
            &schema,
            vec![OsString::from("--format"), OsString::from("json")],
        )
        .expect_err("hidden startup arg should fail");
        assert!(error.contains("managed internally"));
    }

    #[test]
    fn schema_prefill_renders_help_actions() {
        let schema = command_schema("listobs");
        let result = parse_schema_prefill_args(&schema, vec![OsString::from("--help")])
            .expect("help action should parse");
        let super::SchemaPrefillParse::PrintText(text) = result else {
            panic!("expected help text");
        };
        assert!(text.contains("Usage:"));
        assert!(text.contains("listobs"));
    }
}
