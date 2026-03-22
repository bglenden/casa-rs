// SPDX-License-Identifier: LGPL-3.0-or-later
//! `tablebrowser` - inspect arbitrary casacore tables from the command line.

use std::env;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::process;

use casacore_tablebrowser_protocol::{
    BrowserCommand, BrowserRequestEnvelope, BrowserViewport, PROTOCOL_VERSION,
};
use casacore_tables::{TableBrowser, TableBrowserView};
use serde_json::json;

const DEFAULT_ROW_LIMIT: usize = 20;
const DEFAULT_CLI_WIDTH: u16 = 160;
const DEFAULT_SECTION_HEIGHT: u16 = 200;
const DEFAULT_INSPECT_HEIGHT: u16 = 48;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputView {
    Overview,
    Columns,
    Keywords,
    Cells,
    Subtables,
    All,
}

impl OutputView {
    fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "overview" => Ok(Self::Overview),
            "columns" => Ok(Self::Columns),
            "keywords" => Ok(Self::Keywords),
            "cells" => Ok(Self::Cells),
            "subtables" => Ok(Self::Subtables),
            "all" => Ok(Self::All),
            other => Err(format!(
                "unsupported --view value {other:?}; expected one of: overview, columns, keywords, cells, subtables, all"
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum InspectTarget {
    Cell { row: usize, column: String },
    TableKeyword { path: Vec<String> },
    ColumnKeyword { column: String, path: Vec<String> },
}

#[derive(Debug)]
struct CliOptions {
    table_path: PathBuf,
    view: OutputView,
    row_start: usize,
    row_limit: usize,
    open_linked: Option<usize>,
    inspect: Option<InspectTarget>,
}

fn main() {
    match run() {
        Ok(()) => {}
        Err(error) => {
            eprintln!("Error: {error}");
            process::exit(1);
        }
    }
}

fn run() -> Result<(), String> {
    let mut args = env::args().skip(1).peekable();
    if args.peek().is_some_and(|arg| arg == "--ui-schema") {
        print!("{}", ui_schema_json()?);
        return Ok(());
    }
    if args.peek().is_some_and(|arg| arg == "--session") {
        args.next();
        if args.peek().is_some() {
            return Err("--session does not accept positional arguments".to_string());
        }
        run_session()?;
        return Ok(());
    }

    let options = parse_args(args)?;
    run_snapshot(options)
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<CliOptions, String> {
    let mut args = args.into_iter();
    let mut view = OutputView::All;
    let mut row_start = 0usize;
    let mut row_limit = DEFAULT_ROW_LIMIT;
    let mut open_linked = None;
    let mut inspect = None;
    let mut table_path = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                print!("{}", usage());
                process::exit(0);
            }
            "--view" => {
                let value = args
                    .next()
                    .ok_or_else(|| "missing value for --view".to_string())?;
                view = OutputView::parse(&value)?;
            }
            "--row-start" => {
                let value = args
                    .next()
                    .ok_or_else(|| "missing value for --row-start".to_string())?;
                row_start = value
                    .parse::<usize>()
                    .map_err(|error| format!("parse --row-start: {error}"))?;
            }
            "--row-limit" => {
                let value = args
                    .next()
                    .ok_or_else(|| "missing value for --row-limit".to_string())?;
                row_limit = value
                    .parse::<usize>()
                    .map_err(|error| format!("parse --row-limit: {error}"))?;
            }
            "--open-linked" => {
                let value = args
                    .next()
                    .ok_or_else(|| "missing value for --open-linked".to_string())?;
                open_linked = Some(
                    value
                        .parse::<usize>()
                        .map_err(|error| format!("parse --open-linked: {error}"))?,
                );
            }
            "--inspect" => {
                let value = args
                    .next()
                    .ok_or_else(|| "missing value for --inspect".to_string())?;
                inspect = Some(parse_inspect_target(&value)?);
            }
            raw if raw.starts_with('-') => return Err(format!("unknown option {raw:?}")),
            raw => {
                if table_path.is_some() {
                    return Err("expected exactly one table path".to_string());
                }
                table_path = Some(PathBuf::from(raw));
            }
        }
    }

    let table_path = table_path.ok_or_else(|| "missing required <table-path>".to_string())?;
    Ok(CliOptions {
        table_path,
        view,
        row_start,
        row_limit,
        open_linked,
        inspect,
    })
}

fn parse_inspect_target(raw: &str) -> Result<InspectTarget, String> {
    if let Some(rest) = raw.strip_prefix("cell:") {
        let mut parts = rest.splitn(3, ':');
        let row = parts
            .next()
            .ok_or_else(|| "missing row in cell inspect target".to_string())?
            .parse::<usize>()
            .map_err(|error| format!("parse cell row: {error}"))?;
        let column = parts
            .next()
            .ok_or_else(|| "missing column in cell inspect target".to_string())?
            .to_string();
        if column.is_empty() {
            return Err("cell inspect target requires a column name".to_string());
        }
        return Ok(InspectTarget::Cell { row, column });
    }

    if let Some(rest) = raw.strip_prefix("keyword:") {
        let path = parse_keyword_path(rest)?;
        return Ok(InspectTarget::TableKeyword { path });
    }

    if let Some(rest) = raw.strip_prefix("column-keyword:") {
        let mut parts = rest.splitn(2, ':');
        let column = parts
            .next()
            .ok_or_else(|| "missing column in column-keyword inspect target".to_string())?;
        let path =
            parse_keyword_path(parts.next().ok_or_else(|| {
                "missing keyword path in column-keyword inspect target".to_string()
            })?)?;
        return Ok(InspectTarget::ColumnKeyword {
            column: column.to_string(),
            path,
        });
    }

    Err(
        "unsupported --inspect target; expected cell:<row>:<column>, keyword:<path>, or column-keyword:<column>:<path>"
            .to_string(),
    )
}

fn parse_keyword_path(raw: &str) -> Result<Vec<String>, String> {
    let path = raw
        .split('.')
        .filter(|segment| !segment.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if path.is_empty() {
        return Err("keyword inspect target requires a non-empty path".to_string());
    }
    Ok(path)
}

fn run_snapshot(options: CliOptions) -> Result<(), String> {
    let mut browser = TableBrowser::open(&options.table_path).map_err(|error| error.to_string())?;
    if let Some(index) = options.open_linked {
        browser.set_view(TableBrowserView::Subtables);
        if !browser.select_subtable(index) {
            return Err(format!("linked table index {index} is out of range"));
        }
        browser
            .open_selected_subtable()
            .map_err(|error| error.to_string())?;
    }

    if let Some(target) = options.inspect.as_ref() {
        print_inspector(&mut browser, target)?;
        return Ok(());
    }

    match options.view {
        OutputView::Overview => print_section(
            TableBrowserView::Overview.label(),
            &browser.render_view_lines(
                TableBrowserView::Overview,
                BrowserViewport::new(DEFAULT_CLI_WIDTH, DEFAULT_SECTION_HEIGHT),
            ),
        ),
        OutputView::Columns => print_section(
            TableBrowserView::Columns.label(),
            &browser.render_view_lines(
                TableBrowserView::Columns,
                BrowserViewport::new(DEFAULT_CLI_WIDTH, DEFAULT_SECTION_HEIGHT),
            ),
        ),
        OutputView::Keywords => print_section(
            TableBrowserView::Keywords.label(),
            &browser.render_view_lines(
                TableBrowserView::Keywords,
                BrowserViewport::new(DEFAULT_CLI_WIDTH, DEFAULT_SECTION_HEIGHT),
            ),
        ),
        OutputView::Cells => {
            if browser.column_count() > 0 {
                browser
                    .select_cell_position(options.row_start, 0)
                    .map_err(|error| error.to_string())?;
            }
            print_section(
                TableBrowserView::Cells.label(),
                &browser.render_view_lines(
                    TableBrowserView::Cells,
                    BrowserViewport::new(
                        DEFAULT_CLI_WIDTH,
                        cells_viewport_height(options.row_limit),
                    ),
                ),
            );
        }
        OutputView::Subtables => print_section(
            TableBrowserView::Subtables.label(),
            &browser.render_view_lines(
                TableBrowserView::Subtables,
                BrowserViewport::new(DEFAULT_CLI_WIDTH, DEFAULT_SECTION_HEIGHT),
            ),
        ),
        OutputView::All => {
            print_section(
                TableBrowserView::Overview.label(),
                &browser.render_view_lines(
                    TableBrowserView::Overview,
                    BrowserViewport::new(DEFAULT_CLI_WIDTH, DEFAULT_SECTION_HEIGHT),
                ),
            );
            println!();
            print_section(
                TableBrowserView::Columns.label(),
                &browser.render_view_lines(
                    TableBrowserView::Columns,
                    BrowserViewport::new(DEFAULT_CLI_WIDTH, DEFAULT_SECTION_HEIGHT),
                ),
            );
            println!();
            print_section(
                TableBrowserView::Keywords.label(),
                &browser.render_view_lines(
                    TableBrowserView::Keywords,
                    BrowserViewport::new(DEFAULT_CLI_WIDTH, DEFAULT_SECTION_HEIGHT),
                ),
            );
            println!();
            print_section(
                TableBrowserView::Subtables.label(),
                &browser.render_view_lines(
                    TableBrowserView::Subtables,
                    BrowserViewport::new(DEFAULT_CLI_WIDTH, DEFAULT_SECTION_HEIGHT),
                ),
            );
            println!();
            if browser.column_count() > 0 {
                browser
                    .select_cell_position(options.row_start, 0)
                    .map_err(|error| error.to_string())?;
            }
            print_section(
                TableBrowserView::Cells.label(),
                &browser.render_view_lines(
                    TableBrowserView::Cells,
                    BrowserViewport::new(
                        DEFAULT_CLI_WIDTH,
                        cells_viewport_height(options.row_limit),
                    ),
                ),
            );
        }
    }

    Ok(())
}

fn print_inspector(browser: &mut TableBrowser, target: &InspectTarget) -> Result<(), String> {
    match target {
        InspectTarget::Cell { row, column } => browser
            .select_cell(*row, column)
            .map_err(|error| error.to_string())?,
        InspectTarget::TableKeyword { path } => browser
            .select_table_keyword(path)
            .map_err(|error| error.to_string())?,
        InspectTarget::ColumnKeyword { column, path } => browser
            .select_column_keyword(column, path)
            .map_err(|error| error.to_string())?,
    }
    let snapshot = browser
        .apply(BrowserCommand::Resize {
            viewport: BrowserViewport::new(DEFAULT_CLI_WIDTH, DEFAULT_INSPECT_HEIGHT),
        })
        .map_err(|error| error.to_string())?;
    let Some(inspector) = snapshot.inspector else {
        return Err("selected target does not expose an inspector payload".to_string());
    };
    print_section("Inspector", &inspector.rendered_lines);
    Ok(())
}

fn run_session() -> Result<(), String> {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut browser: Option<TableBrowser> = None;

    for line_result in stdin.lock().lines() {
        let line = line_result.map_err(|error| format!("read stdin: {error}"))?;
        if line.trim().is_empty() {
            continue;
        }

        let request: BrowserRequestEnvelope = match serde_json::from_str(&line) {
            Ok(request) => request,
            Err(error) => {
                let response = casacore_tablebrowser_protocol::BrowserResponseEnvelope::error(
                    "invalid_json",
                    format!("parse request: {error}"),
                );
                writeln!(
                    stdout,
                    "{}",
                    serde_json::to_string(&response)
                        .map_err(|error| format!("serialize response: {error}"))?
                )
                .map_err(|error| format!("write response: {error}"))?;
                stdout
                    .flush()
                    .map_err(|error| format!("flush response: {error}"))?;
                continue;
            }
        };

        let response = if request.version != PROTOCOL_VERSION {
            casacore_tablebrowser_protocol::BrowserResponseEnvelope::error(
                "unsupported_version",
                format!(
                    "expected protocol version {}, received {}",
                    PROTOCOL_VERSION, request.version
                ),
            )
        } else {
            match (&mut browser, &request.command) {
                (Some(browser), _) => browser.handle_request(request),
                (None, BrowserCommand::OpenRoot { path, viewport }) => {
                    match TableBrowser::open(path) {
                        Ok(mut new_browser) => {
                            let response = new_browser.handle_request(BrowserRequestEnvelope::new(
                                BrowserCommand::Resize {
                                    viewport: *viewport,
                                },
                            ));
                            browser = Some(new_browser);
                            response
                        }
                        Err(error) => {
                            casacore_tablebrowser_protocol::BrowserResponseEnvelope::error(
                                "open_root_failed",
                                error.to_string(),
                            )
                        }
                    }
                }
                (None, _) => casacore_tablebrowser_protocol::BrowserResponseEnvelope::error(
                    "session_not_open",
                    "send open_root before any other browser command",
                ),
            }
        };

        writeln!(
            stdout,
            "{}",
            serde_json::to_string(&response)
                .map_err(|error| format!("serialize response: {error}"))?
        )
        .map_err(|error| format!("write response: {error}"))?;
        stdout
            .flush()
            .map_err(|error| format!("flush response: {error}"))?;
    }

    Ok(())
}

fn ui_schema_json() -> Result<String, String> {
    let schema = json!({
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
                "parser": {
                    "kind": "positional",
                    "metavar": "table-path"
                },
                "value_kind": "path",
                "required": true,
                "default": null,
                "help": "Path to the casacore table root directory",
                "group": "Input",
                "advanced": false,
                "hidden_in_tui": false
            },
            {
                "id": "help",
                "label": "Help",
                "order": 1,
                "parser": {
                    "kind": "action",
                    "flags": ["-h", "--help"],
                    "action": "help"
                },
                "value_kind": "none",
                "required": false,
                "default": null,
                "help": "Print this help message",
                "group": "Input",
                "advanced": true,
                "hidden_in_tui": true
            }
        ],
        "managed_output": null
    });
    serde_json::to_string_pretty(&schema).map_err(|error| format!("serialize ui schema: {error}"))
}

fn print_section(title: &str, lines: &[String]) {
    println!("== {title} ==");
    for line in lines {
        println!("{line}");
    }
}

fn cells_viewport_height(row_limit: usize) -> u16 {
    row_limit.saturating_add(2).max(1).min(u16::MAX as usize) as u16
}

fn usage() -> String {
    format!(
        "\
tablebrowser - inspect arbitrary casacore tables

Usage:
  tablebrowser [OPTIONS] <table-path>
  tablebrowser --session
  tablebrowser --ui-schema

Options:
  --view VIEW         overview | columns | keywords | cells | subtables | all
  --row-start N       first row index for the cells view (default: 0)
  --row-limit N       number of rows to print in snapshot mode (default: {DEFAULT_ROW_LIMIT})
  --open-linked N     follow discovered linked-table index N before rendering
  --inspect TARGET    cell:<row>:<column> | keyword:<path> | column-keyword:<column>:<path>
  --session           run the long-lived JSON Lines browser session on stdio
  --ui-schema         print the launcher schema consumed by casars
  -h, --help          show this help

",
    )
}
