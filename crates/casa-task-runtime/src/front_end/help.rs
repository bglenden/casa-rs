// SPDX-License-Identifier: LGPL-3.0-or-later
//! `--help` for a task binary, generated from the surfaces it hosts.

use std::fmt::Write as _;

use casa_provider_contracts::{SurfaceContractBundle, SurfaceParameterBinding};

use super::inspect::display_value;
use super::options::task_command_line_help;
use crate::ParameterSession;

/// Help for `program`, which hosts `surfaces` (the first is the default), with
/// the parameters of `selected`.
pub(super) fn render_help(
    program: &str,
    surfaces: &[SurfaceContractBundle],
    selected: &SurfaceContractBundle,
) -> String {
    let surface = &selected.surface;
    let word = if surfaces.len() > 1 { " [SURFACE]" } else { "" };
    let mut out = String::new();
    let _ = writeln!(out, "{program} - {}", surface.summary());
    let _ = writeln!(out);
    let _ = writeln!(out, "Usage:");
    let _ = writeln!(out, "  {program}{word} [SOURCE] [OVERRIDES] [CONTROLS]");
    let _ = writeln!(
        out,
        "  {program}{word} --show [SOURCE] [OVERRIDES] [--save-params FILE]"
    );
    let _ = writeln!(
        out,
        "  {program}{word} --template | --describe [NAME] | --validate FILE"
    );
    let _ = writeln!(
        out,
        "  {program} --json-run SOURCE | --json-schema | --protocol-info"
    );
    if surfaces.len() > 1 {
        let names = surfaces
            .iter()
            .map(|bundle| bundle.surface.id())
            .collect::<Vec<_>>();
        let _ = writeln!(out);
        let _ = writeln!(
            out,
            "SURFACE is one of {} (default {}).",
            names.join(", "),
            names[0]
        );
    }
    let _ = writeln!(out);
    out.push_str(task_command_line_help());
    let _ = writeln!(out);
    let _ = writeln!(out, "Parameters of {}:", surface.id());
    let defaults = ParameterSession::defaults(selected.clone()).ok();
    let mut bindings = surface.bindings().iter().collect::<Vec<_>>();
    bindings.sort_by_key(|binding| binding.order);
    for binding in bindings {
        let default = defaults
            .as_ref()
            .and_then(|session| session.states().get(&binding.name))
            .and_then(|state| state.value.as_ref())
            .map_or_else(|| "required".to_string(), display_value);
        let summary = selected
            .catalog
            .concept(&binding.concept)
            .map_or("", |concept| concept.documentation.summary.as_str());
        let _ = writeln!(
            out,
            "  {:<28} {summary} (default {default})",
            spelling(binding)
        );
    }
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "--json-run takes {{\"surface\": ..., \"values\": {{...}}}} from SOURCE, or - for stdin, and\n\
writes one typed reply to stdout; it reads and writes no Last state."
    );
    out
}

fn spelling(binding: &SurfaceParameterBinding) -> String {
    let Some(cli) = binding.projections.cli.as_ref() else {
        return binding.name.clone();
    };
    let mut parts = Vec::new();
    if cli.positional.is_some() {
        parts.push(binding.name.to_uppercase());
    }
    if let Some(flag) = cli.flags.first() {
        parts.push(match &cli.metavar {
            Some(metavar) => format!("{flag} {metavar}"),
            None => flag.clone(),
        });
    }
    if let Some(flag) = cli.false_flags.first() {
        parts.push(flag.clone());
    }
    if parts.is_empty() {
        format!("--{}", binding.name.replace('_', "-"))
    } else {
        parts.join(", ")
    }
}
