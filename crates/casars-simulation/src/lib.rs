// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]

//! The command-line and task surface of casa-rs's simulation tasks:
//! `simobserve` (simulation Phase 5c, #841), with `simanalyze` and
//! `simalma` to come, on the shared task front end (#931).
//!
//! As for the imager, the front end resolves the parameters through the
//! provider-contracts catalog: command-line flags and a `--json-run` request
//! become the same resolved values, CASA's parameters with CASA's defaults,
//! and those values, and nothing else, make the [`SimobserveRequest`] the
//! task runs. The host adds what CASA's task takes from its environment:
//! CASA's data directories, where `alma/simmos` holds the antenna
//! configurations, and the measures data. A run's reply carries its
//! [`SimobserveRunTaskResult`]; simobserve returns None, as CASA's does.

mod schema;
mod task_contract;

use std::path::{Path, PathBuf};
use std::time::Instant;

use casa_provider_contracts::{RunProductKind, SurfaceContractBundle};
use casa_simulation::simobserve::{SimobserveParameters, SimobserveRequest, simobserve};
use casa_simulation::simutil::direction::Directions;
use casa_simulation::{ExecutionControls, SessionResources, SimulatorOptions};
use casa_task_runtime::{
    TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess, resolve_plain_values,
};
use serde_json::{Map, Value};

pub use schema::command_schema;
pub use task_contract::*;

fn simobserve_surface() -> Result<SurfaceContractBundle, String> {
    casa_provider_contracts::builtin_surface_bundle("simobserve")
}

fn workspace() -> Result<PathBuf, String> {
    std::env::current_dir().map_err(|error| format!("the working directory: {error}"))
}

/// The task's parameters from `resolved`, every simobserve parameter.
fn parameters_from(
    resolved: Map<String, Value>,
) -> Result<(SimobserveTaskParameters, SimobserveParameters), String> {
    let missing = |name: &str, what: &str| format!("simobserve parameter {name}: {what}");
    let text = |name: &str| {
        resolved
            .get(name)
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| missing(name, "a string"))
    };
    let texts = |name: &str| -> Result<Vec<String>, String> {
        match resolved.get(name) {
            Some(Value::String(one)) => Ok(vec![one.clone()]),
            Some(Value::Array(many)) => many
                .iter()
                .map(|item| item.as_str().map(str::to_string))
                .collect::<Option<_>>()
                .ok_or_else(|| missing(name, "strings")),
            _ => Err(missing(name, "a list of strings")),
        }
    };
    // The contract resolves a direction string to a list of one, so one
    // direction is taken as a string, and several as the pointings.
    let directions = |name: &str| -> Result<Directions, String> {
        let mut many = texts(name)?;
        Ok(match many.len() {
            0 => Directions::One(String::new()),
            1 => Directions::One(many.remove(0)),
            _ => Directions::List(many),
        })
    };
    let flag = |name: &str| {
        resolved
            .get(name)
            .and_then(Value::as_bool)
            .ok_or_else(|| missing(name, "a boolean"))
    };
    let integer = |name: &str| {
        resolved
            .get(name)
            .and_then(Value::as_i64)
            .ok_or_else(|| missing(name, "an integer"))
    };
    let number = |name: &str| {
        resolved
            .get(name)
            .and_then(Value::as_f64)
            .ok_or_else(|| missing(name, "a number"))
    };
    let parameters = SimobserveParameters {
        project: text("project")?,
        skymodel: text("skymodel")?,
        inbright: text("inbright")?,
        indirection: text("indirection")?,
        incell: text("incell")?,
        incenter: text("incenter")?,
        inwidth: text("inwidth")?,
        complist: text("complist")?,
        compwidth: text("compwidth")?,
        comp_nchan: integer("comp_nchan")?,
        setpointings: flag("setpointings")?,
        ptgfile: text("ptgfile")?,
        integration: text("integration")?,
        direction: directions("direction")?,
        mapsize: texts("mapsize")?,
        maptype: text("maptype")?,
        pointingspacing: text("pointingspacing")?,
        caldirection: text("caldirection")?,
        calflux: text("calflux")?,
        obsmode: text("obsmode")?,
        refdate: text("refdate")?,
        hourangle: text("hourangle")?,
        totaltime: text("totaltime")?,
        antennalist: text("antennalist")?,
        sdantlist: text("sdantlist")?,
        sdant: integer("sdant")?,
        outframe: text("outframe")?,
        thermalnoise: text("thermalnoise")?,
        user_pwv: number("user_pwv")?,
        t_ground: number("t_ground")?,
        t_sky: number("t_sky")?,
        tau0: number("tau0")?,
        seed: integer("seed")?,
        leakage: number("leakage")?,
        correlator: text("correlator")?,
        graphics: text("graphics")?,
        verbose: flag("verbose")?,
        overwrite: flag("overwrite")?,
    };
    Ok((SimobserveTaskParameters(resolved), parameters))
}

/// Resolve `parameters`, catalog-named and sparse, over CASA's defaults:
/// the resolved parameters and the task's.
pub fn resolve_request(
    parameters: &SimobserveTaskParameters,
) -> Result<(SimobserveTaskParameters, SimobserveParameters), String> {
    let resolved = resolve_plain_values(simobserve_surface()?, workspace()?, &parameters.0)
        .map_err(|error| format!("resolve simobserve parameters: {error}"))?;
    parameters_from(resolved)
}

/// CASA's data directories: the measures data's candidates, which hold
/// `alma/simmos` in a CASA data tree.
fn data_roots() -> Result<Vec<PathBuf>, String> {
    casa_measures_data::MeasuresRuntime::discover_candidates()
        .map(|candidates| {
            candidates
                .into_iter()
                .map(|candidate| candidate.path)
                .collect()
        })
        .map_err(|error| format!("CASA's data directories: {error}"))
}

/// Run `task`, resolved as `parameters`, with the host's measures data,
/// CASA data directories and resources.
pub fn run(
    parameters: SimobserveTaskParameters,
    task: SimobserveParameters,
) -> Result<SimobserveRunTaskResult, String> {
    let started = Instant::now();
    let resources = SessionResources::from_controls(&ExecutionControls::default())
        .map_err(|error| error.to_string())?;
    let request = SimobserveRequest {
        parameters: task,
        data_roots: data_roots()?,
        options: SimulatorOptions::from_system(resources).map_err(|error| error.to_string())?,
    };
    let outcome = simobserve(&request).map_err(|error| error.to_string())?;
    Ok(SimobserveRunTaskResult {
        request: parameters,
        run: SimobserveRunReport {
            stem: outcome.stem.clone(),
            directory: outcome.directory.display().to_string(),
            warnings: outcome.warnings.clone(),
            elapsed_ns: u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
        },
        artifacts: outcome
            .products
            .iter()
            .map(|path| artifact(&outcome.stem, path))
            .collect(),
    })
}

/// The product at `path`, named by its suffix after `stem`.
fn artifact(stem: &str, path: &Path) -> SimobserveArtifact {
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let kind = name
        .strip_prefix(stem)
        .and_then(|suffix| suffix.strip_prefix('.'))
        .unwrap_or(&name)
        .to_string();
    let resource_kind = if kind == "ms" || kind.ends_with(".ms") {
        RunProductKind::MeasurementSet
    } else if kind.ends_with("skymodel") || kind.ends_with(".flat") || kind == "newmodel" {
        RunProductKind::CasaImage
    } else if kind.ends_with(".txt") {
        RunProductKind::File
    } else {
        RunProductKind::CasaTable
    };
    SimobserveArtifact {
        label: name,
        kind,
        path: path.display().to_string(),
        exists: path.exists(),
        resource_kind,
    }
}

/// The front end of the `simobserve` binary, which hosts `simobserve`.
pub fn task_front_end()
-> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    TaskFrontEnd::new(
        "simobserve",
        simobserve_task_schema_bundle(),
        &["simobserve"],
        run_task,
    )
    .expect("the simobserve contract carries its surface")
}

/// Run one resolved simobserve request.
fn run_task(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let (parameters, simulation) =
        parameters_from(task.plain_values()).map_err(TaskFailure::validation)?;
    let result = run(parameters, simulation).map_err(TaskFailure::execution)?;
    let text = format!(
        "Wrote {} products of {} in {}\n",
        result.artifacts.len(),
        result.run.stem,
        result.run.directory
    );
    Ok(TaskSuccess::new(&(), &result)?.with_text(text))
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use casa_task_runtime::{ParameterRuntime, TaskCommandLine};

    use super::*;

    /// A command line as the front end resolves it, read into the task's
    /// parameters.
    fn resolve_cli_request(
        args: &[OsString],
    ) -> Result<(SimobserveTaskParameters, SimobserveParameters), String> {
        let bundle = simobserve_surface()?;
        let line = TaskCommandLine::parse(&bundle, args)?;
        let session = ParameterRuntime::default()
            .open_session(line.open_session_request(bundle))
            .map_err(|error| error.to_string())?;
        parameters_from(TaskInvocation::new(&session, Path::new(".")).plain_values())
    }

    #[test]
    fn the_defaults_resolve_to_casas() {
        let (_, parameters) =
            resolve_request(&SimobserveTaskParameters::default()).expect("CASA's defaults");
        assert_eq!(parameters, SimobserveParameters::default());
    }

    #[test]
    fn flags_and_json_resolve_alike() {
        let flags: Vec<OsString> = [
            "--project",
            "p",
            "--antennalist",
            "alma.out10.cfg",
            "--direction",
            "J2000 19h00m00 -23d00m00",
            "--no-overwrite",
        ]
        .into_iter()
        .map(OsString::from)
        .collect();
        let (_, from_flags) = resolve_cli_request(&flags).expect("flags");
        let json = serde_json::json!({
            "project": "p",
            "antennalist": "alma.out10.cfg",
            "direction": "J2000 19h00m00 -23d00m00",
            "overwrite": false,
        });
        let (_, from_json) = resolve_request(&SimobserveTaskParameters(
            json.as_object().expect("an object").clone(),
        ))
        .expect("JSON");
        assert_eq!(from_flags, from_json);
        assert_eq!(
            from_json.direction,
            Directions::One("J2000 19h00m00 -23d00m00".to_string())
        );
        assert!(!from_json.overwrite);
        assert_eq!(from_json.ptgfile, "$project.ptg.txt");
        // A list of one is its direction; several are the pointings.
        let directions = |value: Value| {
            let mut json = json.as_object().expect("an object").clone();
            json.insert("direction".to_string(), value);
            resolve_request(&SimobserveTaskParameters(json))
                .expect("JSON")
                .1
                .direction
        };
        assert_eq!(
            directions(serde_json::json!(["J2000 19h00m00 -23d00m00"])),
            from_json.direction
        );
        assert_eq!(
            directions(serde_json::json!([
                "J2000 19h00m00 -23d00m00",
                "J2000 19h00m01 -23d00m00"
            ])),
            Directions::List(vec![
                "J2000 19h00m00 -23d00m00".to_string(),
                "J2000 19h00m01 -23d00m00".to_string()
            ])
        );
        assert_eq!(
            directions(serde_json::json!([])),
            Directions::One(String::new())
        );
    }

    #[test]
    fn products_are_named_by_their_suffix() {
        let stem = "p.alma.out01";
        let kinds: Vec<_> = [
            "p.alma.out01.ms",
            "p.alma.out01.noisy.ms",
            "p.alma.out01.skymodel.flat",
            "p.alma.out01.ptg.txt",
            "p.alma.out01.fullpol.cal",
        ]
        .into_iter()
        .map(|name| {
            let product = artifact(stem, Path::new(name));
            (product.kind, product.resource_kind)
        })
        .collect();
        assert_eq!(
            kinds,
            [
                ("ms".to_string(), RunProductKind::MeasurementSet),
                ("noisy.ms".to_string(), RunProductKind::MeasurementSet),
                ("skymodel.flat".to_string(), RunProductKind::CasaImage),
                ("ptg.txt".to_string(), RunProductKind::File),
                ("fullpol.cal".to_string(), RunProductKind::CasaTable),
            ]
        );
    }
}
