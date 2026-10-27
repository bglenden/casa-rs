// SPDX-License-Identifier: LGPL-3.0-or-later
//! Run `simobserve` from a JSON object of CASA's parameters, for the parity
//! harness (`tools/simulation/simobserve_parity.py`):
//!
//! ```text
//! simobserve_run <parameters.json> [--data-root DIR]... [--backend cpu|metal|auto]
//! ```
//!
//! The parameters take CASA's names and JSON's types; those not given keep
//! CASA's defaults. The run's products are printed as JSON.

use std::path::PathBuf;
use std::str::FromStr;

use casa_simulation::simobserve::{SimobserveParameters, SimobserveRequest, simobserve};
use casa_simulation::{BackendRequest, ExecutionControls, SessionResources, SimulatorOptions};
use serde_json::Value;

fn text(value: &Value) -> String {
    value.as_str().expect("a string").to_string()
}

fn texts(value: &Value) -> Vec<String> {
    match value {
        Value::String(one) => vec![one.clone()],
        Value::Array(many) => many.iter().map(text).collect(),
        other => panic!("a string or a list, not {other}"),
    }
}

/// `parameters` with the JSON object's values set.
fn apply(parameters: &mut SimobserveParameters, given: &serde_json::Map<String, Value>) {
    for (name, value) in given {
        let number = || value.as_f64().expect("a number");
        let integer = || value.as_i64().expect("an integer");
        let flag = || value.as_bool().expect("a boolean");
        match name.as_str() {
            "project" => parameters.project = text(value),
            "skymodel" => parameters.skymodel = text(value),
            "inbright" => parameters.inbright = text(value),
            "indirection" => parameters.indirection = text(value),
            "incell" => parameters.incell = text(value),
            "incenter" => parameters.incenter = text(value),
            "inwidth" => parameters.inwidth = text(value),
            "complist" => parameters.complist = text(value),
            "compwidth" => parameters.compwidth = text(value),
            "comp_nchan" => parameters.comp_nchan = integer(),
            "setpointings" => parameters.setpointings = flag(),
            "ptgfile" => parameters.ptgfile = text(value),
            "integration" => parameters.integration = text(value),
            "direction" => parameters.direction = texts(value),
            "mapsize" => parameters.mapsize = texts(value),
            "maptype" => parameters.maptype = text(value),
            "pointingspacing" => parameters.pointingspacing = text(value),
            "caldirection" => parameters.caldirection = text(value),
            "calflux" => parameters.calflux = text(value),
            "obsmode" => parameters.obsmode = text(value),
            "refdate" => parameters.refdate = text(value),
            "hourangle" => parameters.hourangle = text(value),
            "totaltime" => parameters.totaltime = text(value),
            "antennalist" => parameters.antennalist = text(value),
            "sdantlist" => parameters.sdantlist = text(value),
            "sdant" => parameters.sdant = integer(),
            "outframe" => parameters.outframe = text(value),
            "thermalnoise" => parameters.thermalnoise = text(value),
            "user_pwv" => parameters.user_pwv = number(),
            "t_ground" => parameters.t_ground = number(),
            "t_sky" => parameters.t_sky = number(),
            "tau0" => parameters.tau0 = number(),
            "seed" => parameters.seed = integer(),
            "leakage" => parameters.leakage = number(),
            "correlator" => parameters.correlator = text(value),
            "graphics" => parameters.graphics = text(value),
            "verbose" => parameters.verbose = flag(),
            "overwrite" => parameters.overwrite = flag(),
            other => panic!("simobserve has no parameter {other}"),
        }
    }
}

fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();
    let mut args = std::env::args().skip(1);
    let file = PathBuf::from(args.next().expect("parameters.json"));
    let mut data_roots = Vec::new();
    let mut controls = ExecutionControls::default();
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--data-root" => data_roots.push(PathBuf::from(args.next().expect("a directory"))),
            "--backend" => {
                controls.backend = BackendRequest::from_str(&args.next().expect("backend"))
                    .expect("auto, cpu or metal");
            }
            other => panic!("unknown argument {other}"),
        }
    }
    let given: Value =
        serde_json::from_str(&std::fs::read_to_string(&file).expect("read the parameters"))
            .expect("JSON");
    let mut parameters = SimobserveParameters::default();
    apply(&mut parameters, given.as_object().expect("an object"));
    let resources = SessionResources::from_controls(&controls).expect("session resources");
    let request = SimobserveRequest {
        parameters,
        data_roots,
        options: SimulatorOptions::from_system(resources).expect("measures data"),
    };
    match simobserve(&request) {
        Ok(outcome) => {
            let products: Vec<String> = outcome
                .products
                .iter()
                .map(|path| path.display().to_string())
                .collect();
            println!(
                "{}",
                serde_json::json!({"stem": outcome.stem, "products": products})
            );
        }
        Err(error) => {
            println!("{}", serde_json::json!({"error": error.to_string()}));
            std::process::exit(1);
        }
    }
}
