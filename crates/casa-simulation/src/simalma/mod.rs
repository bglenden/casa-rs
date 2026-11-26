// SPDX-License-Identifier: LGPL-3.0-or-later
//! ALMA/ACA observation, imaging and primary-beam-weighted combination.
mod combine;
mod configuration;
mod imaging;
mod plan;
mod pointings;

use crate::plot_sink::{SimalmaCombinationInput, SimalmaFinishInput, SimulationPlotSink};
use crate::{
    SimulatorOptions,
    simobserve::{
        Cancel, SimobserveError, SimobserveOutcome, check_cancel, simobserve_with_controls,
    },
};
use casa_imaging_runtime::{Demand, admit, charge_process_caches};
use serde_json::{Map, Value, json};
use std::path::PathBuf;

/// CASA-named task parameters and the simulator's explicit environment.
pub struct SimalmaRequest {
    /// Sparse CASA simalma parameters; defaults come from the shared catalog.
    pub parameters: Map<String, Value>,
    /// Antenna configuration repositories, as for simobserve.
    pub data_roots: Vec<PathBuf>,
    /// Measures, clock, memory, workers and backend.
    pub options: SimulatorOptions,
}

fn failure(error: impl std::fmt::Display) -> SimobserveError {
    SimobserveError::raised(format!("simalma: {error}"))
}

struct Parameters(Map<String, Value>);
impl Parameters {
    fn text(&self, name: &str) -> &str {
        self.0[name].as_str().expect("resolved string")
    }
    fn flag(&self, name: &str) -> bool {
        self.0[name].as_bool().expect("resolved flag")
    }
    fn integer(&self, name: &str) -> i64 {
        self.0[name].as_i64().expect("resolved integer")
    }
    fn strings(&self, name: &str) -> Vec<String> {
        match &self.0[name] {
            Value::String(one) => vec![one.clone()],
            Value::Array(many) => many
                .iter()
                .map(|v| v.as_str().expect("resolved string").to_owned())
                .collect(),
            _ => unreachable!("resolved strings"),
        }
    }
}

/// Return the canonical CASA 6.7.6 `simalma` parameter defaults as plain JSON.
#[must_use]
pub fn default_parameters() -> Map<String, Value> {
    let value = json!({
        "project": "sim",
        "dryrun": true,
        "skymodel": "",
        "inbright": "",
        "indirection": "",
        "incell": "",
        "incenter": "",
        "inwidth": "",
        "complist": "",
        "compwidth": "\"8GHz\"",
        "setpointings": true,
        "ptgfile": "$project.ptg.txt",
        "integration": "10s",
        "direction": [],
        "mapsize": ["", ""],
        "antennalist": ["alma.cycle1.1.cfg", "aca.cycle1.cfg"],
        "correlator": ["BLC", "BLC", "ACASpec"],
        "hourangle": "transit",
        "totaltime": ["20min", "1h"],
        "tpnant": 0,
        "tptime": "0s",
        "pwv": 0.5,
        "image": true,
        "imsize": [128, 128],
        "imdirection": "",
        "cell": "",
        "niter": 0,
        "threshold": "0.0001Jy",
        "graphics": "both",
        "verbose": false,
        "overwrite": false
    });
    match value {
        Value::Object(map) => map,
        _ => unreachable!("default map is an object"),
    }
}

fn resolve_defaults(overrides: &Map<String, Value>) -> Map<String, Value> {
    let mut merged = default_parameters();
    for (key, value) in overrides {
        merged.insert(key.clone(), value.clone());
    }
    merged
}

fn graphics_mode(p: &Parameters) -> &str {
    if p.text("graphics") == "screen" {
        "both"
    } else {
        p.text("graphics")
    }
}

/// Simulate each requested array and TP antenna, image, and combine them.
///
/// # Errors
/// Invalid input/configuration/time, an existing protected project or input,
/// resource refusal, or a native observation/imaging failure.
pub fn simalma(request: &SimalmaRequest) -> Result<SimobserveOutcome, SimobserveError> {
    simalma_with_controls(request, None, &Cancel::new())
}

/// Simulate and emit complete figure inputs through `sink`.
///
/// # Errors
/// Simulation errors or any error reported by `sink`.
pub fn simalma_with_plot_sink(
    request: &SimalmaRequest,
    sink: &mut dyn SimulationPlotSink,
) -> Result<SimobserveOutcome, SimobserveError> {
    simalma_with_controls(request, Some(sink), &Cancel::new())
}

/// Run the ALMA simulation using one cancellation token for its observation
/// and imaging steps. A sink receives the optional figure inputs.
///
/// # Errors
/// Simulation, cancellation, and figure sink errors.
pub fn simalma_with_controls<'a, 'b>(
    request: &SimalmaRequest,
    plot_sink: Option<&'a mut (dyn SimulationPlotSink + 'b)>,
    cancel: &Cancel,
) -> Result<SimobserveOutcome, SimobserveError> {
    check_cancel(cancel)?;
    let resources = request.options.resources;
    charge_process_caches(resources.host(), resources.policy()).map_err(failure)?;
    let _planning = admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: "ALMA simulation planning",
            memory: (2 << 20)
                + request
                    .parameters
                    .iter()
                    .map(|(name, value)| {
                        128 + name.len() as u64 + crate::simanalyze::parameter_bytes(value)
                    })
                    .sum::<u64>()
                    .saturating_mul(4),
        },
    )
    .map_err(failure)?;
    let values = resolve_defaults(&request.parameters);
    let p = Parameters(values);
    let mut plan = plan::prepare(&p, request)?;
    let mut outcome = SimobserveOutcome {
        directory: plan.directory.clone(),
        stem: plan.project.clone(),
        products: Vec::new(),
        warnings: Vec::new(),
    };
    if p.flag("dryrun") {
        outcome.warnings.extend(plan.arrays.iter().map(|array| {
            format!(
                "dryrun: simulate {} for {:.6} minutes",
                array.configuration, array.minutes
            )
        }));
        if plan.tp_antennas > 0 {
            outcome.warnings.push(format!(
                "dryrun: simulate {} TP antennas for {} minutes each",
                plan.tp_antennas,
                plan.tp_minutes.trunc()
            ));
        }
        return Ok(outcome);
    }
    let graphics_requested = matches!(p.text("graphics"), "file" | "screen" | "both");
    if p.text("graphics") != "none" && plot_sink.is_none() {
        outcome
            .warnings
            .push("simalma PNG rendering needs the Python host".into());
    }
    let mut sink = if graphics_requested { plot_sink } else { None };
    if sink.is_some() {
        plan.observation.graphics = graphics_mode(&p).to_owned();
    }
    plan::publish_directory(&p, &plan)?;
    for array in &plan.arrays {
        let _sky = admit(
            resources.host(),
            resources.policy(),
            &Demand {
                phase: "ALMA observation sky preparation",
                memory: plan.model.preparation_bytes,
            },
        )
        .map_err(failure)?;
        let mut observation = plan.observation.clone();
        observation.antennalist = array.configuration.clone();
        observation.totaltime = format!("{:.6}min", array.minutes);
        observation.correlator = array.correlator.clone();
        let result = simobserve_with_controls(
            &crate::simobserve::SimobserveRequest {
                parameters: observation,
                data_roots: request.data_roots.clone(),
                options: request.options.clone(),
            },
            sink.as_deref_mut(),
            cancel,
        )?;
        outcome.products.extend(result.products);
        outcome.warnings.extend(result.warnings);
    }
    if plan.tp_antennas > 0 {
        let (mut observation, _pointings) = pointings::observation(&p, &plan, resources)?;
        for antenna in 0..plan.tp_antennas {
            let _sky = admit(
                resources.host(),
                resources.policy(),
                &Demand {
                    phase: "ALMA TP sky preparation",
                    memory: plan.model.preparation_bytes,
                },
            )
            .map_err(failure)?;
            observation.sdant = antenna as i64;
            // Independent streams per dish. Random sample identity is not a
            // CASA contract; the native simulator owns its seeded stream.
            observation.seed = i64::from(crate::DEFAULT_SEED) + antenna as i64;
            let result = simobserve_with_controls(
                &crate::simobserve::SimobserveRequest {
                    parameters: observation.clone(),
                    data_roots: request.data_roots.clone(),
                    options: request.options.clone(),
                },
                sink.as_deref_mut(),
                cancel,
            )?;
            for mut path in result.products {
                if plan.tp_antennas > 1 && path.to_string_lossy().ends_with(".ms") {
                    let mut target = path.as_os_str().to_os_string();
                    target.push(format!(".Ant{antenna}"));
                    let target = PathBuf::from(target);
                    std::fs::rename(&path, &target).map_err(failure)?;
                    path = target;
                }
                if !outcome.products.contains(&path) {
                    outcome.products.push(path);
                }
            }
            let noisy = if plan.noisy { ".noisy" } else { "" };
            let suffix = if plan.tp_antennas > 1 {
                format!(".Ant{antenna}")
            } else {
                String::new()
            };
            plan.tp_vis.push(
                plan.directory
                    .join(format!("{}{noisy}.sd.ms{suffix}", plan.tp_stem)),
            );
        }
    }
    if p.flag("image") {
        check_cancel(cancel)?;
        imaging::execute(
            &p,
            &plan,
            resources,
            sink.as_deref_mut(),
            &mut outcome,
            cancel,
        )?;
    }
    if let Some(sink) = sink {
        let combination = if p.flag("image") && !plan.tp_vis.is_empty() {
            let best = plan
                .arrays
                .iter()
                .min_by(|a, b| a.resolution.total_cmp(&b.resolution))
                .unwrap();
            let suffix = if plan.noisy { ".noisy" } else { "" };
            let high_stem = if plan.arrays.len() > 1 {
                format!("{}.concat", plan.project)
            } else {
                format!("{}{suffix}", best.stem)
            };
            Some(SimalmaCombinationInput {
                directory: &plan.directory,
                project: &plan.project,
                best_stem: best.stem.clone(),
                high_stem,
                model_preparation_bytes: plan.model.preparation_bytes,
                resources,
            })
        } else {
            None
        };
        sink.finish_simalma(SimalmaFinishInput {
            graphics_mode: p.text("graphics"),
            combination,
        })?;
    }
    outcome.products.sort();
    outcome.products.dedup();
    std::fs::write(
        plan.directory
            .join(format!("{}.simalma.report.txt", plan.project)),
        format!(
            "simalma arrays={} tp_antennas={} products={}\n",
            plan.arrays.len(),
            plan.tp_antennas,
            outcome.products.len()
        ),
    )
    .map_err(failure)?;
    Ok(outcome)
}
