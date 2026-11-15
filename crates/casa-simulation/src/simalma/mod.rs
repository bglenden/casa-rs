// SPDX-License-Identifier: LGPL-3.0-or-later
//! ALMA/ACA observation, imaging and primary-beam-weighted combination.
mod combine;
mod configuration;
mod imaging;
mod plan;
mod pointings;

use crate::{
    SimulatorOptions,
    simobserve::{SimobserveError, SimobserveOutcome, simobserve},
};
use casa_imaging_runtime::{Demand, admit, charge_process_caches};
use casa_provider_contracts::builtin_surface_bundle;
use casa_task_runtime::resolve_plain_values;
use serde_json::{Map, Value};
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

/// Simulate each requested array and TP antenna, image, and combine them.
///
/// # Errors
/// Invalid input/configuration/time, an existing protected project or input,
/// resource refusal, or a native observation/imaging failure.
pub fn simalma(request: &SimalmaRequest) -> Result<SimobserveOutcome, SimobserveError> {
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
    let values = resolve_plain_values(
        builtin_surface_bundle("simalma").map_err(failure)?,
        std::env::current_dir().map_err(failure)?,
        &request.parameters,
    )
    .map_err(failure)?;
    let p = Parameters(values);
    let mut plan = plan::prepare(&p, request)?;
    let mut outcome = SimobserveOutcome {
        directory: plan.directory.clone(),
        stem: plan.project.clone(),
        products: Vec::new(),
        warnings: Vec::new(),
        plots: Vec::new(),
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
    if p.text("graphics") != "none" {
        outcome.warnings.push("simalma graphics remain in simulation Phase 6i (#841); numerical products are complete independently of graphics".into());
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
        let result = simobserve(&crate::simobserve::SimobserveRequest {
            parameters: observation,
            data_roots: request.data_roots.clone(),
            options: request.options.clone(),
        })?;
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
            let result = simobserve(&crate::simobserve::SimobserveRequest {
                parameters: observation.clone(),
                data_roots: request.data_roots.clone(),
                options: request.options.clone(),
            })?;
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
        outcome
            .products
            .extend(imaging::execute(&p, &plan, resources)?);
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
