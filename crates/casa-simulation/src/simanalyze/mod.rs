// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's simulation imaging and analysis, composed from native applications.
//! Project discovery, weighted synthesis concatenation, total-power gridding,
//! seed models, feathering and image analysis share the native applications.
mod feather;
pub(crate) mod imaging;
mod inputs;
mod memory;
mod plan;
mod preflight;
mod total_power;

use crate::plot_sink::{SimanalyzePlotInput, SimulationPlotSink};
use crate::simobserve::SimobserveError;
use crate::simutil::analysis::{self, Fidelity};
use crate::{SessionResources, simutil};
use casa_imaging_runtime::{Demand, admit, charge_process_caches};
use serde_json::{Map, Value, json};
use std::path::{Path, PathBuf};

/// One task invocation, with CASA-named parameters and explicit resources.
pub struct SimanalyzeRequest {
    /// Sparse or resolved CASA parameters; the catalog supplies defaults.
    pub parameters: Map<String, Value>,
    /// Admission, worker and backend policy shared with simulation.
    pub resources: SessionResources,
}

/// Published numerical products and native-prepared figures.
pub struct SimanalyzeOutcome {
    /// The project directory.
    pub directory: PathBuf,
    /// Image filename stem, including its array configuration.
    pub stem: String,
    /// Produced CASA images and other files.
    pub products: Vec<PathBuf>,
    /// Informational diagnostics and explicitly unavailable displays.
    pub warnings: Vec<String>,
    /// Analysis statistics, when requested.
    pub fidelity: Option<Fidelity>,
    /// The resolved tclean call, useful for dry runs.
    pub imaging_parameters: Map<String, Value>,
}

/// Return the canonical CASA 6.7.6 `simanalyze` parameter defaults as plain JSON.
#[must_use]
pub fn default_parameters() -> Map<String, Value> {
    let value = json!({
        "project": "sim",
        "image": true,
        "imagename": "default",
        "skymodel": "",
        "vis": "default",
        "modelimage": "",
        "imsize": [0, 0],
        "imdirection": "",
        "cell": [""],
        "interactive": false,
        "niter": 0,
        "threshold": "0.0001Jy",
        "weighting": "natural",
        "mask": [],
        "outertaper": [],
        "pbcor": true,
        "stokes": "I",
        "featherimage": "",
        "analyze": false,
        "showuv": true,
        "showpsf": true,
        "showmodel": true,
        "showconvolved": false,
        "showclean": true,
        "showresidual": false,
        "showdifference": true,
        "showfidelity": true,
        "graphics": "both",
        "verbose": false,
        "overwrite": true,
        "dryrun": false,
        "logfile": ""
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
    if let Some(imsize) = merged.get_mut("imsize")
        && !imsize.is_array()
    {
        *imsize = Value::Array(vec![imsize.clone()]);
    }
    merged
}

pub(super) fn failure(error: impl std::fmt::Display) -> SimobserveError {
    SimobserveError::raised(format!("simanalyze: {error}"))
}

pub(super) struct Parameters<'a>(pub &'a Map<String, Value>);
impl Parameters<'_> {
    fn text(&self, key: &str) -> &str {
        self.0[key].as_str().expect("resolved string")
    }
    fn flag(&self, key: &str) -> bool {
        self.0[key].as_bool().expect("resolved boolean")
    }
    fn strings(&self, key: &str) -> Vec<String> {
        match &self.0[key] {
            Value::String(one) => vec![one.clone()],
            Value::Array(many) => many
                .iter()
                .map(|v| v.as_str().expect("resolved string").to_owned())
                .collect(),
            _ => unreachable!("resolved string list"),
        }
    }
}

/// Image and analyze a simulation project using CASA's task semantics.
///
/// # Errors
/// Invalid parameters, missing inputs, unsupported input metadata or selections,
/// resource admission or native imaging/analysis failures.
pub fn simanalyze(request: &SimanalyzeRequest) -> Result<SimanalyzeOutcome, SimobserveError> {
    simanalyze_with_controls(request, None, &casa_imaging_application::Cancel::new())
}

/// Image, analyze, and emit figure inputs through `sink`.
///
/// # Errors
/// Numerical errors, invalid parameters, or any error reported by `sink`.
pub fn simanalyze_with_plot_sink(
    request: &SimanalyzeRequest,
    sink: &mut dyn SimulationPlotSink,
) -> Result<SimanalyzeOutcome, SimobserveError> {
    simanalyze_with_controls(
        request,
        Some(sink),
        &casa_imaging_application::Cancel::new(),
    )
}

/// Analyze a simulation with caller-owned cancellation and an optional
/// plot sink. The imaging passes observe the same token.
///
/// # Errors
/// Analysis errors, cancellation, or figure sink errors.
pub fn simanalyze_with_controls<'a, 'b>(
    request: &SimanalyzeRequest,
    sink: Option<&'a mut (dyn SimulationPlotSink + 'b)>,
    cancel: &casa_imaging_application::Cancel,
) -> Result<SimanalyzeOutcome, SimobserveError> {
    run(request, sink.map(|sink| (sink, None)), cancel)
}

pub(crate) fn simanalyze_with_plot_sink_and_override(
    request: &SimanalyzeRequest,
    sink: &mut dyn SimulationPlotSink,
    output_file_override: Option<PathBuf>,
    cancel: &casa_imaging_application::Cancel,
) -> Result<SimanalyzeOutcome, SimobserveError> {
    run(request, Some((sink, output_file_override)), cancel)
}

fn run<'a, 'b>(
    request: &SimanalyzeRequest,
    plot_sink: Option<(&'a mut (dyn SimulationPlotSink + 'b), Option<PathBuf>)>,
    cancel: &casa_imaging_application::Cancel,
) -> Result<SimanalyzeOutcome, SimobserveError> {
    crate::simobserve::check_cancel(cancel)?;
    let resources = request.resources;
    charge_process_caches(resources.host(), resources.policy()).map_err(failure)?;
    // Fixed provider/planning metadata plus four live representations of
    // the caller's values during normalization and tclean request building.
    let _planning = admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: "simulation analysis planning",
            memory: (1 << 20)
                + request
                    .parameters
                    .iter()
                    .map(|(name, value)| 128 + name.len() as u64 + parameter_bytes(value))
                    .sum::<u64>()
                    .saturating_mul(4),
        },
    )
    .map_err(failure)?;
    let values = resolve_defaults(&request.parameters);
    let parameters = Parameters(&values);
    let mut report = if parameters.text("logfile").is_empty() {
        None
    } else {
        use std::io::Write;
        let mut report = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(parameters.text("logfile"))
            .map_err(failure)?;
        writeln!(report, "simanalyze project={}", parameters.text("project")).map_err(failure)?;
        Some(report)
    };
    let mut plan = plan::prepare(&parameters, resources)?;
    let mut outcome = SimanalyzeOutcome {
        directory: plan.directory.clone(),
        stem: plan.stem.clone(),
        products: Vec::new(),
        warnings: Vec::new(),
        fidelity: None,
        imaging_parameters: plan.imaging.clone(),
    };
    if !parameters.flag("image") && !parameters.flag("analyze") {
        return Ok(outcome);
    }
    if parameters.flag("dryrun") {
        return Ok(outcome);
    }
    if parameters.flag("image") {
        outcome.products = imaging::execute(&parameters, &plan, resources, cancel)?;
        crate::simobserve::check_cancel(cancel)?;
        outcome.products.extend(plan.products()?);
        let originals = [plan.file(".image.flat"), plan.file(".residual.flat")];
        feather::combine(&parameters, &mut plan, resources)?;
        outcome
            .products
            .extend(originals.into_iter().filter(|path| path.exists()));
        outcome.stem = plan.stem.clone();
    }
    if !plan.file(".image").exists() {
        return Err(failure("you must image before analyzing."));
    }
    let graphics = matches!(parameters.text("graphics"), "file" | "screen" | "both");
    let _memory = admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: "simulation image analysis",
            memory: plan.analysis_bytes(parameters.flag("analyze") || graphics)?,
        },
    )
    .map_err(failure)?;
    let flat = simutil::skymodel::flat_image(&plan.file(".image"), None).map_err(failure)?;
    if plan.file(".residual").exists() {
        simutil::skymodel::flat_image(&plan.file(".residual"), None).map_err(failure)?;
    }
    let model_flat = simutil::skymodel::flat_image(&plan.model, None).map_err(failure)?;
    let convolved = if parameters.flag("analyze") || graphics {
        Some(analysis::convimage(&model_flat, &flat).map_err(failure)?)
    } else {
        None
    };
    if parameters.flag("analyze") {
        outcome.fidelity = Some(
            analysis::fidelity(
                &convolved.as_ref().expect("analysis model").convolved,
                &flat,
                &plan.root,
            )
            .map_err(failure)?,
        );
    }
    if graphics {
        if let Some((sink, output_file_override)) = plot_sink {
            sink.record_simanalyze(SimanalyzePlotInput {
                graphics_mode: parameters.text("graphics"),
                directory: &plan.directory,
                stem: &plan.stem,
                root: &plan.root,
                model: &plan.model,
                ms: &plan.ms,
                model_flat: &model_flat,
                has_synthesis: !plan.inputs.synthesis.is_empty(),
                image: parameters.flag("image"),
                analyze: parameters.flag("analyze"),
                showuv: parameters.flag("showuv"),
                showpsf: parameters.flag("showpsf"),
                showmodel: parameters.flag("showmodel"),
                showconvolved: parameters.flag("showconvolved"),
                showclean: parameters.flag("showclean"),
                showresidual: parameters.flag("showresidual"),
                showdifference: parameters.flag("showdifference"),
                showfidelity: parameters.flag("showfidelity"),
                output_file_override,
                resources,
            })?;
        } else {
            outcome
                .warnings
                .push("simulation PNG rendering needs the Python host".into());
        }
    }
    replace(&plan.file(".residual.flat"), true)?;
    outcome.products.extend(plan.products()?);
    outcome.products.sort();
    outcome.products.dedup();
    if let Some(report) = &mut report {
        use std::io::Write;
        for path in &outcome.products {
            writeln!(report, "{}", path.display()).map_err(failure)?;
        }
        if let Some(statistics) = &outcome.fidelity {
            writeln!(report, "fidelity {statistics:?}").map_err(failure)?;
        }
    }
    Ok(outcome)
}

fn replace(path: &Path, overwrite: bool) -> Result<(), SimobserveError> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(failure(error)),
    };
    if !overwrite {
        return Err(failure(format!("{} already exists", path.display())));
    }
    if metadata.is_dir() {
        std::fs::remove_dir_all(path).map_err(failure)
    } else {
        std::fs::remove_file(path).map_err(failure)
    }
}

// Value payload plus container slots, without serializing another copy.
pub(crate) fn parameter_bytes(value: &Value) -> u64 {
    match value {
        Value::String(value) => value.len() as u64,
        Value::Array(values) => values.iter().map(|value| 32 + parameter_bytes(value)).sum(),
        Value::Object(values) => values
            .iter()
            .map(|(name, value)| 128 + name.len() as u64 + parameter_bytes(value))
            .sum(),
        _ => 32,
    }
}
