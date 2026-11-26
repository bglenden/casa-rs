// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's simulation-scoped `concat` task over the observation creation plan.
//!
//! Inputs are ordered by their earliest integration, their weights travel
//! with them, and an existing destination is the first input. Publication
//! uses the creation plan's staged replacement; input stores stay read-only.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use casa_contracts::{CasaValue, TaskProviderContract};
use casa_observation::{MergeTolerance, ObservationError};
use casa_task_runtime::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};
use casa_types::quanta::{Quantity, Unit};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::observation_io::creation::{
    CreationInput, CreationOptions, CreationPlan, Destination, HistoryRow, Loss, LossKind,
    PaddingRows, default_memory_bytes,
};
use crate::observation_io::{Msv2Observation, OpenOptions};

/// CASA's nine task arguments, after its scalar/list coercions.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ConcatRequest {
    /// MeasurementSets to combine, in any chronological order.
    pub vis: Vec<PathBuf>,
    /// New output, or existing MeasurementSet to append to.
    pub concatvis: PathBuf,
    /// Spectral-window matching tolerance; empty means 1 Hz.
    pub freqtol: Value,
    /// Field matching tolerance; empty means 1 mas.
    pub dirtol: Value,
    /// Field-name matching, outside this simulation subset.
    pub respectname: bool,
    /// Global row sorting, outside this simulation subset.
    pub timesort: bool,
    /// Carry pointing; false deliberately empties the output table.
    pub copypointing: bool,
    /// Positive per-input weight factors, or empty for all ones.
    pub visweightscale: Vec<f64>,
    /// Ephemeris-field forcing, outside this simulation subset.
    pub forcesingleephemfield: Value,
}

/// What the task wrote, without retaining scientific payload in its reply.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ConcatReport {
    /// Published output MeasurementSet.
    pub concatvis: PathBuf,
    /// Actual chronological inputs, including an existing destination.
    pub inputs: Vec<PathBuf>,
    /// Rows written.
    pub rows: usize,
    /// Visibility samples read, including padding omitted from the output.
    pub samples: usize,
    /// Number of bounded batch reads.
    pub batches: usize,
    /// Deliberate defaulting and history handling reported by the writer.
    pub notices: Vec<String>,
}

/// The canonical task contract on the shared CLI/Python envelope.
pub fn concat_task_schema_bundle() -> TaskProviderContract {
    crate::task_contract::one_shot_contract::<ConcatRequest, ConcatReport>(
        "casa_ms_concat_task",
        &["concat"],
    )
}

/// The command-line host; all scientific work is in [`concat()`].
pub fn concat_front_end()
-> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    TaskFrontEnd::new_with_deferred_schema(
        "concat",
        concat_task_schema_bundle(),
        &["concat"],
        |task| {
            let request: ConcatRequest =
                serde_json::from_value(serde_json::json!(task.plain_values()))
                    .map_err(|e| TaskFailure::validation(e.to_string()))?;
            let report = concat(&request).map_err(|e| TaskFailure::execution(e.to_string()))?;
            crate::ms_task_success(CasaValue::None, &report)
        },
    )
    .expect("concat contract")
}

/// Concatenate simulation MeasurementSets through the bounded creation plan.
///
/// All arguments and losses are checked before writing. New outputs publish
/// atomically, and appending holds the destination lock through replacement.
///
/// # Errors
///
/// Invalid arguments, unsupported behavior (named with #939), unrepresentable
/// inputs, insufficient resources, or a storage/locking/publication failure.
pub fn concat(request: &ConcatRequest) -> Result<ConcatReport, ObservationError> {
    concat_with_memory(request, default_memory_bytes()?)
}

/// As [`concat()`], within the composing application's memory limit in bytes.
///
/// This lets simulation tasks keep concatenation inside their admitted run
/// policy instead of selecting the standalone task's default limit.
///
/// # Errors
/// The validation, admission and publication errors of [`concat()`].
pub fn concat_with_memory(
    request: &ConcatRequest,
    memory_bytes: usize,
) -> Result<ConcatReport, ObservationError> {
    validate(request)?;
    let inputs = ordered_inputs(request, memory_bytes)?;
    let history = HistoryRow {
        time_s: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| invalid(e.to_string()))?
            .as_secs_f64()
            + 40_587.0 * 86_400.0,
        observation: -1,
        message: "MeasurementSets concatenated".into(),
        priority: "NORMAL".into(),
        origin: "concat".into(),
        application: "casa-rs".into(),
        parameters: vec![serde_json::to_string(request).map_err(|e| invalid(e.to_string()))?],
        command: Vec::new(),
    };
    let options = CreationOptions {
        copy_pointing: request.copypointing,
        data_groups: None,
        // Inspect individual losses below before allowing publication.
        accept: LossKind::ALL.to_vec(),
        batch_bytes: (16 << 20).min(memory_bytes / 8),
        memory_bytes,
        padding: PaddingRows::Omitted,
        tolerance: MergeTolerance {
            frequency_hz: tolerance(&request.freqtol, "freqtol", "Hz", 1.0)?,
            direction_rad: tolerance(
                &request.dirtol,
                "dirtol",
                "rad",
                std::f64::consts::PI / 648_000_000.0,
            )?,
            ..MergeTolerance::default()
        },
        history: vec![history],
        creation_date: String::new(),
    };
    let destination = if request.concatvis.exists() {
        Destination::Replace(request.concatvis.clone())
    } else {
        Destination::New(request.concatvis.clone())
    };
    let plan = CreationPlan::new(&inputs, &options, destination)?;
    let unsupported: Vec<_> = plan
        .losses()
        .iter()
        .filter(|loss| match loss {
            Loss::Defaulted(_) => false,
            Loss::Table(name) if matches!(name.as_str(), "HISTORY" | "SORTED_TABLE") => false,
            _ => true,
        })
        .map(ToString::to_string)
        .collect();
    if !unsupported.is_empty() {
        return Err(invalid(format!(
            "concat cannot preserve {}; outside the simulation subset (#939)",
            unsupported.join("; ")
        )));
    }
    let report = plan.write()?;
    Ok(ConcatReport {
        concatvis: request.concatvis.clone(),
        inputs: inputs.into_iter().map(|i| i.path).collect(),
        rows: report.rows,
        samples: report.samples,
        batches: report.batches,
        notices: report.losses.iter().map(ToString::to_string).collect(),
    })
}

fn validate(request: &ConcatRequest) -> Result<(), ObservationError> {
    if request.vis.is_empty() || request.concatvis.as_os_str().is_empty() {
        return Err(invalid("vis and concatvis must name MeasurementSets"));
    }
    if request.respectname
        || request.timesort
        || request.forcesingleephemfield != Value::String(String::new())
    {
        return Err(invalid(
            "respectname, timesort and forcesingleephemfield outside the simulation subset (#939)",
        ));
    }
    if !request.visweightscale.is_empty() && request.visweightscale.len() != request.vis.len() {
        return Err(invalid("visweightscale must have one factor per input"));
    }
    if request
        .visweightscale
        .iter()
        .any(|s| !s.is_finite() || *s <= 0.0)
    {
        return Err(invalid(
            "visweightscale factors must be finite and positive; zero has inconsistent CASA behavior (#939)",
        ));
    }
    Ok(())
}

fn ordered_inputs(
    request: &ConcatRequest,
    budget: usize,
) -> Result<Vec<CreationInput>, ObservationError> {
    let output = if request.concatvis.exists() {
        Some(std::fs::canonicalize(&request.concatvis).map_err(|e| invalid(e.to_string()))?)
    } else {
        None
    };
    let mut timed = Vec::with_capacity(request.vis.len());
    for (index, path) in request.vis.iter().enumerate() {
        let path =
            std::fs::canonicalize(path).map_err(|e| invalid(format!("{}: {e}", path.display())))?;
        if timed
            .iter()
            .any(|(_, input): &(f64, CreationInput)| input.path == path)
        {
            return Err(invalid("vis repeats a MeasurementSet"));
        }
        let time = earliest(&path, budget)?;
        timed.push((
            time,
            CreationInput {
                path,
                weight_scale: request.visweightscale.get(index).copied().unwrap_or(1.0),
            },
        ));
    }
    timed.sort_by(|a, b| a.0.total_cmp(&b.0));
    if let Some(output) = output {
        if let Some(index) = timed.iter().position(|(_, input)| input.path == output) {
            if index != 0 {
                return Err(invalid(
                    "concatvis included in vis must be the chronologically first input",
                ));
            }
        } else {
            timed.insert(0, (f64::NEG_INFINITY, CreationInput::new(output)));
        }
    }
    Ok(timed.into_iter().map(|(_, input)| input).collect())
}

fn earliest(path: &Path, budget: usize) -> Result<f64, ObservationError> {
    let observation =
        Msv2Observation::open_granted(path, &OpenOptions::default(), &mut |required| {
            if required > budget {
                Err(ObservationError::OverBudget {
                    what: "concat input ordering",
                    required,
                    budget,
                })
            } else {
                Ok(())
            }
        })?;
    observation
        .earliest_time_s()
        .ok_or_else(|| invalid("concat input has no integrations"))
}

fn tolerance(value: &Value, name: &str, unit: &str, default: f64) -> Result<f64, ObservationError> {
    if value.as_str() == Some("") {
        return Ok(default);
    }
    let quantity = if let Some(text) = value.as_str() {
        Quantity::read(text).map_err(|e| invalid(format!("{name}: {e}")))?
    } else if let (Some(value), Some(unit)) = (value["value"].as_f64(), value["unit"].as_str()) {
        Quantity::new(value, unit).map_err(|e| invalid(format!("{name}: {e}")))?
    } else {
        return Err(invalid(format!(
            "{name} must be a frequency/direction quantity"
        )));
    };
    let converted = quantity
        .get_value_in(&Unit::new(unit).map_err(|e| invalid(e.to_string()))?)
        .map_err(|e| invalid(format!("{name}: {e}")))?;
    if !converted.is_finite() || converted < 0.0 {
        return Err(invalid(format!("{name} must be finite and non-negative")));
    }
    if (converted - default).abs() > default * 1.0e-12 {
        return Err(invalid(format!(
            "non-default {name} is outside the simulation subset (#939)"
        )));
    }
    Ok(converted)
}

fn invalid(message: impl Into<String>) -> ObservationError {
    ObservationError::InvalidRequest {
        reason: message.into(),
    }
}
