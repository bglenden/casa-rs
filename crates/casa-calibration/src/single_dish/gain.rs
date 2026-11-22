// SPDX-License-Identifier: LGPL-3.0-or-later
//! The double-circle single-dish gain solver.

mod footprint;
mod normalization;
mod options;
mod spectral;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use casa_ms::{MeasurementSet, MsSelection, ResolvedMsSelection};
use casa_provider_contracts::CasaValue;
use casa_tables::TableWriteLock;
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};
use casa_types::{ArrayValue, Complex32, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, Axis, IxDyn};

use super::{
    edges::{self, Integration},
    selector,
};
use crate::constants::*;
use crate::execute::SolvePriorApply;
use crate::session::resolve_casa_calibration_selection;
use crate::table_maintenance::{overlaps_measurement_set, write_standard_calibration_subtables};
use crate::writer::{
    CalibrationTableDescriptor, CalibrationTableWriter, new_calibration_table_schema,
};
use crate::{ApplyMode, ApplyPlanRequest, CalibrationOutputMode, plan_apply};
use normalization::GainSample;

const SUBTYPE: &str = "SDGAIN_OTFD";

#[derive(Clone, Copy)]
pub(super) struct Controls {
    pub(super) radius: Option<f64>,
    pub(super) smooth: bool,
}

pub(crate) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let outfile = task.text("outfile")?.unwrap_or_default();
    let overwrite = task.flag("overwrite")?;
    if outfile.is_empty() {
        return Err(TaskFailure::validation("outfile is empty.").at("outfile"));
    }
    let output = Path::new(&outfile);
    if output.exists() && !overwrite {
        return Err(TaskFailure::execution(format!("{outfile} exists.")).at("outfile"));
    }
    let infile = task.input_path("infile")?;
    let selection = MsSelection {
        selectdata: true,
        antenna: selector(task, "antenna")?.map(|antenna| format!("{antenna}&&&")),
        field: selector(task, "field")?,
        spw: selector(task, "spw")?,
        scan: selector(task, "scan")?,
        intent: selector(task, "intent")?,
        ..MsSelection::default()
    };
    let ms = MeasurementSet::open(&infile)
        .map_err(|error| TaskFailure::execution(error.to_string()).at("infile"))?;
    let selected = resolve_casa_calibration_selection(&ms, &selection).map_err(|error| {
        TaskFailure::execution(format!("Error in data selection specification: {error}"))
    })?;
    let tables = options::priors(task)?;
    let priors = if tables.is_empty() {
        None
    } else {
        let plan = plan_apply(
            &ms,
            &ApplyPlanRequest {
                selection,
                apply_mode: ApplyMode::CalFlag,
                parang: false,
                calibration_tables: tables,
            },
        )
        .map_err(|_| options::setapply_error("applytable"))?;
        Some(
            SolvePriorApply::single_dish(&ms, plan)
                .map_err(|_| options::setapply_error("applytable"))?,
        )
    };
    let controls = Controls {
        radius: options::radius(task)?,
        smooth: task.flag("smooth")?,
    };
    if overlaps_measurement_set(&ms, output).map_err(TaskFailure::execution)? {
        return Err(TaskFailure::execution(
            "The MeasurementSet and output calibration table overlap.",
        )
        .at("outfile"));
    }
    let _output_lock = if overwrite && output.join("table.dat").is_file() {
        Some(
            TableWriteLock::acquire_to_reread(output, 1)
                .map_err(|error| TaskFailure::execution(error.to_string()).at("outfile"))?,
        )
    } else {
        None
    };
    let mode = if overwrite {
        CalibrationOutputMode::Replace
    } else {
        CalibrationOutputMode::Create
    };
    let count = generate(&ms, &selected, priors.as_ref(), output, mode, controls)?;
    TaskSuccess::new(
        CasaValue::None,
        &serde_json::json!({"caltable": outfile, "row_count": count}),
    )
}

pub(super) fn generate(
    ms: &MeasurementSet,
    selection: &ResolvedMsSelection,
    priors: Option<&SolvePriorApply>,
    output: &Path,
    mode: CalibrationOutputMode,
    controls: Controls,
) -> Result<Option<usize>, TaskFailure> {
    let integrations = selected_integrations(ms, selection).map_err(solve_failure)?;
    let references =
        footprint::select(ms, &integrations, controls.radius).map_err(solve_failure)?;
    let channels = spectral::channels(ms, selection, &references)
        .map_err(|message| TaskFailure::execution(message).at("spw"))?;
    let samples = solve(
        ms,
        references,
        priors,
        controls.radius,
        controls.smooth,
        &channels,
    )
    .map_err(solve_failure)?;
    samples
        .map(|samples| write(ms, output, mode, samples, &channels))
        .transpose()
        .map_err(solve_failure)
}

fn selected_integrations(
    ms: &MeasurementSet,
    selection: &ResolvedMsSelection,
) -> Result<Vec<Integration>, String> {
    let descriptions = ms.data_description().map_err(error_text)?;
    let windows = (0..descriptions.row_count())
        .map(|row| descriptions.spectral_window_id(row).map(|id| id as usize))
        .collect::<Result<Vec<_>, _>>()
        .map_err(error_text)?;
    let rows = selection.row_indices().collect::<Vec<_>>();
    edges::read_integrations(ms, &rows, &windows)
}

fn solve<'a>(
    ms: &MeasurementSet,
    references: Vec<&'a Integration>,
    priors: Option<&SolvePriorApply>,
    radius: Option<f64>,
    smooth: bool,
    channels: &BTreeMap<usize, usize>,
) -> Result<Option<Vec<GainSample<'a>>>, String> {
    let mut groups = BTreeMap::<_, Vec<_>>::new();
    for row in references {
        groups
            .entry((row.window, row.field, row.antenna))
            .or_default()
            .push(row);
    }
    let spectral = ms.spectral_window().map_err(error_text)?;
    let antennas = ms.antenna().map_err(error_text)?;
    let descriptions = ms.data_description().map_err(error_text)?;
    let pol = ms.polarization().map_err(error_text)?;
    let correlations = (0..descriptions.row_count())
        .map(|row| {
            descriptions
                .polarization_id(row)
                .and_then(|id| pol.corr_type(id as usize))
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(error_text)?;
    let mut results = Vec::new();
    let mut has_solving_data = false;
    for ((window, _, antenna), mut group) in groups {
        group.sort_by(|first, second| first.time.total_cmp(&second.time));
        let channel = channels[&window];
        let frequency = spectral.chan_freq(window).map_err(error_text)?[channel];
        let radius = normalization::radius(
            radius,
            &[frequency],
            antennas
                .dish_diameter(antenna as usize)
                .map_err(error_text)?,
        )?;
        let mut samples = group
            .into_iter()
            .filter_map(|row| read_sample(ms, row, priors, &correlations, channel).transpose())
            .collect::<Result<Vec<_>, _>>()?;
        has_solving_data |= !samples.is_empty();
        if samples.len() < 100 {
            continue;
        }
        normalization::normalize(&mut samples, radius, smooth)?;
        results.extend(samples);
    }
    results.sort_by(|first, second| {
        let left = first.integration;
        let right = second.integration;
        left.description
            .cmp(&right.description)
            .then_with(|| left.time.total_cmp(&right.time))
            .then_with(|| left.antenna.cmp(&right.antenna))
            .then_with(|| left.field.cmp(&right.field))
    });
    Ok(has_solving_data.then_some(results))
}

fn read_sample<'a>(
    ms: &MeasurementSet,
    row: &'a Integration,
    priors: Option<&SolvePriorApply>,
    correlations: &[Vec<i32>],
    channel: usize,
) -> Result<Option<GainSample<'a>>, String> {
    let main = ms.main_table();
    let source = crate::model_columns::observed_column(ms);
    let data = main
        .cell_accessor(row.row, source)
        .and_then(|cell| cell.array())
        .map_err(error_text)?;
    let mut flags = main
        .cell_accessor(row.row, "FLAG")
        .and_then(|cell| cell.array())
        .map_err(error_text)?
        .clone();
    if row.flagged
        && let ArrayValue::Bool(flags) = &mut flags
    {
        flags.fill(true);
    }
    let types = correlations
        .get(row.description as usize)
        .ok_or("Invalid DATA_DESC_ID")?;
    let (data, flags) = if let Some(priors) = priors {
        let applied = priors
            .correct_single_dish(row.row, types, data, flags)
            .map_err(error_text)?
            .ok_or("Prior application omitted a selected gain integration.")?;
        (applied.corrected_data, applied.flags)
    } else {
        (data.clone(), flags)
    };
    let data = match data {
        ArrayValue::Float32(values) => values.mapv(|value| Complex32::new(value, 0.)),
        ArrayValue::Complex32(values) => values,
        _ => return Err("Invalid single-dish visibility shape.".into()),
    };
    let ArrayValue::Bool(flags) = flags else {
        return Err("FLAG is not Boolean.".into());
    };
    if data.ndim() != 2 || data.shape() != flags.shape() || channel >= data.shape()[1] {
        return Err("Visibility data and flags do not cover the selected gain channel.".into());
    }
    let (data, flags) = if data.shape()[1] == 1 {
        (data, flags)
    } else {
        (
            data.select(Axis(1), &[channel]),
            flags.select(Axis(1), &[channel]),
        )
    };
    // CASA's solve buffers share the channel admission flag across the
    // selected correlations. An unusable scalar sample has no table row.
    if flags.iter().any(|&flagged| flagged) {
        return Ok(None);
    }
    Ok(Some(GainSample {
        integration: row,
        data,
        flags,
    }))
}

fn write(
    ms: &MeasurementSet,
    output: &Path,
    mode: CalibrationOutputMode,
    samples: Vec<GainSample<'_>>,
    channels: &BTreeMap<usize, usize>,
) -> Result<usize, String> {
    let count = samples.len();
    let mut writer = CalibrationTableWriter::with_mode(
        ms,
        CalibrationTableDescriptor {
            output,
            schema: new_calibration_table_schema(true),
            subtype: SUBTYPE,
            parameter_type: Some("Complex"),
            measurement_set_name: ms
                .path()
                .map_or_else(|| "none".into(), |path| path.display().to_string()),
            include_polarization_basis: true,
            time_extra_precision_column: None,
        },
        mode,
    )
    .map_err(error_text)?;
    let mut windows = BTreeSet::new();
    for sample in samples {
        windows.insert(sample.integration.window);
        writer.append(gain_record(sample)?).map_err(error_text)?;
    }
    write_standard_calibration_subtables(ms, writer.output_path(), None)?;
    spectral::write_axis(
        &writer.output_path().join("SPECTRAL_WINDOW"),
        channels,
        &windows,
    )?;
    writer.save_main().map_err(error_text)?;
    Ok(count)
}

fn gain_record(sample: GainSample<'_>) -> Result<RecordValue, String> {
    let row = sample.integration;
    let shape = sample.data.shape();
    if shape.len() != 2 || !(1..=2).contains(&shape[0]) {
        return Err("Double-circle gain needs one or two receptors.".into());
    }
    let (receptors, channels) = (shape[0], shape[1]);
    let dimensions = IxDyn(&[2, channels]);
    let mut values = ArrayD::zeros(dimensions.clone());
    let mut flags = ArrayD::from_elem(dimensions.clone(), true);
    let mut error = ArrayD::zeros(dimensions.clone());
    let mut snr = ArrayD::zeros(dimensions);
    for receptor in 0..receptors {
        values
            .index_axis_mut(Axis(0), receptor)
            .assign(&sample.data.index_axis(Axis(0), receptor));
        flags
            .index_axis_mut(Axis(0), receptor)
            .assign(&sample.flags.index_axis(Axis(0), receptor));
        error.index_axis_mut(Axis(0), receptor).fill(0.1);
        snr.index_axis_mut(Axis(0), receptor).fill(1.);
    }
    let int = |name, value| RecordField::new(name, Value::Scalar(ScalarValue::Int32(value)));
    let float = |name, value| RecordField::new(name, Value::Scalar(ScalarValue::Float64(value)));
    let array = |name, value| RecordField::new(name, Value::Array(value));
    Ok(RecordValue::new(vec![
        float(COL_TIME, row.time),
        float(COL_INTERVAL, 0.),
        int(COL_ANTENNA1, row.antenna),
        int(COL_ANTENNA2, -1),
        int(COL_FIELD_ID, row.field),
        int(COL_SCAN_NUMBER, row.scan),
        int(COL_OBSERVATION_ID, row.observation),
        int(COL_SPECTRAL_WINDOW_ID, row.window as i32),
        array(COL_CPARAM, ArrayValue::Complex32(values)),
        array(COL_FLAG, ArrayValue::Bool(flags)),
        array(COL_PARAMERR, ArrayValue::Float32(error)),
        array(COL_SNR, ArrayValue::Float32(snr)),
    ]))
}

fn solve_failure(error: String) -> TaskFailure {
    tracing::error!(%error, "Single-dish gain solve failed");
    TaskFailure::execution("Error in Calibrater::solve.")
}

fn error_text(error: impl std::fmt::Display) -> String {
    error.to_string()
}
