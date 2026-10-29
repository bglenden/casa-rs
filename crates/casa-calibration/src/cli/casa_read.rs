// SPDX-License-Identifier: LGPL-3.0-or-later
//! Reads of the CASA-named calibration surfaces, `applycal`, `gaincal`,
//! `bandpass`, `fluxscale` and `gencal`, into their requests (#931).
//!
//! These surfaces take CASA 6.7.6's parameters and values, read as CASA's
//! task scripts read them:
//!
//! - `field`, `spw` and `intent` always select; the other selectors only
//!   while `selectdata` is set, as CASA resets them otherwise.
//! - The chain is `callib` when `docallib` is set, else `gaintable`. Its
//!   per-table lists are positional: `gaintable[i]` takes `gainfield[i]`,
//!   `interp[i]` and `spwmap[i]`, and a list shorter than `gaintable` leaves
//!   the later tables CASA's defaults (every field, `'linear'`, no window
//!   map). An empty `gaintable` entry is skipped. `calwt` with one value
//!   holds for every table; a solve applies each table with `calwt=True`.
//!
//! A value CASA takes that casa-rs does not implement yet is refused naming
//! #979; the session already refuses the parameters casa-rs does not
//! implement at all unless they hold CASA's default.

use std::path::{Path, PathBuf};

use casa_ms::{MeasurementSet, MsSelection};
use casa_task_runtime::{TaskFailure, TaskInvocation};

use super::read::{count, parse_gainfield_value, parse_interp_value, parse_refant_selector};
use crate::{
    ApplyCalibrationTableSpec, ApplyMode, BandpassSolveCombine, BandpassType,
    CalibrationTaskRequest, ExecuteApplyTaskRequest, FluxScaleRequest, GainSolveCombine,
    GainSolveInterval, GainSolveMode, GainSolveModelSource, GainType, GencalTaskRequest,
    GencalType, RefAntSelector, SolveBandpassTaskRequest, SolveGainTaskRequest,
    load_apply_specs_from_callib,
};

/// The request a CASA-named surface makes.
pub(super) fn task_request(
    task: &TaskInvocation<'_>,
) -> Result<CalibrationTaskRequest, TaskFailure> {
    Ok(match task.surface() {
        "applycal" => CalibrationTaskRequest::ExecuteApply(applycal(task)?),
        "gaincal" => CalibrationTaskRequest::SolveGain(gaincal(task)?),
        "bandpass" => CalibrationTaskRequest::SolveBandpass(bandpass(task)?),
        "fluxscale" => CalibrationTaskRequest::FluxScale(fluxscale(task)?),
        "gencal" => CalibrationTaskRequest::Gencal(gencal(task)?),
        other => {
            return Err(TaskFailure::validation(format!(
                "calibrate runs no surface {other:?}"
            )));
        }
    })
}

fn applycal(task: &TaskInvocation<'_>) -> Result<ExecuteApplyTaskRequest, TaskFailure> {
    let calibration_tables = chain(task, Chain::Apply)?;
    if calibration_tables.is_empty() {
        // CASA's Calibrater::correct fails with nothing to apply; the
        // parallactic-angle correction alone is not implemented yet.
        if task.flag("parang")? {
            return Err(not_yet("parang", "True with no gaintable"));
        }
        return Err(TaskFailure::validation(
            "applycal has nothing to apply; set gaintable, or docallib and callib",
        )
        .at("gaintable"));
    }
    Ok(ExecuteApplyTaskRequest {
        measurement_set: task.path("vis")?,
        selection: selection(task)?,
        calibration_tables,
        apply_mode: applymode(task)?,
        parang: task.flag("parang")?,
        flagbackup: task.flag("flagbackup")?,
    })
}

fn gaincal(task: &TaskInvocation<'_>) -> Result<SolveGainTaskRequest, TaskFailure> {
    let measurement_set = task.path("vis")?;
    let min_snr = task.number("minsnr")? as f32;
    if !min_snr.is_finite() || min_snr < 0.0 {
        return Err(
            TaskFailure::validation("minsnr must be a finite non-negative number").at("minsnr"),
        );
    }
    let (model_source, smodel) = model(task, &measurement_set)?;
    Ok(SolveGainTaskRequest {
        selection: selection(task)?,
        output_table: PathBuf::from(task.required_text("caltable")?),
        gain_type: gaintype(task)?,
        solve_mode: calmode(task)?,
        solve_interval: solint(task)?,
        combine: combine(task)?,
        refant: refant(task)?,
        prior_calibration_tables: chain(task, Chain::Solve)?,
        parang: task.flag("parang")?,
        model_source,
        normalize_average_amplitude: task.flag("solnorm")?,
        min_snr,
        min_baselines_per_antenna: count(task, "minblperant")?,
        smodel,
        measurement_set,
    })
}

fn bandpass(task: &TaskInvocation<'_>) -> Result<SolveBandpassTaskRequest, TaskFailure> {
    let measurement_set = task.path("vis")?;
    let (model_source, smodel) = model(task, &measurement_set)?;
    if matches!(model_source, GainSolveModelSource::ModelColumn) {
        return Err(not_yet("smodel", "[] with a MODEL_DATA column"));
    }
    let band_type = match task.required_text("bandtype")?.as_str() {
        "B" => BandpassType::B,
        "BPOLY" => BandpassType::BPoly,
        other => return Err(unknown("bandtype", other, "'B' or 'BPOLY'")),
    };
    let combine = combine(task)?;
    Ok(SolveBandpassTaskRequest {
        selection: selection(task)?,
        output_table: PathBuf::from(task.required_text("caltable")?),
        refant: refant(task)?,
        prior_calibration_tables: chain(task, Chain::Solve)?,
        parang: task.flag("parang")?,
        combine: BandpassSolveCombine {
            scans: combine.scans,
            fields: combine.fields,
        },
        band_type,
        normalize_average_amplitude: task.flag("solnorm")?,
        amplitude_degree: count(task, "degamp")?,
        phase_degree: count(task, "degphase")?,
        smodel,
        measurement_set,
    })
}

fn fluxscale(task: &TaskInvocation<'_>) -> Result<FluxScaleRequest, TaskFailure> {
    // CASA opens `vis` for its field and window names; casa-rs reads them
    // from the gain table, but a MeasurementSet that is not there is still
    // an error, as it is to CASA.
    let vis = task.path("vis")?;
    if !vis.exists() {
        return Err(TaskFailure::validation(format!(
            "MeasurementSet {} does not exist",
            vis.display()
        ))
        .at("vis"));
    }
    let reference_fields = fields(task, "reference")?;
    if reference_fields.is_empty() {
        return Err(
            TaskFailure::validation("fluxscale requires at least one reference field")
                .at("reference"),
        );
    }
    let refspwmap = task
        .integers("refspwmap")?
        .into_iter()
        .map(|id| i32::try_from(id).map_err(|_| out_of_range("refspwmap", id)))
        .collect::<Result<Vec<_>, _>>()?;
    let gainthreshold = task.number("gainthreshold")?;
    Ok(FluxScaleRequest {
        input_table: PathBuf::from(task.required_text("caltable")?),
        output_table: PathBuf::from(task.required_text("fluxtable")?),
        reference_fields,
        transfer_fields: fields(task, "transfer")?,
        // CASA's [-1] maps no window.
        refspwmap: if refspwmap == [-1] {
            Vec::new()
        } else {
            refspwmap
        },
        // CASA's -1 excludes no gain.
        gainthreshold: (gainthreshold >= 0.0).then_some(gainthreshold),
        incremental: task.flag("incremental")?,
    })
}

fn gencal(task: &TaskInvocation<'_>) -> Result<GencalTaskRequest, TaskFailure> {
    let caltype = match task.required_text("caltype")?.as_str() {
        "" => {
            return Err(TaskFailure::validation("gencal requires a caltype").at("caltype"));
        }
        "antpos" => GencalType::Antpos,
        "gceff" => GencalType::Gceff,
        "opac" => GencalType::Opac,
        other => return Err(not_yet("caltype", other)),
    };
    Ok(GencalTaskRequest {
        measurement_set: task.path("vis")?,
        output_table: PathBuf::from(task.required_text("caltable")?),
        caltype,
        antenna: task.text("antenna")?.unwrap_or_default(),
        spw: task.text("spw")?.unwrap_or_default(),
        parameter: task.numbers("parameter")?,
        gaincurve_table: task.text("gaincurve_table")?.map(PathBuf::from),
    })
}

/// CASA's selection: `field` and `spw` always; the others are inactive,
/// and so unset, while `selectdata` is not.
fn selection(task: &TaskInvocation<'_>) -> Result<MsSelection, TaskFailure> {
    let text = |name: &str| task.text(name);
    Ok(MsSelection {
        selectdata: true,
        field: text("field")?,
        spw: text("spw")?,
        antenna: text("antenna")?,
        scan: text("scan")?,
        observation: text("observation")?,
        array: text("array")?,
        timerange: text("timerange")?,
        msselect: text("msselect")?,
        ..MsSelection::default()
    })
}

/// Whether a chain is applied, each table with its own `calwt`, or
/// pre-applied while solving, each with `calwt=True`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Chain {
    Apply,
    Solve,
}

/// The chain `callib` names when `docallib` is set, else `gaintable` with
/// its positional lists.
fn chain(
    task: &TaskInvocation<'_>,
    chain: Chain,
) -> Result<Vec<ApplyCalibrationTableSpec>, TaskFailure> {
    if task.flag("docallib")? {
        let callib = task.text("callib")?.ok_or_else(|| {
            TaskFailure::validation("docallib requires callib, a calibration library file")
                .at("callib")
        })?;
        return load_apply_specs_from_callib(Path::new(&callib))
            .map_err(|error| TaskFailure::validation(error.to_string()).at("callib"));
    }
    let tables = task.texts("gaintable")?;
    let gainfield = task.texts("gainfield")?;
    let interp = task.texts("interp")?;
    let spwmap = task.integer_lists("spwmap")?;
    let calwt = match chain {
        Chain::Apply => calwt(task, tables.len())?,
        Chain::Solve => vec![true; tables.len()],
    };
    let mut specs = Vec::new();
    for (index, table) in tables.iter().enumerate() {
        if table.is_empty() {
            continue;
        }
        let mut spec = ApplyCalibrationTableSpec::new(table);
        spec.gainfield = gainfield
            .get(index)
            .and_then(|field| parse_gainfield_value(field));
        spec.interp = match interp.get(index) {
            Some(text) => parse_interp_value(text).map_err(|_| not_yet("interp", text))?,
            None => crate::ApplyInterpolationMode::Linear,
        };
        spec.spwmap = match spwmap.get(index) {
            Some(map) => window_map(map)?,
            None => Vec::new(),
        };
        spec.calwt = calwt[index];
        specs.push(spec);
    }
    Ok(specs)
}

/// `calwt` for each of `tables` tables: one value holds for all of them.
fn calwt(task: &TaskInvocation<'_>, tables: usize) -> Result<Vec<bool>, TaskFailure> {
    let calwt = task.flags("calwt")?;
    match calwt.as_slice() {
        [one] => Ok(vec![*one; tables]),
        _ if calwt.len() >= tables => Ok(calwt),
        _ => Err(TaskFailure::validation(format!(
            "calwt gives {} values for {tables} gain tables; give one, or one for each table",
            calwt.len()
        ))
        .at("calwt")),
    }
}

/// One table's window map; CASA's `[-1]`, or none, maps no window.
fn window_map(map: &[i64]) -> Result<Vec<i32>, TaskFailure> {
    if map == [-1] {
        return Ok(Vec::new());
    }
    map.iter()
        .map(|&id| i32::try_from(id).map_err(|_| out_of_range("spwmap", id)))
        .collect()
}

/// The sky model of a solve: CASA's empty `smodel` is the `MODEL_DATA`
/// column where the MeasurementSet has one and a 1 Jy point source where it
/// does not; `[I, Q, U, V]` is a point source.
fn model(
    task: &TaskInvocation<'_>,
    measurement_set: &Path,
) -> Result<(GainSolveModelSource, [f32; 4]), TaskFailure> {
    let smodel = task.numbers("smodel")?;
    match smodel.as_slice() {
        [] if has_model_column(measurement_set)? => {
            Ok((GainSolveModelSource::ModelColumn, [1.0, 0.0, 0.0, 0.0]))
        }
        [] => Ok((GainSolveModelSource::PointSource, [1.0, 0.0, 0.0, 0.0])),
        &[i, q, u, v] => {
            if !(i > 0.0 && i.is_finite()) {
                return Err(TaskFailure::validation(
                    "smodel's Stokes I must be a positive finite flux density",
                )
                .at("smodel"));
            }
            if [q, u, v] != [0.0; 3] {
                return Err(not_yet("smodel", &format!("{smodel:?}")));
            }
            Ok((GainSolveModelSource::PointSource, [i as f32, 0.0, 0.0, 0.0]))
        }
        _ => Err(TaskFailure::validation(format!(
            "smodel takes [I, Q, U, V], or [] for the MODEL_DATA column; found {} values",
            smodel.len()
        ))
        .at("smodel")),
    }
}

/// Whether the MeasurementSet at `path` has a `MODEL_DATA` column.
fn has_model_column(path: &Path) -> Result<bool, TaskFailure> {
    let ms = MeasurementSet::open(path).map_err(|error| {
        TaskFailure::validation(format!("open MeasurementSet {}: {error}", path.display()))
            .at("vis")
    })?;
    Ok(ms
        .main_table()
        .schema()
        .is_some_and(|schema| schema.contains_column("MODEL_DATA")))
}

fn applymode(task: &TaskInvocation<'_>) -> Result<ApplyMode, TaskFailure> {
    match task.required_text("applymode")?.as_str() {
        "" | "calflag" => Ok(ApplyMode::CalFlag),
        "calonly" => Ok(ApplyMode::CalOnly),
        "trial" => Ok(ApplyMode::Trial),
        other => Err(not_yet("applymode", other)),
    }
}

fn gaintype(task: &TaskInvocation<'_>) -> Result<GainType, TaskFailure> {
    match task.required_text("gaintype")?.as_str() {
        "G" => Ok(GainType::G),
        "T" => Ok(GainType::T),
        other => Err(not_yet("gaintype", other)),
    }
}

fn calmode(task: &TaskInvocation<'_>) -> Result<GainSolveMode, TaskFailure> {
    match task.required_text("calmode")?.as_str() {
        "p" => Ok(GainSolveMode::Phase),
        "ap" => Ok(GainSolveMode::AmplitudePhase),
        other => Err(not_yet("calmode", other)),
    }
}

/// CASA's `solint`: `'inf'`, `'int'`, or seconds; CASA's other time units
/// and channel intervals are not implemented yet.
fn solint(task: &TaskInvocation<'_>) -> Result<GainSolveInterval, TaskFailure> {
    let text = task.required_text("solint")?;
    super::read::parse_gain_solve_interval(&text).map_err(|_| not_yet("solint", &text))
}

/// The axes CASA's `combine` names, `','`-separated; `'scan'` and
/// `'field'` are implemented.
fn combine(task: &TaskInvocation<'_>) -> Result<GainSolveCombine, TaskFailure> {
    let text = task.required_text("combine")?;
    let mut combine = GainSolveCombine::default();
    for axis in text
        .split(',')
        .map(str::trim)
        .filter(|axis| !axis.is_empty())
    {
        match axis {
            "scan" => combine.scans = true,
            "field" => combine.fields = true,
            "spw" | "obs" => return Err(not_yet("combine", &text)),
            other => return Err(unknown("combine", other, "'scan', 'field', 'spw' or 'obs'")),
        }
    }
    Ok(combine)
}

/// CASA's `refant`: one antenna, by name or id; CASA's ranked lists are not
/// implemented yet.
fn refant(task: &TaskInvocation<'_>) -> Result<RefAntSelector, TaskFailure> {
    let text = task.required_text("refant")?;
    if text.is_empty() {
        return Err(TaskFailure::validation("refant is required").at("refant"));
    }
    if text.contains(',') {
        return Err(not_yet("refant", &text));
    }
    Ok(parse_refant_selector(text.trim()))
}

/// The fields `name` lists; an item may itself list them, `','`-separated.
fn fields(task: &TaskInvocation<'_>, name: &str) -> Result<Vec<String>, TaskFailure> {
    Ok(task
        .texts(name)?
        .iter()
        .flat_map(|item| item.split(','))
        .map(str::trim)
        .filter(|field| !field.is_empty())
        .map(str::to_string)
        .collect())
}

/// The refusal of a CASA value casa-rs does not implement yet.
fn not_yet(name: &str, text: &str) -> TaskFailure {
    TaskFailure::validation(format!(
        "{name}={text:?} is not implemented by casa-rs yet (#979)"
    ))
    .at(name)
}

/// The refusal of a value CASA does not take either.
fn unknown(name: &str, text: &str, expected: &str) -> TaskFailure {
    TaskFailure::validation(format!("{name}={text:?} is not valid; expected {expected}")).at(name)
}

fn out_of_range(name: &str, id: i64) -> TaskFailure {
    TaskFailure::validation(format!("{name} value {id} is out of range")).at(name)
}
