// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed reads of one resolved calibration run into its
//! [`CalibrationTaskRequest`].
//!
//! The front end has resolved every value and checked it against its
//! concept; these reads give each workflow the meaning the calibration
//! parameters carry: comma-separated table and field lists, the
//! semicolon-separated per-table chain lists aligned to `gaintables`, and
//! CASA's selection strings. On the `calibrate` surface a workflow reads the
//! parameters the contract's provider projection emits for its `mode`, and
//! no others; a CASA-named surface reads every parameter it declares.

use std::path::PathBuf;

use casa_ms::MsSelection;
use casa_provider_contracts::DefaultSpec;
use casa_task_runtime::{TaskFailure, TaskInvocation};

use crate::{
    ApplyCalibrationTableSpec, ApplyInterpolationMode, ApplyMode, BandpassSolveCombine,
    BandpassType, CalibrationStatsAxis, CalibrationTaskRequest, ExecuteApplyTaskRequest,
    ExportCorrectedDataTaskRequest, FluxScaleRequest, GainFieldSelector, GainSolveCombine,
    GainSolveInterval, GainSolveMode, GainSolveModelSource, GainType, GencalTaskRequest,
    GencalType, RefAntSelector, SolveBandpassTaskRequest, SolveGainTaskRequest, StatsTaskRequest,
    SummaryTaskRequest, load_apply_specs_from_callib,
};

/// How a command line prints a run's report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OutputFormat {
    /// The human-readable report.
    Text,
    /// The report as JSON.
    Json,
}

/// Where and how a run's report is written: `format`, `output` and
/// `overwrite`, which every calibration surface declares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Report {
    pub format: OutputFormat,
    pub output: Option<PathBuf>,
    pub overwrite: bool,
}

/// The report controls `task` sets.
pub(super) fn report(task: &TaskInvocation<'_>) -> Result<Report, TaskFailure> {
    Ok(Report {
        format: parsed(task, "format", parse_output_format)?,
        output: task.text("output")?.map(PathBuf::from),
        overwrite: task.flag("overwrite")?,
    })
}

/// The request `task` makes.
pub(super) fn task_request(
    task: &TaskInvocation<'_>,
) -> Result<CalibrationTaskRequest, TaskFailure> {
    Ok(match mode(task)?.as_str() {
        "apply" => CalibrationTaskRequest::ExecuteApply(apply(task)?),
        "summary" => CalibrationTaskRequest::Summary(summary(task)?),
        "stats" => CalibrationTaskRequest::Stats(stats(task)?),
        "export_corrected_data" => {
            CalibrationTaskRequest::ExportCorrectedData(ExportCorrectedDataTaskRequest {
                input_ms: task.path("vis")?,
                output_ms: PathBuf::from(task.required_text("outputvis")?),
                selection: MsSelection::new(),
            })
        }
        "solve_gain" => CalibrationTaskRequest::SolveGain(solve_gain(task)?),
        "solve_bandpass" => CalibrationTaskRequest::SolveBandpass(solve_bandpass(task)?),
        "fluxscale" => CalibrationTaskRequest::FluxScale(fluxscale(task)?),
        "gencal" => CalibrationTaskRequest::Gencal(gencal(task)?),
        other => {
            return Err(
                TaskFailure::validation(format!("unsupported calibration mode {other:?}"))
                    .at("mode"),
            );
        }
    })
}

/// The workflow `task` runs: `calibrate`'s `mode`, or the one a CASA-named
/// surface stands for.
fn mode(task: &TaskInvocation<'_>) -> Result<String, TaskFailure> {
    let fixed = match task.surface() {
        "calibrate" => return task.required_text("mode"),
        "applycal" => "apply",
        "gaincal" => "solve_gain",
        "bandpass" => "solve_bandpass",
        "fluxscale" => "fluxscale",
        "gencal" => "gencal",
        other => {
            return Err(TaskFailure::validation(format!(
                "calibrate runs no surface {other:?}"
            )));
        }
    };
    Ok(fixed.to_string())
}

fn apply(task: &TaskInvocation<'_>) -> Result<ExecuteApplyTaskRequest, TaskFailure> {
    Ok(ExecuteApplyTaskRequest {
        measurement_set: task.path("vis")?,
        selection: selection(task)?,
        calibration_tables: calibration_tables(task, Chain::Apply)?,
        apply_mode: parsed(task, "apply_mode", parse_apply_mode)?,
        parang: task.flag("parang")?,
        flagbackup: task.flag("flagbackup")?,
    })
}

fn summary(task: &TaskInvocation<'_>) -> Result<SummaryTaskRequest, TaskFailure> {
    let paths = task.paths("summary_paths")?;
    if paths.is_empty() {
        return Err(TaskFailure::validation(
            "summary requires at least one calibration-table path",
        )
        .at("summary_paths"));
    }
    Ok(SummaryTaskRequest { paths })
}

fn stats(task: &TaskInvocation<'_>) -> Result<StatsTaskRequest, TaskFailure> {
    Ok(StatsTaskRequest {
        path: PathBuf::from(task.required_text("table_path")?),
        axis: CalibrationStatsAxis::parse(&task.required_text("stats_axis")?),
        datacolumn: task.text("stats_datacolumn")?,
        use_flags: task.flag("use_flags")?,
    })
}

fn solve_gain(task: &TaskInvocation<'_>) -> Result<SolveGainTaskRequest, TaskFailure> {
    let min_snr = task.number("min_snr")? as f32;
    if !min_snr.is_finite() || min_snr < 0.0 {
        return Err(
            TaskFailure::validation("min_snr must be a finite non-negative number").at("min_snr"),
        );
    }
    Ok(SolveGainTaskRequest {
        measurement_set: task.path("vis")?,
        selection: selection(task)?,
        output_table: PathBuf::from(task.required_text("out_table")?),
        gain_type: parsed(task, "gain_type", parse_gain_type)?,
        solve_mode: parsed(task, "solve_mode", parse_gain_solve_mode)?,
        solve_interval: parsed(task, "solint", parse_gain_solve_interval)?,
        combine: optional_parsed(task, "gain_combine", parse_gain_solve_combine)?,
        refant: parse_refant_selector(&task.required_text("refant")?),
        prior_calibration_tables: calibration_tables(task, Chain::Solve)?,
        parang: declared_flag(task, "parang")?,
        model_source: parsed(task, "gain_model_source", parse_gain_solve_model_source)?,
        normalize_average_amplitude: task.flag("solnorm")?,
        min_snr,
        min_baselines_per_antenna: count(task, "minblperant")?,
        smodel: parsed(task, "smodel", parse_stokes_smodel)?,
    })
}

fn solve_bandpass(task: &TaskInvocation<'_>) -> Result<SolveBandpassTaskRequest, TaskFailure> {
    // The bandpass surface takes CASA's `minsnr`, which the native solver
    // does not apply yet; a value other than the default is refused, not
    // ignored. `calibrate`'s `min_snr` is its gain solve's.
    if task.surface() == "bandpass" && !takes_default(task, "min_snr") {
        return Err(TaskFailure::validation(
            "bandpass does not apply min_snr yet; leave it at its default",
        )
        .at("min_snr"));
    }
    Ok(SolveBandpassTaskRequest {
        measurement_set: task.path("vis")?,
        selection: selection(task)?,
        output_table: PathBuf::from(task.required_text("out_table")?),
        refant: parse_refant_selector(&task.required_text("refant")?),
        prior_calibration_tables: calibration_tables(task, Chain::Solve)?,
        parang: declared_flag(task, "parang")?,
        combine: optional_parsed(task, "bandpass_combine", parse_bandpass_combine)?,
        band_type: parsed(task, "bandtype", parse_bandpass_type)?,
        normalize_average_amplitude: task.flag("solnorm")?,
        amplitude_degree: count(task, "degamp")?,
        phase_degree: count(task, "degphase")?,
        smodel: parsed(task, "smodel", parse_stokes_smodel)?,
    })
}

fn fluxscale(task: &TaskInvocation<'_>) -> Result<FluxScaleRequest, TaskFailure> {
    let reference_fields = parse_string_list(&task.required_text("reference_fields")?);
    if reference_fields.is_empty() {
        return Err(
            TaskFailure::validation("fluxscale requires at least one reference field")
                .at("reference_fields"),
        );
    }
    let gainthreshold = task.optional_number("gainthreshold")?;
    Ok(FluxScaleRequest {
        input_table: PathBuf::from(task.required_text("fluxscale_input")?),
        output_table: PathBuf::from(task.required_text("out_table")?),
        reference_fields,
        transfer_fields: parse_string_list(&task.text("transfer_fields")?.unwrap_or_default()),
        refspwmap: optional_parsed(task, "refspwmap", |value| {
            parse_i32_list("refspwmap", value)
        })?,
        gainthreshold,
        incremental: task.flag("incremental")?,
    })
}

fn gencal(task: &TaskInvocation<'_>) -> Result<GencalTaskRequest, TaskFailure> {
    Ok(GencalTaskRequest {
        measurement_set: task.path("vis")?,
        output_table: PathBuf::from(task.required_text("out_table")?),
        caltype: parsed(task, "caltype", str::parse::<GencalType>)?,
        antenna: task.text("antenna")?.unwrap_or_default(),
        spw: task.text("spw")?.unwrap_or_default(),
        parameter: optional_parsed(task, "parameter", |value| {
            parse_f64_list("parameter", value)
        })?,
        gaincurve_table: task.text("gaincurve_table")?.map(PathBuf::from),
    })
}

/// CASA's MeasurementSet selection; the selectors other than `field` are
/// active only while `selectdata` is.
fn selection(task: &TaskInvocation<'_>) -> Result<MsSelection, TaskFailure> {
    let text = |name: &str| task.text(name);
    Ok(MsSelection {
        selectdata: task.flag("selectdata")?,
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

/// Whether a calibration chain is applied, which needs at least one table,
/// or pre-applied while solving, where CASA applies each table with
/// `calwt=True`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Chain {
    Apply,
    Solve,
}

/// The chain `gaintables` or `callib` names, with the per-table
/// `gainfield`, `interp`, `spwmap` and, for an apply, `calwt` lists.
fn calibration_tables(
    task: &TaskInvocation<'_>,
    chain: Chain,
) -> Result<Vec<ApplyCalibrationTableSpec>, TaskFailure> {
    let tables = task.paths("gaintables")?;
    let callib = task.text("callib")?.map(PathBuf::from);
    let gainfield = task.text("gainfield")?;
    let interp = task.text("interp")?;
    let spwmap = task.text("spwmap")?;
    let lists = ChainLists {
        gainfield: gainfield.as_deref(),
        interp: interp.as_deref(),
        spwmap: spwmap.as_deref(),
    };
    match chain {
        Chain::Apply => {
            let calwt = task.text("calwt")?;
            build_apply_specs(callib, tables, lists, calwt.as_deref())
        }
        Chain::Solve => build_solver_specs(callib, tables, lists),
    }
}

/// `name`'s flag, or false on a surface that does not declare it, as
/// gaincal and bandpass leave out `parang`.
fn declared_flag(task: &TaskInvocation<'_>, name: &str) -> Result<bool, TaskFailure> {
    let declared = task
        .session()
        .bundle()
        .surface
        .bindings()
        .iter()
        .any(|binding| binding.name == name);
    if declared { task.flag(name) } else { Ok(false) }
}

/// Whether `name` holds its binding's default.
fn takes_default(task: &TaskInvocation<'_>, name: &str) -> bool {
    task.session()
        .bundle()
        .surface
        .bindings()
        .iter()
        .find(|binding| binding.name == name)
        .is_some_and(|binding| match &binding.default {
            DefaultSpec::Literal { value } => task.value(name) == Some(value),
            _ => false,
        })
}

/// `name`, a non-negative integer.
fn count(task: &TaskInvocation<'_>, name: &str) -> Result<usize, TaskFailure> {
    let value = task.integer(name)?;
    usize::try_from(value).map_err(|_| {
        TaskFailure::validation(format!(
            "parameter {name:?} must be a non-negative integer, found {value}"
        ))
        .at(name)
    })
}

/// `name`'s text through `parse`; a refusal names the parameter.
fn parsed<T>(
    task: &TaskInvocation<'_>,
    name: &str,
    parse: impl Fn(&str) -> Result<T, String>,
) -> Result<T, TaskFailure> {
    parse(&task.required_text(name)?).map_err(|error| TaskFailure::validation(error).at(name))
}

/// [`parsed`] for a parameter whose absence (`"none"`) means its type's
/// default: no combine axes, no remap, no numbers.
fn optional_parsed<T: Default>(
    task: &TaskInvocation<'_>,
    name: &str,
    parse: impl Fn(&str) -> Result<T, String>,
) -> Result<T, TaskFailure> {
    match task.text(name)? {
        Some(text) => parse(&text).map_err(|error| TaskFailure::validation(error).at(name)),
        None => Ok(T::default()),
    }
}

fn parse_output_format(value: &str) -> Result<OutputFormat, String> {
    match value {
        "text" => Ok(OutputFormat::Text),
        "json" => Ok(OutputFormat::Json),
        other => Err(format!(
            "unsupported format {other:?}; expected text or json"
        )),
    }
}

fn parse_bool_literal(value: &str) -> Result<bool, String> {
    match value {
        "true" | "t" | "1" | "yes" | "y" => Ok(true),
        "false" | "f" | "0" | "no" | "n" => Ok(false),
        other => Err(format!(
            "parse boolean value {other:?}: expected true or false"
        )),
    }
}

fn parse_apply_mode(value: &str) -> Result<ApplyMode, String> {
    match value {
        "calflag" => Ok(ApplyMode::CalFlag),
        "calonly" => Ok(ApplyMode::CalOnly),
        "trial" => Ok(ApplyMode::Trial),
        other => Err(format!(
            "unsupported apply_mode {other:?}; expected calflag, calonly, or trial"
        )),
    }
}

fn parse_gain_type(value: &str) -> Result<GainType, String> {
    match value {
        "g" | "G" => Ok(GainType::G),
        "t" | "T" => Ok(GainType::T),
        other => Err(format!("unsupported gain_type {other:?}; expected g or t")),
    }
}

fn parse_gain_solve_mode(value: &str) -> Result<GainSolveMode, String> {
    match value {
        "p" => Ok(GainSolveMode::Phase),
        "ap" => Ok(GainSolveMode::AmplitudePhase),
        other => Err(format!(
            "unsupported solve_mode {other:?}; expected p or ap"
        )),
    }
}

fn parse_gain_solve_model_source(value: &str) -> Result<GainSolveModelSource, String> {
    match value.to_ascii_lowercase().as_str() {
        "point" | "point-source" | "smodel" => Ok(GainSolveModelSource::PointSource),
        "model" | "model-column" | "model_data" | "model-data" => {
            Ok(GainSolveModelSource::ModelColumn)
        }
        other => Err(format!(
            "unsupported gain_model_source {other:?}; expected point or model-column"
        )),
    }
}

fn parse_bandpass_type(value: &str) -> Result<BandpassType, String> {
    match value.to_ascii_lowercase().as_str() {
        "b" => Ok(BandpassType::B),
        "bpoly" => Ok(BandpassType::BPoly),
        other => Err(format!(
            "unsupported bandtype {other:?}; expected b or bpoly"
        )),
    }
}

pub(super) fn parse_gain_solve_interval(value: &str) -> Result<GainSolveInterval, String> {
    match value {
        "inf" => Ok(GainSolveInterval::Infinite),
        "int" => Ok(GainSolveInterval::Integration),
        other => {
            let raw = other.strip_suffix('s').unwrap_or(other);
            let seconds = raw
                .parse::<f64>()
                .map_err(|error| format!("parse solint {other:?}: {error}"))?;
            Ok(GainSolveInterval::Seconds(seconds))
        }
    }
}

fn parse_gain_solve_combine(value: &str) -> Result<GainSolveCombine, String> {
    let mut combine = GainSolveCombine::default();
    for axis in value
        .split(',')
        .map(str::trim)
        .filter(|axis| !axis.is_empty() && !axis.eq_ignore_ascii_case("none"))
    {
        match axis {
            "scan" => combine.scans = true,
            "field" => combine.fields = true,
            other => {
                return Err(format!(
                    "unsupported gain_combine axis {other:?}; expected scan and/or field"
                ));
            }
        }
    }
    Ok(combine)
}

fn parse_bandpass_combine(value: &str) -> Result<BandpassSolveCombine, String> {
    let mut combine = BandpassSolveCombine::default();
    for part in value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty() && *part != "none")
    {
        match part {
            "scan" => combine.scans = true,
            "field" => combine.fields = true,
            other => {
                return Err(format!(
                    "unsupported bandpass_combine {other:?}; expected scan, field, scan,field, or none"
                ));
            }
        }
    }
    Ok(combine)
}

fn parse_refant_selector(value: &str) -> RefAntSelector {
    match value.parse::<i32>() {
        Ok(id) => RefAntSelector::AntennaId(id),
        Err(_) => RefAntSelector::AntennaName(value.to_string()),
    }
}

fn parse_string_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn parse_semicolon_segments(value: &str) -> Vec<String> {
    if !value.contains(';') {
        return vec![value.trim().to_string()];
    }
    value
        .split(';')
        .map(|item| item.trim().to_string())
        .collect()
}

fn expand_aligned_values<T: Clone>(
    name: &str,
    values: Vec<T>,
    len: usize,
    default: T,
) -> Result<Vec<T>, String> {
    if values.is_empty() {
        Ok(vec![default; len])
    } else if values.len() == 1 {
        Ok(vec![values[0].clone(); len])
    } else if values.len() == len {
        Ok(values)
    } else {
        Err(format!(
            "{name} provided {} values for {} calibration tables; expected one value or one per table",
            values.len(),
            len
        ))
    }
}

pub(super) fn parse_gainfield_value(value: &str) -> Option<GainFieldSelector> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if value.eq_ignore_ascii_case("nearest") {
        return Some(GainFieldSelector::Nearest);
    }
    if let Ok(field_id) = value.parse::<i32>() {
        return Some(GainFieldSelector::FieldId(field_id));
    }
    Some(GainFieldSelector::FieldName(value.to_string()))
}

pub(super) fn parse_interp_value(value: &str) -> Result<ApplyInterpolationMode, String> {
    let normalized = value.trim().to_ascii_lowercase().replace(' ', "");
    match normalized.as_str() {
        "" | "linear" => Ok(ApplyInterpolationMode::Linear),
        "nearest" => Ok(ApplyInterpolationMode::Nearest),
        "nearest,linear" | "nearestlinear" => Ok(ApplyInterpolationMode::NearestLinear),
        _ => Err(format!(
            "interp value {value:?} is unsupported; expected nearest, linear, or nearest,linear"
        )),
    }
}

fn parse_spwmap_value(value: &str) -> Result<Vec<i32>, String> {
    let value = value.trim();
    let inner = value
        .strip_prefix('[')
        .and_then(|candidate| candidate.strip_suffix(']'))
        .unwrap_or(value);
    parse_i32_list("spwmap", inner)
}

/// The per-table chain lists, each one value for every table or one per
/// table, `;`-separated.
#[derive(Debug, Clone, Copy, Default)]
struct ChainLists<'a> {
    gainfield: Option<&'a str>,
    interp: Option<&'a str>,
    spwmap: Option<&'a str>,
}

impl ChainLists<'_> {
    fn is_empty(&self) -> bool {
        self.gainfield.is_none() && self.interp.is_none() && self.spwmap.is_none()
    }
}

/// A chain refusal at the parameter `name`.
fn refused(name: &'static str) -> impl Fn(String) -> TaskFailure {
    move |message| TaskFailure::validation(message).at(name)
}

/// One spec per table in `paths`, with the aligned chain lists.
fn build_calibration_table_specs(
    paths: Vec<PathBuf>,
    lists: ChainLists<'_>,
    calwt: Option<&str>,
) -> Result<Vec<ApplyCalibrationTableSpec>, TaskFailure> {
    let segments = |list: Option<&str>| list.map(parse_semicolon_segments).unwrap_or_default();
    let gainfield = segments(lists.gainfield)
        .iter()
        .map(|segment| parse_gainfield_value(segment))
        .collect();
    let interp = segments(lists.interp)
        .iter()
        .map(|segment| parse_interp_value(segment))
        .collect::<Result<_, _>>()
        .map_err(refused("interp"))?;
    let spwmap = segments(lists.spwmap)
        .iter()
        .map(|segment| parse_spwmap_value(segment))
        .collect::<Result<_, _>>()
        .map_err(refused("spwmap"))?;
    let calwt = match calwt {
        Some(raw) => parse_bool_list("calwt", raw).map_err(refused("calwt"))?,
        None => Vec::new(),
    };
    let len = paths.len();
    let gainfield =
        expand_aligned_values("gainfield", gainfield, len, None).map_err(refused("gainfield"))?;
    let interp = expand_aligned_values("interp", interp, len, ApplyInterpolationMode::Linear)
        .map_err(refused("interp"))?;
    let spwmap =
        expand_aligned_values("spwmap", spwmap, len, Vec::new()).map_err(refused("spwmap"))?;
    let calwt = expand_aligned_values("calwt", calwt, len, false).map_err(refused("calwt"))?;
    Ok(paths
        .into_iter()
        .zip(gainfield)
        .zip(interp)
        .zip(spwmap)
        .zip(calwt)
        .map(|((((path, gainfield), interp), spwmap), calwt)| {
            let mut spec = ApplyCalibrationTableSpec::new(path);
            spec.gainfield = gainfield;
            spec.interp = interp;
            spec.spwmap = spwmap;
            spec.calwt = calwt;
            spec
        })
        .collect())
}

/// The chain an apply runs: `callib`, or `gaintables` with its lists.
fn build_apply_specs(
    callib: Option<PathBuf>,
    tables: Vec<PathBuf>,
    lists: ChainLists<'_>,
    calwt: Option<&str>,
) -> Result<Vec<ApplyCalibrationTableSpec>, TaskFailure> {
    match (callib, tables.is_empty()) {
        (Some(_), false) => Err(refused("callib")(
            "set either callib or gaintables, not both".to_string(),
        )),
        (Some(callib), true) => {
            if !lists.is_empty() || calwt.is_some() {
                return Err(refused("callib")("gainfield, interp, spwmap and calwt cannot be combined with callib; set per-table chain controls inside the callibrary file".to_string()));
            }
            load_apply_specs_from_callib(&callib)
                .map_err(|error| refused("callib")(error.to_string()))
        }
        (None, true) => Err(refused("gaintables")(
            "apply requires calibration input; set callib or gaintables".to_string(),
        )),
        (None, false) => build_calibration_table_specs(tables, lists, calwt),
    }
}

/// The chain a solve pre-applies: none, `callib`, or `gaintables` with its
/// lists, each table with `calwt=True` as CASA's gaincal and bandpass do.
fn build_solver_specs(
    callib: Option<PathBuf>,
    tables: Vec<PathBuf>,
    lists: ChainLists<'_>,
) -> Result<Vec<ApplyCalibrationTableSpec>, TaskFailure> {
    match (callib, tables.is_empty()) {
        (Some(_), false) => Err(refused("callib")(
            "set either callib or gaintables, not both".to_string(),
        )),
        (Some(callib), true) => {
            if !lists.is_empty() {
                return Err(refused("callib")("gainfield, interp and spwmap cannot be combined with callib; set per-table chain controls inside the callibrary file".to_string()));
            }
            load_apply_specs_from_callib(&callib)
                .map_err(|error| refused("callib")(error.to_string()))
        }
        (None, true) => Ok(Vec::new()),
        (None, false) => build_calibration_table_specs(tables, lists, Some("true")),
    }
}

fn parse_i32_list(name: &str, value: &str) -> Result<Vec<i32>, String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|item| {
            item.parse::<i32>()
                .map_err(|error| format!("parse {name} value {item:?}: {error}"))
        })
        .collect()
}

fn parse_f64_list(name: &str, value: &str) -> Result<Vec<f64>, String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|item| {
            item.parse::<f64>()
                .map_err(|error| format!("parse {name} value {item:?}: {error}"))
        })
        .collect()
}

fn parse_stokes_smodel(value: &str) -> Result<[f32; 4], String> {
    let values = parse_f64_list("smodel", value)?;
    let [i, q, u, v]: [f64; 4] = values.try_into().map_err(|values: Vec<f64>| {
        format!("smodel requires exactly 4 values, got {}", values.len())
    })?;
    let smodel = [i as f32, q as f32, u as f32, v as f32];
    if smodel.iter().all(|value| value.is_finite()) {
        Ok(smodel)
    } else {
        Err("smodel values must be finite".to_string())
    }
}

fn parse_bool_list(name: &str, value: &str) -> Result<Vec<bool>, String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|item| {
            parse_bool_literal(item)
                .map_err(|_| format!("parse {name} value {item:?}: expected true/false"))
        })
        .collect()
}
