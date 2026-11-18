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
use casa_provider_contracts::{ParameterValue, python_repr};
use casa_task_runtime::{TaskFailure, TaskInvocation};
use casa_types::quanta::{Quantity, Unit};

use super::read::{count, parse_gainfield_value, parse_interp_value, parse_refant_selector};
use crate::{
    ApplyArgumentError, ApplyCalibrationTableSpec, ApplyMode, BandpassSolveCombine,
    BandpassSolveCuts, BandpassType, CalibrationTaskRequest, ExecuteApplyTaskRequest,
    FluxScaleRequest, GainNormalization, GainSolveCombine, GainSolveInterval, GainSolveMode,
    GainSolveModelSource, GainType, GencalTaskRequest, GencalType, RefAntSelector,
    SolveBandpassTaskRequest, SolveGainTaskRequest, load_apply_specs_from_callib,
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
    let (calibration_tables, callibrary_error, argument_error) = if task.flag("docallib")? {
        let source = task.text("callib")?.unwrap_or_default();
        match load_apply_specs_from_callib(Path::new(&source)) {
            Ok(tables) => (tables, None, None),
            Err(error) => (Vec::new(), Some(error.to_string()), None),
        }
    } else {
        let (tables, error) = traditional_chain(task, Chain::Apply)?;
        (tables, None, error)
    };
    let mut selection = selection(task)?;
    selection.intent = task.text("intent")?.filter(|text| !text.is_empty());
    selection.uvrange = uv_selector_text(task.value("uvrange"));
    // calibrater.selectvis converts these variants with toCasaString, which
    // removes one outer bracket pair even when the input was a string.
    for selector in [
        &mut selection.field,
        &mut selection.spw,
        &mut selection.intent,
        &mut selection.timerange,
        &mut selection.uvrange,
        &mut selection.antenna,
        &mut selection.scan,
        &mut selection.observation,
    ] {
        if let Some(text) = selector
            && let Some(inner) = text
                .strip_prefix('[')
                .and_then(|text| text.strip_suffix(']'))
        {
            *selector = (!inner.is_empty()).then(|| inner.to_string());
        }
    }
    // CASA accepts these parameters even when disabled, retaining their input
    // values for HISTORY while passing empty selectors to calibrater.selectvis.
    if !task.flag("selectdata")? {
        selection.timerange = None;
        selection.uvrange = None;
        selection.antenna = None;
        selection.scan = None;
        selection.observation = None;
        selection.msselect = None;
    }
    Ok(ExecuteApplyTaskRequest {
        measurement_set: task.path("vis")?,
        selection,
        calibration_tables,
        callibrary_error,
        argument_error,
        apply_mode: applymode(task)?,
        parang: task.flag("parang")?,
        flagbackup: task.flag("flagbackup")?,
    })
}

fn gaincal(task: &TaskInvocation<'_>) -> Result<SolveGainTaskRequest, TaskFailure> {
    let measurement_set = task.path("vis")?;
    let min_snr = min_snr(task)?;
    let (model_source, smodel) = model(task, &measurement_set)?;
    Ok(SolveGainTaskRequest {
        append: task.flag("append")?,
        selection: selection(task)?,
        output_table: PathBuf::from(task.required_text("caltable")?),
        gain_type: gaintype(task)?,
        solve_mode: calmode(task)?,
        solve_interval: solint(task)?,
        preaverage_seconds: task.number("preavg")?,
        combine: combine(task)?,
        refant: refant(task)?,
        prior_calibration_tables: chain(task, Chain::Solve)?,
        parang: task.flag("parang")?,
        model_source,
        normalize_average_amplitude: task.flag("solnorm")?,
        normalization_type: match task.required_text("normtype")?.as_str() {
            "mean" => GainNormalization::Mean,
            "median" => GainNormalization::Median,
            other => return Err(unknown("normtype", other, "'mean' or 'median'")),
        },
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
    // CASA reads maskedge only for BPOLY.
    let mask_edge_percent = match band_type {
        BandpassType::B => BandpassSolveCuts::CASA_DEFAULTS.mask_edge_percent,
        BandpassType::BPoly => {
            let percent = task.number("maskedge")?;
            if !(0.0..50.0).contains(&percent) {
                return Err(TaskFailure::validation(
                    "maskedge must be a percentage of the channels from 0 to below 50",
                )
                .at("maskedge"));
            }
            percent as f32
        }
    };
    Ok(SolveBandpassTaskRequest {
        append: task.flag("append")?,
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
        cuts: Some(BandpassSolveCuts {
            min_baselines_per_antenna: count(task, "minblperant")?,
            min_snr: min_snr(task)?,
            mask_edge_percent,
        }),
        measurement_set,
    })
}

/// `minsnr`, which CASA takes as any number; casa-rs takes it finite and
/// not negative.
fn min_snr(task: &TaskInvocation<'_>) -> Result<f32, TaskFailure> {
    let min_snr = task.number("minsnr")? as f32;
    if !min_snr.is_finite() || min_snr < 0.0 {
        return Err(
            TaskFailure::validation("minsnr must be a finite non-negative number").at("minsnr"),
        );
    }
    Ok(min_snr)
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
        options: crate::FluxScaleOptions {
            measurement_set: Some(vis),
            antenna: task.text("antenna")?.unwrap_or_default(),
            timerange: task.text("timerange")?.unwrap_or_default(),
            scan: task.text("scan")?.unwrap_or_default(),
            fitorder: i32::try_from(task.integer("fitorder")?).map_err(|_| {
                TaskFailure::validation("fitorder exceeds CASA's integer range").at("fitorder")
            })?,
            listfile: task
                .text("listfile")?
                .filter(|text| !text.is_empty())
                .map(PathBuf::from),
            display: task.flag("display")?,
        },
        append: task.flag("append")?,
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
    let antenna = task.text("antenna")?.unwrap_or_default();
    match caltype {
        // CASA looks the corrections up (the VLA's position database, the
        // ALMA tables) when no antenna is named.
        GencalType::Antpos if antenna.trim().is_empty() => {
            return Err(not_yet("antenna", &antenna));
        }
        // With no window named, CASA takes one opacity for all of them.
        GencalType::Opac
            if task.text("spw")?.is_none_or(|spw| spw.trim().is_empty())
                && task.numbers("parameter")?.len() > 1 =>
        {
            return Err(TaskFailure::validation(
                "opac with spw='' takes one opacity for every window; name the windows to give \
                 one for each",
            )
            .at("parameter"));
        }
        _ => {}
    }
    Ok(GencalTaskRequest {
        measurement_set: task.path("vis")?,
        output_table: PathBuf::from(task.required_text("caltable")?),
        caltype,
        antenna,
        spw: task.text("spw")?.unwrap_or_default(),
        parameter: task.numbers("parameter")?,
        gaincurve_table: task.text("gaincurve_table")?.map(PathBuf::from),
    })
}

/// CASA's selection: `field`, `spw` and `intent` always; the others are inactive,
/// and so unset, while `selectdata` is not.
fn selection(task: &TaskInvocation<'_>) -> Result<MsSelection, TaskFailure> {
    let text = |name: &str| {
        task.text(name).map(|value| {
            if task.surface() == "applycal" {
                value.filter(|text| !text.is_empty())
            } else {
                value
            }
        })
    };
    Ok(MsSelection {
        selectdata: true,
        field: text("field")?,
        spw: text("spw")?,
        antenna: text("antenna")?,
        scan: text("scan")?,
        observation: if task.surface() == "applycal" {
            selector_text(task.value("observation"))
        } else {
            text("observation")?
        },
        array: text("array")?,
        timerange: text("timerange")?,
        msselect: text("msselect")?,
        ..MsSelection::default()
    })
}

fn selector_text(value: Option<&ParameterValue>) -> Option<String> {
    let text = match value? {
        ParameterValue::String(text) => text.clone(),
        ParameterValue::NumpyScalar { value, .. } | ParameterValue::ScalarArray(value) => {
            return selector_text(Some(value));
        }
        other => python_repr(other),
    };
    (!text.is_empty()).then_some(text)
}

fn uv_selector_text(value: Option<&ParameterValue>) -> Option<String> {
    match value? {
        ParameterValue::Array(values) => {
            let text = values
                .iter()
                .filter_map(|value| selector_text(Some(value)))
                .collect::<Vec<_>>()
                .join(",");
            (!text.is_empty()).then_some(text)
        }
        ParameterValue::EmptyArray { .. } => None,
        value => selector_text(Some(value)),
    }
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
    traditional_chain(task, chain).map(|(tables, _)| tables)
}

fn traditional_chain(
    task: &TaskInvocation<'_>,
    chain: Chain,
) -> Result<(Vec<ApplyCalibrationTableSpec>, Option<ApplyArgumentError>), TaskFailure> {
    let tables = task.texts("gaintable")?;
    let gainfield = task.texts("gainfield")?;
    let interp = task.texts("interp")?;
    let spwmap = match apply_spw_maps(task.value("spwmap")) {
        Ok(maps) => maps,
        Err(error) if chain == Chain::Apply => return Ok((Vec::new(), Some(error))),
        Err(error) => return Err(TaskFailure::validation(error.to_string()).at("spwmap")),
    };
    let calwt = match chain {
        Chain::Apply => task.flags("calwt")?,
        Chain::Solve => vec![true],
    };
    let mut specs = Vec::new();
    for (index, table) in tables.iter().enumerate() {
        if table.is_empty() {
            continue;
        }
        let weight = match calwt.as_slice() {
            [one] => *one,
            values => match values.get(index) {
                Some(weight) => *weight,
                // Python indexes calwt immediately before setapply for an active
                // table. All earlier table setup still takes place first.
                None => return Ok((specs, Some(ApplyArgumentError::MissingCalwt))),
            },
        };
        let mut spec = ApplyCalibrationTableSpec::new(table);
        spec.gainfield = gainfield
            .get(index)
            .and_then(|field| parse_gainfield_value(field));
        spec.interp = match interp.get(index) {
            Some(text) => {
                parse_interp_value(text).unwrap_or(crate::ApplyInterpolationMode::Invalid)
            }
            None => crate::ApplyInterpolationMode::Linear,
        };
        if let Some(map) = spwmap.get(index) {
            match apply_spw_map(map) {
                Ok(map) => spec.spwmap = map,
                Err(error) if chain == Chain::Apply => return Ok((specs, Some(error))),
                Err(error) => return Err(TaskFailure::validation(error.to_string()).at("spwmap")),
            }
        }
        spec.calwt = weight;
        specs.push(spec);
    }
    Ok((specs, None))
}

fn apply_spw_maps(
    value: Option<&ParameterValue>,
) -> Result<Vec<ParameterValue>, ApplyArgumentError> {
    Ok(match value {
        Some(ParameterValue::Array(items))
            if !items.is_empty() && !matches!(items[0], ParameterValue::Array(_)) =>
        {
            vec![ParameterValue::Array(items.clone())]
        }
        Some(ParameterValue::Array(items)) => items.clone(),
        Some(ParameterValue::String(text)) if text.is_empty() => Vec::new(),
        Some(ParameterValue::Table(entries)) if entries.is_empty() => Vec::new(),
        Some(ParameterValue::Table(_)) => return Err(ApplyArgumentError::SpwMap("0".into())),
        Some(ParameterValue::EmptyArray { .. }) | None => Vec::new(),
        Some(value) => {
            let ty = match value {
                ParameterValue::Integer(_) => "int",
                ParameterValue::Float(_) => "float",
                ParameterValue::Bool(_) => "bool",
                _ => {
                    return Err(ApplyArgumentError::SpwMap(
                        "expected an integer window map".into(),
                    ));
                }
            };
            return Err(ApplyArgumentError::SpwMap(format!(
                "object of type '{ty}' has no len()"
            )));
        }
    })
}

fn apply_spw_map(value: &ParameterValue) -> Result<Vec<i32>, ApplyArgumentError> {
    let ParameterValue::Array(items) = value else {
        return Err(ApplyArgumentError::SpwMap(
            "expected an integer window map".into(),
        ));
    };
    let ids = items
        .iter()
        .map(|item| match item {
            ParameterValue::Integer(id) => Ok(*id),
            ParameterValue::Bool(id) => Ok(i64::from(*id)),
            ParameterValue::NumpyScalar { value, .. }
                if matches!(value.as_ref(), ParameterValue::Integer(_)) =>
            {
                let ParameterValue::Integer(id) = value.as_ref() else {
                    unreachable!()
                };
                Ok(*id)
            }
            _ => Err(ApplyArgumentError::SpwMap(
                "expected an integer window map".into(),
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;
    window_map(&ids).map_err(|failure| ApplyArgumentError::SpwMap(failure.message))
}

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
/// does not; `[I, Q, U, V]` is a point source, and a shorter list, `[I]` to
/// `[I, Q, U]`, is too, its missing Stokes zero (probed with CASA 6.7.6.14,
/// whose `gaincal` aborts on more than four). CASA reads a field's virtual
/// model (`setjy` with `usescratch=False`) before `MODEL_DATA`; casa-rs
/// does not implement it yet, so an empty `smodel` on a MeasurementSet
/// that holds one is refused.
fn model(
    task: &TaskInvocation<'_>,
    measurement_set: &Path,
) -> Result<(GainSolveModelSource, [f32; 4]), TaskFailure> {
    let smodel = task.numbers("smodel")?;
    match smodel.as_slice() {
        [] => {
            let ms = MeasurementSet::open(measurement_set).map_err(|error| {
                TaskFailure::validation(format!(
                    "open MeasurementSet {}: {error}",
                    measurement_set.display()
                ))
                .at("vis")
            })?;
            let virtual_fields = casa_ms::scratch_columns::virtual_model_fields(&ms);
            if !virtual_fields.is_empty() {
                return Err(TaskFailure::validation(format!(
                    "smodel=[] reads the virtual model of field(s) {virtual_fields:?} (setjy with \
                     usescratch=False), which casa-rs does not implement yet (#979); give smodel, \
                     or write MODEL_DATA"
                ))
                .at("smodel"));
            }
            let source = if has_model_column(&ms) {
                GainSolveModelSource::ModelColumn
            } else {
                GainSolveModelSource::PointSource
            };
            Ok((source, [1.0, 0.0, 0.0, 0.0]))
        }
        &[i, ref polarized @ ..] if polarized.len() < 4 => {
            if !(i > 0.0 && i.is_finite()) {
                return Err(TaskFailure::validation(
                    "smodel's Stokes I must be a positive finite flux density",
                )
                .at("smodel"));
            }
            if polarized.iter().any(|stokes| *stokes != 0.0) {
                return Err(not_yet("smodel", &format!("{smodel:?}")));
            }
            Ok((GainSolveModelSource::PointSource, [i as f32, 0.0, 0.0, 0.0]))
        }
        _ => Err(TaskFailure::validation(format!(
            "smodel takes [I, Q, U, V], or fewer with the rest zero, or [] for the MODEL_DATA \
             column; found {} values",
            smodel.len()
        ))
        .at("smodel")),
    }
}

/// Whether `ms` has a `MODEL_DATA` column.
fn has_model_column(ms: &MeasurementSet) -> bool {
    ms.main_table()
        .schema()
        .is_some_and(|schema| schema.contains_column("MODEL_DATA"))
}

fn applymode(task: &TaskInvocation<'_>) -> Result<ApplyMode, TaskFailure> {
    match task.required_text("applymode")?.as_str() {
        "" | "calflag" => Ok(ApplyMode::CalFlag),
        "calflagstrict" => Ok(ApplyMode::CalFlagStrict),
        "flagonly" => Ok(ApplyMode::FlagOnly),
        "flagonlystrict" => Ok(ApplyMode::FlagOnlyStrict),
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
        "a" => Ok(GainSolveMode::Amplitude),
        "ap" => Ok(GainSolveMode::AmplitudePhase),
        other => Err(not_yet("calmode", other)),
    }
}

/// CASA's `solint`, read as `SolvableVisCal::setSolve` reads it: empty
/// text, or text that contains `inf`, in any case, is one solution per
/// scan; text that contains `int`, one per integration; anything else is a
/// time, in seconds when it has no unit. A time of zero is CASA's `inf`,
/// and a negative one its `int`. Channel intervals (`'inf,2ch'`) are not
/// implemented yet.
fn solint(task: &TaskInvocation<'_>) -> Result<GainSolveInterval, TaskFailure> {
    let text = task.text("solint")?.unwrap_or_default();
    if text.contains(',') {
        return Err(not_yet("solint", &text));
    }
    let upper = text.to_ascii_uppercase();
    if upper.is_empty() || upper.contains("INF") {
        return Ok(GainSolveInterval::Infinite);
    }
    if upper.contains("INT") {
        return Ok(GainSolveInterval::Integration);
    }
    let invalid = || unknown("solint", &text, "'inf', 'int' or a time such as '60s'");
    let quantity = text.parse::<Quantity>().map_err(|_| invalid())?;
    let seconds = if quantity.unit().name().is_empty() {
        quantity.value()
    } else {
        let second = Unit::new("s").map_err(|_| invalid())?;
        quantity.get_value_in(&second).map_err(|_| invalid())?
    };
    if !seconds.is_finite() {
        return Err(invalid());
    }
    Ok(if seconds > 0.0 {
        GainSolveInterval::Seconds(seconds)
    } else if seconds == 0.0 {
        GainSolveInterval::Infinite
    } else {
        GainSolveInterval::Integration
    })
}

/// The axes CASA's `combine` names, found as `SolvableVisCal` finds them:
/// by the axis name anywhere in the text, in any case (`'Scan,Field'` and
/// `'scan field'` both combine scans and fields). `'scan'` and `'field'`
/// are implemented; `'spw'` and `'obs'` are not yet.
fn combine(task: &TaskInvocation<'_>) -> Result<GainSolveCombine, TaskFailure> {
    let text = task.required_text("combine")?;
    let upper = text.to_ascii_uppercase();
    if upper.contains("SPW") || upper.contains("OBS") {
        return Err(not_yet("combine", &text));
    }
    Ok(GainSolveCombine {
        scans: upper.contains("SCAN"),
        fields: upper.contains("FIELD"),
    })
}

/// CASA's `refant`: one antenna, by name or id. CASA's ranked lists, and
/// its default `''`, a solve with no reference antenna, are not
/// implemented yet.
fn refant(task: &TaskInvocation<'_>) -> Result<RefAntSelector, TaskFailure> {
    let text = task.required_text("refant")?;
    if text.trim().is_empty() {
        return Err(not_yet("refant", &text));
    }
    if text.contains(',') {
        return Err(not_yet("refant", &text));
    }
    Ok(parse_refant_selector(text.trim()))
}

/// The fields `name` lists; an item may itself list them, `','`-separated.
fn fields(task: &TaskInvocation<'_>, name: &str) -> Result<Vec<String>, TaskFailure> {
    task.texts(name)
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
