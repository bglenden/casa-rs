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

use crate::{TaskFailure, TaskInvocation};
use casa_contracts::{ParameterValue, python_repr};
use casa_ms::{MeasurementSet, MsSelection};

use super::read::{count, parse_gainfield_value, parse_interp_value};
use crate::calibration::{
    CalibrationTaskRequest, ExecuteApplyTaskRequest, GencalTaskRequest, SolveBandpassTaskRequest,
    SolveGainTaskRequest,
};
use casa_calibration::{
    ApplyArgumentError, ApplyCalibrationTableSpec, ApplyMode, BandpassFrequencyInterval,
    BandpassSolveCombine, BandpassSolveCuts, BandpassType, FluxScaleRequest, GainNormalization,
    GainSolveCombine, GainSolveInterval, GainSolveMode, GainSolveModelSource, GainSplineControls,
    GainType, GencalRequest, GencalType, RefAntSelector, load_apply_specs_from_callib,
};

/// The request a CASA-named surface makes.
pub(super) fn task_request(
    task: &TaskInvocation<'_>,
) -> Result<CalibrationTaskRequest, TaskFailure> {
    Ok(match task.surface() {
        "applycal" => CalibrationTaskRequest::ExecuteApply(applycal(task)?),
        "gaincal" => CalibrationTaskRequest::SolveGain(gaincal(task)?),
        "fringefit" => CalibrationTaskRequest::SolveGain(fringefit(task)?),
        "accor" => CalibrationTaskRequest::SolveAccor(accor(task)?),
        "blcal" => blcal(task)?,
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
    Ok(ExecuteApplyTaskRequest {
        measurement_set: task.path("vis")?,
        selection: selection(task)?,
        calibration_tables,
        callibrary_error,
        argument_error,
        apply_mode: applymode(task)?,
        parang: task.flag("parang")?,
        flagbackup: task.flag("flagbackup")?,
    })
}

fn gaincal(task: &TaskInvocation<'_>) -> Result<SolveGainTaskRequest, TaskFailure> {
    let gain_type = gaintype(task)?;
    let spline = gain_type == GainType::GSpline;
    let spline_controls = if spline {
        Some(GainSplineControls {
            splinetime: task.number("splinetime")?,
            npointaver: task.integer("npointaver")?,
            phasewrap: task.number("phasewrap")?,
        })
    } else {
        None
    };
    let measurement_set = task.path("vis")?;
    let min_snr = if spline { 0. } else { min_snr(task)? };
    let (model_source, smodel) = model(task, &measurement_set, true)?;
    let reference = task.required_text("refant")?;
    let strict = match task.required_text("refantmode")?.as_str() {
        _ if spline => false,
        "flex" => false,
        "strict" => true,
        other => return Err(unknown("refantmode", other, "'flex' or 'strict'")),
    };
    Ok(SolveGainTaskRequest {
        append: task.flag("append")?,
        selection: selection(task)?,
        output_table: PathBuf::from(task.required_text("caltable")?),
        gain_type,
        spline_controls,
        fringe_controls: None,
        solve_mode: calmode(task)?,
        solve_interval: if spline {
            GainSolveInterval::Infinite
        } else {
            solint(task)?
        },
        preaverage_seconds: task.number("preavg")?,
        correlation_dependent_flags: task.flag("corrdepflags")?,
        combine: if spline {
            GainSolveCombine::default()
        } else {
            combine(task)?
        },
        refant: if reference.is_empty() {
            RefAntSelector::Unspecified
        } else {
            RefAntSelector::Expression {
                selection: reference,
                strict,
            }
        },
        prior_calibration_tables: chain(task, Chain::Solve)?,
        parang: task.flag("parang")?,
        model_source,
        normalize_average_amplitude: task.flag("solnorm")?,
        normalization_type: match task.required_text("normtype")?.as_str() {
            "mean" => GainNormalization::Mean,
            "median" => GainNormalization::Median,
            other => return Err(unknown("normtype", other, "'mean' or 'median'")),
        },
        robustness: if spline {
            Default::default()
        } else {
            robust_gain_solve(task)?
        },
        min_snr,
        min_baselines_per_antenna: if spline {
            0
        } else {
            count(task, "minblperant")?
        },
        smodel,
        measurement_set,
    })
}

fn fringefit(task: &TaskInvocation<'_>) -> Result<SolveGainTaskRequest, TaskFailure> {
    let reference = task.required_text("refant")?;
    let active_parameters = task.flags("paramactive")?;
    let active_parameters = match active_parameters.as_slice() {
        [] => [true, true, false],
        [delay, rate, dispersion] => [*delay, *rate, *dispersion],
        _ => {
            return Err(TaskFailure::validation(
                "paramactive vector must have exactly three entries",
            ));
        }
    };
    let search_window = |name| -> Result<[f64; 2], TaskFailure> {
        Ok(task.numbers(name)?.try_into().unwrap_or([-1e6, 1e6]))
    };
    let iterations = usize::try_from(task.integer("niter")?).unwrap_or(usize::MAX);
    let correlation_combination = match task.required_text("corrcomb")?.as_str() {
        "none" => casa_calibration::FringeCorrelation::Separate,
        "stokes" => casa_calibration::FringeCorrelation::Stokes,
        "parallel" => casa_calibration::FringeCorrelation::Parallel,
        other => return Err(unknown("corrcomb", other, "'none', 'stokes' or 'parallel'")),
    };
    Ok(SolveGainTaskRequest {
        measurement_set: task.path("vis")?,
        selection: selection(task)?,
        output_table: PathBuf::from(task.required_text("caltable")?),
        append: task.flag("append")?,
        gain_type: GainType::K,
        spline_controls: None,
        fringe_controls: Some(casa_calibration::FringeControls {
            delay_window: search_window("delaywindow")?,
            rate_window: search_window("ratewindow")?,
            global_solve: task.flag("globalsolve")?,
            iterations,
            active_parameters,
            zero_rates: task.flag("zerorates")?,
            correlation_combination,
            concatenate_windows: task.flag("concatspws")?,
        }),
        solve_mode: GainSolveMode::Phase,
        solve_interval: solint(task)?,
        preaverage_seconds: 0.,
        correlation_dependent_flags: task.flag("corrdepflags")?,
        combine: combine(task)?,
        refant: if reference.is_empty() {
            RefAntSelector::Unspecified
        } else {
            RefAntSelector::Expression {
                selection: reference,
                strict: false,
            }
        },
        prior_calibration_tables: chain(task, Chain::Solve)?,
        parang: task.flag("parang")?,
        model_source: GainSolveModelSource::FromMeasurementSet,
        normalize_average_amplitude: false,
        normalization_type: GainNormalization::Mean,
        robustness: Default::default(),
        min_snr: task.number("minsnr")? as f32,
        min_baselines_per_antenna: 0,
        smodel: [1., 0., 0., 0.],
    })
}

fn accor(task: &TaskInvocation<'_>) -> Result<SolveGainTaskRequest, TaskFailure> {
    Ok(SolveGainTaskRequest {
        measurement_set: task.path("vis")?,
        selection: selection(task)?,
        output_table: PathBuf::from(task.required_text("caltable")?),
        append: task.flag("append")?,
        gain_type: GainType::G,
        spline_controls: None,
        fringe_controls: None,
        solve_mode: GainSolveMode::Amplitude,
        solve_interval: solint(task)?,
        preaverage_seconds: 0.,
        correlation_dependent_flags: task.flag("corrdepflags")?,
        combine: combine(task)?,
        refant: RefAntSelector::Unspecified,
        prior_calibration_tables: chain(task, Chain::Solve)?,
        parang: false,
        model_source: GainSolveModelSource::FromMeasurementSet,
        normalize_average_amplitude: false,
        normalization_type: GainNormalization::Mean,
        robustness: Default::default(),
        min_snr: 0.,
        min_baselines_per_antenna: 0,
        smodel: [1., 0., 0., 0.],
    })
}

fn blcal(task: &TaskInvocation<'_>) -> Result<CalibrationTaskRequest, TaskFailure> {
    if task.flag("selectdata")?
        && let Some(ParameterValue::Bool(observation)) = task.value("observation")
    {
        let name = if *observation { "True" } else { "False" };
        let first = name.chars().next().expect("boolean text");
        return Err(TaskFailure::execution(format!(
            "Error in data selection specification: Scan Expression: Parse error at or near '{first}'\n(near char. 1 in string \"{name}\")"
        )));
    }
    let frequency_dependent = task.flag("freqdep")?;
    let output_table = task.required_text("caltable")?;
    if output_table.is_empty() {
        return Err(TaskFailure::execution("Error in Calibrater::setsolve."));
    }
    let supplied_interval = selector_text(task.value("solint")).unwrap_or_default();
    let (time, frequency) = supplied_interval
        .split_once(',')
        .unwrap_or((&supplied_interval, ""));
    let frequency_interval = if frequency_dependent {
        parse_frequency_solint(frequency)?
    } else {
        None
    };
    Ok(CalibrationTaskRequest::SolveBaseline {
        frequency_dependent,
        frequency_interval,
        gain: SolveGainTaskRequest {
            measurement_set: task.path("vis")?,
            selection: selection(task)?,
            output_table: PathBuf::from(output_table),
            append: false,
            gain_type: GainType::G,
            spline_controls: None,
            fringe_controls: None,
            solve_mode: calmode(task)?,
            solve_interval: parse_time_solint(time)?,
            preaverage_seconds: 0.,
            correlation_dependent_flags: false,
            combine: combine(task)?,
            refant: RefAntSelector::Unspecified,
            prior_calibration_tables: traditional_chain(task, Chain::Solve)?.0,
            parang: task.flag("parang")?,
            model_source: GainSolveModelSource::FromMeasurementSet,
            normalize_average_amplitude: task.flag("solnorm")?,
            normalization_type: GainNormalization::Mean,
            robustness: Default::default(),
            min_snr: 0.,
            min_baselines_per_antenna: 0,
            smodel: [1., 0., 0., 0.],
        },
    })
}

fn bandpass(task: &TaskInvocation<'_>) -> Result<SolveBandpassTaskRequest, TaskFailure> {
    let measurement_set = task.path("vis")?;
    let band_type = match task.required_text("bandtype")?.as_str() {
        "B" => BandpassType::B,
        "BPOLY" => BandpassType::BPoly,
        other => return Err(unknown("bandtype", other, "'B' or 'BPOLY'")),
    };
    let (model_source, smodel) = model(task, &measurement_set, true)?;
    let supplied_interval = selector_text(task.value("solint")).unwrap_or_default();
    let (time_interval, frequency_interval) = supplied_interval
        .split_once(',')
        .unwrap_or((&supplied_interval, ""));
    let combine = combine(task)?;
    let degrees = [task.integer("degamp")?, task.integer("degphase")?];
    // CASA reads maskedge only for BPOLY.
    let mask_edge_percent = match band_type {
        BandpassType::B => BandpassSolveCuts::CASA_DEFAULTS.mask_edge_percent,
        BandpassType::BPoly => task.number("maskedge")? as f32,
    };
    Ok(SolveBandpassTaskRequest {
        append: task.flag("append")?,
        selection: selection(task)?,
        output_table: PathBuf::from(task.required_text("caltable")?),
        refant: bandpass_refant(&task.required_text("refant")?),
        prior_calibration_tables: chain(task, Chain::Solve)?,
        parang: task.flag("parang")?,
        combine: BandpassSolveCombine {
            scans: combine.scans,
            fields: combine.fields,
            observations: combine.observations,
            spectral_windows: combine.spectral_windows,
        },
        band_type,
        normalize_average_amplitude: task.flag("solnorm")?,
        amplitude_degree: degrees[0].max(0) as usize,
        phase_degree: degrees[1].max(0) as usize,
        model_source,
        solve_interval: parse_time_solint(time_interval)?,
        frequency_interval: parse_frequency_solint(frequency_interval)?,
        correlation_dependent_flags: task.flag("corrdepflags")?,
        smodel,
        cuts: Some(BandpassSolveCuts {
            min_baselines_per_antenna: task.integer("minblperant")?.max(0) as usize,
            min_snr: task.number("minsnr")? as f32,
            fill_gaps: match band_type {
                BandpassType::B => {
                    usize::try_from(task.integer("fillgaps")?.max(0)).unwrap_or(usize::MAX)
                }
                BandpassType::BPoly => 0,
            },
            mask_edge_percent,
            mask_center_channels: if band_type == BandpassType::BPoly {
                usize::try_from(task.integer("maskcenter")?.max(0)).unwrap_or(usize::MAX)
            } else {
                0
            },
            normalize_visibilities: band_type == BandpassType::BPoly && task.flag("visnorm")?,
            omit_polynomial: degrees.map(|degree| degree == -1),
        }),
        measurement_set,
    })
}

pub(super) fn leakage_request(
    task: &TaskInvocation<'_>,
    vis: &Path,
) -> Result<casa_calibration::solve::grouping::leakage::LeakageSolveRequest, TaskFailure> {
    use casa_calibration::solve::grouping::leakage::{LeakageSolveKind, LeakageSolveRequest};

    let gain = task.surface() == "gaincal";
    let poltype = task.required_text(if gain { "gaintype" } else { "poltype" })?;
    let kind = match poltype.as_str() {
        "D" => LeakageSolveKind::D,
        "Df" => LeakageSolveKind::Df,
        "D+QU" => LeakageSolveKind::DQu,
        "Df+QU" => LeakageSolveKind::DfQu,
        "D+X" => LeakageSolveKind::DX,
        "Df+X" => LeakageSolveKind::DfX,
        "Dgen" => LeakageSolveKind::DGen,
        "Dfgen" => LeakageSolveKind::DfGen,
        "Dgen+QU" => LeakageSolveKind::DGenQu,
        "Dfgen+QU" => LeakageSolveKind::DfGenQu,
        "Dgen+X" => LeakageSolveKind::DGenX,
        "Dfgen+X" => LeakageSolveKind::DfGenX,
        "Xf" => LeakageSolveKind::Xf,
        "Xj" => LeakageSolveKind::Xj,
        "PosAng" => LeakageSolveKind::PosAng,
        "Xparang+QU" => LeakageSolveKind::Xparang,
        "Xfparang+QU" => LeakageSolveKind::Xfparang,
        "XY+QU" if gain => LeakageSolveKind::XY,
        "XYf+QU" if gain => LeakageSolveKind::XYf,
        "X" => {
            return Err(TaskFailure::execution(
                "Can't do selfGatherAndSolve for X Mueller",
            ));
        }
        "Dlls" => LeakageSolveKind::Dlls,
        "Dflls" => LeakageSolveKind::Dflls,
        _ => {
            return Err(TaskFailure::validation(format!(
                "poltype={poltype:?} is under implementation in #1522"
            ))
            .at("poltype"));
        }
    };
    let (model_source, stokes) = model(task, vis, true)?;
    let interval = selector_text(task.value("solint")).unwrap_or_default();
    let (time, frequency) = interval.split_once(',').unwrap_or((&interval, ""));
    Ok(LeakageSolveRequest {
        output: PathBuf::from(task.required_text("caltable")?),
        selection: selection(task)?,
        kind,
        model_source,
        stokes,
        priors: chain(task, Chain::Solve)?,
        parang_terms: usize::from(!gain || task.flag("parang")?),
        correlation_dependent_flags: gain && task.flag("corrdepflags")?,
        solve_mode: if gain {
            calmode(task)?
        } else {
            GainSolveMode::AmplitudePhase
        },
        normalization: None,
        interval: parse_time_solint(time)?,
        frequency_interval: parse_frequency_solint(frequency)?,
        combine: combine(task)?,
        preaverage_seconds: task.number("preavg")?,
        min_baselines: task.integer("minblperant")?.max(0) as usize,
        min_snr: task.number("minsnr")? as f32,
        refant: bandpass_refant(&task.required_text("refant")?),
        append: task.flag("append")?,
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
        options: casa_calibration::FluxScaleOptions {
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
        "amp" => GencalType::Amp,
        "ph" => GencalType::Ph,
        "sbd" => GencalType::Sbd,
        "mbd" => GencalType::Mbd,
        "antpos" => GencalType::Antpos,
        "antposvla" => GencalType::AntposVla,
        "gc" => GencalType::Gc,
        "gceff" => GencalType::Gceff,
        "eff" => GencalType::Eff,
        "tsys" => GencalType::Tsys,
        "opac" => GencalType::Opac,
        "swpow" => GencalType::Swpow,
        "evlagain" => GencalType::Evlagain,
        "swp/rq" => GencalType::SwpOverRq,
        "swpwts" => GencalType::Swpwts,
        "rq" => GencalType::Rq,
        "tecim" => GencalType::Tecim,
        "eop" => GencalType::Eop,
        other => return Err(not_yet("caltype", other)),
    };
    let antenna = task.text("antenna")?.unwrap_or_default();
    let infile = if caltype.is_gain_curve()
        || matches!(
            caltype,
            GencalType::Antpos | GencalType::Tecim | GencalType::Eop
        ) {
        task.text("infile")?
            .filter(|path| !path.is_empty() && !(caltype == GencalType::Antpos && path == "None"))
            .map(PathBuf::from)
    } else {
        None
    };
    if caltype == GencalType::Tecim && !infile.as_ref().is_some_and(|path| path.exists()) {
        return Err(
            TaskFailure::validation("An existing tec map must be specified in infile").at("infile"),
        );
    }
    Ok(GencalTaskRequest {
        prior: GencalRequest {
            measurement_set: task.path("vis")?,
            output_table: PathBuf::from(task.required_text("caltable")?),
            caltype,
            antenna,
            spw: task.text("spw")?.unwrap_or_default(),
            parameter: task.numbers("parameter")?,
            gaincurve_table: task.text("gaincurve_table")?.map(PathBuf::from),
        },
        polarization: task.text("pol")?.unwrap_or_default(),
        gaincurve_infile: infile.clone().filter(|_| caltype.is_gain_curve()),
        antpos_infile: infile.clone().filter(|_| caltype == GencalType::Antpos),
        tec_image: infile.clone().filter(|_| caltype == GencalType::Tecim),
        eop_infile: infile.filter(|_| caltype == GencalType::Eop),
        ant_pos_time_limit: (caltype == GencalType::Antpos)
            .then(|| task.integer("ant_pos_time_limit"))
            .transpose()?,
        uniform: (caltype == GencalType::Tsys)
            .then(|| task.flag("uniform"))
            .transpose()?,
    })
}

/// CASA's selection: `field`, `spw` and `intent` always; the others are inactive,
/// and so unset, while `selectdata` is not.
fn selection(task: &TaskInvocation<'_>) -> Result<MsSelection, TaskFailure> {
    let text = |name: &str| {
        task.text(name)
            .map(|value| value.filter(|text| !text.is_empty()))
    };
    let mut selection = MsSelection {
        selectdata: true,
        field: text("field")?,
        spw: text("spw")?,
        antenna: text("antenna")?,
        scan: text("scan")?,
        observation: selector_text(task.value("observation")),
        array: text("array")?,
        timerange: text("timerange")?,
        msselect: text("msselect")?,
        ..MsSelection::default()
    };
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
    if matches!(task.surface(), "gaincal" | "bandpass" | "polcal") {
        // The public task always adds this predicate, including when the
        // optional selectors are disabled.
        selection.msselect = Some(match selection.msselect {
            Some(predicate) => format!("({predicate}) && ANTENNA1!=ANTENNA2"),
            None => "ANTENNA1!=ANTENNA2".into(),
        });
    }
    Ok(selection)
}

pub(super) fn selector_text(value: Option<&ParameterValue>) -> Option<String> {
    let text = match value? {
        ParameterValue::String(text) => text.clone(),
        ParameterValue::NumpyScalar { value, .. } | ParameterValue::ScalarArray(value) => {
            return selector_text(Some(value));
        }
        other => python_repr(other),
    };
    (!text.is_empty()).then_some(text)
}

pub(super) fn uv_selector_text(value: Option<&ParameterValue>) -> Option<String> {
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
            Some(text) => parse_interp_value(text)
                .unwrap_or(casa_calibration::ApplyInterpolationMode::Invalid),
            None => casa_calibration::ApplyInterpolationMode::Linear,
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

/// Parse the point-source Stokes vector, with omitted Stokes set to zero.
/// Gaincal and channelized bandpass resolve an empty vector from the MS.
fn model(
    task: &TaskInvocation<'_>,
    measurement_set: &Path,
    use_stored_models: bool,
) -> Result<(GainSolveModelSource, [f32; 4]), TaskFailure> {
    let smodel = task.numbers("smodel")?;
    match smodel.as_slice() {
        [] if use_stored_models => {
            std::fs::metadata(measurement_set).map_err(|error| {
                TaskFailure::validation(format!(
                    "open MeasurementSet {}: {error}",
                    measurement_set.display()
                ))
                .at("vis")
            })?;
            Ok((
                GainSolveModelSource::FromMeasurementSet,
                [1.0, 0.0, 0.0, 0.0],
            ))
        }
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
        &[_, ref polarized @ ..] if polarized.len() < 4 => {
            if !use_stored_models && polarized.iter().any(|stokes| *stokes != 0.0) {
                return Err(not_yet("smodel", &format!("{smodel:?}")));
            }
            let stokes = casa_calibration::point_model_stokes(&smodel).ok_or_else(|| {
                TaskFailure::validation("smodel needs finite Stokes values representable as floats")
                    .at("smodel")
            })?;
            Ok((GainSolveModelSource::PointSource, stokes))
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
        "K" => Ok(GainType::K),
        "KCROSS" => Ok(GainType::KCross),
        "GSPLINE" => Ok(GainType::GSpline),
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

fn robust_gain_solve(
    task: &TaskInvocation<'_>,
) -> Result<casa_calibration::GainSolveRobustness, TaskFailure> {
    let mode = task.required_text("solmode")?;
    let thresholds = if matches!(mode.as_str(), "R" | "L1R") {
        task.numbers("rmsthresh")?
    } else {
        Vec::new()
    };
    casa_calibration::GainSolveRobustness::from_casa(&mode, thresholds)
        .ok_or_else(|| unknown("solmode", &mode, "'', 'L1', 'R' or 'L1R'"))
}

/// CASA's `solint`, read as `SolvableVisCal::setSolve` reads it: empty
/// text, or text that contains `inf`, in any case, is one solution per
/// scan; text that contains `int`, one per integration; anything else is a
/// time, in seconds when it has no unit. A time of zero is CASA's `inf`,
/// and a negative one its `int`. G and T have a single gain per spectral
/// window, so CASA ignores the frequency interval after a comma.
fn solint(task: &TaskInvocation<'_>) -> Result<GainSolveInterval, TaskFailure> {
    let supplied = selector_text(task.value("solint")).unwrap_or_default();
    let time = supplied
        .split_once(',')
        .map_or(supplied.as_str(), |(time, _)| time);
    parse_time_solint(time)
}

fn parse_time_solint(text: &str) -> Result<GainSolveInterval, TaskFailure> {
    GainSolveInterval::from_casa(text)
        .ok_or_else(|| unknown("solint", text, "'inf', 'int' or a time such as '60s'"))
}

fn parse_frequency_solint(text: &str) -> Result<Option<BandpassFrequencyInterval>, TaskFailure> {
    BandpassFrequencyInterval::from_casa(text).ok_or_else(|| {
        unknown(
            "solint",
            text,
            "'none', a channel count, or a bandwidth such as '3MHz'",
        )
    })
}

/// The axes CASA's `combine` names, found as `SolvableVisCal` finds them:
/// by the axis name anywhere in the text, in any case (`'Scan,Field'` and
/// `'scan field'` both combine scans and fields).
fn combine(task: &TaskInvocation<'_>) -> Result<GainSolveCombine, TaskFailure> {
    Ok(GainSolveCombine::from_casa(&task.required_text("combine")?))
}

fn bandpass_refant(text: &str) -> RefAntSelector {
    if text.trim().is_empty() {
        RefAntSelector::Unspecified
    } else {
        RefAntSelector::Expression {
            selection: text.trim().to_string(),
            strict: false,
        }
    }
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
