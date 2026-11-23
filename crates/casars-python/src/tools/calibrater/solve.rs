// SPDX-License-Identifier: LGPL-3.0-or-later
//! Translate the direct calibrater solve controls to the shared Rust requests.

use std::path::PathBuf;

use casa_calibration::{
    BandpassFrequencyInterval, BandpassSolveCombine, BandpassSolveCuts, BandpassSolveRequest,
    BandpassType, CalibrationSolveRequest, FringeControls, FringeCorrelation, GainNormalization,
    GainSolveCombine, GainSolveInterval, GainSolveMode, GainSolveModelSource, GainSolveRequest,
    GainSolveRobustness, GainType, RefAntSelector, SingleDishSolveControls,
};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::arguments::{listing_selector, number, strict_bool, text};

struct StandardSolver {
    output_table: PathBuf,
    solve_interval: GainSolveInterval,
    preaverage_seconds: f64,
    solve_mode: GainSolveMode,
    refant: RefAntSelector,
    min_baselines_per_antenna: usize,
    normalize: bool,
    normalization: GainNormalization,
    min_snr: f32,
    combine: GainSolveCombine,
    robustness: GainSolveRobustness,
}

impl StandardSolver {
    fn gain(self, gain_type: GainType) -> GainSolveRequest {
        GainSolveRequest {
            selection: Default::default(),
            output_table: self.output_table,
            gain_type,
            solve_mode: self.solve_mode,
            solve_interval: self.solve_interval,
            preaverage_seconds: self.preaverage_seconds,
            correlation_dependent_flags: false,
            combine: self.combine,
            refant: self.refant,
            prior_calibration_tables: Vec::new(),
            parang: false,
            model_source: GainSolveModelSource::FromMeasurementSet,
            normalize_average_amplitude: self.normalize,
            normalization_type: self.normalization,
            robustness: self.robustness,
            min_snr: self.min_snr,
            min_baselines_per_antenna: self.min_baselines_per_antenna,
            smodel: [1., 0., 0., 0.],
        }
    }
}

fn robust_mode(value: &str) -> &'static str {
    match (value.contains("L1"), value.contains('R')) {
        (true, true) => "L1R",
        (true, false) => "L1",
        (false, true) => "R",
        (false, false) => "",
    }
}

pub(super) fn setup_error() -> PyErr {
    PyRuntimeError::new_err("Error in Calibrater::setsolve.")
}

pub(super) fn refant(value: Option<&Bound<'_, PyAny>>, mode: &str) -> PyResult<RefAntSelector> {
    let selection = listing_selector(value)?;
    if selection.is_empty() {
        Ok(RefAntSelector::Unspecified)
    } else {
        Ok(RefAntSelector::Expression {
            selection,
            strict: mode.eq_ignore_ascii_case("strict"),
        })
    }
}

fn bandpass_frequency(value: Option<&str>) -> PyResult<Option<BandpassFrequencyInterval>> {
    BandpassFrequencyInterval::from_casa(value.unwrap_or_default()).ok_or_else(setup_error)
}

fn setsolve_bool(value: Option<&Bound<'_, PyAny>>, position: u8, default: bool) -> PyResult<bool> {
    value
        .map(|value| strict_bool(value, "setsolve", position))
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn setsolve_text(value: Option<&Bound<'_, PyAny>>, name: &str, default: &str) -> PyResult<String> {
    text(value.cloned(), name, default)
}

fn setsolve_mode(value: &str, phase_only: bool) -> (String, GainSolveMode, bool) {
    let apmode = if phase_only {
        "P".into()
    } else {
        value.to_ascii_uppercase()
    };
    let (mode, invalid) = match apmode.as_str() {
        "P" => (GainSolveMode::Phase, false),
        "A" => (GainSolveMode::Amplitude, false),
        "AP" | "" => (GainSolveMode::AmplitudePhase, false),
        _ => (GainSolveMode::AmplitudePhase, true),
    };
    (apmode, mode, invalid)
}

fn single_dish_controls(
    family: &str,
    fraction: Option<&Bound<'_, PyAny>>,
    numedge: Option<&Bound<'_, PyAny>>,
    radius: Option<&Bound<'_, PyAny>>,
    smooth: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<SingleDishSolveControls>> {
    if !matches!(
        family,
        "SDSKY_PS" | "SDSKY_OTF" | "SDSKY_RASTER" | "SDGAIN_OTFD"
    ) {
        return Ok(None);
    }
    let radius = radius
        .map(|value| {
            value.extract::<String>().map_err(|_| {
                pyo3::exceptions::PyTypeError::new_err("argument _radius must be a string")
            })
        })
        .transpose()?
        .unwrap_or_default();
    Ok(Some(SingleDishSolveControls {
        fraction: number(fraction.cloned(), "setsolve", 20, 0.1)? as f32,
        numedge: setsolve_long(numedge, 21, -1)?,
        radius,
        smooth: setsolve_bool(smooth, 23, true)?,
        invalid_apmode: false,
    }))
}

fn setsolve_integer(
    value: Option<&Bound<'_, PyAny>>,
    position: u8,
    default: usize,
) -> PyResult<usize> {
    setsolve_long(value, position, default as i64).map(|value| usize::try_from(value).unwrap_or(0))
}

fn setsolve_long(value: Option<&Bound<'_, PyAny>>, position: u8, default: i64) -> PyResult<i64> {
    use pyo3::exceptions::PyTypeError;
    let Some(value) = value else {
        return Ok(default);
    };
    value.extract::<i64>().map_err(|_| {
        PyTypeError::new_err(format!(
            "in method 'calibrater_setsolve', argument {position} of type 'long'"
        ))
    })
}

fn fringe_window(value: Option<&Bound<'_, PyAny>>) -> PyResult<[f64; 2]> {
    let window = value
        .map(|value| value.extract::<crate::convert::DoubleList>())
        .transpose()?
        .map_or_else(Vec::new, |values| values.0);
    Ok(match window.as_slice() {
        [lower, upper, ..] => [*lower, *upper],
        _ => [-1e6, 1e6],
    })
}

impl StandardSolver {
    fn bandpass(
        self,
        frequency_interval: Option<casa_calibration::BandpassFrequencyInterval>,
        fill_gaps: usize,
    ) -> BandpassSolveRequest {
        BandpassSolveRequest {
            selection: Default::default(),
            output_table: self.output_table,
            refant: self.refant,
            prior_calibration_tables: Vec::new(),
            parang: false,
            combine: BandpassSolveCombine {
                scans: self.combine.scans,
                fields: self.combine.fields,
                observations: self.combine.observations,
                spectral_windows: self.combine.spectral_windows,
            },
            band_type: BandpassType::B,
            normalize_average_amplitude: self.normalize,
            amplitude_degree: 0,
            phase_degree: 0,
            model_source: GainSolveModelSource::FromMeasurementSet,
            solve_interval: self.solve_interval,
            frequency_interval,
            correlation_dependent_flags: false,
            smodel: [1., 0., 0., 0.],
            cuts: Some(BandpassSolveCuts {
                min_baselines_per_antenna: self.min_baselines_per_antenna,
                min_snr: self.min_snr,
                fill_gaps,
                ..BandpassSolveCuts::CASA_DEFAULTS
            }),
        }
    }
}

fn standard_request(
    family: &str,
    standard: StandardSolver,
    frequency: Option<&str>,
    fill_gaps: usize,
    fringe: Option<(FringeControls, bool)>,
) -> PyResult<CalibrationSolveRequest> {
    Ok(match family {
        "G" | "G JONES" => CalibrationSolveRequest::Gain(standard.gain(GainType::G)),
        "T" | "T JONES" | "T NOISE" => CalibrationSolveRequest::Gain(standard.gain(GainType::T)),
        "K" | "K JONES" => CalibrationSolveRequest::Gain(standard.gain(GainType::K)),
        "KCROSS" | "KCROSS JONES" => CalibrationSolveRequest::Gain(standard.gain(GainType::KCross)),
        "B" | "B JONES" => CalibrationSolveRequest::Bandpass(
            standard.bandpass(bandpass_frequency(frequency)?, fill_gaps),
        ),
        "M" | "M MUELLER" | "MF" | "MF MUELLER" => {
            let frequency_dependent = family.starts_with("MF");
            CalibrationSolveRequest::Baseline {
                gain: standard.gain(GainType::G),
                frequency_dependent,
                frequency_interval: if frequency_dependent {
                    bandpass_frequency(frequency)?
                } else {
                    None
                },
            }
        }
        "ACCOR" | "ACCOR JONES" => CalibrationSolveRequest::Accor(standard.gain(GainType::G)),
        "FRINGE" | "FRINGE JONES" => {
            let (controls, active_phase) = fringe.expect("parsed fringe controls");
            CalibrationSolveRequest::FringeTool {
                gain: standard.gain(GainType::K),
                controls,
                active_phase,
            }
        }
        _ if casa_calibration::CalibraterLifecycle::recognizes_calibration_type(family) => {
            return crate::tools::not_yet_implemented_in("calibrater", "setsolve", 1528);
        }
        _ => return Err(setup_error()),
    })
}

#[pymethods]
#[allow(clippy::too_many_arguments, unused_variables)]
impl super::PyCalibrater {
    #[pyo3(signature = (r#type = None, t = None, table = None, append = None, preavg = None, phaseonly = None, apmode = None, refant = None, refantmode = None, minblperant = None, solnorm = None, normtype = None, minsnr = None, combine = None, fillgaps = None, cfcache = None, painc = None, fitorder = None, fraction = None, numedge = None, radius = None, smooth = None, zerorates = None, globalsolve = None, niter = None, corrcomb = None, delaywindow = None, ratewindow = None, paramactive = None, concatspws = None, solmode = None, rmsthresh = None), text_signature = "($self, type='MF', t=[], table='', append=False, preavg=-1.0, phaseonly=False, apmode='AP', refant=[], refantmode='flex', minblperant=4, solnorm=False, normtype='mean', minsnr=0.0, combine='', fillgaps=0, cfcache='', painc=360.0, fitorder=0, fraction=0.1, numedge=-1, radius='', smooth=True, zerorates=False, globalsolve=True, niter=100, corrcomb='none', delaywindow=[], ratewindow=[], paramactive=[], concatspws=True, solmode='', rmsthresh=[])")]
    fn setsolve(
        &self,
        py: Python<'_>,
        r#type: Option<&str>,
        t: Option<&Bound<'_, PyAny>>,
        table: Option<&str>,
        append: Option<&Bound<'_, PyAny>>,
        preavg: Option<&Bound<'_, PyAny>>,
        phaseonly: Option<&Bound<'_, PyAny>>,
        apmode: Option<&Bound<'_, PyAny>>,
        refant: Option<&Bound<'_, PyAny>>,
        refantmode: Option<&Bound<'_, PyAny>>,
        minblperant: Option<&Bound<'_, PyAny>>,
        solnorm: Option<&Bound<'_, PyAny>>,
        normtype: Option<&Bound<'_, PyAny>>,
        minsnr: Option<&Bound<'_, PyAny>>,
        combine: Option<&Bound<'_, PyAny>>,
        fillgaps: Option<&Bound<'_, PyAny>>,
        cfcache: Option<&str>,
        painc: Option<f64>,
        fitorder: Option<&Bound<'_, PyAny>>,
        fraction: Option<&Bound<'_, PyAny>>,
        numedge: Option<&Bound<'_, PyAny>>,
        radius: Option<&Bound<'_, PyAny>>,
        smooth: Option<&Bound<'_, PyAny>>,
        zerorates: Option<&Bound<'_, PyAny>>,
        globalsolve: Option<&Bound<'_, PyAny>>,
        niter: Option<&Bound<'_, PyAny>>,
        corrcomb: Option<&Bound<'_, PyAny>>,
        delaywindow: Option<&Bound<'_, PyAny>>,
        ratewindow: Option<&Bound<'_, PyAny>>,
        paramactive: Option<&Bound<'_, PyAny>>,
        concatspws: Option<&Bound<'_, PyAny>>,
        solmode: Option<&Bound<'_, PyAny>>,
        rmsthresh: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        use crate::tools::{LogLine, emit};
        use casa_calibration::CalibrationOutputMode;

        let append = setsolve_bool(append, 5, false)?;
        let preavg = number(preavg.cloned(), "setsolve", 6, -1.)?;
        let phase_only = setsolve_bool(phaseonly, 7, false)?;
        let apmode = setsolve_text(apmode, "apmode", "AP")?;
        let refant_mode = setsolve_text(refantmode, "refantmode", "flex")?;
        let baselines = setsolve_integer(minblperant, 11, 4)?;
        let normalize = setsolve_bool(solnorm, 12, false)?;
        let normtype = setsolve_text(normtype, "normtype", "mean")?;
        let min_snr = number(minsnr.cloned(), "setsolve", 14, 0.)? as f32;
        let combine = setsolve_text(combine, "combine", "")?;
        let fill_gaps = setsolve_integer(fillgaps, 16, 0)?;
        let solve_mode = setsolve_text(solmode, "solmode", "")?.to_ascii_uppercase();
        if !self.with_lifecycle(py, |state| Ok(state.dataset_path().is_some()))? {
            emit(
                py,
                super::LOGGER,
                &[LogLine::error("Must first open a MeasurementSet.")],
            )?;
            return Ok(false);
        }
        let elapsed = listing_selector(t)?;
        let reference = self::refant(refant, &refant_mode)?;
        let thresholds = rmsthresh
            .map(|value| value.extract::<crate::convert::DoubleList>())
            .transpose()?
            .map(|values| values.0)
            .unwrap_or_default();
        let mode = if append {
            CalibrationOutputMode::Append
        } else {
            CalibrationOutputMode::Replace
        };
        let family = r#type.unwrap_or("MF").to_ascii_uppercase();
        let single_dish = single_dish_controls(&family, fraction, numedge, radius, smooth)?;
        let polarization =
            casa_calibration::CalibraterLifecycle::recognizes_polarization_solve_type(&family);
        let baseline = matches!(family.as_str(), "M" | "M MUELLER" | "MF" | "MF MUELLER");
        let accor = matches!(family.as_str(), "ACCOR" | "ACCOR JONES");
        let fringe = if matches!(family.as_str(), "FRINGE" | "FRINGE JONES") {
            let active = paramactive
                .map(|value| value.extract::<Vec<bool>>())
                .transpose()?
                .unwrap_or_default();
            let [active_phase, delay, rate, dispersion] =
                std::array::from_fn(|index| active.get(index).copied().unwrap_or(false));
            let correlation = setsolve_text(corrcomb, "corrcomb", "none")?;
            let correlation_combination = if correlation.contains("stokes") {
                FringeCorrelation::Stokes
            } else if correlation.contains("parallel") {
                FringeCorrelation::Parallel
            } else {
                FringeCorrelation::Separate
            };
            Some((
                FringeControls {
                    delay_window: fringe_window(delaywindow)?,
                    rate_window: fringe_window(ratewindow)?,
                    global_solve: setsolve_bool(globalsolve, 25, true)?,
                    iterations: usize::try_from(setsolve_long(niter, 26, 100)?)
                        .unwrap_or(usize::MAX),
                    active_parameters: [delay, rate, dispersion],
                    zero_rates: setsolve_bool(zerorates, 24, false)?,
                    correlation_combination,
                    concatenate_windows: setsolve_bool(concatspws, 31, true)?,
                },
                active_phase,
            ))
        } else {
            None
        };
        let table = PathBuf::from(table.unwrap_or(""));
        if phase_only {
            emit(
                py,
                super::LOGGER,
                &[LogLine::warning(concat!(
                    "Please note that the phaseonly parameter will soon be eliminated.\n",
                    "You should begin using apmode='P' for phase-only."
                ))],
            )?;
        }
        self.with_lifecycle(py, move |state| {
            if accor || fringe.is_some() || polarization {
                state
                    .resolve_primary_tool_refant(reference.clone())
                    .map_err(super::runtime_error)?;
            }
            let reference = if baseline {
                state
                    .resolve_primary_tool_refant(reference)
                    .map_err(super::runtime_error)?
            } else {
                reference
            };
            state.reset(false, true).map_err(super::runtime_error)?;
            if family == "EP" || family == "EP JONES" {
                state
                    .set_tool_ep_solve(
                        &table,
                        std::path::Path::new(cfcache.unwrap_or("")),
                        painc.unwrap_or(360.),
                        (&elapsed, preavg),
                        mode,
                        reference,
                    )
                    .map_err(|_| setup_error())?;
                return Ok(true);
            }
            if table.as_os_str().is_empty() {
                return Err(setup_error());
            }
            let (time, frequency) = elapsed
                .split_once(',')
                .map_or((elapsed.as_str(), None), |(time, frequency)| {
                    (time, Some(frequency))
                });
            let (apmode, solve_mode_value, invalid_mode) = setsolve_mode(&apmode, phase_only);
            let normtype = normtype.to_ascii_uppercase();
            let normalization = if normtype.contains("MEAN") {
                GainNormalization::Mean
            } else if normtype.contains("MED") {
                GainNormalization::Median
            } else {
                return Err(setup_error());
            };
            let standard = StandardSolver {
                output_table: table,
                solve_interval: GainSolveInterval::from_casa(time).ok_or_else(setup_error)?,
                preaverage_seconds: f64::from(preavg as f32),
                solve_mode: solve_mode_value,
                refant: reference,
                min_baselines_per_antenna: baselines,
                normalize,
                normalization,
                min_snr,
                combine: GainSolveCombine::from_casa(&combine),
                robustness: GainSolveRobustness::from_casa(robust_mode(&solve_mode), thresholds)
                    .expect("recognized CASA robust mode"),
            };
            if let Some(mut controls) = single_dish {
                controls.invalid_apmode = invalid_mode;
                state
                    .set_tool_single_dish_solve(&family, standard.gain(GainType::G), mode, controls)
                    .map_err(|_| setup_error())?;
                state.record_solve_description(&elapsed, &apmode, &refant_mode, None);
                return Ok(true);
            }
            if polarization {
                state
                    .set_tool_polarization(
                        &family,
                        standard.gain(GainType::G),
                        frequency,
                        append,
                        invalid_mode,
                    )
                    .map_err(|_| setup_error())?;
                state.record_solve_description(&elapsed, &apmode, &refant_mode, None);
                return Ok(true);
            }
            if matches!(
                family.as_str(),
                "TF" | "TF JONES" | "TF NOISE" | "TOPAC" | "TFOPAC"
            ) {
                let channelized = family != "TOPAC";
                let frequency = channelized
                    .then(|| bandpass_frequency(frequency))
                    .transpose()?
                    .flatten();
                let gain = standard.gain(GainType::T);
                let configured = if family.ends_with("OPAC") {
                    state.set_tool_opacity_solve(gain, mode, channelized)
                } else {
                    state.set_tool_tf(gain, mode, frequency, invalid_mode)
                };
                configured.map_err(|_| setup_error())?;
                state.record_solve_description(&elapsed, &apmode, &refant_mode, None);
                return Ok(true);
            }
            let request = standard_request(&family, standard, frequency, fill_gaps, fringe)?;
            state
                .set_tool_solve(
                    request,
                    mode,
                    (!invalid_mode || fringe.is_some()).then_some(solve_mode_value),
                    normalization,
                    preavg,
                )
                .map_err(|_| setup_error())?;
            state.record_solve_description(&elapsed, &apmode, &refant_mode, None);
            Ok(true)
        })
    }

    #[pyo3(signature = (corrdepflags), text_signature = "($self, corrdepflags)")]
    fn setcorrdepflags(&self, py: Python<'_>, corrdepflags: &Bound<'_, PyAny>) -> PyResult<bool> {
        use crate::tools::{LogLine, emit};
        let value = strict_bool(corrdepflags, "setcorrdepflags", 2)?;
        self.with_lifecycle(py, |state| {
            state.set_correlation_dependent_flags(value);
            Ok(())
        })?;
        let value = if value { "True" } else { "False" };
        emit(
            py,
            super::LOGGER,
            &[LogLine::info(format!(
                "Setting correlation dependent flags = {value}"
            ))],
        )?;
        Ok(true)
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn solve(&self, py: Python<'_>) -> PyResult<bool> {
        use crate::tools::{LogLine, emit};
        let result = self.with_lifecycle(py, |state| {
            if state.dataset_path().is_none() {
                return Ok(None);
            }
            state
                .solve_tool()
                .map_err(|_| PyRuntimeError::new_err("Error in Calibrater::solve."))?;
            Ok(Some(true))
        })?;
        if let Some(solved) = result {
            return Ok(solved);
        }
        emit(
            py,
            super::LOGGER,
            &[LogLine::error("Must first open a MeasurementSet.")],
        )?;
        Ok(false)
    }
}
