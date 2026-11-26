// SPDX-License-Identifier: LGPL-3.0-or-later
//! Specialized BPOLY and GSPLINE configuration for the calibrater tool.

use std::path::PathBuf;

use casa_calibration::{
    BandpassFrequencyInterval, BandpassSolveCombine, BandpassSolveCuts, BandpassSolveRequest,
    BandpassType, CalibraterLifecycle, CalibrationOutputMode, CalibrationSolveRequest,
    GainSolveCombine, GainSolveInterval, GainSolveMode, GainSolveModelSource, GainSolveRequest,
    GainSplineControls, GainType, RefAntSelector,
};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

use super::arguments::{listing_selector, number, strict_bool, text};
use super::solve::{refant, setup_error};
use super::{LOGGER, PyCalibrater};
use crate::tools::{LogLine, emit};

fn output_mode(append: Option<&Bound<'_, PyAny>>, method: &str) -> PyResult<CalibrationOutputMode> {
    let append = append
        .map(|value| strict_bool(value, method, 3))
        .transpose()?
        .unwrap_or(false);
    Ok(if append {
        CalibrationOutputMode::Append
    } else {
        CalibrationOutputMode::Replace
    })
}

fn integer(
    value: Option<&Bound<'_, PyAny>>,
    method: &str,
    position: u8,
    default: i64,
) -> PyResult<i64> {
    let Some(value) = value else {
        return Ok(default);
    };
    value.extract().map_err(|_| {
        PyTypeError::new_err(format!(
            "in method 'calibrater_{method}', argument {position} of type 'long'"
        ))
    })
}

impl PyCalibrater {
    fn configure_polynomial(
        &self,
        py: Python<'_>,
        reference: RefAntSelector,
        configure: impl FnOnce(&mut CalibraterLifecycle, RefAntSelector) -> PyResult<()> + Send,
    ) -> PyResult<bool> {
        let configured = self.with_lifecycle(py, |state| {
            if state.dataset_path().is_none() {
                return Ok(false);
            }
            let reference = state
                .resolve_primary_tool_refant(reference)
                .map_err(super::runtime_error)?;
            state.reset(false, true).map_err(super::runtime_error)?;
            configure(state, reference)?;
            Ok(true)
        })?;
        if !configured {
            emit(
                py,
                LOGGER,
                &[LogLine::error("Must first open a MeasurementSet.")],
            )?;
        }
        Ok(configured)
    }
}

#[pymethods]
#[allow(clippy::too_many_arguments)]
impl PyCalibrater {
    #[pyo3(signature = (table = None, append = None, t = None, combine = None, degamp = None, degphase = None, visnorm = None, solnorm = None, maskcenter = None, maskedge = None, refant = None), text_signature = "($self, table='', append=False, t=[], combine='', degamp=3, degphase=3, visnorm=False, solnorm=True, maskcenter=0, maskedge=5.0, refant=[])")]
    fn setsolvebandpoly(
        &self,
        py: Python<'_>,
        table: Option<&Bound<'_, PyAny>>,
        append: Option<&Bound<'_, PyAny>>,
        t: Option<&Bound<'_, PyAny>>,
        combine: Option<&Bound<'_, PyAny>>,
        degamp: Option<&Bound<'_, PyAny>>,
        degphase: Option<&Bound<'_, PyAny>>,
        visnorm: Option<&Bound<'_, PyAny>>,
        solnorm: Option<&Bound<'_, PyAny>>,
        maskcenter: Option<&Bound<'_, PyAny>>,
        maskedge: Option<&Bound<'_, PyAny>>,
        refant: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let method = "setsolvebandpoly";
        let append = output_mode(append, method)?;
        let table = PathBuf::from(text(table.cloned(), "table", "")?);
        let elapsed = listing_selector(t)?;
        let combine = GainSolveCombine::from_casa(&text(combine.cloned(), "combine", "")?);
        let degrees = [
            integer(degamp, method, 6, 3)?,
            integer(degphase, method, 7, 3)?,
        ];
        let visnorm = visnorm
            .map(|value| strict_bool(value, method, 8))
            .transpose()?
            .unwrap_or(false);
        let solnorm = solnorm
            .map(|value| strict_bool(value, method, 9))
            .transpose()?
            .unwrap_or(true);
        let maskcenter = integer(maskcenter, method, 10, 0)?.max(0) as usize;
        let maskedge = number(maskedge.cloned(), method, 11, 5.)? as f32;
        let reference = self::refant(refant, "flex")?;
        let display_reference = reference.clone();
        self.configure_polynomial(py, reference, move |state, reference| {
            if table.as_os_str().is_empty() || degrees.iter().any(|&degree| degree < -1) {
                return Err(setup_error());
            }
            let (time, frequency) = elapsed.split_once(',').unwrap_or((&elapsed, ""));
            let request = CalibrationSolveRequest::Bandpass(BandpassSolveRequest {
                selection: Default::default(),
                output_table: table,
                refant: reference,
                prior_calibration_tables: Vec::new(),
                parang: false,
                combine: BandpassSolveCombine {
                    scans: combine.scans,
                    fields: combine.fields,
                    observations: combine.observations,
                    spectral_windows: combine.spectral_windows,
                },
                band_type: BandpassType::BPoly,
                normalize_average_amplitude: solnorm,
                amplitude_degree: degrees[0].max(0) as usize,
                phase_degree: degrees[1].max(0) as usize,
                model_source: GainSolveModelSource::FromMeasurementSet,
                solve_interval: GainSolveInterval::from_casa(time).ok_or_else(setup_error)?,
                frequency_interval: BandpassFrequencyInterval::from_casa(frequency)
                    .ok_or_else(setup_error)?,
                correlation_dependent_flags: false,
                smodel: [1., 0., 0., 0.],
                cuts: Some(BandpassSolveCuts {
                    mask_center_channels: maskcenter,
                    mask_edge_percent: maskedge,
                    normalize_visibilities: visnorm,
                    omit_polynomial: degrees.map(|degree| degree == -1),
                    ..BandpassSolveCuts::CASA_DEFAULTS
                }),
            });
            state
                .set_solve(request, append)
                .map_err(super::runtime_error)?;
            state.record_solve_description(&elapsed, "", "flex", Some(&display_reference));
            Ok(())
        })
    }

    #[pyo3(signature = (table = None, append = None, mode = None, splinetime = None, preavg = None, npointaver = None, phasewrap = None, refant = None), text_signature = "($self, table='', append=False, mode='PHAS', splinetime=10800.0, preavg=0.0, npointaver=10, phasewrap=250.0, refant=[])")]
    fn setsolvegainspline(
        &self,
        py: Python<'_>,
        table: Option<&Bound<'_, PyAny>>,
        append: Option<&Bound<'_, PyAny>>,
        mode: Option<&Bound<'_, PyAny>>,
        splinetime: Option<&Bound<'_, PyAny>>,
        preavg: Option<&Bound<'_, PyAny>>,
        npointaver: Option<&Bound<'_, PyAny>>,
        phasewrap: Option<&Bound<'_, PyAny>>,
        refant: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let method = "setsolvegainspline";
        let append = output_mode(append, method)?;
        let table = PathBuf::from(text(table.cloned(), "table", "")?);
        let mode = text(mode.cloned(), "mode", "PHAS")?.to_ascii_uppercase();
        let preavg = number(preavg.cloned(), method, 6, 0.)?;
        let controls = GainSplineControls {
            splinetime: number(splinetime.cloned(), method, 5, 10800.)?,
            npointaver: integer(npointaver, method, 7, 10)?,
            phasewrap: number(phasewrap.cloned(), method, 8, 250.)?,
        };
        let reference = self::refant(refant, "flex")?;
        let display_reference = reference.clone();
        self.configure_polynomial(py, reference, move |state, reference| {
            if table.as_os_str().is_empty() {
                return Err(setup_error());
            }
            state
                .set_tool_spline(
                    GainSolveRequest {
                        selection: Default::default(),
                        output_table: table,
                        gain_type: GainType::GSpline,
                        solve_mode: GainSolveMode::AmplitudePhase,
                        solve_interval: GainSolveInterval::Infinite,
                        preaverage_seconds: preavg,
                        correlation_dependent_flags: false,
                        combine: Default::default(),
                        refant: reference,
                        prior_calibration_tables: Vec::new(),
                        parang: false,
                        model_source: GainSolveModelSource::FromMeasurementSet,
                        normalize_average_amplitude: false,
                        normalization_type: Default::default(),
                        robustness: Default::default(),
                        min_snr: 0.,
                        min_baselines_per_antenna: 0,
                        smodel: [1., 0., 0., 0.],
                    },
                    controls,
                    append,
                    mode.clone(),
                )
                .map_err(super::runtime_error)?;
            state.record_solve_description("inf", &mode, "flex", Some(&display_reference));
            Ok(())
        })
    }
}
