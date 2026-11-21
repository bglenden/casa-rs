// SPDX-License-Identifier: LGPL-3.0-or-later
//! Public calibrater signatures whose implementations are assigned to open issues.

use std::path::Path;

use casa_calibration::{
    CalibrationOutputMode, CalibrationSolveRequest, GainSolveMode, GainSolveModelSource,
    GainSolveRequest, GainType, RefAntSelector,
};
use pyo3::prelude::*;

use super::{PyCalibrater, runtime_error};
use crate::tools::not_yet_implemented_in;

#[pymethods]
#[allow(
    non_snake_case,
    unused_variables,
    clippy::too_many_arguments,
    clippy::unused_self
)]
impl PyCalibrater {
    #[pyo3(signature = (), text_signature = "($self)")]
    fn activityrec(&self) -> PyResult<Py<PyAny>> {
        not_yet_implemented_in("calibrater", "activityrec", 1528)
    }

    #[pyo3(signature = (applymode = None), text_signature = "($self, applymode='')")]
    fn correct(&self, applymode: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        not_yet_implemented_in("calibrater", "correct", 1528)
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn corrupt(&self) -> PyResult<Py<PyAny>> {
        not_yet_implemented_in("calibrater", "corrupt", 1528)
    }

    #[pyo3(signature = (tablein, reference = None, tableout = None, transfer = None, listfile = None, append = None, refspwmap = None, gainthreshold = None, antenna = None, timerange = None, scan = None, incremental = None, fitorder = None, display = None), text_signature = "($self, tablein, reference=[], tableout='', transfer=[], listfile='', append=False, refspwmap=[-1], gainthreshold=-1.0, antenna='', timerange='', scan='', incremental=False, fitorder=1, display=False)")]
    fn fluxscale(
        &self,
        tablein: &Bound<'_, PyAny>,
        reference: Option<&Bound<'_, PyAny>>,
        tableout: Option<&Bound<'_, PyAny>>,
        transfer: Option<&Bound<'_, PyAny>>,
        listfile: Option<&Bound<'_, PyAny>>,
        append: Option<&Bound<'_, PyAny>>,
        refspwmap: Option<&Bound<'_, PyAny>>,
        gainthreshold: Option<&Bound<'_, PyAny>>,
        antenna: Option<&Bound<'_, PyAny>>,
        timerange: Option<&Bound<'_, PyAny>>,
        scan: Option<&Bound<'_, PyAny>>,
        incremental: Option<&Bound<'_, PyAny>>,
        fitorder: Option<&Bound<'_, PyAny>>,
        display: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        not_yet_implemented_in("calibrater", "fluxscale", 1528)
    }

    #[pyo3(signature = (tablein = None, tableout = None, fields = None), text_signature = "($self, tablein='', tableout='', fields=[])")]
    fn linpolcor(
        &self,
        tablein: Option<&Bound<'_, PyAny>>,
        tableout: Option<&Bound<'_, PyAny>>,
        fields: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        not_yet_implemented_in("calibrater", "linpolcor", 1528)
    }

    #[pyo3(signature = (filein), text_signature = "($self, filein)")]
    fn parsecallibfile(&self, filein: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        not_yet_implemented_in("calibrater", "parsecallibfile", 1528)
    }

    #[pyo3(signature = (posangcor, tablein, tableout = None), text_signature = "($self, posangcor, tablein, tableout='')")]
    fn posangcal(
        &self,
        posangcor: &Bound<'_, PyAny>,
        tablein: &Bound<'_, PyAny>,
        tableout: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        not_yet_implemented_in("calibrater", "posangcal", 1528)
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn returndict(&self) -> PyResult<Py<PyAny>> {
        not_yet_implemented_in("calibrater", "returndict", 1528)
    }

    #[pyo3(signature = (r#type = None, t = None, table = None, field = None, interp = None, select = None, calwt = None, spwmap = None, opacity = None), text_signature = "($self, type='', t=0.0, table='', field=[], interp='linear', select='', calwt=False, spwmap=[-1], opacity=[0.0])")]
    fn setapply(
        &self,
        r#type: Option<&Bound<'_, PyAny>>,
        t: Option<&Bound<'_, PyAny>>,
        table: Option<&Bound<'_, PyAny>>,
        field: Option<&Bound<'_, PyAny>>,
        interp: Option<&Bound<'_, PyAny>>,
        select: Option<&Bound<'_, PyAny>>,
        calwt: Option<&Bound<'_, PyAny>>,
        spwmap: Option<&Bound<'_, PyAny>>,
        opacity: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        not_yet_implemented_in("calibrater", "setapply", 1528)
    }

    #[pyo3(signature = (callib = None), text_signature = "($self, callib={})")]
    fn setcallib(&self, callib: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        not_yet_implemented_in("calibrater", "setcallib", 1528)
    }

    #[pyo3(signature = (corrdepflags), text_signature = "($self, corrdepflags)")]
    fn setcorrdepflags(&self, corrdepflags: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        not_yet_implemented_in("calibrater", "setcorrdepflags", 1528)
    }

    #[pyo3(signature = (modelimage), text_signature = "($self, modelimage)")]
    fn setmodel(&self, py: Python<'_>, modelimage: &str) -> PyResult<bool> {
        self.with_lifecycle(py, |state| {
            state
                .set_image_model(Path::new(modelimage))
                .map_err(runtime_error)?;
            Ok(true)
        })
    }

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
        if r#type.is_some_and(|kind| kind.eq_ignore_ascii_case("EP")) {
            return self.attached(py, |state| {
                state.set_ep_solve(
                    Path::new(table.unwrap_or("")),
                    Path::new(cfcache.unwrap_or("")),
                    painc.unwrap_or(360.),
                )
            });
        }
        if r#type.is_some_and(|kind| kind.eq_ignore_ascii_case("G"))
            && cfcache.is_none()
            && painc.is_none()
            && [
                t,
                preavg,
                phaseonly,
                apmode,
                refant,
                refantmode,
                minblperant,
                solnorm,
                normtype,
                minsnr,
                combine,
                fillgaps,
                fitorder,
                fraction,
                numedge,
                radius,
                smooth,
                zerorates,
                globalsolve,
                niter,
                corrcomb,
                delaywindow,
                ratewindow,
                paramactive,
                concatspws,
                solmode,
                rmsthresh,
            ]
            .into_iter()
            .all(|value| value.is_none())
        {
            let mode = if append
                .map(|value| value.extract::<bool>())
                .transpose()?
                .unwrap_or(false)
            {
                CalibrationOutputMode::Append
            } else {
                CalibrationOutputMode::Replace
            };
            let request = GainSolveRequest {
                output_table: Path::new(table.unwrap_or("")).to_owned(),
                selection: Default::default(),
                gain_type: GainType::G,
                solve_mode: GainSolveMode::AmplitudePhase,
                solve_interval: Default::default(),
                preaverage_seconds: -1.,
                correlation_dependent_flags: false,
                combine: Default::default(),
                refant: RefAntSelector::Unspecified,
                prior_calibration_tables: Vec::new(),
                parang: false,
                model_source: GainSolveModelSource::FromMeasurementSet,
                normalize_average_amplitude: false,
                normalization_type: Default::default(),
                robustness: Default::default(),
                min_snr: 0.,
                min_baselines_per_antenna: 4,
                smodel: [1., 0., 0., 0.],
            };
            return self.attached(py, |state| {
                state.set_solve(CalibrationSolveRequest::Gain(request), mode)
            });
        }
        not_yet_implemented_in("calibrater", "setsolve", 1528)
    }

    #[pyo3(signature = (table = None, append = None, t = None, combine = None, degamp = None, degphase = None, visnorm = None, solnorm = None, maskcenter = None, maskedge = None, refant = None), text_signature = "($self, table='', append=False, t=[], combine='', degamp=3, degphase=3, visnorm=False, solnorm=True, maskcenter=0, maskedge=5.0, refant=[])")]
    fn setsolvebandpoly(
        &self,
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
    ) -> PyResult<Py<PyAny>> {
        not_yet_implemented_in("calibrater", "setsolvebandpoly", 1528)
    }

    #[pyo3(signature = (table = None, append = None, mode = None, splinetime = None, preavg = None, npointaver = None, phasewrap = None, refant = None), text_signature = "($self, table='', append=False, mode='PHAS', splinetime=10800.0, preavg=0.0, npointaver=10, phasewrap=250.0, refant=[])")]
    fn setsolvegainspline(
        &self,
        table: Option<&Bound<'_, PyAny>>,
        append: Option<&Bound<'_, PyAny>>,
        mode: Option<&Bound<'_, PyAny>>,
        splinetime: Option<&Bound<'_, PyAny>>,
        preavg: Option<&Bound<'_, PyAny>>,
        npointaver: Option<&Bound<'_, PyAny>>,
        phasewrap: Option<&Bound<'_, PyAny>>,
        refant: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        not_yet_implemented_in("calibrater", "setsolvegainspline", 1528)
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn solve(&self) -> PyResult<Py<PyAny>> {
        not_yet_implemented_in("calibrater", "solve", 1528)
    }

    #[pyo3(signature = (caltable = None, time = None, spw = None, antenna = None, pol = None, caltype = None, parameter = None, infile = None, uniform = None), text_signature = "($self, caltable='', time='', spw='', antenna='', pol='', caltype='', parameter=[1.0], infile='', uniform=True)")]
    fn specifycal(
        &self,
        caltable: Option<&Bound<'_, PyAny>>,
        time: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        antenna: Option<&Bound<'_, PyAny>>,
        pol: Option<&Bound<'_, PyAny>>,
        caltype: Option<&Bound<'_, PyAny>>,
        parameter: Option<&Bound<'_, PyAny>>,
        infile: Option<&Bound<'_, PyAny>>,
        uniform: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        not_yet_implemented_in("calibrater", "specifycal", 1528)
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn state(&self) -> PyResult<Py<PyAny>> {
        not_yet_implemented_in("calibrater", "state", 1528)
    }

    #[pyo3(signature = (callib = None), text_signature = "($self, callib={})")]
    fn validatecallib(&self, callib: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        not_yet_implemented_in("calibrater", "validatecallib", 1528)
    }
}
