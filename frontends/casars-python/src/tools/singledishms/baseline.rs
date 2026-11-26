// SPDX-License-Identifier: LGPL-3.0-or-later
//! Baseline fitting and application on the attached single-dish MS.

use std::path::Path;
use std::sync::PoisonError;

use casa_calibration::{
    SingleDishBaseline, SingleDishBaselineBasis, SingleDishBaselineDiagnostics,
    SingleDishBaselineMode, SingleDishLineFinder, SingleDishSinusoid,
};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::convert::{IntList, Variant};
use crate::tools::calibrater::selector;

use super::PySingleDishMs;

#[pymethods]
impl PySingleDishMs {
    #[pyo3(signature = (bltable="", datacolumn="data", spw=Variant::Empty, updateweight=false, sigmavalue="stddev", outfile=""), text_signature = "($self, bltable='', datacolumn='data', spw=[], updateweight=False, sigmavalue='stddev', outfile='')")]
    #[allow(clippy::too_many_arguments)]
    fn apply_baseline_table(
        &self,
        py: Python<'_>,
        bltable: &str,
        datacolumn: &str,
        spw: Variant,
        updateweight: bool,
        sigmavalue: &str,
        outfile: &str,
    ) -> PyResult<bool> {
        let mask = selector(&spw).unwrap_or_default();
        self.run_baseline(
            py,
            SingleDishBaseline {
                mode: SingleDishBaselineMode::Apply {
                    table: Path::new(bltable),
                },
                datacolumn,
                mask: &mask,
                output: Some(Path::new(outfile)),
                subtract: true,
                overwrite: true,
                update_weight: updateweight,
                sigma_value: sigmavalue,
                history: None,
            },
        )
    }

    #[pyo3(signature = (datacolumn="data", outfile="", bloutput="", dosubtract=true, spw=Variant::Empty, updateweight=false, sigmavalue="stddev", blfunc="poly", order=5, clip_threshold_sigma=3.0, num_fitting_max=1, linefinding=false, threshold=5.0, avg_limit=4, minwidth=4, edge=None), text_signature = "($self, datacolumn='data', outfile='', bloutput='', dosubtract=True, spw=[], updateweight=False, sigmavalue='stddev', blfunc='poly', order=5, clip_threshold_sigma=3.0, num_fitting_max=1, linefinding=False, threshold=5.0, avg_limit=4, minwidth=4, edge=[0, 0])")]
    #[allow(clippy::too_many_arguments)]
    fn subtract_baseline(
        &self,
        py: Python<'_>,
        datacolumn: &str,
        outfile: &str,
        bloutput: &str,
        dosubtract: bool,
        spw: Variant,
        updateweight: bool,
        sigmavalue: &str,
        blfunc: &str,
        order: i32,
        clip_threshold_sigma: f64,
        num_fitting_max: i32,
        linefinding: bool,
        threshold: f64,
        avg_limit: i32,
        minwidth: i32,
        edge: Option<IntList>,
    ) -> PyResult<bool> {
        let order = usize::try_from(order)
            .map_err(|_| PyRuntimeError::new_err("order must be positive or zero."))?;
        self.run_fit(
            py,
            ToolOutput {
                datacolumn,
                outfile,
                bloutput,
                dosubtract,
                spw,
                updateweight,
                sigmavalue,
            },
            ToolFit::Fixed {
                basis: SingleDishBaselineBasis::Polynomial {
                    order,
                    chebyshev: blfunc.eq_ignore_ascii_case("chebyshev"),
                },
                clip_sigma: clip_threshold_sigma,
                num_fitting_max,
                line_finder: line_finder(
                    linefinding,
                    threshold,
                    avg_limit,
                    minwidth,
                    edge.as_ref(),
                ),
            },
        )
    }

    #[pyo3(signature = (datacolumn="data", outfile="", bloutput="", dosubtract=true, spw=Variant::Empty, updateweight=false, sigmavalue="stddev", npiece=5, clip_threshold_sigma=3.0, num_fitting_max=1, linefinding=false, threshold=5.0, avg_limit=4, minwidth=4, edge=None), text_signature = "($self, datacolumn='data', outfile='', bloutput='', dosubtract=True, spw=[], updateweight=False, sigmavalue='stddev', npiece=5, clip_threshold_sigma=3.0, num_fitting_max=1, linefinding=False, threshold=5.0, avg_limit=4, minwidth=4, edge=[0, 0])")]
    #[allow(clippy::too_many_arguments)]
    fn subtract_baseline_cspline(
        &self,
        py: Python<'_>,
        datacolumn: &str,
        outfile: &str,
        bloutput: &str,
        dosubtract: bool,
        spw: Variant,
        updateweight: bool,
        sigmavalue: &str,
        npiece: i32,
        clip_threshold_sigma: f64,
        num_fitting_max: i32,
        linefinding: bool,
        threshold: f64,
        avg_limit: i32,
        minwidth: i32,
        edge: Option<IntList>,
    ) -> PyResult<bool> {
        let pieces = usize::try_from(npiece)
            .ok()
            .filter(|&pieces| pieces != 0)
            .ok_or_else(|| PyRuntimeError::new_err("npiece must be positive."))?;
        self.run_fit(
            py,
            ToolOutput {
                datacolumn,
                outfile,
                bloutput,
                dosubtract,
                spw,
                updateweight,
                sigmavalue,
            },
            ToolFit::Fixed {
                basis: SingleDishBaselineBasis::CubicSpline { pieces },
                clip_sigma: clip_threshold_sigma,
                num_fitting_max,
                line_finder: line_finder(
                    linefinding,
                    threshold,
                    avg_limit,
                    minwidth,
                    edge.as_ref(),
                ),
            },
        )
    }

    #[pyo3(signature = (datacolumn="data", outfile="", bloutput="", dosubtract=true, spw=Variant::Empty, updateweight=false, sigmavalue="stddev", addwn="0", rejwn="", applyfft=false, fftmethod="fft", fftthresh=Variant::Empty, clip_threshold_sigma=3.0, num_fitting_max=1, linefinding=false, threshold=5.0, avg_limit=4, minwidth=4, edge=None), text_signature = "($self, datacolumn='data', outfile='', bloutput='', dosubtract=True, spw=[], updateweight=False, sigmavalue='stddev', addwn='0', rejwn='', applyfft=False, fftmethod='fft', fftthresh=[], clip_threshold_sigma=3.0, num_fitting_max=1, linefinding=False, threshold=5.0, avg_limit=4, minwidth=4, edge=[0, 0])")]
    #[allow(clippy::too_many_arguments)]
    fn subtract_baseline_sinusoid(
        &self,
        py: Python<'_>,
        datacolumn: &str,
        outfile: &str,
        bloutput: &str,
        dosubtract: bool,
        spw: Variant,
        updateweight: bool,
        sigmavalue: &str,
        addwn: &str,
        rejwn: &str,
        applyfft: bool,
        fftmethod: &str,
        fftthresh: Variant,
        clip_threshold_sigma: f64,
        num_fitting_max: i32,
        linefinding: bool,
        threshold: f64,
        avg_limit: i32,
        minwidth: i32,
        edge: Option<IntList>,
    ) -> PyResult<bool> {
        let fftthresh = selector(&fftthresh).unwrap_or_default();
        let sinusoid = SingleDishSinusoid::from_tool(addwn, rejwn, applyfft, fftmethod, &fftthresh)
            .map_err(PyRuntimeError::new_err)?;
        self.run_fit(
            py,
            ToolOutput {
                datacolumn,
                outfile,
                bloutput,
                dosubtract,
                spw,
                updateweight,
                sigmavalue,
            },
            ToolFit::Fixed {
                basis: SingleDishBaselineBasis::Sinusoid(sinusoid),
                clip_sigma: clip_threshold_sigma,
                num_fitting_max,
                line_finder: line_finder(
                    linefinding,
                    threshold,
                    avg_limit,
                    minwidth,
                    edge.as_ref(),
                ),
            },
        )
    }

    #[pyo3(signature = (datacolumn="data", outfile="", bloutput="", dosubtract=true, spw=Variant::Empty, updateweight=false, sigmavalue="stddev", blparam="", verbose=false), text_signature = "($self, datacolumn='data', outfile='', bloutput='', dosubtract=True, spw=[], updateweight=False, sigmavalue='stddev', blparam='', verbose=False)")]
    #[allow(clippy::too_many_arguments)]
    fn subtract_baseline_variable(
        &self,
        py: Python<'_>,
        datacolumn: &str,
        outfile: &str,
        bloutput: &str,
        dosubtract: bool,
        spw: Variant,
        updateweight: bool,
        sigmavalue: &str,
        blparam: &str,
        verbose: bool,
    ) -> PyResult<bool> {
        self.run_fit(
            py,
            ToolOutput {
                datacolumn,
                outfile,
                bloutput,
                dosubtract,
                spw,
                updateweight,
                sigmavalue,
            },
            ToolFit::Variable {
                parameters: Path::new(blparam),
                verbose,
            },
        )
    }
}

struct ToolOutput<'a> {
    datacolumn: &'a str,
    outfile: &'a str,
    bloutput: &'a str,
    dosubtract: bool,
    spw: Variant,
    updateweight: bool,
    sigmavalue: &'a str,
}

enum ToolFit<'a> {
    Fixed {
        basis: SingleDishBaselineBasis,
        clip_sigma: f64,
        num_fitting_max: i32,
        line_finder: Option<SingleDishLineFinder>,
    },
    Variable {
        parameters: &'a Path,
        verbose: bool,
    },
}

fn diagnostic_path(path: &str) -> Option<&Path> {
    (!path.is_empty()).then(|| Path::new(path))
}

fn line_finder(
    enabled: bool,
    threshold: f64,
    avg_limit: i32,
    minwidth: i32,
    edge: Option<&IntList>,
) -> Option<SingleDishLineFinder> {
    enabled.then(|| {
        SingleDishLineFinder::new(
            threshold,
            i64::from(avg_limit),
            i64::from(minwidth),
            edge.map_or(&[], |edge| edge.0.as_slice()),
        )
    })
}

impl PySingleDishMs {
    fn run_fit(&self, py: Python<'_>, output: ToolOutput<'_>, fit: ToolFit<'_>) -> PyResult<bool> {
        let ToolOutput {
            datacolumn,
            outfile,
            bloutput,
            dosubtract,
            spw,
            updateweight,
            sigmavalue,
        } = output;
        let paths = bloutput.splitn(3, ',').collect::<Vec<_>>();
        let (csv, text, table) = match paths.as_slice() {
            [""] => (None, None, None),
            [csv, text, table] => (
                diagnostic_path(csv),
                diagnostic_path(text),
                diagnostic_path(table),
            ),
            _ => {
                return Err(PyRuntimeError::new_err(
                    "bloutput must specify CSV, text and table paths.",
                ));
            }
        };
        let diagnostics = SingleDishBaselineDiagnostics {
            csv,
            text,
            heading: None,
        };
        let mode = match fit {
            ToolFit::Fixed {
                basis,
                clip_sigma,
                num_fitting_max,
                line_finder,
            } => {
                let max_fits = usize::try_from(num_fitting_max)
                    .map_err(|_| PyRuntimeError::new_err("Number of fits must be positive."))?;
                SingleDishBaselineMode::Fit {
                    basis,
                    clip_sigma,
                    max_fits,
                    line_finder,
                    table,
                    diagnostics,
                }
            }
            ToolFit::Variable {
                parameters,
                verbose,
            } => SingleDishBaselineMode::Variable {
                parameters,
                verbose,
                table,
                diagnostics,
            },
        };
        let mask = selector(&spw).unwrap_or_default();
        self.run_baseline(
            py,
            SingleDishBaseline {
                mode,
                datacolumn,
                mask: &mask,
                output: (!outfile.is_empty() || dosubtract).then(|| Path::new(outfile)),
                subtract: dosubtract,
                overwrite: true,
                update_weight: updateweight,
                sigma_value: sigmavalue,
                history: None,
            },
        )
    }

    fn run_baseline(&self, py: Python<'_>, request: SingleDishBaseline<'_>) -> PyResult<bool> {
        py.detach(|| {
            self.session
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .baseline(request)
                .map(|_| true)
        })
        .map_err(PyRuntimeError::new_err)
    }
}
