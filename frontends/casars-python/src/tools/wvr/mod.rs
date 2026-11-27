// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's ALMA water-vapour radiometer calibration tool.

pub(crate) mod surface;

use std::path::{Path, PathBuf};

use casa_calibration::wvr::{
    WvrError, WvrRequest, WvrSelectors, execute_wvr_with_log, resolve_wvr_selectors,
};
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;

use super::{LogLine, emit};

#[pyclass(name = "wvr", module = "casars.tools", frozen)]
pub(crate) struct PyWvr;

#[pymethods]
impl PyWvr {
    #[new]
    fn new() -> Self {
        Self
    }

    #[pyo3(signature = (vis = "", output = "", toffset = 0.0, nsol = 1, segsource = false, reverse = false, reversespw = None, disperse = false, cont = false, wvrflag = "", sourceflag = None, statfield = None, statsource = None, tie = None, smooth = 1, scale = 1.0, maxdistm = 500.0, minnumants = 2, mingoodfrac = 0.8, usefieldtab = false, spw = None, wvrspw = None, refant = "", offsets = "", rseed = 0, logfile = "wvrgcal.log"), text_signature = "($self, vis='', output='', toffset=0.0, nsol=1, segsource=False, reverse=False, reversespw=[], disperse=False, cont=False, wvrflag='', sourceflag=[], statfield=[], statsource=[], tie=[], smooth=1, scale=1.0, maxdistm=500.0, minnumants=2, mingoodfrac=0.8, usefieldtab=False, spw=[], wvrspw=[], refant='', offsets='', rseed=0, logfile='wvrgcal.log')")]
    #[allow(clippy::too_many_arguments)]
    fn gcal(
        &self,
        py: Python<'_>,
        vis: &str,
        output: &str,
        toffset: f64,
        nsol: i64,
        segsource: bool,
        reverse: bool,
        reversespw: Option<&Bound<'_, PyAny>>,
        disperse: bool,
        cont: bool,
        wvrflag: &str,
        sourceflag: Option<&Bound<'_, PyAny>>,
        statfield: Option<&Bound<'_, PyAny>>,
        statsource: Option<&Bound<'_, PyAny>>,
        tie: Option<&Bound<'_, PyAny>>,
        smooth: i64,
        scale: f64,
        maxdistm: f64,
        minnumants: i64,
        mingoodfrac: f64,
        usefieldtab: bool,
        spw: Option<&Bound<'_, PyAny>>,
        wvrspw: Option<&Bound<'_, PyAny>>,
        refant: &str,
        offsets: &str,
        rseed: i64,
        logfile: &str,
    ) -> PyResult<i32> {
        let Ok(seed) = u32::try_from(rseed) else {
            return warn(py, "rseed must be an integer from 0 through 4294967295", -1);
        };
        let selectors = WvrSelectors {
            flagged_antennas: vec![wvrflag.into()],
            reference_antennas: vec![refant.into()],
            flagged_sources: string_vector(sourceflag, "sourceflag")?,
            tied_sources: string_vector(tie, "tie")?,
            statistic_sources: string_vector(statsource, "statsource")?,
            statistic_fields: string_vector(statfield, "statfield")?,
            ..WvrSelectors::default()
        };
        if vis.is_empty() || output.is_empty() {
            return warn(
                py,
                "An input MeasurementSet and output table are required",
                -1,
            );
        }
        if !segsource
            && (!selectors.flagged_sources.is_empty() || !selectors.tied_sources.is_empty())
        {
            return warn(py, "sourceflag and tie require segsource", -1);
        }
        let mut request = WvrRequest {
            measurement_set: PathBuf::from(vis),
            output_table: PathBuf::from(output),
            time_offset_seconds: toffset,
            temperature_offsets: (!offsets.is_empty()).then(|| PathBuf::from(offsets)),
            disperse,
            radiometer_windows: integer_vector(wvrspw, "wvrspw")?,
            output_windows: integer_vector(spw, "spw")?,
            reverse,
            reversed_windows: integer_vector(reversespw, "reversespw")?,
            segment_sources: segsource,
            solution_count: usize::try_from(nsol).unwrap_or(0),
            tied_sources: Vec::new(),
            flagged_sources: Vec::new(),
            statistic_sources: Vec::new(),
            statistic_fields: Vec::new(),
            use_field_table: usefieldtab,
            continuum: cont,
            seed,
            smoothing_samples: usize::try_from(smooth).unwrap_or(0),
            scale,
            flagged_antennas: Vec::new(),
            reference_antennas: Vec::new(),
            max_distance_meters: maxdistm,
            minimum_neighbors: usize::try_from(minnumants).unwrap_or(usize::MAX),
            minimum_good_fraction: mingoodfrac,
        };
        let logfile = if logfile.is_empty() {
            "wvrgcal.log"
        } else {
            logfile
        };
        let outcome = py.detach(move || {
            resolve_wvr_selectors(&mut request, &selectors)?;
            let report = execute_wvr_with_log(&request, Path::new(logfile))?;
            Ok::<_, WvrError>(report.status)
        });
        match outcome {
            Ok(status) => Ok(status),
            Err(WvrError::Invalid(message)) => warn(py, &message, -1),
            Err(WvrError::Input(error)) => warn(py, &error.to_string(), 1),
            Err(
                error @ (WvrError::NoSolution(_)
                | WvrError::ZeroEvidence(_)
                | WvrError::ReverseWindow(_)),
            ) => warn(py, &error.to_string(), 1),
            Err(error @ WvrError::ReferenceUnavailable) => warn(py, &error.to_string(), -1),
            Err(error @ WvrError::RadiometerWindow(_)) => warn(py, &error.to_string(), -10),
            Err(error @ WvrError::OutputWindow(_)) => warn(py, &error.to_string(), -11),
            Err(error) => Err(PyRuntimeError::new_err(error.to_string())),
        }
    }
}

fn warn(py: Python<'_>, message: &str, status: i32) -> PyResult<i32> {
    emit(py, "casars.tools.wvr", &[LogLine::warning(message)])?;
    Ok(status)
}

fn string_vector(value: Option<&Bound<'_, PyAny>>, name: &str) -> PyResult<Vec<String>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if let Ok(text) = value.extract::<String>() {
        return Ok(vec![text]);
    }
    if value.extract::<i64>().is_ok() {
        return Ok(Vec::new());
    }
    value
        .extract::<Vec<String>>()
        .map_err(|_| PyTypeError::new_err(format!("{name} must contain strings")))
}

fn integer_vector(value: Option<&Bound<'_, PyAny>>, name: &str) -> PyResult<Vec<i32>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if let Ok(integer) = value.extract::<i32>() {
        return Ok(vec![integer]);
    }
    let error = || PyTypeError::new_err(format!("{name} must contain integers"));
    let items = value
        .extract::<Vec<Bound<'_, PyAny>>>()
        .map_err(|_| error())?;
    items
        .iter()
        .map(|item| {
            item.extract::<i32>()
                .or_else(|_| item.extract::<String>()?.parse().map_err(|_| error()))
        })
        .collect()
}
