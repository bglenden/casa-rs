// SPDX-License-Identifier: LGPL-3.0-or-later
//! Python access to the shared calibrater attachment and visibility equation.

mod stubs;
pub(crate) mod surface;

use std::{path::Path, sync::Mutex};

use casa_calibration::{
    CalibraterLifecycle, CalibraterLifecycleError, VisibilityFitRequest, VisibilityFitShape,
};
use casa_ms::MsSelection;
use casa_types::quanta::{Quantity, Unit};
use numpy::PyArray1;
use pyo3::{
    exceptions::{PyRuntimeError, PyTypeError},
    prelude::*,
};

use crate::convert::{DoubleList, Variant, quantity};
use crate::tools::{LogLine, emit};

const LOGGER: &str = "casars.tools.calibrater";

/// CASA's calibration tool.
#[pyclass(name = "calibrater", module = "casars.tools", frozen)]
pub(crate) struct PyCalibrater {
    lifecycle: Mutex<CalibraterLifecycle>,
}

impl PyCalibrater {
    fn with_lifecycle<T: Send>(
        &self,
        py: Python<'_>,
        operation: impl FnOnce(&mut CalibraterLifecycle) -> PyResult<T> + Send,
    ) -> PyResult<T> {
        py.detach(|| {
            let mut lifecycle = self.lifecycle.lock().map_err(runtime_error)?;
            operation(&mut lifecycle)
        })
    }

    fn attached(
        &self,
        py: Python<'_>,
        operation: impl FnOnce(&mut CalibraterLifecycle) -> Result<(), CalibraterLifecycleError> + Send,
    ) -> PyResult<bool> {
        let result = self.with_lifecycle(py, |state| {
            Ok(match operation(state) {
                Err(CalibraterLifecycleError::Unattached) => false,
                result => {
                    result.map_err(runtime_error)?;
                    true
                }
            })
        })?;
        if !result {
            emit(
                py,
                LOGGER,
                &[LogLine::error("Must first open a MeasurementSet.")],
            )?;
        }
        Ok(result)
    }
}

#[pymethods]
impl PyCalibrater {
    #[new]
    fn new() -> Self {
        Self {
            lifecycle: Mutex::default(),
        }
    }

    #[pyo3(signature = (filename, compress = false, addcorr = true, addmodel = true), text_signature = "($self, filename, compress=False, addcorr=True, addmodel=True)")]
    fn open(
        &self,
        py: Python<'_>,
        filename: &str,
        compress: bool,
        addcorr: bool,
        addmodel: bool,
    ) -> PyResult<bool> {
        self.with_lifecycle(py, |state| {
            state
                .open_with_scratch(Path::new(filename), compress, addcorr, addmodel)
                .map_err(runtime_error)?;
            Ok(true)
        })
    }

    fn close(&self, py: Python<'_>) -> PyResult<bool> {
        self.with_lifecycle(py, |state| {
            state.close();
            Ok(true)
        })
    }

    fn done(&self, py: Python<'_>) -> PyResult<bool> {
        self.close(py)
    }

    #[pyo3(signature = (old = false, quiet = false), text_signature = "($self, old=False, quiet=False)")]
    fn setvi(&self, py: Python<'_>, old: bool, quiet: bool) -> PyResult<bool> {
        let (changed, was_old) = self.with_lifecycle(py, |state| {
            let was_old = state.legacy_iterator();
            Ok((state.set_legacy_iterator(old), was_old))
        })?;
        if !changed {
            emit(
                py,
                LOGGER,
                &[LogLine::error("Must call setvi _before_ open!")],
            )?;
        } else if !quiet {
            let message = match (old, was_old) {
                (true, false) => "Forcing use of OLD VisibilityIterator.",
                (true, true) => "Already using OLD VisibilityIterator.",
                (false, true) => "Forcing use of NEW VisibilityIterator.",
                (false, false) => "Already using NEW VisibilityIterator.",
            };
            emit(py, LOGGER, &[LogLine::warning(message)])?;
        }
        Ok(changed)
    }

    #[pyo3(signature = (time = Variant::Empty, spw = Variant::Empty, scan = Variant::Empty, field = Variant::Empty, intent = Variant::Empty, observation = Variant::Empty, baseline = Variant::Empty, uvrange = Variant::Empty, chanmode = "none", nchan = 1, start = 0, step = 1, mstart = Variant::Text("0km/s".into()), mstep = Variant::Text("0km/s".into()), msselect = ""), text_signature = "($self, time=[], spw=[], scan=[], field=[], intent=[], observation=[], baseline=[], uvrange=[], chanmode='none', nchan=1, start=0, step=1, mstart={'value': 0.0, 'unit': 'km/s'}, mstep={'value': 0.0, 'unit': 'km/s'}, msselect='')")]
    #[allow(clippy::too_many_arguments)]
    fn selectvis(
        &self,
        py: Python<'_>,
        time: Variant,
        spw: Variant,
        scan: Variant,
        field: Variant,
        intent: Variant,
        observation: Variant,
        baseline: Variant,
        uvrange: Variant,
        chanmode: &str,
        nchan: i64,
        start: i64,
        step: i64,
        mstart: Variant,
        mstep: Variant,
        msselect: &str,
    ) -> PyResult<bool> {
        let mstart = velocity_argument(&mstart, "mstart")?;
        let mstep = velocity_argument(&mstep, "mstep")?;
        let selection = MsSelection {
            timerange: selector(&time),
            spw: selector(&spw),
            scan: selector(&scan),
            field: selector(&field),
            intent: selector(&intent),
            observation: selector(&observation),
            antenna: selector(&baseline),
            uvrange: selector(&uvrange),
            msselect: (!msselect.is_empty()).then(|| msselect.to_owned()),
            ..MsSelection::default()
        };
        let result = self.with_lifecycle(py, |state| {
            if state.dataset_path().is_none() {
                return Ok(false);
            }
            let velocity = Unit::new("m/s").expect("a velocity unit");
            for quantity in [&mstart, &mstep] {
                if !quantity.unit().conformant(&velocity) {
                    return Err(runtime_error(format!(
                        "Quantum::assure non-conforming unit type '{}'",
                        quantity.unit().name(),
                    )));
                }
            }
            state
                .select_with_channel_mode(selection, chanmode, [nchan, start, step])
                .map_err(runtime_error)?;
            Ok(true)
        })?;
        if !result {
            emit(
                py,
                LOGGER,
                &[LogLine::error("Must first open a MeasurementSet.")],
            )?;
        }
        Ok(result)
    }

    #[pyo3(signature = (apply = true, solve = true), text_signature = "($self, apply=True, solve=True)")]
    fn reset(&self, py: Python<'_>, apply: bool, solve: bool) -> PyResult<bool> {
        self.attached(py, |state| state.reset(apply, solve))
    }

    #[pyo3(signature = (calset = 0), text_signature = "($self, calset=0)")]
    fn initcalset(&self, py: Python<'_>, calset: i64) -> PyResult<bool> {
        self.attached(py, |state| state.init_cal_set(calset))
    }

    #[pyo3(signature = (otf = false, field = Variant::Empty, spw = Variant::Empty, scr = false), text_signature = "($self, otf=False, field=[], spw=[], scr=False)")]
    fn delmod(
        &self,
        py: Python<'_>,
        otf: bool,
        field: Variant,
        spw: Variant,
        scr: bool,
    ) -> PyResult<bool> {
        if !otf && !scr {
            let lines = self.with_lifecycle(py, |state| match state.model_header_lines() {
                Ok(lines) => Ok(Some(
                    lines.into_iter().map(LogLine::info).collect::<Vec<_>>(),
                )),
                Err(CalibraterLifecycleError::Unattached) => Ok(None),
                Err(error) => Err(runtime_error(error)),
            })?;
            return if let Some(lines) = lines {
                emit(py, LOGGER, &lines)?;
                Ok(true)
            } else {
                emit(
                    py,
                    LOGGER,
                    &[LogLine::error("Must first open a MeasurementSet.")],
                )?;
                Ok(false)
            };
        }
        self.attached(py, |state| {
            state.delete_model(
                selector(&field).as_deref(),
                selector(&spw).as_deref(),
                otf,
                scr,
            )
        })
    }

    #[pyo3(signature = (stokes = DoubleList::of(&[0., 0., 0., 0.])), text_signature = "($self, stokes=[0.0, 0.0, 0.0, 0.0])")]
    fn setptmodel(&self, py: Python<'_>, stokes: DoubleList) -> PyResult<bool> {
        self.with_lifecycle(py, |state| {
            state
                .set_point_model(&stokes.0)
                .map_err(|error| match error {
                    CalibraterLifecycleError::Unattached => {
                        runtime_error("Error in Calibrater::setModel: no VisEquation.")
                    }
                    error => runtime_error(error),
                })?;
            Ok(true)
        })
    }

    #[pyo3(signature = (vary, niter = 0, compshape = "P", par = DoubleList::of(&[1., 0., 0.]), file = ""), text_signature = "($self, vary, niter=0, compshape='P', par=[1.0, 0.0, 0.0], file='')")]
    fn modelfit<'py>(
        &self,
        py: Python<'py>,
        #[pyo3(from_py_with = bool_vector)] vary: Vec<bool>,
        niter: i64,
        compshape: &str,
        par: DoubleList,
        file: &str,
    ) -> PyResult<Bound<'py, PyArray1<f64>>> {
        let result = self.with_lifecycle(py, |state| {
            if !state.legacy_iterator() {
                return Err(runtime_error(
                    CalibraterLifecycleError::FitNeedsLegacyIterator,
                ));
            }
            let shape = match compshape {
                "P" => VisibilityFitShape::Point,
                "G" => VisibilityFitShape::Gaussian,
                "D" => VisibilityFitShape::Disk,
                _ => return Err(runtime_error("Error in Calibrater::modelfit.")),
            };
            state
                .fit_model(
                    VisibilityFitRequest {
                        shape,
                        initial: &par.0,
                        vary: &vary,
                        iterations: niter,
                    },
                    (!file.is_empty()).then(|| Path::new(file)),
                )
                .map_err(|_| runtime_error("Error in Calibrater::modelfit."))
        })?;
        Ok(PyArray1::from_vec(py, result))
    }
}

fn velocity_argument(value: &Variant, name: &str) -> PyResult<Quantity> {
    if let Variant::Record(record) = value {
        match (record.get("value"), record.get("unit")) {
            (None, None) => return Quantity::new(0., "km/s").map_err(runtime_error),
            (None, Some(Variant::Text(unit))) => {
                return Quantity::new(0., unit).map_err(runtime_error);
            }
            (Some(Variant::Number(value)), None) => {
                return Quantity::new(*value, "km/s").map_err(runtime_error);
            }
            _ => {}
        }
    }
    if !matches!(value, Variant::Text(_) | Variant::Record(_)) {
        return Err(PyTypeError::new_err(format!("_{name} is not a dictionary")));
    }
    quantity(value).map_err(runtime_error)
}

fn bool_vector(value: &Bound<'_, PyAny>) -> PyResult<Vec<bool>> {
    let flag = |item: &Bound<'_, PyAny>| {
        item.extract::<bool>()
            .or_else(|_| item.extract::<i64>().map(|value| value != 0))
    };
    if let Ok(flag) = flag(value) {
        return Ok(vec![flag]);
    }
    value.try_iter()?.map(|item| flag(&item?)).collect()
}

fn selector(value: &Variant) -> Option<String> {
    match value {
        Variant::Empty => None,
        Variant::Text(text) if text.is_empty() => None,
        Variant::Texts(values) => Some(values.join(",")),
        Variant::Numbers(values) => Some(
            values
                .iter()
                .map(f64::to_string)
                .collect::<Vec<_>>()
                .join(","),
        ),
        value => Some(value.describe()),
    }
}

fn runtime_error(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}
