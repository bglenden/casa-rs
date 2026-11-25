// SPDX-License-Identifier: LGPL-3.0-or-later

use std::sync::Arc;

use casa_ms::selection::MsSelectionIoBudget;
use casa_ms::subtables::{MsSpectralWindow, linked_subtable_row};
use casa_ms::transform::{
    MsRegridOptions, MsTransformError, ToolGridFrameTables, ToolGridPhaseCentre,
    ToolSpectralWindow, tool_channel_frequencies, tool_grid_frame_at_epoch,
    tool_grid_reference_epoch, tool_spectral_grid_frame,
};
use casa_ms::{MsResult, SubtableId};
use casa_types::measures::MeasuresProvider;
use casa_types::measures::epoch::{EpochRef, MEpoch};
use casa_types::quanta::Quantity;
use numpy::{PyArray1, PyUntypedArray};
use pyo3::exceptions::{PySystemError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyFloat, PyInt, PyList, PyString};

use crate::convert::{Variant, integer_vector, measures};

use crate::tools::{LogLine, emit};

use super::{AttachedMs, PyMsTool, runtime_error, strict_bool};

#[pymethods]
impl PyMsTool {
    /// Calculate channel centres without modifying the attached MeasurementSet.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (spwids = vec![0], fieldids = vec![0], obstime = String::new(), mode = "channel".to_owned(), nchan = -1, start = None, width = None, phasec = None, restfreq = None, outframe = String::new(), veltype = "radio".to_owned(), verbose = true), text_signature = "($self, spwids=[0], fieldids=[0], obstime='', mode='channel', nchan=-1, start=[], width=[], phasec=[], restfreq=[], outframe='', veltype='radio', verbose=True)")]
    fn cvelfreqs<'py>(
        &self,
        py: Python<'py>,
        #[pyo3(from_py_with = spw_ids)] spwids: Vec<i32>,
        #[pyo3(from_py_with = field_ids)] fieldids: Vec<i32>,
        obstime: String,
        mode: String,
        #[pyo3(from_py_with = channel_count)] nchan: i64,
        start: Option<Variant>,
        width: Option<Variant>,
        phasec: Option<Variant>,
        restfreq: Option<Variant>,
        outframe: String,
        veltype: String,
        #[pyo3(from_py_with = strict_bool)] verbose: bool,
    ) -> PyResult<Bound<'py, PyArray1<f64>>> {
        let (veltype, warning) =
            if mode == "velocity" && !matches!(veltype.as_str(), "radio" | "optical") {
                let warning = LogLine::warning(format!(
                    "Invalid velocity type {veltype}, setting type to \"radio\""
                ));
                ("radio".into(), Some(warning))
            } else {
                (veltype, None)
            };
        let options = MsRegridOptions {
            start: axis_argument(&mode, start),
            width: axis_argument(&mode, width),
            mode,
            nchan,
            nspw: 1,
            interpolation: "linear".into(),
            phasecenter: String::new(),
            restfreq: axis_argument("", restfreq),
            outframe,
            veltype,
            preaverage: false,
        };
        let mut log_grid = false;
        let centres = self.attachment_info(py, |attached| {
            let Some(ms) = attached.filter(|_| !spwids.is_empty()) else {
                return Ok(Vec::new());
            };
            log_grid = true;
            let link = ms.working_metadata_link();
            link.verify_directory().map_err(runtime_error)?;
            let (spectral_tables, fields) = ms
                .subtables
                .spectral_grid_tables(&ms.main, !options.outframe.is_empty())
                .map_err(runtime_error)?;
            let mut windows = Vec::with_capacity(spwids.len());
            for &id in &spwids {
                let (table, row) =
                    linked_subtable_row(&spectral_tables, id as usize, SubtableId::SpectralWindow)
                        .map_err(runtime_error)?;
                let window = MsSpectralWindow::new(table);
                windows.push(ToolSpectralWindow {
                    centres_hz: window.chan_freq(row).map_err(runtime_error)?,
                    widths_hz: window.chan_width(row).map_err(runtime_error)?,
                    reference: window
                        .reference_frequency_reference(row)
                        .map_err(runtime_error)?,
                });
            }
            let epoch = if obstime.is_empty() {
                None
            } else {
                let Some(epoch) = Quantity::read(&obstime)
                    .ok()
                    .and_then(|value| MEpoch::from_quantity(&value, EpochRef::UTC).ok())
                else {
                    return Ok(Vec::new());
                };
                Some(epoch)
            };
            let phase = phasec.as_ref().unwrap_or(&Variant::Empty);
            let empty_phase = phase.is_empty_sequence()
                || matches!(phase, Variant::Text(text) if text.is_empty());
            let numeric_phase = match phase {
                Variant::Number(id) => Some(*id as i32),
                Variant::Numbers(ids) => ids.first().map(|&id| id as i32),
                _ => None,
            };
            let phase = if empty_phase || numeric_phase.is_some() {
                let id = numeric_phase.unwrap_or_else(|| fieldids.first().copied().unwrap_or(0));
                let row = linked_subtable_row(&fields, id as usize, SubtableId::Field);
                if let Err(error) = row {
                    return if empty_phase {
                        Err(runtime_error(error))
                    } else {
                        Ok(Vec::new())
                    };
                }
                ToolGridPhaseCentre::Field(id as usize)
            } else {
                let provider = ms.spectral_query_measures().map_err(runtime_error)?;
                let Some(direction) = measures::direction(phase, &provider).ok().flatten() else {
                    return Ok(Vec::new());
                };
                ToolGridPhaseCentre::Direction(direction)
            };
            let budget = MsSelectionIoBudget::from_system_memory(1, size_of::<f64>(), None)
                .map_err(runtime_error)?
                .available_bytes;
            let centres = tool_channel_frequencies(windows, &options, budget, || {
                let source_error = |error| MsTransformError::OpenMeasurementSet {
                    path: ms.path.display().to_string(),
                    source: error,
                };
                let measures = ms.spectral_query_measures().map_err(source_error)?;
                let selected = link.selected_rows().map_err(source_error)?;
                let geometry_ids = [
                    SubtableId::Observation,
                    SubtableId::Antenna,
                    SubtableId::DataDescription,
                ];
                let count = if epoch.is_some() { 2 } else { 3 };
                let geometry = ms
                    .subtables
                    .spectral_grid_geometry(&ms.main, &geometry_ids[..count])
                    .map_err(source_error)?;
                let epoch = match epoch {
                    Some(epoch) => epoch,
                    None => tool_grid_reference_epoch(
                        &ms.main,
                        selected.as_deref(),
                        &geometry[&SubtableId::DataDescription],
                        &spwids,
                        &fieldids,
                        ms.plain_rows.is_some(),
                    )?,
                };
                let tables = ToolGridFrameTables {
                    main: &ms.main,
                    first_row: selected
                        .as_deref()
                        .and_then(|rows| rows.first())
                        .copied()
                        .unwrap_or(0),
                    antenna: &geometry[&SubtableId::Antenna],
                    field: &fields,
                    observation: &geometry[&SubtableId::Observation],
                };
                if let Some(frame) =
                    tool_grid_frame_at_epoch(tables, &epoch, &phase, Arc::clone(&measures))?
                {
                    return Ok(frame);
                }
                let (source, _) = link.source_and_rows().map_err(source_error)?;
                tool_spectral_grid_frame(
                    &source.ms,
                    &epoch,
                    &phase,
                    options.outframe.eq_ignore_ascii_case("SOURCE"),
                    measures,
                )
            });
            link.verify_directory().map_err(runtime_error)?;
            match centres {
                Ok(centres) => Ok(centres),
                Err(MsTransformError::RegridParameter { .. } | MsTransformError::Spectrum(_)) => {
                    Ok(Vec::new())
                }
                Err(error) => Err(runtime_error(error)),
            }
        })?;
        if log_grid && (verbose || warning.is_some()) {
            let mut lines = Vec::new();
            if verbose {
                lines.push(LogLine::info("Calculating grid ..."));
            }
            lines.extend(warning);
            emit(py, "casars.tools.ms", &lines)?;
        }
        Ok(PyArray1::from_vec(py, centres))
    }
}

fn axis_argument(mode: &str, value: Option<Variant>) -> String {
    let Some(value) = value.filter(|value| !value.is_empty_sequence()) else {
        return String::new();
    };
    let text = value.describe();
    if matches!(mode, "channel" | "channel_b") && !matches!(text.as_str(), "" | "[]") {
        return (integer_vector::number_prefix(&text) as i32).to_string();
    }
    text
}

impl AttachedMs {
    fn spectral_query_measures(&self) -> MsResult<Arc<dyn MeasuresProvider>> {
        if let Some(measures) = self.visibility.time_measures.get() {
            return Ok(Arc::clone(measures));
        }
        let measures = casa_ms::open_measures_runtime()?;
        Ok(Arc::clone(
            self.visibility.time_measures.get_or_init(|| measures),
        ))
    }
}

fn channel_count(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    if value.is_none() {
        return Err(binding_error());
    }
    value.extract::<i64>().map_err(|error| {
        if error.is_instance_of::<PyTypeError>(value.py()) {
            PyTypeError::new_err("argument _nchan must be an integer")
        } else {
            error
        }
    })
}

fn binding_error() -> PyErr {
    PySystemError::new_err(
        "<built-in function ms_cvelfreqs> returned a result with an exception set",
    )
}

fn spw_ids(value: &Bound<'_, PyAny>) -> PyResult<Vec<i32>> {
    grid_ids(value, "spwids")
}

fn field_ids(value: &Bound<'_, PyAny>) -> PyResult<Vec<i32>> {
    grid_ids(value, "fieldids")
}

fn grid_ids(value: &Bound<'_, PyAny>, name: &str) -> PyResult<Vec<i32>> {
    if value.is_instance_of::<PyString>() || value.is_instance_of::<PyBytes>() {
        return Err(PyTypeError::new_err(format!(
            "argument _{name} must not be a string"
        )));
    }
    if !value.is_instance_of::<PyList>()
        && !value.is_instance_of::<PyUntypedArray>()
        && !value.is_instance_of::<PyInt>()
        && !value.is_instance_of::<PyFloat>()
    {
        return Err(binding_error());
    }
    let (ids, pending) = integer_vector::flatten(
        value,
        "cyclic spectral grid ids",
        "non-finite spectral grid id",
    )?;
    if pending {
        return Err(binding_error());
    }
    Ok(ids.into_iter().map(|id| id as i32).collect())
}
