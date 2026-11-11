// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's public figure lifecycle; scientific panels are prepared in Rust.

use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyList, PyModule};

use super::{LOGGER, PySimutil};
use crate::convert::{DoubleList, StringList};
use crate::convert::{Variant, measures, value_in};
use crate::tools::{LogLine, emit};

pub(super) fn draw(
    py: Python<'_>,
    document: &casa_ms::plot_document::PlotDocument,
    payload: &std::path::Path,
    preserve_title: bool,
) -> PyResult<()> {
    py.import("casars._plot_document")?.call_method1(
        "render_current",
        (pythonize::pythonize(py, document)?, payload, preserve_title),
    )?;
    Ok(())
}

fn pyplot(py: Python<'_>) -> PyResult<Bound<'_, PyModule>> {
    py.import("matplotlib.pyplot")
}

fn small_ticks(plot: &Bound<'_, PyModule>) -> PyResult<()> {
    let axis = plot.call_method0("gca")?;
    let kwargs = [("fontsize", "x-small")].into_py_dict(plot.py())?;
    for method in ["get_xticklabels", "get_yticklabels"] {
        plot.call_method("setp", (axis.call_method0(method)?,), Some(&kwargs))?;
    }
    Ok(())
}

#[pymethods]
impl PySimutil {
    /// Draw the source elevation, transit and optional observation range.
    /// A known telescope supplies its location; `cofa` supplies an unknown
    /// telescope's. A missing `direction` uses this object's direction.
    #[pyo3(signature = (date, usehourangle = true, direction = None, telescope = None, ms = None, cofa = None), text_signature = "($self, date, usehourangle=True, direction=None, telescope=None, ms=None, cofa=None)")]
    #[allow(clippy::too_many_arguments)]
    fn ephemeris(
        &self,
        py: Python<'_>,
        date: Variant,
        usehourangle: bool,
        direction: Option<&Bound<'_, PyAny>>,
        telescope: Option<String>,
        ms: Option<std::path::PathBuf>,
        cofa: Option<Variant>,
    ) -> PyResult<()> {
        use casa_simulation::simutil::graphics::{EphemerisPlotRequest, ephemeris_plot};
        use casa_types::measures::{
            MPosition,
            direction::{DirectionRef, MDirection},
        };
        use pyo3::exceptions::{PyAttributeError, PyRuntimeError, PyValueError};
        use std::str::FromStr;

        let argument = direction.unwrap_or(self.direction.bind(py));
        let shown = argument.str()?.to_string();
        let split = super::ops::directions(argument)?
            .split()
            .map_err(super::ops::raised)?;
        let reference = DirectionRef::from_str(split.epoch.trim())
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let source = MDirection::from_angles(
            value_in(&split.longitude, "rad")?,
            value_in(&split.latitude, "rad")?,
            reference,
        );
        let telescope = match telescope {
            Some(telescope) => telescope,
            None => self
                .telescopename
                .as_ref()
                .ok_or_else(|| {
                    PyAttributeError::new_err("'simutil' object has no attribute 'telescopename'")
                })?
                .extract::<String>(py)?,
        };
        let date = value_in(&super::ops::quantity(&date)?, "d")?;
        let measurement_set = ms.as_deref().filter(|path| !path.as_os_str().is_empty());
        let duration_hours = if measurement_set.is_some() {
            0.0
        } else {
            let duration = super::ops::quantity(&self.totaltime.extract::<Variant>(py)?)?;
            if duration.value() == 0.0 {
                0.0
            } else {
                value_in(&duration, "h")?
            }
        };
        let file = tempfile::NamedTempFile::new()?;
        let document = py.detach(|| {
            let data = casa_ms::open_measures_runtime().map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            let position = MPosition::from_observatory_name(&telescope, &*data)
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            let position = match position {
                Some(position) => position,
                None => cofa.as_ref().map(|value| measures::position(value, &data)).transpose()?.flatten()
                    .ok_or_else(|| PyRuntimeError::new_err("simutil::ephemeris needs either a known observatory or an explicitly specified cofa measure"))?,
            };
            ephemeris_plot(&EphemerisPlotRequest { source, date_mjd: date, position, measures: data,
                use_hour_angle: usehourangle, measurement_set, duration_hours }, file.path())
                .map_err(|error| if error.to_string().contains("never visible") { PyValueError::new_err(format!("{shown} is not visible from {telescope}")) }
                    else { PyRuntimeError::new_err(error.to_string()) })
        })?;
        draw(py, &document, file.path(), true)
    }

    /// Plot all antenna positions and dish sizes in their local tangent
    /// plane on the current axes, retaining CASA's marker/label choices.
    #[pyo3(signature = (x, y, z, d, name), text_signature = "($self, x, y, z, d, name)")]
    #[allow(clippy::too_many_arguments)]
    fn plotants(
        &self,
        py: Python<'_>,
        x: DoubleList,
        y: DoubleList,
        z: DoubleList,
        d: DoubleList,
        name: StringList,
    ) -> PyResult<()> {
        use pyo3::exceptions::PyRuntimeError;
        if x.0.len() != y.0.len() || x.0.len() != z.0.len() {
            return Err(PyRuntimeError::new_err("x, y and z have different lengths"));
        }
        let file = tempfile::NamedTempFile::new()?;
        let positions: Vec<_> =
            x.0.iter()
                .zip(y.0)
                .zip(z.0)
                .map(|((&x, y), z)| [x, y, z])
                .collect();
        let document = py
            .detach(|| {
                casa_simulation::simutil::graphics::antenna_plot(
                    &positions,
                    &d.0,
                    &name.0,
                    file.path(),
                )
            })
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        draw(py, &document, file.path(), true)
    }

    /// Clear the current figure and optionally select a multi-panel layout.
    /// The caller's `multi` list is retained and advanced by `nextfig`, as
    /// in CASA. `show` selects Matplotlib's interactive state.
    #[pyo3(signature = (multi = None, show = true), text_signature = "($self, multi=0, show=True)")]
    fn newfig(
        &mut self,
        py: Python<'_>,
        multi: Option<Bound<'_, PyAny>>,
        show: bool,
    ) -> PyResult<()> {
        let plot = pyplot(py)?;
        plot.call_method0(if show { "ion" } else { "ioff" })?;
        plot.call_method0("clf")?;
        if let Some(multi) = multi {
            if multi.eq(0)? {
                return Ok(());
            }
            if !multi.is_instance_of::<PyList>() || multi.len()? != 3 {
                emit(
                    py,
                    LOGGER,
                    &[LogLine::warning(format!(
                        "internal error setting multi-panel figure with multi={}",
                        multi.str()?
                    ))],
                )?;
            }
            self.pmulti = multi.clone().unbind();
            plot.call_method1(
                "subplot",
                (multi.get_item(0)?, multi.get_item(1)?, multi.get_item(2)?),
            )?;
            let kwargs = [
                ("left", 0.05),
                ("right", 0.98),
                ("bottom", 0.09),
                ("top", 0.95),
                ("hspace", 0.2),
                ("wspace", 0.2),
            ]
            .into_py_dict(py)?;
            plot.call_method("subplots_adjust", (), Some(&kwargs))?;
        }
        Ok(())
    }

    /// Apply CASA's small tick labels and advance to the next subplot.
    /// Beyond the last panel, keep the current axes and continue advancing
    /// the retained list, matching CASA's stateful behavior.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn nextfig(&self, py: Python<'_>) -> PyResult<()> {
        let plot = pyplot(py)?;
        small_ticks(&plot)?;
        let multi = self.pmulti.bind(py);
        if !multi.eq(0)? {
            let next = multi.get_item(2)?.extract::<i64>()? + 1;
            multi.set_item(2, next)?;
            let rows = multi.get_item(0)?.extract::<i64>()?;
            let columns = multi.get_item(1)?.extract::<i64>()?;
            if next <= rows * columns {
                plot.call_method1("subplot", (rows, columns, next))?;
            }
        }
        Ok(())
    }

    /// Apply small tick labels, draw when requested, save when a filename is
    /// supplied and disable interactive mode, as CASA does.
    #[pyo3(signature = (show = true, filename = ""), text_signature = "($self, show=True, filename='')")]
    fn endfig(&self, py: Python<'_>, show: bool, filename: &str) -> PyResult<()> {
        let plot = pyplot(py)?;
        small_ticks(&plot)?;
        if show {
            plot.call_method0("draw")?;
        }
        if !filename.is_empty() {
            plot.call_method1("savefig", (filename,))?;
        }
        plot.call_method0("ioff")?;
        Ok(())
    }
}
