// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 image creation wrappers delegating to [`casa_imageanalysis::tool::create`].

use std::path::PathBuf;

use casa_core::RecordValue;
use casa_imageanalysis::tool::ToolLogLine;
use casa_imageanalysis::tool::create::{
    CasaArrayPayload, CasaCreateRequest, ComponentListSource, PixelArrayInput,
    create_casa_complist_image, create_casa_image,
};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::convert::IntList;
use crate::tools::componentlist::component_image_creation_model;
use crate::tools::table::casa_record;

use super::PyImageTool;
use super::handle::{emit_tool_logs, external_open_check, raised};
use super::lifecycle::{resolve_image_path, resolve_image_path_result};
use super::pixels::Pixels;

impl Pixels {
    fn into_input_and_history(self, kind: &str) -> (PixelArrayInput, String) {
        let history = self.history();
        let input = if self.is_complex() {
            if kind.eq_ignore_ascii_case("d") || kind.eq_ignore_ascii_case("cd") {
                PixelArrayInput::DComplex(self.c64())
            } else {
                PixelArrayInput::Complex(self.c32())
            }
        } else if kind.eq_ignore_ascii_case("d") {
            PixelArrayInput::Double(self.f64())
        } else {
            PixelArrayInput::Float(self.f32())
        };
        (input, history)
    }
}

#[pymethods]
#[allow(clippy::too_many_arguments)]
impl PyImageTool {
    #[pyo3(signature = (outfile = "", shape = None, cl = None, csys = None, overwrite = false, log = true, cache = true), text_signature = "($self, outfile='', shape=[], cl='', csys={}, overwrite=False, log=True, cache=True)")]
    fn fromcomplist(
        &self,
        py: Python<'_>,
        outfile: &str,
        shape: Option<IntList>,
        cl: Option<&Bound<'_, PyAny>>,
        csys: Option<&Bound<'_, PyDict>>,
        overwrite: bool,
        log: bool,
        cache: bool,
    ) -> PyResult<bool> {
        let source = component_list_source(py, cl, !outfile.is_empty());
        let expanded = resolve_image_path(py, outfile);
        let record = casa_record(py, csys)?;
        let shape = shape.map_or_else(Vec::new, |shape| shape.0);
        let messages = py.detach(|| {
            let mut state = self.state();
            state.image = None;
            let expanded = expanded?;
            let source = source?;
            let (attachment, logs) = create_casa_complist_image(
                outfile,
                expanded.as_ref(),
                &shape,
                record,
                source,
                overwrite,
                log,
                cache,
                state.history,
                external_open_check(),
            )
            .map_err(raised)?;
            state.image = Some(attachment);
            Ok::<_, PyErr>(logs)
        })?;
        emit_tool_logs(py, &messages)?;
        Ok(true)
    }

    #[pyo3(signature = (outfile = "", shape = None, csys = None, linear = false, overwrite = false, log = true, r#type = "f"), text_signature = "($self, outfile='', shape=[0], csys={}, linear=False, overwrite=False, log=True, type='f')")]
    fn fromshape(
        &self,
        py: Python<'_>,
        outfile: &str,
        shape: Option<IntList>,
        csys: Option<&Bound<'_, PyDict>>,
        linear: bool,
        overwrite: bool,
        log: bool,
        r#type: &str,
    ) -> PyResult<bool> {
        let record = casa_record(py, csys)?;
        let expanded = resolve_image_path_result(py, outfile);
        let shape = shape.map(|s| s.0);
        py.detach(|| {
            let mut state = self.state();
            state.image = None;
            let request = CasaCreateRequest {
                method: "fromshape",
                outfile,
                expanded: expanded.as_deref().map_err(String::as_str),
                csys_record: record,
                linear,
                overwrite,
                log,
                kind: r#type,
            };
            state.image = Some(
                create_casa_image(
                    &request,
                    shape.as_deref(),
                    None,
                    state.history,
                    external_open_check(),
                )
                .map_err(raised)?,
            );
            Ok(true)
        })
    }

    #[pyo3(signature = (outfile = "", pixels = None, csys = None, linear = false, overwrite = false, log = true, r#type = "f"), text_signature = "($self, outfile='', pixels=[], csys={}, linear=False, overwrite=False, log=True, type='f')")]
    fn fromarray(
        &self,
        py: Python<'_>,
        outfile: &str,
        pixels: Option<&Bound<'_, PyAny>>,
        csys: Option<&Bound<'_, PyDict>>,
        linear: bool,
        overwrite: bool,
        log: bool,
        r#type: &str,
    ) -> PyResult<bool> {
        let record = casa_record(py, csys)?;
        let expanded = resolve_image_path_result(py, outfile);
        let pixels = Pixels::capture(py, pixels, true);
        py.detach(|| {
            let mut state = self.state();
            state.image = None;
            let pixels = pixels?;
            let (input, hist) = pixels.into_input_and_history(r#type);
            let request = CasaCreateRequest {
                method: "fromarray",
                outfile,
                expanded: expanded.as_deref().map_err(String::as_str),
                csys_record: record,
                linear,
                overwrite,
                log,
                kind: r#type,
            };
            state.image = Some(
                create_casa_image(
                    &request,
                    None,
                    Some(CasaArrayPayload {
                        pixels: input,
                        history_pixels: hist,
                    }),
                    state.history,
                    external_open_check(),
                )
                .map_err(raised)?,
            );
            Ok(true)
        })
    }

    #[pyo3(signature = (outfile = "", shape = None, csys = None, linear = false, overwrite = false, log = true, r#type = "f"), text_signature = "($self, outfile='', shape=[0], csys={}, linear=False, overwrite=False, log=True, type='f')")]
    fn newimagefromshape(
        &self,
        py: Python<'_>,
        outfile: &str,
        shape: Option<IntList>,
        csys: Option<&Bound<'_, PyDict>>,
        linear: bool,
        overwrite: bool,
        log: bool,
        r#type: &str,
    ) -> PyResult<Py<Self>> {
        let record = casa_record(py, csys)?;
        let expanded = resolve_image_path_result(py, outfile);
        let shape = shape.map(|s| s.0);
        let image = py.detach(|| {
            let history = self.state().history;
            let request = CasaCreateRequest {
                method: "newimagefromshape",
                outfile,
                expanded: expanded.as_deref().map_err(String::as_str),
                csys_record: record,
                linear,
                overwrite,
                log,
                kind: r#type,
            };
            create_casa_image(
                &request,
                shape.as_deref(),
                None,
                history,
                external_open_check(),
            )
            .map_err(raised)
        })?;
        Py::new(py, Self::attached(image))
    }

    #[pyo3(signature = (outfile = "", pixels = None, csys = None, linear = false, overwrite = false, log = true, r#type = "f"), text_signature = "($self, outfile='', pixels=[], csys={}, linear=False, overwrite=False, log=True, type='f')")]
    fn newimagefromarray(
        &self,
        py: Python<'_>,
        outfile: &str,
        pixels: Option<&Bound<'_, PyAny>>,
        csys: Option<&Bound<'_, PyDict>>,
        linear: bool,
        overwrite: bool,
        log: bool,
        r#type: &str,
    ) -> PyResult<Py<Self>> {
        let record = casa_record(py, csys)?;
        let expanded = resolve_image_path_result(py, outfile);
        let pixels = Pixels::capture(py, pixels, true);
        let image = py.detach(|| {
            let history = self.state().history;
            let pixels = pixels?;
            let (input, hist) = pixels.into_input_and_history(r#type);
            let request = CasaCreateRequest {
                method: "newimagefromarray",
                outfile,
                expanded: expanded.as_deref().map_err(String::as_str),
                csys_record: record,
                linear,
                overwrite,
                log,
                kind: r#type,
            };
            create_casa_image(
                &request,
                None,
                Some(CasaArrayPayload {
                    pixels: input,
                    history_pixels: hist,
                }),
                history,
                external_open_check(),
            )
            .map_err(raised)
        })?;
        Py::new(py, Self::attached(image))
    }
}

fn component_list_source(
    py: Python<'_>,
    input: Option<&Bound<'_, PyAny>>,
    persistent: bool,
) -> PyResult<ComponentListSource> {
    let Some(input) = input else {
        return Err(raised(
            "Exception: Component list table name cannot be empty.",
        ));
    };
    if let Ok(record) = input.cast::<PyDict>() {
        let record: RecordValue = casa_record(py, Some(record))?;
        let mut logs = Vec::new();
        let (components, spectral_records) =
            component_image_creation_model(&record, &mut logs, persistent)?;
        let logs = logs
            .into_iter()
            .map(|line| match line.level {
                crate::tools::Level::Info => ToolLogLine::info(line.message),
                crate::tools::Level::Warning | crate::tools::Level::Error => {
                    ToolLogLine::warning(line.message)
                }
                crate::tools::Level::Debug => ToolLogLine::debug(line.message),
            })
            .collect();
        return Ok(ComponentListSource::Loaded {
            components,
            spectral_records,
            logs,
        });
    }
    match input.extract::<String>() {
        Ok(name) if name.is_empty() => Err(raised(
            "Exception: Component list table name cannot be empty.",
        )),
        Ok(name) => resolve_image_path(py, &name)
            .map(|expanded| ComponentListSource::Table(PathBuf::from(expanded.as_ref()))),
        Err(_) => Err(raised("Unsupported type for parameter cl")),
    }
}
