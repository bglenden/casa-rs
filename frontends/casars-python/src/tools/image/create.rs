// SPDX-License-Identifier: LGPL-3.0-or-later

//! PyO3 image creation methods and publication helpers delegating to `casa_imageanalysis`.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_components::ComponentList;
use casa_coordinates::CoordinateSystem;
use casa_core::{RecordValue, Value};
use casa_imageanalysis::ComponentListImage;
use casa_imageanalysis::tool::concatenate::owned_members::OwnedOperands;
use casa_imageanalysis::tool::coords::default_record_for_shape;
use casa_imageanalysis::tool::publish::{
    StagedImage, publish_attached_image,
    publish_owned_virtual_concat as pure_publish_owned_virtual_concat, publish_staged_pair,
    publish_virtual_concat as pure_publish_virtual_concat,
};
use casa_images::ImagePixelType;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::convert::IntList;
use crate::tools::componentlist::component_image_creation_model;
use crate::tools::coordsys::validate_restored_linear_coordinates;
use crate::tools::table::casa_record;
use crate::tools::{LogLine, emit};

use super::PyImageTool;
use super::attachment::AttachedImage;
use super::handle::{ImageHandle, Storage, external_open_check, raised};
use super::lifecycle::resolve_image_path;
use super::pixels::Pixels;

pub(super) fn publish_image(
    outfile: &str,
    overwrite: bool,
    build: impl FnOnce(Option<&Path>) -> PyResult<ImageHandle>,
) -> PyResult<AttachedImage> {
    publish_attached_image(outfile, overwrite, external_open_check(), |path| {
        build(path).map_err(|e| casa_imageanalysis::tool::ImageToolError::new(e.to_string()))
    })
    .map_err(raised)
}

pub(super) fn publish_virtual_concat(
    outfile: &str,
    expanded: &str,
    overwrite: bool,
    build: impl FnOnce(&Path, &Path) -> PyResult<ImageHandle>,
) -> PyResult<AttachedImage> {
    pure_publish_virtual_concat(
        outfile,
        expanded,
        overwrite,
        external_open_check(),
        |p, s| {
            build(p, s).map_err(|e| casa_imageanalysis::tool::ImageToolError::new(e.to_string()))
        },
    )
    .map_err(raised)
}

pub(super) fn publish_owned_virtual_concat(
    outfile: &str,
    expanded: &str,
    overwrite: bool,
    prepare: impl FnOnce(&Path, &Path) -> PyResult<OwnedOperands>,
) -> PyResult<(AttachedImage, Vec<String>)> {
    pure_publish_owned_virtual_concat(
        outfile,
        expanded,
        overwrite,
        external_open_check(),
        |p, s| {
            prepare(p, s).map_err(|e| casa_imageanalysis::tool::ImageToolError::new(e.to_string()))
        },
    )
    .map_err(raised)
}

enum CreationInput {
    Shape(Option<IntList>),
    Pixels(PyResult<Pixels>),
}

struct Create<'a> {
    method: &'static str,
    outfile: &'a str,
    shape: Vec<usize>,
    record: RecordValue,
    linear: bool,
    overwrite: bool,
    log: bool,
    kind: String,
}

struct CreationGeometry {
    record: RecordValue,
    coordinates: CoordinateSystem,
    pixel_type: ImagePixelType,
}

impl Create<'_> {
    fn geometry(&self, pixels: Option<&Pixels>) -> PyResult<CreationGeometry> {
        let record = if self.record.fields().is_empty() {
            default_record_for_shape(&self.shape, self.linear).map_err(raised)?
        } else if let [field] = self.record.fields()
            && let Value::Record(record) = &field.value
        {
            record.clone()
        } else {
            self.record.clone()
        };
        let coordinates = CoordinateSystem::from_record(&record)
            .map_err(|_| raised("Exception: Invalid coordinate system record"))?;
        if !self.record.is_empty() {
            validate_restored_linear_coordinates(&coordinates, &record)?;
        }
        let kind = self.kind.to_ascii_lowercase();
        let pixel_type = match (kind.as_str(), pixels.is_some_and(Pixels::is_complex)) {
            ("f", false) => ImagePixelType::Float32,
            ("d", false) => ImagePixelType::Float64,
            ("c", _) | ("f", true) => ImagePixelType::Complex32,
            ("cd", _) | ("d", true) => ImagePixelType::Complex64,
            _ => {
                return Err(raised(
                    "Exception: Input parm type must be either 'f', 'c', 'd', or 'cd'",
                ));
            }
        };
        Ok(CreationGeometry {
            record,
            coordinates,
            pixel_type,
        })
    }

    fn build(
        &self,
        geometry: CreationGeometry,
        pixels: Option<Pixels>,
        path: Option<&Path>,
        history: bool,
    ) -> PyResult<ImageHandle> {
        let CreationGeometry {
            record,
            coordinates,
            pixel_type,
        } = geometry;
        if coordinates.n_pixel_axes() != self.shape.len() {
            return Err(raised(
                "Exception: Supplied CoordinateSystem and image shape are inconsistent.",
            ));
        }
        let shape = self.shape.clone();
        let mut image = match pixel_type {
            ImagePixelType::Float32 => ImageHandle::Float(
                Storage::create(
                    shape,
                    coordinates,
                    path,
                    pixels.as_ref().map(Pixels::f32).as_ref(),
                )
                .map_err(raised)?,
            ),
            ImagePixelType::Float64 => ImageHandle::Double(
                Storage::create(
                    shape,
                    coordinates,
                    path,
                    pixels.as_ref().map(Pixels::f64).as_ref(),
                )
                .map_err(raised)?,
            ),
            ImagePixelType::Complex32 => ImageHandle::Complex(
                Storage::create(
                    shape,
                    coordinates,
                    path,
                    pixels.as_ref().map(Pixels::c32).as_ref(),
                )
                .map_err(raised)?,
            ),
            ImagePixelType::Complex64 => ImageHandle::DComplex(
                Storage::create(
                    shape,
                    coordinates,
                    path,
                    pixels.as_ref().map(Pixels::c64).as_ref(),
                )
                .map_err(raised)?,
            ),
        };
        image.set_coordinate_record(record).map_err(raised)?;
        self.history(&mut image, pixels.as_ref(), history)?;
        image.save().map_err(raised)?;
        Ok(image)
    }

    fn history(
        &self,
        image: &mut ImageHandle,
        pixels: Option<&Pixels>,
        enabled: bool,
    ) -> PyResult<()> {
        let (storage, filename) = if self.outfile.is_empty() {
            ("Temp", String::new())
        } else {
            ("Paged", format!("'{}'", self.outfile))
        };
        let pixel_type = match image {
            ImageHandle::Float(_) => "float",
            ImageHandle::Double(_) => "double",
            ImageHandle::Complex(_) => "Complex",
            ImageHandle::DComplex(_) => "DComplex",
        };
        let creation = format!(
            "Created {storage} image {filename} of shape {:?} with {pixel_type} valued pixels.",
            self.shape
        );
        image
            .append_history_at("::ImageFactory::createImage", &[creation])
            .map_err(raised)?;
        if enabled {
            let args = match pixels {
                Some(pixels) if self.method == "newimagefromarray" => {
                    format!("outfile={:?}, pixels={}", self.outfile, pixels.history())
                }
                Some(pixels) => format!("pixels={}", pixels.history()),
                None => format!(
                    "outfile={:?}, shape={}",
                    self.outfile,
                    format!("{:?}", self.shape).replace(' ', ""),
                ),
            };
            let kind = if self.method == "newimagefromarray" {
                String::new()
            } else {
                format!(", type={:?}", self.kind)
            };
            let call = format!(
                "ia.{}({args}, csys={{...}}, linear={}, overwrite={}, log={}{kind})",
                self.method, self.linear, self.overwrite, self.log
            );
            image.record_history(self.method, call).map_err(raised)?;
        }
        Ok(())
    }

    fn image_shape(shape: Option<IntList>) -> PyResult<Vec<usize>> {
        let shape = shape.unwrap_or_else(|| IntList::of(&[0])).0;
        if shape.is_empty() {
            return Err(raised(
                "Exception: The shape must have more than zero elements.",
            ));
        }
        if shape.iter().any(|&length| length <= 0) {
            return Err(raised("Exception: All elements of shape must be positive."));
        }
        Ok(shape.into_iter().map(|length| length as usize).collect())
    }

    fn run(
        mut self,
        input: CreationInput,
        history: bool,
        expanded: PyResult<Cow<'_, str>>,
    ) -> PyResult<AttachedImage> {
        let pixels = match input {
            CreationInput::Shape(shape) => {
                self.shape = Self::image_shape(shape)?;
                None
            }
            CreationInput::Pixels(pixels) => {
                if !matches!(self.kind.to_ascii_lowercase().as_str(), "f" | "d") {
                    return Err(raised(format!(
                        "Exception: Unsupported value for type: {:?}. Supported values are \"d\" and \"f\"",
                        self.kind
                    )));
                }
                let pixels = pixels?;
                self.shape = pixels.shape().to_vec();
                Some(pixels)
            }
        };
        let geometry = self.geometry(pixels.as_ref())?;
        let primary =
            StagedImage::allocate_creation(self.outfile, expanded?.as_ref(), self.overwrite)
                .map_err(raised)?;
        publish_staged_pair(
            primary,
            None,
            self.overwrite,
            external_open_check(),
            |path, _| {
                self.build(geometry, pixels, path, history)
                    .map(|img| (img, None))
                    .map_err(|e| casa_imageanalysis::tool::ImageToolError::new(e.to_string()))
            },
        )
        .map_err(raised)
    }

    fn replace_from(
        self,
        py: Python<'_>,
        tool: &PyImageTool,
        input: CreationInput,
    ) -> PyResult<bool> {
        let expanded = resolve_image_path(py, self.outfile);
        py.detach(|| {
            let mut state = tool.state();
            state.image = None;
            state.image = Some(self.run(input, state.history, expanded)?);
            Ok(true)
        })
    }

    fn new_from(
        self,
        py: Python<'_>,
        tool: &PyImageTool,
        input: CreationInput,
    ) -> PyResult<Py<PyImageTool>> {
        let expanded = resolve_image_path(py, self.outfile);
        let image = py.detach(|| {
            let history = tool.state().history;
            self.run(input, history, expanded)
        })?;
        Py::new(py, PyImageTool::attached(image))
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
        let input = component_list_input(py, cl);
        let expanded = resolve_image_path(py, outfile);
        let record = casa_record(py, csys)?;
        let shape = shape.map_or_else(Vec::new, |shape| shape.0);
        let messages = py.detach(|| {
            let mut state = self.state();
            state.image = None;
            let expanded = expanded?;
            let input = input?;
            if !(2..=4).contains(&shape.len()) {
                return Err(raised("ComponentListImages must have 2, 3, or 4 dimensions"));
            }
            if shape.iter().any(|&length| length <= 0) {
                return Err(raised("TiledShape: shape has to be > 0"));
            }
            let shape: Vec<usize> = shape.into_iter().map(|length| length as usize).collect();
            let geometry = Create {
                method: "fromcomplist",
                outfile,
                shape: shape.clone(),
                record,
                linear: false,
                overwrite,
                log,
                kind: "f".into(),
            }
            .geometry(None)?;
            let stage = StagedImage::allocate_creation(outfile, &expanded, overwrite)
                .map_err(raised)?;
            let mut messages = Vec::new();
            let (components, spectral_records) = input.load(stage.is_some(), &mut messages)?;
            let message = format!("Created ComponentListImage {outfile} of shape {shape:?}.");
            let source =
                ComponentListImage::new(shape.clone(), components, geometry.coordinates, cache)
                    .map_err(raised)?;
            let attachment = publish_staged_pair(
                stage,
                None,
                overwrite,
                external_open_check(),
                |path, _| {
                    if let Some(path) = path {
                        source.write_native(path, &geometry.record, &spectral_records)?;
                    }
                    let mut image = ImageHandle::from_component(source, geometry.record)?;
                    if path.is_none() {
                        image.append_history_at(
                            "ImageFactory::createComponentListImage",
                            std::slice::from_ref(&message),
                        )?;
                        if state.history {
                            image.record_history(
                                "fromcomplist",
                                format!("ia.fromcomplist(outfile={outfile:?}, shape={shape:?}, cl={{...}}, csys={{...}}, overwrite={overwrite}, log={log}, cache={cache})"),
                            )?;
                        }
                    }
                    Ok((image, None))
                },
            )
            .map_err(raised)?;
            if !cache && !outfile.is_empty() {
                attachment.lock().disable_component_cache().map_err(raised)?;
            }
            state.image = Some(attachment);
            if log {
                messages.push(LogLine::info(message));
            }
            Ok::<_, PyErr>(messages)
        })?;
        emit(py, "casars.tools.image", &messages)?;
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
        Create {
            method: "fromshape",
            outfile,
            shape: Vec::new(),
            record: casa_record(py, csys)?,
            linear,
            overwrite,
            log,
            kind: r#type.into(),
        }
        .replace_from(py, self, CreationInput::Shape(shape))
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
        Create {
            method: "fromarray",
            outfile,
            shape: Vec::new(),
            record: casa_record(py, csys)?,
            linear,
            overwrite,
            log,
            kind: r#type.into(),
        }
        .replace_from(
            py,
            self,
            CreationInput::Pixels(Pixels::capture(py, pixels, true)),
        )
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
        Create {
            method: "newimagefromshape",
            outfile,
            shape: Vec::new(),
            record: casa_record(py, csys)?,
            linear,
            overwrite,
            log,
            kind: r#type.into(),
        }
        .new_from(py, self, CreationInput::Shape(shape))
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
        Create {
            method: "newimagefromarray",
            outfile,
            shape: Vec::new(),
            record: casa_record(py, csys)?,
            linear,
            overwrite,
            log,
            kind: r#type.into(),
        }
        .new_from(
            py,
            self,
            CreationInput::Pixels(Pixels::capture(py, pixels, true)),
        )
    }
}

enum ComponentListInput {
    Record(RecordValue),
    Table(PathBuf),
}

impl ComponentListInput {
    fn load(
        self,
        persistent: bool,
        logs: &mut Vec<LogLine>,
    ) -> PyResult<(ComponentList, BTreeMap<usize, RecordValue>)> {
        match self {
            Self::Record(record) => Ok(component_image_creation_model(&record, logs, persistent)?),
            Self::Table(path) => {
                let components = ComponentList::read_table(path).map_err(raised)?;
                let record = components.to_record().map_err(raised)?;
                Ok((
                    ComponentList::from_record(&record).map_err(raised)?,
                    BTreeMap::new(),
                ))
            }
        }
    }
}

fn component_list_input(
    py: Python<'_>,
    input: Option<&Bound<'_, PyAny>>,
) -> PyResult<ComponentListInput> {
    let Some(input) = input else {
        return Err(raised(
            "Exception: Component list table name cannot be empty.",
        ));
    };
    if let Ok(record) = input.cast::<PyDict>() {
        return casa_record(py, Some(record)).map(ComponentListInput::Record);
    }
    match input.extract::<String>() {
        Ok(name) if name.is_empty() => Err(raised(
            "Exception: Component list table name cannot be empty.",
        )),
        Ok(name) => resolve_image_path(py, &name)
            .map(|expanded| ComponentListInput::Table(PathBuf::from(expanded.as_ref()))),
        Err(_) => Err(raised("Unsupported type for parameter cl")),
    }
}
