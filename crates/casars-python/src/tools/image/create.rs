// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;
use std::path::Path;

use casa_coordinates::CoordinateSystem;
use casa_tables::publication::{exchange_directories, rename_without_replacing};
use casa_types::{RecordValue, Value};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::convert::IntList;
use crate::tools::table::casa_record;

use super::PyImageTool;
use super::attachment::{self, AttachedImage};
use super::handle::{ImageHandle, Storage, raised};
use super::pixels::Pixels;

enum CreationInput {
    Shape(Option<IntList>),
    Pixels(PyResult<Pixels>),
}

struct Create {
    method: &'static str,
    outfile: String,
    shape: Vec<usize>,
    record: RecordValue,
    linear: bool,
    overwrite: bool,
    log: bool,
    kind: String,
}

impl Create {
    fn build(
        &self,
        pixels: Option<Pixels>,
        path: Option<&Path>,
        history: bool,
    ) -> PyResult<ImageHandle> {
        let record = if self.record.fields().is_empty() {
            super::default_record::for_shape(&self.shape, self.linear)?
        } else if let [field] = self.record.fields()
            && let Value::Record(record) = &field.value
        {
            record.clone()
        } else {
            self.record.clone()
        };
        let coordinates = CoordinateSystem::from_record(&record)
            .map_err(|_| raised("Exception: Invalid coordinate system record"))?;
        if coordinates.n_pixel_axes() != self.shape.len() {
            return Err(raised(
                "Exception: Supplied CoordinateSystem and image shape are inconsistent.",
            ));
        }
        let kind = self.kind.to_ascii_lowercase();
        let shape = self.shape.clone();
        let mut image = match (
            kind.as_str(),
            pixels.as_ref().is_some_and(Pixels::is_complex),
        ) {
            ("f", false) => ImageHandle::Float(
                Storage::create(
                    shape,
                    coordinates,
                    path,
                    pixels.as_ref().map(Pixels::f32).as_ref(),
                )
                .map_err(raised)?,
            ),
            ("d", false) => ImageHandle::Double(
                Storage::create(
                    shape,
                    coordinates,
                    path,
                    pixels.as_ref().map(Pixels::f64).as_ref(),
                )
                .map_err(raised)?,
            ),
            ("c", _) | ("f", true) => ImageHandle::Complex(
                Storage::create(
                    shape,
                    coordinates,
                    path,
                    pixels.as_ref().map(Pixels::c32).as_ref(),
                )
                .map_err(raised)?,
            ),
            ("cd", _) | ("d", true) => ImageHandle::DComplex(
                Storage::create(
                    shape,
                    coordinates,
                    path,
                    pixels.as_ref().map(Pixels::c64).as_ref(),
                )
                .map_err(raised)?,
            ),
            _ => {
                return Err(raised(
                    "Exception: Input parm type must be either 'f', 'c', 'd', or 'cd'",
                ));
            }
        };
        image.set_coordinate_record(record)?;
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
        image.append_history_at("::ImageFactory::createImage", &[creation])?;
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
            image.record_history(self.method, call)?;
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

    fn run(mut self, input: CreationInput, history: bool) -> PyResult<AttachedImage> {
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
        publish_image(&self.outfile, self.overwrite, |path| {
            self.build(pixels, path, history)
        })
    }
}

pub(super) fn publish_image(
    outfile: &str,
    overwrite: bool,
    build: impl FnOnce(Option<&Path>) -> PyResult<ImageHandle>,
) -> PyResult<AttachedImage> {
    if outfile.is_empty() {
        return build(None).map(AttachedImage::temporary);
    }
    let path = std::path::absolute(outfile).map_err(raised)?;
    let parent = path
        .parent()
        .ok_or_else(|| raised("Invalid output image path"))?;
    let stage = tempfile::tempdir_in(parent).map_err(raised)?;
    let product = stage.path().join("image");
    drop(build(Some(&product))?);
    let mut registry = attachment::registry();
    let mut replacement_locks = BTreeMap::new();
    let existed = match path.symlink_metadata() {
        Ok(_) if !overwrite => {
            return Err(raised(format!(
                "Exception: File {} exists, and the user does not want to remove it..",
                outfile
            )));
        }
        Ok(_) => {
            let key = attachment::key(&path)?;
            if attachment::conflicting_images(&registry, &key, 0)
                || key
                    .ancestors()
                    .filter(|parent| parent.join("table.dat").is_file())
                    .any(crate::tools::table::path_is_in_use)
            {
                return Err(raised(format!(
                    "Invalid Table operation: SetupNewTable {} is already opened (is in the table cache)",
                    path.display()
                )));
            }
            let empty = path
                .read_dir()
                .is_ok_and(|mut entries| entries.next().is_none());
            if !empty
                && casa_tables::Table::open_metadata_only(casa_tables::TableOptions::new(&path))
                    .is_err()
            {
                return Err(raised(format!(
                    "Table {} already exists (and is not a true table directory)",
                    path.display()
                )));
            }
            attachment::lock_table_tree(&key, &mut replacement_locks)?;
            true
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(raised(error)),
    };
    if existed {
        exchange_directories(&product, &path).map_err(raised)?;
    } else {
        rename_without_replacing(&product, &path).map_err(raised)?;
    }
    match AttachedImage::open(&mut registry, &path) {
        Ok(image) => {
            // The old table is inside the private staging directory after an exchange.
            drop(stage);
            drop(replacement_locks);
            Ok(image)
        }
        Err(error) => {
            let restore = if existed {
                exchange_directories(&product, &path)
            } else {
                rename_without_replacing(&path, &product)
            };
            if let Err(restore) = restore {
                let backup_dir = stage.keep();
                return Err(raised(format!(
                    "{error}. Image publication could not be rolled back; inspect {}: {restore}",
                    backup_dir.display()
                )));
            }
            Err(error)
        }
    }
}

impl PyImageTool {
    fn replace_from(
        &self,
        py: Python<'_>,
        request: Create,
        input: CreationInput,
    ) -> PyResult<bool> {
        py.detach(|| {
            let mut state = self.state();
            state.image = None;
            state.image = Some(request.run(input, state.history)?);
            Ok(true)
        })
    }

    fn new_from(
        &self,
        py: Python<'_>,
        request: Create,
        input: CreationInput,
    ) -> PyResult<Py<Self>> {
        let image = py.detach(|| {
            let history = self.state().history;
            request.run(input, history)
        })?;
        Py::new(py, Self::attached(image))
    }
}

#[pymethods]
#[allow(clippy::too_many_arguments)]
impl PyImageTool {
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
        let request = Create {
            method: "fromshape",
            shape: Vec::new(),
            record: casa_record(py, csys)?,
            outfile: outfile.into(),
            linear,
            overwrite,
            log,
            kind: r#type.into(),
        };
        self.replace_from(py, request, CreationInput::Shape(shape))
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
        let request = Create {
            method: "fromarray",
            shape: Vec::new(),
            record: casa_record(py, csys)?,
            outfile: outfile.into(),
            linear,
            overwrite,
            log,
            kind: r#type.into(),
        };
        self.replace_from(
            py,
            request,
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
        let request = Create {
            method: "newimagefromshape",
            shape: Vec::new(),
            record: casa_record(py, csys)?,
            outfile: outfile.into(),
            linear,
            overwrite,
            log,
            kind: r#type.into(),
        };
        self.new_from(py, request, CreationInput::Shape(shape))
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
        let request = Create {
            method: "newimagefromarray",
            shape: Vec::new(),
            record: casa_record(py, csys)?,
            outfile: outfile.into(),
            linear,
            overwrite,
            log,
            kind: r#type.into(),
        };
        self.new_from(
            py,
            request,
            CreationInput::Pixels(Pixels::capture(py, pixels, true)),
        )
    }
}
