// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_coordinates::CoordinateSystem;
use casa_tables::TableWriteLock;
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

struct StagedImage {
    requested: String,
    target: PathBuf,
    directory: tempfile::TempDir,
    product: PathBuf,
}

impl StagedImage {
    fn allocate(name: &str) -> PyResult<Option<Self>> {
        if name.is_empty() {
            return Ok(None);
        }
        let target = std::path::absolute(name).map_err(raised)?;
        let parent = target
            .parent()
            .ok_or_else(|| raised("Invalid output image path"))?;
        let directory = tempfile::tempdir_in(parent).map_err(raised)?;
        let product = directory.path().join("image");
        Ok(Some(Self {
            requested: name.to_owned(),
            target,
            directory,
            product,
        }))
    }

    fn key(&self) -> PyResult<PathBuf> {
        if self.target.symlink_metadata().is_ok() {
            return attachment::key(&self.target);
        }
        let parent = self
            .target
            .parent()
            .and_then(|parent| parent.canonicalize().ok());
        let name = self.target.file_name();
        parent
            .zip(name)
            .map(|(parent, name)| parent.join(name))
            .ok_or_else(|| raised("Invalid output image path"))
    }

    fn check(
        &self,
        overwrite: bool,
        registry: &attachment::OpenImages,
        locks: &mut BTreeMap<PathBuf, TableWriteLock>,
    ) -> PyResult<bool> {
        let path = &self.target;
        match path.symlink_metadata() {
            Ok(_) if !overwrite => Err(raised(format!(
                "Exception: File {} exists, and the user does not want to remove it..",
                self.requested
            ))),
            Ok(_) => {
                let key = attachment::key(path)?;
                if attachment::conflicting_images(registry, &key, 0)
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
                    && casa_tables::Table::open_metadata_only(casa_tables::TableOptions::new(path))
                        .is_err()
                {
                    return Err(raised(format!(
                        "Table {} already exists (and is not a true table directory)",
                        path.display()
                    )));
                }
                attachment::lock_table_tree(&key, locks)?;
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(raised(error)),
        }
    }

    fn install(&self, existed: bool) -> std::io::Result<()> {
        if existed {
            exchange_directories(&self.product, &self.target)
        } else {
            rename_without_replacing(&self.product, &self.target)
        }
    }

    fn restore(self, existed: bool, original: PyErr) -> PyErr {
        let restored = if existed {
            exchange_directories(&self.product, &self.target)
        } else {
            rename_without_replacing(&self.target, &self.product)
        };
        match restored {
            Ok(()) => original,
            Err(error) => {
                let recovery = self.directory.keep();
                raised(format!(
                    "{original}. Image publication could not be rolled back. Inspect {} ({error})",
                    recovery.display()
                ))
            }
        }
    }
}

pub(super) fn publish_image(
    outfile: &str,
    overwrite: bool,
    build: impl FnOnce(Option<&Path>) -> PyResult<ImageHandle>,
) -> PyResult<AttachedImage> {
    publish_image_pair(outfile, "", overwrite, |target, _| {
        Ok((build(target)?, None))
    })
}

pub(super) fn publish_image_pair(
    outfile: &str,
    companion: &str,
    overwrite: bool,
    build: impl FnOnce(Option<&Path>, Option<&Path>) -> PyResult<(ImageHandle, Option<ImageHandle>)>,
) -> PyResult<AttachedImage> {
    let primary = StagedImage::allocate(outfile)?;
    let companion = StagedImage::allocate(companion)?;
    if let (Some(primary), Some(companion)) = (&primary, &companion) {
        let first = primary.key()?;
        let second = companion.key()?;
        if first.starts_with(&second) || second.starts_with(&first) {
            return Err(raised("Output images must have separate destinations"));
        }
    }
    let (image, secondary) = build(
        primary.as_ref().map(|stage| stage.product.as_path()),
        companion.as_ref().map(|stage| stage.product.as_path()),
    )?;
    drop(secondary);
    let temporary = if primary.is_none() {
        Some(AttachedImage::temporary(image))
    } else {
        drop(image);
        None
    };
    if primary.is_none() && companion.is_none() {
        return Ok(temporary.expect("temporary result"));
    }
    let target = primary.as_ref().map(|stage| stage.target.clone());
    let mut registry = attachment::registry();
    let mut locks = BTreeMap::new();
    let mut prepared = Vec::new();
    for stage in [companion, primary].into_iter().flatten() {
        let existed = stage.check(overwrite, &registry, &mut locks)?;
        prepared.push((stage, existed));
    }
    let mut installed: Vec<(StagedImage, bool)> = Vec::new();
    for (stage, existed) in prepared {
        if let Err(error) = stage.install(existed) {
            let error = installed
                .into_iter()
                .rev()
                .fold(raised(error), |error, (stage, existed)| {
                    stage.restore(existed, error)
                });
            return Err(error);
        }
        installed.push((stage, existed));
    }
    match target
        .as_deref()
        .map(|path| AttachedImage::open_created(&mut registry, path))
        .transpose()
    {
        Ok(native) => Ok(native.or(temporary).expect("image result")),
        Err(error) => Err(installed
            .into_iter()
            .rev()
            .fold(error, |error, (stage, existed)| {
                stage.restore(existed, error)
            })),
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
