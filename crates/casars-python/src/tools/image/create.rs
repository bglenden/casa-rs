// SPDX-License-Identifier: LGPL-3.0-or-later

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

struct Create {
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
        image.add_history(creation)?;
        if enabled {
            let (method, args) = match pixels {
                Some(pixels) => ("fromarray", format!("pixels={}", pixels.history())),
                None => (
                    "fromshape",
                    format!(
                        "outfile={:?}, shape={}",
                        self.outfile,
                        format!("{:?}", self.shape).replace(' ', "")
                    ),
                ),
            };
            let call = format!(
                "ia.{method}({args}, csys={{...}}, linear={}, overwrite={}, log={}, type={:?})",
                self.linear, self.overwrite, self.log, self.kind
            );
            image.record_history(method, call)?;
        }
        Ok(())
    }

    fn run(&self, pixels: Option<Pixels>, history: bool) -> PyResult<AttachedImage> {
        if self.outfile.is_empty() {
            return self
                .build(pixels, None, history)
                .map(AttachedImage::temporary);
        }
        let path = std::path::absolute(&self.outfile).map_err(raised)?;
        let parent = path
            .parent()
            .ok_or_else(|| raised("Invalid output image path"))?;
        let stage = tempfile::tempdir_in(parent).map_err(raised)?;
        let product = stage.path().join("image");
        drop(self.build(pixels, Some(&product), history)?);
        let mut registry = attachment::registry();
        let existed = match path.symlink_metadata() {
            Ok(_) if !self.overwrite => {
                return Err(raised(format!(
                    "Exception: File {} exists, and the user does not want to remove it..",
                    self.outfile
                )));
            }
            Ok(_) => {
                if registry
                    .get(&attachment::key(&path)?)
                    .is_some_and(|image| image.strong_count() > 0)
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
            Ok(image) => Ok(image),
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
        let shape = shape.unwrap_or_else(|| IntList::of(&[0])).0;
        let record = casa_record(py, csys)?;
        py.detach(|| {
            let mut state = self.state();
            state.image = None;
            if shape.is_empty() {
                return Err(raised(
                    "Exception: The shape must have more than zero elements.",
                ));
            }
            if shape.iter().any(|&length| length <= 0) {
                return Err(raised("Exception: All elements of shape must be positive."));
            }
            let request = Create {
                shape: shape.into_iter().map(|length| length as usize).collect(),
                record,
                outfile: outfile.into(),
                linear,
                overwrite,
                log,
                kind: r#type.into(),
            };
            state.image = Some(request.run(None, state.history)?);
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
        let pixels = Pixels::capture(py, pixels, true);
        py.detach(|| {
            let mut state = self.state();
            state.image = None;
            if !matches!(r#type.to_ascii_lowercase().as_str(), "f" | "d") {
                return Err(raised(format!("Exception: Unsupported value for type: {type_name:?}. Supported values are \"d\" and \"f\"", type_name = r#type)));
            }
            let pixels = pixels?;
            let request = Create {
                shape: pixels.shape().to_vec(), record, outfile: outfile.into(), linear, overwrite,
                log, kind: r#type.into(),
            };
            state.image = Some(request.run(Some(pixels), state.history)?);
            Ok(true)
        })
    }
}
