// SPDX-License-Identifier: LGPL-3.0-or-later

use std::borrow::Cow;
use std::path::{Path, PathBuf};

use casa_images::fits_read::FitsToolImport;
use pyo3::prelude::*;

use super::PyImageTool;
use super::attachment::AttachedImage;
use super::create::publish_image;
use super::handle::{ImageHandle, Storage, raised, with_storage};
use super::lifecycle::resolve_image_path;

struct ImportFits<'a> {
    outfile: &'a str,
    infile: &'a str,
    whichrep: i64,
    whichhdu: i64,
    zeroblanks: bool,
    overwrite: bool,
}

impl ImportFits<'_> {
    fn command(&self) -> String {
        format!(
            "ia.fromfits(outfile={:?}, fitsfile={:?}, whichrep={}, whichhdu={}, zeroBlanks={}, overwrite={})",
            self.outfile,
            self.infile,
            self.whichrep,
            self.whichhdu,
            self.zeroblanks,
            self.overwrite,
        )
    }

    fn run(
        &self,
        expanded: PyResult<Cow<'_, str>>,
        history: Option<(&str, String)>,
    ) -> PyResult<AttachedImage> {
        let whichrep = usize::try_from(self.whichrep).map_err(|_| {
            raised("Exception: The Coordinate Representation index must be non-negative.")
        })?;
        let input = expanded?;
        let input = Path::new(input.as_ref());
        let mut source =
            FitsToolImport::open_named(input, Path::new(self.infile), whichrep, self.whichhdu)
                .map_err(raised)?;
        if source.is_tile_compressed() {
            return Err(raised(format!(
                "Exception: No image at specified location in file {}.",
                self.infile
            )));
        }
        if self.overwrite
            && !self.outfile.is_empty()
            && let Ok(target) = Path::new(self.outfile).canonicalize()
            && input.ancestors().any(|part| {
                part.canonicalize()
                    .is_ok_and(|resolved| resolved.starts_with(&target))
            })
        {
            return Err(raised(
                "Cannot overwrite an output that contains the source FITS file",
            ));
        }
        publish_image(self.outfile, self.overwrite, |path| {
            let mut storage = Storage::create(
                source.shape().to_vec(),
                source.coordinates().clone(),
                path,
                None,
            )
            .map_err(raised)?;
            with_storage!(&mut storage, image => source.copy_pixels(&mut **image, self.zeroblanks))
                .map_err(raised)?;
            with_storage!(&mut storage, image => image.set_units(source.units)).map_err(raised)?;
            with_storage!(&mut storage, image => image.set_image_info_record(source.image_info))
                .map_err(raised)?;
            with_storage!(&mut storage, image => image.set_misc_info(source.misc_info))
                .map_err(raised)?;
            with_storage!(&mut storage, image => image.append_history_entries(&source.history))
                .map_err(raised)?;
            let mut image = ImageHandle::Float(storage);
            image.set_coordinate_record(source.coordinate_record)?;
            if let Some((method, command)) = history {
                image.record_history(method, command)?;
            }
            image.save().map_err(raised)?;
            Ok(image)
        })
    }
}

fn test_image_path() -> PathBuf {
    let sample = Path::new("demo/Images/imagetestimage.fits");
    if sample.is_file() {
        return sample.to_path_buf();
    }
    let measures_root = std::env::var_os("CASA_RS_MEASURESPATH").map(PathBuf::from);
    let legacy_root = ["CASAPATH", "AIPSPATH"].into_iter().find_map(|variable| {
        std::env::var(variable).ok().and_then(|value| {
            value
                .split_whitespace()
                .next()
                .map(|root| Path::new(root).join("data"))
        })
    });
    measures_root
        .into_iter()
        .chain(legacy_root)
        .map(|root| root.join(sample))
        .find(|candidate| candidate.is_file())
        .unwrap_or_else(|| sample.to_path_buf())
}

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (outfile="", infile="", whichrep=0, whichhdu=0, zeroblanks=false, overwrite=false), text_signature = "($self, outfile='', infile='', whichrep=0, whichhdu=0, zeroblanks=False, overwrite=False)")]
    #[allow(clippy::too_many_arguments)]
    fn fromfits(
        &self,
        py: Python<'_>,
        outfile: &str,
        infile: &str,
        whichrep: i64,
        whichhdu: i64,
        zeroblanks: bool,
        overwrite: bool,
    ) -> PyResult<bool> {
        let expanded = resolve_image_path(py, infile);
        py.detach(|| {
            let mut state = self.state();
            let request = ImportFits {
                outfile,
                infile,
                whichrep,
                whichhdu,
                zeroblanks,
                overwrite,
            };
            state.image = Some(request.run(
                expanded,
                state.history.then(|| ("fromfits", request.command())),
            )?);
            Ok(true)
        })
    }

    #[pyo3(signature = (outfile="", infile="", whichrep=0, whichhdu=0, zeroblanks=false, overwrite=false), text_signature = "($self, outfile='', infile='', whichrep=0, whichhdu=0, zeroblanks=False, overwrite=False)")]
    #[allow(clippy::too_many_arguments)]
    fn newimagefromfits(
        &self,
        py: Python<'_>,
        outfile: &str,
        infile: &str,
        whichrep: i64,
        whichhdu: i64,
        zeroblanks: bool,
        overwrite: bool,
    ) -> PyResult<Py<Self>> {
        let expanded = resolve_image_path(py, infile);
        let image = py.detach(|| {
            ImportFits {
                outfile,
                infile,
                whichrep,
                whichhdu,
                zeroblanks,
                overwrite,
            }
            .run(expanded, None)
        })?;
        Py::new(py, Self::attached(image))
    }

    #[pyo3(signature = (outfile="", overwrite=false), text_signature = "($self, outfile='', overwrite=False)")]
    fn maketestimage(&self, py: Python<'_>, outfile: &str, overwrite: bool) -> PyResult<bool> {
        py.detach(|| {
            let mut state = self.state();
            state.image = None;
            let sample = test_image_path();
            let infile = sample
                .to_str()
                .ok_or_else(|| raised("The CASA test image path is not UTF-8"))?;
            let request = ImportFits {
                outfile,
                infile,
                whichrep: 0,
                whichhdu: 0,
                zeroblanks: false,
                overwrite,
            };
            let history = state.history.then(|| {
                (
                    "maketestimage",
                    format!("ia.maketestimage(outfile={outfile:?}, overwrite={overwrite})"),
                )
            });
            state.image = Some(request.run(Ok(Cow::Borrowed(infile)), history)?);
            Ok(true)
        })
    }
}
